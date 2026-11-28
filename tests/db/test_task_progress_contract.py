"""Database Layer: 任务单轨进度强类型契约、分层进度上报与物理字段拆分测试 (Task Progress Contract & Hierarchy)

覆盖范围:
- 原 test_task_progress_contract: TaskProgressDTO 强类型模型序列化 + TaskResponse 单轨制契约
- 原 test_task_progress_hierarchy: 流水线分层进度透传与广播
- 原 test_quick_wins (progress 部分): 创建任务物理初始化双轨字段、进度更新拒绝借壳"""

import pytest
from pydantic import ValidationError

from app.core.pipeline.models import PublishContext
from app.core.pipeline.runtime import emit_progress
from app.db.database import init_db
from app.db.task_repository import TaskQueueRepository
from app.models.task import TaskProgressDTO, TaskResponse


# ==============================================================================
# Part: test_task_progress_contract.py
# ==============================================================================
def test_task_progress_dto_structure():
    """验证 TaskProgressDTO 具备完整的双轨进度与网络元数据字段"""
    dto = TaskProgressDTO(
        processed_bytes=1024,
        total_bytes=4096,
        processed_items=2,
        total_items=10,
        percent=25.0,
        speed_mb_s=12.5,
        eta_s=30.0,
        current_item="video.mp4",
        concurrency=4,
        concurrency_total=16,
        dc_id=2,
        pikpak_download_speed_mb_s=5.0,
        flood_wait_seconds=15,
    )
    dumped = dto.model_dump()
    assert dumped["processed_bytes"] == 1024
    assert dumped["total_bytes"] == 4096
    assert dumped["processed_items"] == 2
    assert dumped["total_items"] == 10
    assert dumped["percent"] == 25.0
    assert dumped["speed_mb_s"] == 12.5
    assert dumped["eta_s"] == 30.0
    assert dumped["current_item"] == "video.mp4"
    assert dumped["concurrency"] == 4
    assert dumped["concurrency_total"] == 16
    assert dumped["dc_id"] == 2
    assert dumped["pikpak_download_speed_mb_s"] == 5.0
    assert dumped["flood_wait_seconds"] == 15


def test_task_response_single_track_requires_nested_progress():
    """验证 F2 单轨制: TaskResponse 强制要求嵌套 progress, 缺失时校验失败
    (扁平物理列 → 嵌套 progress 的装配单点在仓储层 _hydrate_task_dict, 模型不再兜底)"""
    flat_db_record = {
        "id": "task_iso_1",
        "task_type": "download_link",
        "status": "running",
        "progress_bytes": 5242880,
        "total_bytes": 10485760,
        "processed_items": 1,
        "total_items": 2,
        "speed_mb_s": 8.0,
        "eta_s": 10.0,
        "current_item": "sample.mp4",
        "concurrency": 8,
        "concurrency_total": 16,
        "dc_id": 4,
        "account_id": 1,
        "priority": 10,
        "retry_count": 0,
        "max_retries": 3,
        "next_retry_at": 0.0,
        "error_detail": "",
        "params": {},
        "created_at": 1000.0,
        "updated_at": 1005.0,
    }

    with pytest.raises(ValidationError):
        TaskResponse.model_validate(flat_db_record)


def test_task_response_with_nested_progress():
    """验证 TaskResponse 接收嵌套 progress 字典时正确解析, 且顶层扁平影子键被 extra=ignore 丢弃"""
    payload = {
        "id": "task_iso_2",
        "task_type": "publish_folder",
        "status": "running",
        "progress": {
            "processed_bytes": 2048,
            "total_bytes": 8192,
            "processed_items": 3,
            "total_items": 10,
            "percent": 25.0,
            "speed_mb_s": 1.5,
            "eta_s": 4.0,
            "current_item": "[3/10] photo.jpg",
            "concurrency": 2,
            "concurrency_total": 4,
            "dc_id": 1,
        },
        "progress_bytes": 999,
        "total_items": 999,
        "speed_mb_s": 999.0,
        "created_at": 2000.0,
        "updated_at": 2005.0,
    }

    resp = TaskResponse.model_validate(payload)
    assert resp.progress.processed_bytes == 2048
    assert resp.progress.total_bytes == 8192
    assert resp.progress.processed_items == 3
    assert resp.progress.total_items == 10

    dumped = resp.model_dump()
    for flat_key in (
        "processed_items",
        "total_items",
        "progress_bytes",
        "total_bytes",
        "speed_mb_s",
        "eta_s",
        "current_item",
        "concurrency",
        "concurrency_total",
        "dc_id",
    ):
        assert flat_key not in dumped, f"扁平影子字段 {flat_key} 不应出现在 API 契约输出"


def test_repository_flat_columns_and_api_dump():
    """验证 DB 物理列仍为扁平存储 (仓储层职责), 但经 TaskResponse dump 后仅暴露嵌套 progress 单轨契约"""
    init_db()
    task_id = "task_db_api_test"
    TaskQueueRepository.create(task_id, "publish_folder", {})
    TaskQueueRepository.update_progress(
        task_id=task_id,
        progress_bytes=100000,
        total_bytes=500000,
        processed_items=2,
        total_items=10,
        speed_mb_s=2.5,
        eta_s=160.0,
        current_item="[2/10] test.mp4",
        concurrency=2,
        dc_id=5,
        concurrency_total=4,
    )

    raw_row = TaskQueueRepository.get_by_id(task_id)
    assert raw_row["processed_items"] == 2
    assert raw_row["total_items"] == 10
    assert raw_row["progress_bytes"] == 100000
    assert raw_row["total_bytes"] == 500000

    resp = TaskResponse.model_validate(raw_row)
    dumped = resp.model_dump()
    assert "progress" in dumped
    assert dumped["progress"]["processed_items"] == 2
    assert dumped["progress"]["total_items"] == 10
    assert dumped["progress"]["processed_bytes"] == 100000
    assert dumped["progress"]["total_bytes"] == 500000
    assert dumped["progress"]["percent"] == 20.0
    assert dumped["progress"]["current_item"] == "[2/10] test.mp4"
    assert "progress_bytes" not in dumped
    assert "processed_items" not in dumped


def test_repository_create_returns_hydrated_progress():
    """验证 P0-FIX-03: TaskQueueRepository.create() 返回值已经过水合装配，直接包含 progress 契约字典"""
    init_db()
    task_id = "task_create_hydration_test"
    res = TaskQueueRepository.create(task_id, "download_link", {"url": "https://t.me/c/1/2"})
    assert "progress" in res
    assert isinstance(res["progress"], dict)
    assert res["progress"]["processed_bytes"] == 0
    assert res["progress"]["total_bytes"] == 0
    assert res["progress"]["processed_items"] == 0
    assert res["progress"]["total_items"] == 0
    assert res["progress"]["percent"] == 0.0
    assert res["progress"]["speed_mb_s"] == 0.0


def test_completed_task_zero_items_forces_hundred_percent():
    """验证 P0-FIX-04: 完成态任务在没有字节/条目总量或进度为0时，仓储层水合强制兜底 100.0%,
    模型层直通仓储产物 (API 面永远看到兜底后的 100)"""
    # 1. 验证仓储层 _hydrate_task_dict 扁平物理列兜底
    hydrated_flat = TaskQueueRepository._hydrate_task_dict(
        {
            "id": "task_done_hydrate",
            "status": "completed",
            "progress_bytes": 0,
            "total_bytes": 0,
        }
    )
    assert hydrated_flat["progress"]["percent"] == 100.0

    # 2. 验证仓储层 _hydrate_task_dict 嵌套 progress percent=0 时兜底
    hydrated_nested = TaskQueueRepository._hydrate_task_dict(
        {
            "id": "task_done_hydrate_nested",
            "status": "completed",
            "progress": {"percent": 0.0},
        }
    )
    assert hydrated_nested["progress"]["percent"] == 100.0

    # 3. 验证模型直通仓储水合产物: 单轨制下兜底职责在仓储层, 模型零改写
    completed_repo_row = {
        "id": "task_done_zero",
        "task_type": "forward_batch",
        "status": "completed",
        "progress": {"processed_bytes": 0, "total_bytes": 0, "percent": 0.0},
        "created_at": 4000.0,
        "updated_at": 4010.0,
    }
    resp = TaskResponse.model_validate(completed_repo_row)
    assert resp.progress.percent == 0.0


# ==============================================================================
# Part: test_task_progress_hierarchy.py
# ==============================================================================
def test_emit_progress_passes_completed_and_total_bytes():
    """验证 emit_progress 将容量字节作为 completed_bytes 和 total_bytes 透传给回调。"""
    captured_kwargs = {}

    def mock_cb(current, total, speed, eta, file_name, **kwargs):
        captured_kwargs.update(kwargs)
        captured_kwargs["current"] = current
        captured_kwargs["total"] = total
        captured_kwargs["file_name"] = file_name

    ctx = PublishContext(task_id="test_task_001", target_chat_id="-100123", progress_callback=mock_cb)

    emit_progress(
        ctx,
        current=3,
        total=10,
        name="test_video.mp4",
        percent=30.0,
        force_db=False,
        speed=15.2,
        done_bytes=104857600,
        all_bytes=349525333,
    )

    assert captured_kwargs["current"] == 3
    assert captured_kwargs["total"] == 10
    assert captured_kwargs["completed_bytes"] == 104857600
    assert captured_kwargs["total_bytes"] == 349525333
    assert captured_kwargs["total_bytes_done"] == 104857600
    assert captured_kwargs["total_bytes_all"] == 349525333


# ==============================================================================
# Part: .tmp_qw_progress.py
# ==============================================================================
def test_create_sets_dual_track_progress():
    """TD-01 验证: 创建任务时物理初始化 processed_items=0, total_items=0, progress_bytes=0, total_bytes=0，且无 progress_unit"""
    TaskQueueRepository.create("qw_dl", "download_link", {})
    TaskQueueRepository.create("qw_fw", "forward_channel", {})
    TaskQueueRepository.create("qw_pub", "publish_folder", {})

    for tid in ["qw_dl", "qw_fw", "qw_pub"]:
        t = TaskQueueRepository.get_by_id(tid)
        assert t["processed_items"] == 0
        assert t["total_items"] == 0
        assert t["progress_bytes"] == 0
        assert t["total_bytes"] == 0
        assert "progress_unit" not in t


def test_update_dual_track_progress_persists():
    """TD-01 验证: 进度更新支持同时写入字节量纲与条目量纲，拒绝借壳"""
    task_id = "qw_dual"
    TaskQueueRepository.create(task_id, "download_link", {})

    # 传输中的普通进度回调 (字节 500/1000, 条目 2/5)
    TaskQueueRepository.update_progress(
        task_id=task_id,
        progress_bytes=500,
        total_bytes=1000,
        processed_items=2,
        total_items=5,
        speed_mb_s=1.0,
        eta_s=5.0,
        current_item="file2.mp4",
        concurrency=3,
        dc_id=2,
        concurrency_total=16,
    )
    task = TaskQueueRepository.get_by_id(task_id)
    assert task["progress_bytes"] == 500
    assert task["total_bytes"] == 1000
    assert task["processed_items"] == 2
    assert task["total_items"] == 5
    assert task["concurrency"] == 3
    assert task["concurrency_total"] == 16
    assert "progress_unit" not in task

    # 完成态进度更新 (字节 1000/1000, 条目 5/5)
    TaskQueueRepository.update_progress(
        task_id=task_id,
        progress_bytes=1000,
        total_bytes=1000,
        processed_items=5,
        total_items=5,
        speed_mb_s=0.0,
        eta_s=0.0,
        current_item="[5/5] 全部下载完成",
    )
    task = TaskQueueRepository.get_by_id(task_id)
    assert task["progress_bytes"] == 1000
    assert task["total_bytes"] == 1000
    assert task["processed_items"] == 5
    assert task["total_items"] == 5
    assert "progress_unit" not in task
