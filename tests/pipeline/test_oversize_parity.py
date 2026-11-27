"""Pipeline Core: 本地/流式超限处理策略一致性与虚拟切片流拆分测试 (Oversize Parity & Stream Splitter)

覆盖范围:
- 原 test_publish_oversize_parity: 本地超限对齐、终态公式 skipped 豁免、子文件夹批次复合头与客户端标签
- 原 test_stream_splitter: VirtualStreamSplitter 阈值派生公式、常量源、切分边界与切片标题格式化"""

import ast
import os
import shutil
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _safeio import safe_write_bytes

from app.config import settings
from app.core.pipeline.cleanup import PikPakStructuredArchiveStrategy
from app.core.pipeline.engines import LocalDiskTransportEngine, TransportError
from app.core.pipeline.models import PublishContext, PublishItem, TransportOutcome
from app.core.pipeline.sources import LocalFolderSource, PikPakSnapshotSource
from app.core.scheduler import FatalTaskError
from app.core.stream_splitter import (
    PartSpec,
    VirtualStreamSplitter,
    derive_split_threshold,
    format_part_caption,
)
from app.db.task_repository import TaskQueueRepository
from app.models.publish import OversizedFileInfo
from app.services.publish_runner import run_publish_folder_task
from app.services.publisher_service import PublisherService, PublishFolderResult

# ==============================================================================
# Part: test_publish_oversize_parity.py
# ==============================================================================
_MB = 1024 * 1024


def _make_acc(limit_mb=1):
    mock_client = AsyncMock()
    mock_client.is_connected = MagicMock(return_value=True)
    mock_acc = MagicMock(
        client=mock_client,
        is_ready=lambda: True,
        is_premium=False,
        effective_max_file_size_bytes=lambda: limit_mb * _MB,
    )
    mock_acc.probe_premium = AsyncMock()
    return mock_acc, mock_client


def _local_folder(tmp_dir, files):
    tmp = os.path.join(tmp_dir, f"oversize_{uuid.uuid4().hex[:8]}")
    os.makedirs(tmp, exist_ok=True)
    for name, size in files:
        safe_write_bytes(os.path.join(tmp, name), b"0" * size)
    return tmp


# ---------------------------------------------------------------------------
# D3: 本地超限 → skipped_files + 占位卡 (真门面)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_oversize_skips_with_placeholder_and_completes(tmp_dir):
    """超限文件 → skipped_files (非 failed) + 占位卡; 任务可完成 (终态由执行器公式豁免)。"""
    tmp = _local_folder(tmp_dir, [("small.mp4", 100 * 1024), ("huge.mp4", 2 * _MB)])
    try:
        client = AsyncMock()
        client.is_connected = MagicMock(return_value=True)
        client.send_file = AsyncMock(return_value=MagicMock(id=1))
        client.send_message = AsyncMock()
        mock_acc, _ = _make_acc(limit_mb=1)
        mock_acc.client = client

        with (
            patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc),
            patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock) as mock_up,
        ):
            mock_up.return_value = MagicMock()
            res = await PublisherService.publish_folder(
                folder_path=tmp,
                target_chat_id="-100777",
                album_mode=False,
                delay_seconds=0.0,
                task_id="v2_d3_skip",
            )

        assert res.published_count == 1
        assert res.failed_files == []
        assert len(res.skipped_files) == 1
        sf = res.skipped_files[0]
        assert isinstance(sf, OversizedFileInfo)
        assert sf.file_name == "huge.mp4"
        # 占位卡 (缺集说明) + 文件夹形态批次总结卡 + 任务汇总卡 —— 三条频道消息
        assert client.send_message.await_count == 3
        texts = [c.args[1] for c in client.send_message.await_args_list]
        assert any("【缺集说明】" in t and "huge.mp4" in t for t in texts)
        assert any("发布文件夹" in t for t in texts)
        assert any("任务汇总" in t for t in texts)
        # 超限文件零上传
        assert mock_up.await_count == 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.asyncio
async def test_local_oversize_strict_fail_aborts(tmp_dir):
    """oversize_policy='strict_fail': 本地域熔断 (FatalTaskError), 不进入传输。"""
    tmp = _local_folder(tmp_dir, [("huge.mp4", 2 * _MB)])
    try:
        client = AsyncMock()
        client.is_connected = MagicMock(return_value=True)
        client.send_file = AsyncMock()
        client.send_message = AsyncMock()
        mock_acc, _ = _make_acc(limit_mb=1)
        mock_acc.client = client

        with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
            with pytest.raises(FatalTaskError) as ei:
                await PublisherService.publish_folder(
                    folder_path=tmp,
                    target_chat_id="-100777",
                    album_mode=False,
                    delay_seconds=0.0,
                    task_id="v2_d3_strict",
                    oversize_policy="strict_fail",
                )
        assert "严格模式" in str(ei.value)
        client.send_file.assert_not_awaited()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.asyncio
async def test_local_oversize_split_maps_to_skip(tmp_dir):
    """D3: split 请求值在本地门面映射为 skip_and_report (映射原因见门面注释) —— 行为等同跳过。"""
    tmp = _local_folder(tmp_dir, [("huge.mp4", 2 * _MB)])
    try:
        client = AsyncMock()
        client.is_connected = MagicMock(return_value=True)
        client.send_file = AsyncMock(return_value=MagicMock(id=1))
        client.send_message = AsyncMock()
        mock_acc, _ = _make_acc(limit_mb=1)
        mock_acc.client = client

        with patch("app.core.client_manager.client_manager.get_account", return_value=mock_acc):
            res = await PublisherService.publish_folder(
                folder_path=tmp,
                target_chat_id="-100777",
                album_mode=False,
                delay_seconds=0.0,
                task_id="v2_d3_splitmap",
                oversize_policy="split",
            )
        assert res.published_count == 0
        assert len(res.skipped_files) == 1
        assert res.failed_files == []
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# 执行器终态公式: skipped 豁免 (对齐 pikpak 版)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_executor_terminal_status_completed_with_skips(monkeypatch):
    """纯超限跳过任务 → 终态 completed (旧行为为 failed, D3 豁免)。"""
    TaskQueueRepository.create(
        task_id="v2_term_skip",
        task_type="publish_folder",
        params={"folder_path": "X", "target_channel": "-100777"},
        status="pending",
    )

    async def fake_publish(**kwargs):
        return PublishFolderResult(
            published_count=1,
            total_files_count=2,
            failed_files=[],
            skipped_files=[
                OversizedFileInfo(
                    file_name="huge.mp4",
                    rel_path="huge.mp4",
                    file_size_bytes=2 * _MB,
                    file_size_mb=2.0,
                    file_size_gb=0.0,
                    limit_mb=1.0,
                    exceeded_mb=1.0,
                    oversize_kind="non_premium_limit",
                )
            ],
            is_folder_publish=True,
            folder_name="X",
            newly_published_count=1,
        )

    monkeypatch.setattr(PublisherService, "publish_folder", staticmethod(fake_publish))
    monkeypatch.setattr("app.core.client_manager.client_manager.get_account", lambda account_id=None: None)
    monkeypatch.setattr("app.core.client_manager.client_manager.userbot", AsyncMock(), raising=False)
    monkeypatch.setattr("app.core.client_manager.client_manager.is_premium", False, raising=False)
    monkeypatch.setattr(
        "app.core.client_manager.client_manager.refresh_premium", AsyncMock(return_value=False), raising=False
    )

    await run_publish_folder_task(
        task_id="v2_term_skip",
        folder_path="X",
        target_channel="-100777",
        album_mode=False,
        delay_seconds=0.0,
    )

    rec = TaskQueueRepository.get_by_id("v2_term_skip")
    assert rec["status"] == "completed"
    assert rec["params"].get("skipped_files"), "skipped_files 应 merge_params 落库供前端展示"


# ---------------------------------------------------------------------------
# source hooks 契约 + 复合标头
# ---------------------------------------------------------------------------


def test_source_hooks_contract():
    """LocalFolderSource hooks; pikpak audit 钩子 pikpak-only (本地源恒放行)。"""
    src = LocalFolderSource("D:/fake/whatever")
    ctx = PublishContext(task_id="t", target_chat_id=-1, task_limit_bytes=5 * _MB)
    item = PublishItem(item_id="D:/fake/p/q.mp4", name="q.mp4", size_bytes=10)
    dq = src.dedup_query(item, ctx)
    assert dq.local_path == "D:/fake/p/q.mp4" and dq.size_bytes == 10 and dq.source_type == "local"
    assert src.oversize_limit_bytes(item, ctx) == 5 * _MB
    assert src.audit_block_reason(item, ctx) is None
    assert src.reject_item(item, ctx) is None

    pk = PikPakSnapshotSource([])
    pk_item = PublishItem(
        item_id="F1", name="f.mp4", size_bytes=10, source_type="pikpak", audit={"status": "FAIL", "message": "risk"}
    )
    assert pk.audit_block_reason(pk_item, ctx) is not None
    assert pk.dedup_query(pk_item, ctx).remote_file_id == "F1"
    assert pk.reject_item(PublishItem(item_id="D1", name="d", size_bytes=0, is_dir=True), ctx) is not None


@pytest.mark.asyncio
async def test_subfolder_batch_composite_header_p2_5(tmp_dir):
    """子目录批次标头还原 '#根目录 /子路径' 复合形态。"""
    tmp = os.path.join(tmp_dir, f"header_{uuid.uuid4().hex[:8]}")
    sub = os.path.join(tmp, "sub1")
    os.makedirs(sub, exist_ok=True)
    safe_write_bytes(os.path.join(sub, "a.mp4"), b"x")
    try:
        source = LocalFolderSource(tmp)
        ctx = PublishContext(task_id="t", target_chat_id=-1, folder_name=os.path.basename(tmp))
        batches = await source.get_batches(ctx)
        assert len(batches) == 1
        assert batches[0].folder_name.startswith("#")
        assert f"#{os.path.basename(tmp)} /sub1" == batches[0].folder_name
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# / / /
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_file_parts_invalid_retry_logs_to_task_p2_6():
    """本地 FILE_PARTS_INVALID 自动重试前落任务日志 (旧基线文案还原)。"""
    TaskQueueRepository.create(task_id="v2_p26", task_type="publish_folder", params={}, status="running")
    engine = LocalDiskTransportEngine()
    item = PublishItem(item_id="D:/fake/v2_p26.mp4", name="v2_p26.mp4", size_bytes=1000)
    client = AsyncMock()
    client.send_file = AsyncMock(return_value=MagicMock(id=5))
    ctx = PublishContext(task_id="v2_p26", target_chat_id=-1, userbot_client=client, split_threshold_bytes=10 * _MB)

    calls = {"n": 0}

    async def flaky(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("file_parts_invalid: missing part")
        return MagicMock()

    with (
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=flaky),
        patch("app.core.fast_telethon.resolve_breakpoint_path", return_value="D:/fake/v2_p26.mp4.upload.meta"),
        patch("os.path.exists", return_value=False),
    ):
        outcome = await engine.send_item(item, ctx)

    assert outcome.message_ids == [5] and calls["n"] == 2
    logs, _, _ = TaskQueueRepository.get_task_logs("v2_p26")
    assert any("正在清除失效断点并自动全量重新上传" in e["message"] for e in logs)


def test_p3_9_strict_moved_counting():
    """归档响应缺 moved 字段 → 视为该组失败走回退 (严格口径)。"""
    with pytest.raises(RuntimeError, match="严格口径"):
        PikPakStructuredArchiveStrategy._strict_moved({})
    with pytest.raises(RuntimeError, match="严格口径"):
        PikPakStructuredArchiveStrategy._strict_moved({"moved": "3"})
    assert PikPakStructuredArchiveStrategy._strict_moved({"moved": 3}) == 3


@pytest.mark.asyncio
async def test_p3_12_meta_fallback_name_via_engine():
    """空 task_id 时分卷 meta 兜底命名还原旧基线 pikpak-direct (引擎传参口径)。"""
    from app.core.pipeline.engines import PikPakStreamTransportEngine

    engine = PikPakStreamTransportEngine(
        link_provider=AsyncMock(return_value=["https://cdn.fake.test/dl/F1?X-Sign=FAKESIGN"])
    )
    item = PublishItem(item_id="F1", name="v.mp4", size_bytes=1024, source_type="pikpak")
    client = AsyncMock()
    client.send_file = AsyncMock(return_value=MagicMock(id=7))
    ctx = PublishContext(task_id="", target_chat_id=-1, userbot_client=client, split_threshold_bytes=10 * _MB)
    captured = {}

    async def fake_upload(client_, file_path, **kwargs):
        captured["meta_path"] = kwargs.get("meta_path")
        return MagicMock()

    class _StubProvider:
        def __init__(self, *, file_id, file_name, total_bytes, url_provider, **kw):
            self.total_bytes = total_bytes
            self.file_id = file_id
            self.file_name = file_name

        async def read_chunk(self, offset, size):
            return b"x" * 8

        async def aclose(self):
            pass

    with (
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload),
        patch("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _StubProvider),
    ):
        outcome = await engine.send_item(item, ctx)

    assert outcome.message_ids == [7]
    assert os.path.basename(captured["meta_path"]).startswith("pikpak-direct_F1_")
    assert captured["meta_path"].endswith(".upload.meta")


def test_p3_13_bot_pool_client_label():
    """(SOTA 批2.1 单轨化): Bot 池归因走结构化通道 —— outcome 字段 /
    TransportError.client_label 包装, 不再动态挂载异常属性。"""
    outcome = TransportOutcome(message_ids=[1], speed_mb_s=1.0, client_label="Bot Pool #2")
    assert outcome.client_label.startswith("Bot Pool")

    root = RuntimeError("x")
    wrapped = TransportError(str(root), client_label="Bot Pool #2", cause=root)
    assert wrapped.client_label == "Bot Pool #2"
    assert wrapped.cause is root


# ==============================================================================
# Part: test_stream_splitter.py
# ==============================================================================
SPLITTER_SOURCE = os.path.join("app", "core", "stream_splitter.py")


def test_threshold_formula_matches_config_constants():
    """裁决公式: threshold = base * 975 // 1000, 与 config 常量运算式逐值一致 (防硬编码回归)。"""
    assert derive_split_threshold(False) == settings.MAX_FILE_SIZE_BYTES * 975 // 1000
    assert derive_split_threshold(True) == settings.PREMIUM_MAX_FILE_SIZE_BYTES * 975 // 1000


def test_no_threshold_literals_in_source():
    """AST 扫描: 模块内禁止出现字节量级 (>= 1MB) 的整数字面量 —— 阈值只能由三档常量派生。"""
    with open(SPLITTER_SOURCE, encoding="utf-8") as fh:
        tree = ast.parse(fh.read(), filename=SPLITTER_SOURCE)
    offenders = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, int) and node.value >= 1024 * 1024
    ]
    assert offenders == [], f"stream_splitter.py 出现阈值量级字面量: {offenders}"


def test_premium_two_tiers_and_protocol_cap():
    """两档切换 + 协议帽兜底 + 环境变量覆写跟随 (常量唯一来源)。"""
    non_premium = derive_split_threshold(False)
    premium = derive_split_threshold(True)
    assert non_premium < premium
    assert non_premium <= settings.PROTOCOL_MAX_FILE_SIZE_BYTES
    assert premium <= settings.PROTOCOL_MAX_FILE_SIZE_BYTES

    # 基础档被 env 覆写后, 派生值跟随常量 (而非写死数值)
    original_max = settings.MAX_FILE_SIZE_BYTES
    try:
        settings.MAX_FILE_SIZE_BYTES = 3000 * 1024 * 1024
        assert derive_split_threshold(False) == settings.MAX_FILE_SIZE_BYTES * 975 // 1000
    finally:
        settings.MAX_FILE_SIZE_BYTES = original_max


def test_split_single_part_boundaries():
    threshold = derive_split_threshold(False)
    # 恰等于阈值 → 单 Part
    parts = VirtualStreamSplitter.split(threshold, threshold)
    assert parts == [PartSpec(offset=0, length=threshold, part_index=1)]
    # 0 < total < threshold → 单 Part
    parts = VirtualStreamSplitter.split(threshold - 1, threshold)
    assert parts == [PartSpec(offset=0, length=threshold - 1, part_index=1)]
    # 零字节 → 空列表 (调用方跳过引擎直传并告警)
    assert VirtualStreamSplitter.split(0, threshold) == []


def test_split_multi_part_cover_no_overlap():
    threshold = 1000
    total = threshold * 2 + 345  # 非整除余量
    parts = VirtualStreamSplitter.split(total, threshold)
    assert len(parts) == 3
    assert [p.length for p in parts] == [threshold, threshold, 345]
    # 连续无重叠: 相邻 offset 首尾相接
    for prev, nxt in zip(parts, parts[1:], strict=False):
        assert nxt.offset == prev.offset + prev.length
    # 全量覆盖不重叠
    assert sum(p.length for p in parts) == total
    assert parts[0].offset == 0
    # part_index 为 1..n 连续 (1-based, 供 caption 直接使用)
    assert [p.part_index for p in parts] == [1, 2, 3]


def test_split_rejects_invalid_inputs():
    protocol_cap = settings.PROTOCOL_MAX_FILE_SIZE_BYTES
    # 阈值恰等于协议帽 → 通过 (边界)
    parts = VirtualStreamSplitter.split(protocol_cap, protocol_cap)
    assert len(parts) == 1
    # 阈值超过协议帽 → ValueError (该 Part 不可上传)
    with pytest.raises(ValueError):
        VirtualStreamSplitter.split(protocol_cap * 2, protocol_cap + 1)
    # total_bytes < 0 / threshold <= 0 → ValueError
    with pytest.raises(ValueError):
        VirtualStreamSplitter.split(-1, 100)
    with pytest.raises(ValueError):
        VirtualStreamSplitter.split(100, 0)


def test_caption_formatting():
    assert format_part_caption("a.mp4", 1, 2) == "[1/2] a.mp4"
    assert format_part_caption("a.mp4", 2, 2) == "[2/2] a.mp4"
    # 单 Part 用原名, 无前缀
    assert format_part_caption("a.mp4", 1, 1) == "a.mp4"
