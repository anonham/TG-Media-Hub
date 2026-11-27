"""Pipeline Core: 单文件即时归档与终态对账、逐文件夹批次、双卡制与多媒体相册对齐测试 (Immediate Archive & PikPak Alignment)

覆盖范围:
- 原 test_issue7_pikpak_immediate_archive: 单文件发布后即时移动与终态兜底对账
- 原 test_publish_pikpak_alignment: 逐文件夹批次归组、双卡制、日志口径、相册分组与引擎属性对齐"""

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from _cm_seed import seed_default_account
from pydantic import ValidationError

from app.config import settings
from app.core.client_manager import client_manager
from app.core.pipeline.cleanup import (
    PikPakDeleteCleanupStrategy,
    PikPakStructuredArchiveStrategy,
)
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.stream_chunk_provider import BaseChunkProvider
from app.db.task_repository import TaskQueueRepository
from app.models.pikpak import PikPakFileItem, PikPakPublishRequest
from app.services.pikpak_publisher_service import PikPakPublisherService


# ==============================================================================
# Part: test_issue7_pikpak_immediate_archive.py
# ==============================================================================
@pytest.mark.asyncio
async def test_immediate_archive_single_item_structured():
    """测试单文件上传成功后即时结构化移动"""
    resolve_folder = AsyncMock(side_effect=lambda parent, name="": f"folder_{name or 'root'}")
    archive_to_folder = AsyncMock(return_value={"matched": 1, "moved": 1})
    archive_files = AsyncMock(return_value={"matched": 1, "moved": 1})

    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=resolve_folder,
        archive_files_to_folder=archive_to_folder,
        archive_files=archive_files,
    )

    ctx = PublishContext(
        task_id="test_task_7",
        target_chat_id=-1009000000010,
        source_type="pikpak",
        auto_cleanup_mode="archive",
        dir_tree={"p1": "Season 1"},
    )
    item = PublishItem(item_id="f1", name="ep1.mp4", size_bytes=1000, parent_folder_id="p1")

    # 1. 执行单文件即时归档
    ok = await strategy.archive_single_item(item, ctx)
    assert ok is True
    assert "f1" in strategy._archived_item_ids
    # 验证目录解析与归档调用
    resolve_folder.assert_any_call("")
    resolve_folder.assert_any_call("folder_root", "Season 1")
    archive_to_folder.assert_called_once_with(["f1"], folder_id="folder_Season 1", source_parent_id="p1")

    # 2. 终态对账清理：因为 f1 已经移动过，cleanup 不会再次调用 archive_to_folder
    archive_to_folder.reset_mock()
    summary = await strategy.cleanup([item], ctx)
    assert "即时归档 1" in summary
    assert "补移" not in summary
    archive_to_folder.assert_not_called()


@pytest.mark.asyncio
async def test_reconciliation_fallback_for_unarchived_items():
    """测试即时移动未覆盖或失败的文件，在终态 cleanup 中获得兜底补移"""
    resolve_folder = AsyncMock(side_effect=lambda parent, name="": f"folder_{name or 'root'}")
    archive_to_folder = AsyncMock(return_value={"matched": 1, "moved": 1})
    archive_files = AsyncMock(return_value={"matched": 1, "moved": 1})

    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=resolve_folder,
        archive_files_to_folder=archive_to_folder,
        archive_files=archive_files,
    )

    ctx = PublishContext(
        task_id="test_task_7",
        target_chat_id=-1009000000010,
        source_type="pikpak",
        auto_cleanup_mode="archive",
        dir_tree={"p1": "Season 1"},
    )
    item1 = PublishItem(item_id="f1", name="ep1.mp4", size_bytes=1000, parent_folder_id="p1")
    item2 = PublishItem(item_id="f2", name="ep2.mp4", size_bytes=1000, parent_folder_id="p1")

    # item1 成功即时移动
    await strategy.archive_single_item(item1, ctx)
    assert "f1" in strategy._archived_item_ids
    assert "f2" not in strategy._archived_item_ids

    # 终态 cleanup 传入 [item1, item2]，应该只对 item2 执行补移
    archive_to_folder.reset_mock()
    summary = await strategy.cleanup([item1, item2], ctx)
    assert "即时归档 1" in summary
    assert "补移 1" in summary
    archive_to_folder.assert_called_once_with(["f2"], folder_id="folder_Season 1", source_parent_id="p1")


@pytest.mark.asyncio
async def test_immediate_archive_skipped_when_mode_disabled():
    """测试 auto_cleanup_mode 为 none 或 delete 时不执行归档"""
    resolve_folder = AsyncMock()
    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=resolve_folder,
    )
    ctx = PublishContext(
        task_id="test_task_7",
        target_chat_id=-1009000000010,
        source_type="pikpak",
        auto_cleanup_mode="none",
    )
    item = PublishItem(item_id="f1", name="ep1.mp4", size_bytes=1000)

    ok = await strategy.archive_single_item(item, ctx)
    assert ok is False
    assert len(strategy._archived_item_ids) == 0
    resolve_folder.assert_not_called()


# ==============================================================================
# Part: test_publish_pikpak_alignment.py
# ==============================================================================
TARGET = "-100777"


class _StubProvider(BaseChunkProvider):
    """冻结 Provider 替身: 构造签名一致, 内存读。"""

    def __init__(self, file_id, file_name, total_bytes, url_provider, request_timeout=60.0, **kwargs):
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = total_bytes
        self._url_provider = url_provider

    async def read_chunk(self, offset, size):
        if offset >= self.total_bytes or size <= 0:
            raise ValueError("out of range")
        return b"\xab" * (min(offset + size, self.total_bytes) - offset)

    async def aclose(self):
        pass


def _make_item(file_id="F1", name="a.mp4", size_bytes=1024, parent_id="P0"):
    from app.core.pikpak.expander import ParsedFileItem

    return ParsedFileItem(
        file_id=file_id,
        name=name,
        is_dir=False,
        size_bytes=size_bytes,
        audit={"status": "STATUS_OK"},
        thumbnail_link=None,
        duration_s=None,
        width=None,
        height=None,
        parent_id=parent_id,
    )


@pytest.fixture
def pik_env(tmp_dir, monkeypatch):
    """最小真门面环境: meta 沙箱 / userbot 替身 / 冻结引擎与 CLI 依赖替身 / 探针放行。"""
    sandbox = os.path.join(tmp_dir, "upload_meta")
    monkeypatch.setattr(settings, "PIKPAK_META_DIR", sandbox)

    client = AsyncMock()
    sent = MagicMock()
    sent.id = 4321
    client.send_file = AsyncMock(return_value=sent)
    client.send_message = AsyncMock(return_value=MagicMock())

    monkeypatch.setattr("app.core.fast_telethon.upload_file_parallel", AsyncMock(return_value=MagicMock()))

    async def fake_links(file_ids, **kw):
        return [f"https://cdn.fake.test/dl/{fid}?X-Sign=FAKESIGN" for fid in file_ids]

    monkeypatch.setattr("app.services.pikpak_service.get_download_links", fake_links)
    monkeypatch.setattr("app.core.stream_chunk_provider.PikPakStreamChunkProvider", _StubProvider)

    async def fake_inspect(*a, **k):
        return {"is_accessible": True, "has_write_permission": True, "permission_summary": "ok"}

    monkeypatch.setattr("app.services.channel_inspector.ChannelInspector.inspect_channel", fake_inspect)
    _acc = seed_default_account(client_manager, client=client, is_premium=False)
    monkeypatch.setattr(client_manager, "_accounts", {_acc.account_id: _acc})
    monkeypatch.setattr(client_manager, "refresh_premium", AsyncMock(return_value=False), raising=False)

    # 清理链 CLI 依赖替身 (真门面装配注入 pikpak_service 模块属性 → patch 源头生效)
    async def fake_resolve(parent_id="", name="已发布归档", **kw):
        return "ARC-root"

    async def fake_archive(file_ids, *, source_parent_id="", **kw):
        return {"archive_folder_id": "ARC", "moved": len(file_ids)}

    async def fake_to_folder(file_ids, *, folder_id, source_parent_id="", **kw):
        return {"archive_folder_id": folder_id, "moved": len(file_ids)}

    async def fake_delete(file_ids, *, parent_id="", **kw):
        return None

    async def fake_list(folder_id="", page_token=None, **kw):
        return ([object()], None)  # 非空 → prune 全跳过 (双卡用例不受清理噪声干扰)

    monkeypatch.setattr("app.services.pikpak_service.resolve_archive_folder", fake_resolve)
    monkeypatch.setattr("app.services.pikpak_service.archive_files", fake_archive)
    monkeypatch.setattr("app.services.pikpak_service.archive_files_to_folder", fake_to_folder)
    monkeypatch.setattr("app.services.pikpak_service.delete_files", fake_delete)
    monkeypatch.setattr("app.services.pikpak_service.list_files_raw", fake_list)
    monkeypatch.setattr("app.core.pikpak.thumbnail.fetch_thumbnail", AsyncMock(return_value=None))
    monkeypatch.setattr("app.core.pikpak.thumbnail.extract_frame_ffmpeg", AsyncMock(return_value=None))
    monkeypatch.setattr("app.core.pipeline.runtime.asyncio.sleep", AsyncMock())
    monkeypatch.setattr("app.core.pipeline.notifier.asyncio.sleep", AsyncMock())

    return SimpleNamespace(client=client, sandbox=sandbox)


# ---------------------------------------------------------------------------
# 多批次 (语义对齐 scan_folder_grouped)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multi_batch_grouping_by_parent_dir():
    """目录批次 (每直接含媒体目录一个) + 散装叶子 (is_folder_publish=False, 不发卡)。"""
    from app.core.pipeline.sources import PikPakSnapshotSource

    # 1) 文件夹发布载荷: parent 均在 dir_tree → 文件夹批次 (深目录 rel 排序在前)
    dir_tree = {"R": "Test", "C1": "Test/Test1"}
    raw = [
        {"file_id": "F_C1", "name": "c1.mp4", "size_bytes": 10, "parent_id": "C1"},
        {"file_id": "F_R", "name": "r.mp4", "size_bytes": 10, "parent_id": "R"},
    ]
    src = PikPakSnapshotSource(remote_files=raw)
    ctx = PublishContext(
        task_id="t_mb", target_chat_id=-1, is_folder_publish=True, folder_name="Test", dir_tree=dict(dir_tree)
    )
    batches = await src.get_batches(ctx)
    assert len(batches) == 2
    by_id = {b.batch_id: b for b in batches}
    c1 = by_id["C1"]
    assert c1.is_folder_publish is True and c1.rel_path == "Test/Test1"
    assert c1.folder_name == "#Test1"
    r = by_id["R"]
    assert r.is_folder_publish is True and r.rel_path == "Test"
    assert r.folder_name == "#Test"

    # 2) 散装载荷 (无 dir_tree, 非文件夹发布): 无目录上下文 → 散装批次不发卡
    src2 = PikPakSnapshotSource(
        remote_files=[
            {"file_id": "F_LOOSE", "name": "loose.mp4", "size_bytes": 10, "parent_id": "ZZZ"},
        ]
    )
    ctx2 = PublishContext(task_id="t_mb2", target_chat_id=-1, is_folder_publish=False, dir_tree={})
    batches2 = await src2.get_batches(ctx2)
    assert len(batches2) == 1
    assert batches2[0].is_folder_publish is False
    assert batches2[0].items[0].item_id == "F_LOOSE"


# ---------------------------------------------------------------------------
# 双卡制 (真门面 + 真通知器)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dual_cards_batch_plus_task_summary(pik_env):
    """两个文件夹批次 → 2 张批次卡 + 1 张任务汇总卡 (全局 #folder_name 标头)。"""
    dir_tree = {"R": "Test", "C1": "Test/Test1"}
    raw = [
        _make_item(file_id="F_R", parent_id="R"),
        _make_item(file_id="F_C1", parent_id="C1"),
    ]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="v2_d4",
        is_folder_publish=True,
        folder_name="Test",
        dir_tree=dir_tree,
        auto_cleanup_mode="archive",
        send_delay_seconds=0,
    )
    assert res.published_count == 2
    # 2 张批次卡 + 1 张任务汇总卡
    assert pik_env.client.send_message.await_count == 3
    texts = [c.args[1] for c in pik_env.client.send_message.await_args_list]
    assert sum("发布文件夹" in t for t in texts) == 2
    summary = [t for t in texts if "任务汇总" in t]
    assert len(summary) == 1
    assert "#Test" in summary[0]
    assert "#Test1" in summary[0] and "#Test" in summary[0].split("任务汇总")[1]
    assert "合计: ✅ 2" in summary[0]


# ---------------------------------------------------------------------------
# D5: 日志口径反转 + 失败封顶
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_archive_success_summary_out_of_task_log_d5():
    """归档成功摘要不再落任务日志, 摘要仍返回供卡片归档行。"""
    TaskQueueRepository.create("v2_d5a", "pikpak_publish", {"target_channel": TARGET})

    async def fake_resolve(parent_id="", name="已发布归档", **kw):
        return "ARC-root"

    async def fake_to_folder(file_ids, *, folder_id, source_parent_id="", **kw):
        return {"moved": len(file_ids)}

    async def fake_archive(file_ids, *, source_parent_id="", **kw):
        return {"moved": len(file_ids)}

    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=fake_resolve,
        archive_files_to_folder=fake_to_folder,
        archive_files=fake_archive,
    )
    items = [PublishItem(item_id="F1", name="a.mp4", size_bytes=1, parent_folder_id="P0")]
    ctx = PublishContext(task_id="v2_d5a", target_chat_id=-1, dir_tree={"P0": "Test"}, auto_cleanup_mode="archive")
    summary = await strategy.cleanup(items, ctx)

    assert "已归档 1" in summary
    logs, _, _ = TaskQueueRepository.get_task_logs("v2_d5a")
    assert not any("已归档" in e["message"] for e in logs), "成功摘要必须移出任务日志"


@pytest.mark.asyncio
async def test_archive_failure_log_capped_d5():
    """12 组迁移失败 → 任务日志恰 10 条逐条 + 1 条剩余计数。"""
    TaskQueueRepository.create("v2_d5b", "pikpak_publish", {"target_channel": TARGET})

    async def fake_resolve(parent_id="", name="已发布归档", **kw):
        return "ARC-root"

    async def fail_to_folder(file_ids, *, folder_id, source_parent_id="", **kw):
        raise RuntimeError("mv blocked")

    async def fail_archive(file_ids, *, source_parent_id="", **kw):
        raise RuntimeError("mv blocked")

    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=fake_resolve,
        archive_files_to_folder=fail_to_folder,
        archive_files=fail_archive,
    )
    items = [PublishItem(item_id=f"F{i}", name=f"f{i}.mp4", size_bytes=1, parent_folder_id=f"P{i}") for i in range(12)]
    ctx = PublishContext(
        task_id="v2_d5b",
        target_chat_id=-1,
        dir_tree={f"P{i}": f"Test/D{i}" for i in range(12)},
        auto_cleanup_mode="archive",
    )
    summary = await strategy.cleanup(items, ctx)
    assert summary == ""

    logs, _, _ = TaskQueueRepository.get_task_logs("v2_d5b")
    warn_lines = [e["message"] for e in logs if e["message"].startswith("⚠️")]
    assert len(warn_lines) == 10, "前 10 条逐条"
    assert any("其余 2 条迁移失败已省略" in e["message"] for e in logs), "剩余计数一行"


@pytest.mark.asyncio
async def test_delete_mode_same_caliber_d5():
    """delete 模式同口径: 成功摘要不落任务日志 (返回给卡片); 失败封顶进日志。"""
    TaskQueueRepository.create("v2_d5c", "pikpak_publish", {"target_channel": TARGET})
    delete_calls = []

    async def fake_delete(file_ids, *, parent_id="", **kw):
        delete_calls.append(list(file_ids))

    async def fail_delete(file_ids, *, parent_id="", **kw):
        raise RuntimeError("rm blocked")

    items = [
        PublishItem(item_id="F1", name="a.mp4", size_bytes=1, parent_folder_id="P1"),
        PublishItem(item_id="F2", name="b.mp4", size_bytes=1, parent_folder_id="P2"),
    ]
    ctx = PublishContext(task_id="v2_d5c", target_chat_id=-1, dir_tree={}, auto_cleanup_mode="delete")

    # 1) 成功: 摘要返回, 不落任务日志
    ok = PikPakDeleteCleanupStrategy(delete_files=fake_delete)
    summary = await ok.cleanup(items, ctx)
    assert summary == "🗑️ 已将 2 个文件移入回收站"
    logs, _, _ = TaskQueueRepository.get_task_logs("v2_d5c")
    assert not any("回收站" in e["message"] for e in logs)

    # 2) 失败: 封顶进日志
    TaskQueueRepository.create("v2_d5d", "pikpak_publish", {"target_channel": TARGET})
    bad = PikPakDeleteCleanupStrategy(delete_files=fail_delete)
    ctx2 = PublishContext(task_id="v2_d5d", target_chat_id=-1, dir_tree={}, auto_cleanup_mode="delete")
    summary2 = await bad.cleanup(items, ctx2)
    assert summary2 == ""
    logs2, _, _ = TaskQueueRepository.get_task_logs("v2_d5d")
    assert sum(e["message"].startswith("⚠️") for e in logs2) == 2


# ---------------------------------------------------------------------------
# D2: 空目录自底向上删除安全栏
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prune_bottom_up_and_safety_d2():
    """深层先删 → 父目录随之变空; 选中根 (rel 无 /) 永不删; 非空残留不删并记因。"""
    TaskQueueRepository.create("v2_d2", "pikpak_publish", {"target_channel": TARGET})
    dir_tree = {
        "R": "Test",
        "C1": "Test/Test1",
        "G1": "Test/Test1/G1",
        "C2": "Test/Test2",
    }

    async def fake_list(folder_id="", page_token=None, **kw):
        if folder_id == "C2":
            return ([object()], None)  # 残留 → 非空
        return ([], None)  # G1 / C1 空

    delete_calls = []

    async def fake_delete(file_ids, *, parent_id="", **kw):
        delete_calls.append(list(file_ids))

    async def fake_resolve(parent_id="", name="已发布归档", **kw):
        return "ARC-root"

    async def fake_archive(file_ids, *, source_parent_id="", **kw):
        return {"moved": len(file_ids)}

    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=fake_resolve,
        archive_files_to_folder=fake_archive,
        archive_files=fake_archive,
        list_files_raw=fake_list,
        delete_files=fake_delete,
    )
    items = [PublishItem(item_id="F1", name="a.mp4", size_bytes=1, parent_folder_id="G1")]
    ctx = PublishContext(task_id="v2_d2", target_chat_id=-1, dir_tree=dict(dir_tree), auto_cleanup_mode="archive")
    await strategy.cleanup(items, ctx)

    # 自底向上: G1 先删, C1 随之变空后删; C2 非空不删; 选中根 R 永不删
    assert delete_calls == [["G1"], ["C1"]]
    flat = [fid for call in delete_calls for fid in call]
    assert "R" not in flat and "C2" not in flat
    logs, _, _ = TaskQueueRepository.get_task_logs("v2_d2")
    assert any("Test/Test2 非空" in e["message"] for e in logs)
    assert any("已清理 2 个变空的源目录" in e["message"] for e in logs)


@pytest.mark.asyncio
async def test_delete_mode_prune_symmetric_d2():
    """delete 模式空目录删除对称适用。"""
    TaskQueueRepository.create("v2_d2d", "pikpak_publish", {"target_channel": TARGET})
    dir_tree = {"R": "Test", "C1": "Test/Test1"}

    async def fake_list(folder_id="", page_token=None, **kw):
        return ([], None)

    delete_calls = []

    async def fake_delete(file_ids, *, parent_id="", **kw):
        delete_calls.append(list(file_ids))

    strategy = PikPakDeleteCleanupStrategy(delete_files=fake_delete, list_files_raw=fake_list)
    items = [PublishItem(item_id="F1", name="a.mp4", size_bytes=1, parent_folder_id="C1")]
    ctx = PublishContext(task_id="v2_d2d", target_chat_id=-1, dir_tree=dict(dir_tree), auto_cleanup_mode="delete")
    await strategy.cleanup(items, ctx)

    flat = [fid for call in delete_calls for fid in call]
    assert flat == ["F1", "C1"], "文件删除后 C1 变空被删; 选中根 R 永不删"


# ---------------------------------------------------------------------------
# D2: 网盘根 422 护栏
# ---------------------------------------------------------------------------


def test_drive_root_selection_rejected_422():
    """dir_tree 空路径条目 = 网盘根被选 → 422 (前端 toggleDir 空防御为第一道)。"""
    with pytest.raises(ValidationError):
        PikPakPublishRequest(
            remote_files=[PikPakFileItem(file_id="F1", name="a.mp4", size_bytes=1)],
            target_channel=TARGET,
            dir_tree={"root_id": ""},
        )
    # 合法载荷 (选中根 rel 非空) 放行
    req = PikPakPublishRequest(
        remote_files=[PikPakFileItem(file_id="F1", name="a.mp4", size_bytes=1, parent_id="R")],
        target_channel=TARGET,
        dir_tree={"R": "Test"},
    )
    assert req.dir_tree == {"R": "Test"}


# ---------------------------------------------------------------------------
# 包4: BUG-B 后端 file_id 显式去重 (重复载荷只发布一次)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_leaf_payload_published_once(pik_env):
    """重复叶子载荷 → 引擎调用/发布历史均只一次 + 去重任务日志 (BUG-B 第二道防线)。"""
    from app.db.database import get_db

    TaskQueueRepository.create("v2_dup", "pikpak_publish", {"target_channel": TARGET})
    raw = [_make_item(file_id="FDUP"), _make_item(file_id="FDUP", name="dup2.mp4")]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="v2_dup",
    )
    assert res.published_count == 1
    assert res.total_files_count == 1
    row = get_db().execute("SELECT COUNT(*) AS c FROM publish_history WHERE remote_file_id='FDUP'").fetchone()
    assert row["c"] == 1
    logs, _, _ = TaskQueueRepository.get_task_logs("v2_dup")
    assert any("已按 file_id 去重" in e["message"] for e in logs)


@pytest.mark.asyncio
async def test_all_duplicate_payload_fails_not_silent_success(pik_env):
    """全重复载荷 (同一 file_id 提交两次) → 去重保留首个, 只发布一次 (1/1, 绝不重复发送)。"""
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=[_make_item(file_id="FALL"), _make_item(file_id="FALL")],
        target_chat_id=TARGET,
        task_id="v2_dup_all",
    )
    assert res.published_count == 1
    assert res.total_files_count == 1
    # 终态公式口径: (published + skipped) >= total → 不判 failed
    assert (res.published_count + len(res.skipped_files)) >= res.total_files_count


# ---------------------------------------------------------------------------
# PikPak 相册引擎
# ---------------------------------------------------------------------------

_IMG = 5 * 1024 * 1024  # 5 MiB 图片 (远低于非 premium 分卷阈值 1950 MiB)
_HUGE = (
    2 * 1024 * 1024 * 1024
)  # 2.00 GiB (2048.00 MiB) 图片 (≥ 2000 MiB (1.95 GiB) 阈值 → 绝不入组, Phase 3 走占位卡/跳过)


def _make_image(file_id, size_bytes=_IMG, parent_id="R", name=None):
    # file_id 约定自带图片扩展名 (编排层按 item_id 扩展名判图片); name 缺省复用 file_id
    return _make_item(file_id=file_id, name=name or file_id, size_bytes=size_bytes, parent_id=parent_id)


@pytest.mark.asyncio
async def test_album_mode_groups_images_and_gates_oversize(pik_env):
    """album_mode=True: 同批次图片组相册 (photo 形态); 超限图片绝不入组 (走占位卡/跳过)。"""
    from app.db.database import get_db

    TaskQueueRepository.create("v2_album1", "pikpak_publish", {"target_channel": TARGET})
    # 组发替身: 真实 TG 对 media group 逐张返回消息 → 返回 2 条消息列表 (J5 语义下
    # 非列表 stub 会使第 2 张判为未发送, 与真实形态不符)
    pik_env.client.send_file = AsyncMock(return_value=[MagicMock(id=4321), MagicMock(id=4322)])
    dir_tree = {"R": "Test"}
    raw = [
        _make_image("IMG1.jpg", parent_id="R"),
        _make_image("IMG2.jpg", parent_id="R"),
        _make_image("IMGHUGE.jpg", size_bytes=_HUGE, parent_id="R"),
    ]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="v2_album1",
        is_folder_publish=True,
        folder_name="Test",
        dir_tree=dir_tree,
        album_mode=True,
    )

    assert res.published_count == 2
    assert res.failed_files == []
    assert len(res.skipped_files) == 1 and res.skipped_files[0].file_name == "IMGHUGE.jpg"

    # 组发: send_file 恰一次 (句柄列表) + photo 形态 + 多图组 caption=None; 超限图零上传
    assert pik_env.client.send_file.await_count == 1
    call = pik_env.client.send_file.await_args
    assert isinstance(call.args[1], list) and len(call.args[1]) == 2
    assert call.kwargs.get("force_document") is False
    assert call.kwargs.get("caption") is None
    # 占位卡已发 (超限缺集说明)
    assert any("【缺集说明】" in c.args[1] for c in pik_env.client.send_message.await_args_list)
    # 入库 2 条 (相册成功逐条 record)
    rows = (
        get_db()
        .execute(
            "SELECT COUNT(*) AS c FROM publish_history WHERE source_type='pikpak' AND remote_file_id IN ('IMG1.jpg','IMG2.jpg')"
        )
        .fetchone()
    )
    assert rows["c"] == 2


@pytest.mark.asyncio
async def test_album_group_failure_all_failed_zero_ingestion(pik_env):
    """整组失败 → 全员 failed 且零入库 (对齐本地相册语义)。"""
    from app.db.database import get_db

    TaskQueueRepository.create("v2_album2", "pikpak_publish", {"target_channel": TARGET})
    pik_env.client.send_file = AsyncMock(side_effect=RuntimeError("album send boom"))

    raw = [_make_image("IMGF1.jpg"), _make_image("IMGF2.jpg")]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="v2_album2",
        album_mode=True,
        send_delay_seconds=0,
    )
    assert res.published_count == 0
    assert len(res.failed_files) == 2
    rows = (
        get_db()
        .execute(
            "SELECT COUNT(*) AS c FROM publish_history WHERE source_type='pikpak' AND remote_file_id IN ('IMGF1.jpg','IMGF2.jpg')"
        )
        .fetchone()
    )
    assert rows["c"] == 0, "整组失败必须零入库"


@pytest.mark.asyncio
async def test_album_mode_off_default_single_sends(pik_env):
    """album_mode 缺省 (False) → 行为与包3 终态一致: 图片逐张单发。"""
    raw = [_make_image("IMGS1.jpg"), _make_image("IMGS2.jpg")]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="v2_album3",
        send_delay_seconds=0,
    )
    assert res.published_count == 2
    # 单发: 每张一次 send_file (非列表)
    assert pik_env.client.send_file.await_count == 2
    for c in pik_env.client.send_file.await_args_list:
        assert not isinstance(c.args[1], list)


@pytest.mark.asyncio
async def test_album_single_image_goes_single_path(pik_env):
    """单图不组相册 (caption/形态歧义), 走单文件路径 (caption = 图名)。"""
    raw = [_make_image("IMGS3.jpg")]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="v2_album4",
        album_mode=True,
        send_delay_seconds=0,
    )
    assert res.published_count == 1
    call = pik_env.client.send_file.await_args
    assert not isinstance(call.args[1], list)
    assert call.kwargs.get("caption") == "IMGS3.jpg"


@pytest.mark.asyncio
async def test_album_group_with_dedup_hit(pik_env):
    """组内防重命中: 已发布图片跳过, 剩余单图走单文件路径 (caption = 图名)。"""
    from app.db.publish_repository import PublishHistoryRepository

    PublishHistoryRepository.record(
        TARGET, "pikpak://R/IMGD1.jpg", _IMG, "", source_type="pikpak", remote_file_id="IMGD1.jpg"
    )
    raw = [_make_image("IMGD1.jpg"), _make_image("IMGD2.jpg")]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="v2_album5",
        album_mode=True,
        send_delay_seconds=0,
    )
    assert res.published_count == 2  # 防重命中计 1 + 新发 1
    assert pik_env.client.send_file.await_count == 1
    assert not isinstance(pik_env.client.send_file.await_args.args[1], list)
    assert pik_env.client.send_file.await_args.kwargs.get("caption") == "IMGD2.jpg"


# ---------------------------------------------------------------------------
# 发布修复批次 包1 (P0): item_id 双语义相册修复 / 护栏 / 空 outcome 失败语义 / 相册卡行
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pikkap_album_groups_by_name_extension_p0_fix(pik_env):
    """P0 修复 (8da0da5e): item_id (file_id) 无扩展名、item.name 带扩展名 —— 相册分组
    必须按 name 判定。12 张图 → 组发按 10 张/组调 2 次; publish_history 零 tg_message_id=0;
    任务日志含相册成功行。"""
    from app.db.database import get_db

    TaskQueueRepository.create("p0_album", "pikpak_publish", {"target_channel": TARGET})
    # 组发替身: 10 张组返回 10 条消息, 2 张组返回 2 条 (逐条独立消息 id)
    pik_env.client.send_file = AsyncMock(
        side_effect=[
            [MagicMock(id=1000 + i) for i in range(10)],
            [MagicMock(id=2000 + i) for i in range(2)],
        ]
    )
    raw = [_make_item(file_id=f"fid_img_{i}", name=f"img_{i}.jpg", size_bytes=_IMG, parent_id="R") for i in range(12)]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="p0_album",
        album_mode=True,
        send_delay_seconds=0,
    )
    assert res.published_count == 12
    assert res.failed_files == []
    send_calls = pik_env.client.send_file.await_args_list
    assert len(send_calls) == 2, "12 张图必须按 10 张/组组发 2 次"
    assert isinstance(send_calls[0].args[1], list) and len(send_calls[0].args[1]) == 10
    assert isinstance(send_calls[1].args[1], list) and len(send_calls[1].args[1]) == 2
    rows = (
        get_db()
        .execute(
            "SELECT COUNT(*) AS c FROM publish_history "
            "WHERE source_type='pikpak' AND remote_file_id LIKE 'fid_img_%' AND tg_message_id=0"
        )
        .fetchone()
    )
    assert rows["c"] == 0, "相册成功条目绝不允许 tg_message_id=0 落库 (P0 回归锚)"
    logs, _, _ = TaskQueueRepository.get_task_logs("p0_album")
    assert any("✅ [相册发布成功]" in e["message"] for e in logs)


@pytest.mark.asyncio
async def test_album_guard_warning_on_unclassified_items(pik_env):
    """护栏: album_mode 开 + 批内 0 张识别为图 + 存在无扩展名条目 → warning 任务日志。"""
    TaskQueueRepository.create("p0_guard", "pikpak_publish", {"target_channel": TARGET})
    raw = [
        _make_item(file_id="fid_nox1", name="nox1", size_bytes=1024, parent_id="R"),
        _make_item(file_id="fid_nox2", name="nox2", size_bytes=1024, parent_id="R"),
    ]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="p0_guard",
        album_mode=True,
        send_delay_seconds=0,
    )
    assert res.published_count == 2, "护栏只记日志, 条目照常降级单发"
    logs, _, _ = TaskQueueRepository.get_task_logs("p0_guard")
    assert any("相册模式已开启但本批次未识别到图片" in e["message"] for e in logs)


@pytest.mark.asyncio
async def test_album_guard_no_false_positive_on_video_only_batch(pik_env):
    """护栏不误报: 批内全为已知视频 (有扩展名) 时, 即使 0 张图片也不触发 warning。"""
    TaskQueueRepository.create("p0_guardv", "pikpak_publish", {"target_channel": TARGET})
    raw = [
        _make_item(file_id="fid_v1", name="v1.mp4", size_bytes=1024, parent_id="R"),
        _make_item(file_id="fid_v2", name="v2.mp4", size_bytes=1024, parent_id="R"),
    ]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="p0_guardv",
        album_mode=True,
        send_delay_seconds=0,
    )
    assert res.published_count == 2
    logs, _, _ = TaskQueueRepository.get_task_logs("p0_guardv")
    assert not any("相册模式已开启但本批次未识别到图片" in e["message"] for e in logs)


@pytest.mark.asyncio
async def test_empty_message_ids_single_file_failed_no_history(pik_env):
    """包1e: 引擎未返回有效消息 ID (id=0 哨兵) → 计入失败 + 零入库 + 任务继续,
    绝不静默成功落库 (杜绝未来被防重挡住的静默丢失)。"""
    from app.db.database import get_db

    TaskQueueRepository.create("p0_empty", "pikpak_publish", {"target_channel": TARGET})
    pik_env.client.send_file = AsyncMock(return_value=MagicMock(id=0))
    raw = [_make_item(file_id="fid_e1", name="e1.mp4", size_bytes=1024, parent_id="R")]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="p0_empty",
        send_delay_seconds=0,
    )
    assert res.published_count == 0
    assert len(res.failed_files) == 1
    assert "fid_e1" in res.failed_files[0] or "e1.mp4" in res.failed_files[0]
    assert "发送未返回消息 ID" in res.failed_files[0]
    rows = (
        get_db()
        .execute("SELECT COUNT(*) AS c FROM publish_history WHERE source_type='pikpak' AND remote_file_id='fid_e1'")
        .fetchone()
    )
    assert rows["c"] == 0, "未发送条目绝不落库"
    logs, _, _ = TaskQueueRepository.get_task_logs("p0_empty")
    assert any("发送未返回消息 ID" in e["message"] for e in logs)


@pytest.mark.asyncio
async def test_album_partial_message_ids_failed_no_history(pik_env):
    """包1e: 组发返回短于子项数 (短 sent_list) → 缺位项计入失败零入库, 其余正常落库。"""
    from app.db.database import get_db

    TaskQueueRepository.create("p0_partial", "pikpak_publish", {"target_channel": TARGET})
    pik_env.client.send_file = AsyncMock(return_value=[MagicMock(id=7001)])
    raw = [
        _make_item(file_id="fid_pa1", name="pa1.jpg", size_bytes=_IMG, parent_id="R"),
        _make_item(file_id="fid_pa2", name="pa2.jpg", size_bytes=_IMG, parent_id="R"),
    ]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="p0_partial",
        album_mode=True,
        send_delay_seconds=0,
    )
    assert res.published_count == 1
    assert len(res.failed_files) == 1
    assert "pa2.jpg" in res.failed_files[0] and "发送未返回消息 ID" in res.failed_files[0]
    ok_rows = (
        get_db()
        .execute(
            "SELECT COUNT(*) AS c FROM publish_history WHERE source_type='pikpak' "
            "AND remote_file_id='fid_pa1' AND tg_message_id>0"
        )
        .fetchone()
    )
    assert ok_rows["c"] == 1, "有消息 ID 的子项正常落库"
    lost_rows = (
        get_db()
        .execute("SELECT COUNT(*) AS c FROM publish_history WHERE source_type='pikpak' AND remote_file_id='fid_pa2'")
        .fetchone()
    )
    assert lost_rows["c"] == 0, "缺位子项零入库"


@pytest.mark.asyncio
async def test_batch_summary_card_includes_album_count(pik_env):
    """批次总结卡 (可选行): 相册成功批次卡含 '📷 相册: N 张 (合成发送)'。"""
    TaskQueueRepository.create("p0_card", "pikpak_publish", {"target_channel": TARGET})
    pik_env.client.send_file = AsyncMock(return_value=[MagicMock(id=8001), MagicMock(id=8002)])
    raw = [
        _make_item(file_id="fid_ca1", name="ca1.jpg", size_bytes=_IMG, parent_id="R"),
        _make_item(file_id="fid_ca2", name="ca2.jpg", size_bytes=_IMG, parent_id="R"),
    ]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="p0_card",
        is_folder_publish=True,
        folder_name="Test",
        dir_tree={"R": "Test"},
        album_mode=True,
        send_delay_seconds=0,
    )
    assert res.published_count == 2
    texts = [c.args[1] for c in pik_env.client.send_message.await_args_list]
    assert any("📷 **相册**: 2 张" in t for t in texts), f"批次卡缺相册形态行: {texts}"


# ---------------------------------------------------------------------------
# 发布修复批次 包4: 防重命中聚合 (诊断视图防重表数据源)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dedup_hits_aggregated_to_result(pik_env):
    """包4 (J7): 防重命中聚合到 PublishResult.dedup_skipped (真门面); 计数语义不变。"""
    from app.db.publish_repository import PublishHistoryRepository

    PublishHistoryRepository.record(
        TARGET, "pikpak://R/DD1.mp4", 1024, "", source_type="pikpak", remote_file_id="DD1.mp4"
    )
    PublishHistoryRepository.record(
        TARGET, "pikpak://R/DD2.mp4", 1024, "", source_type="pikpak", remote_file_id="DD2.mp4"
    )
    raw = [
        _make_item(file_id="DD1.mp4", name="DD1.mp4", size_bytes=1024, parent_id="R"),
        _make_item(file_id="DD2.mp4", name="DD2.mp4", size_bytes=1024, parent_id="R"),
        _make_item(file_id="FRESH1", name="fresh.mp4", size_bytes=1024, parent_id="R"),
    ]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="p4_dedup",
        send_delay_seconds=0,
    )
    assert res.published_count == 3, "防重命中计入 published 的既有语义不变"
    assert res.newly_published_count == 1
    assert len(res.dedup_skipped) == 2
    assert {d["file_name"] for d in res.dedup_skipped} == {"DD1.mp4", "DD2.mp4"}
    assert all(d["size_bytes"] == 1024 for d in res.dedup_skipped)


# ---------------------------------------------------------------------------
# 收尾批次 包1: reject/audit 拦截同口径进 failed_details (诊断失败表数据源)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filter_reject_and_audit_enter_failed_details(pik_env):
    """包1 (K1/K2): reject (零字节) 与 audit 拦截同口径产出结构化明细 (file_name/reason/
    client_label), 诊断失败表行数与终态失败计数闭环; b.failed 合并文案形态不变 (K2 冻结面)。"""
    raw = [
        {"file_id": "F_ZERO", "name": "zero.jpg", "size_bytes": 0, "parent_id": "R", "audit": {"status": "STATUS_OK"}},
        {
            "file_id": "F_AUDIT",
            "name": "bad.mp4",
            "size_bytes": 1024,
            "parent_id": "R",
            "audit": {"status": "RISK", "message": "存在违规风险"},
        },
        _make_image("IMG_OK.jpg", parent_id="R"),
    ]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="polish_f1",
        album_mode=False,
        send_delay_seconds=0,
    )
    assert res.published_count == 1
    assert len(res.failed_details) == 2
    by_name = {d["file_name"]: d for d in res.failed_details}
    assert by_name["zero.jpg"]["reason"] == "零字节文件, 跳过上传"
    assert by_name["bad.mp4"]["reason"] == "审核状态未通过 (存在违规风险)"
    assert by_name["zero.jpg"]["client_label"] == by_name["bad.mp4"]["client_label"]
    # K2: b.failed 合并文案形态不变 (终态公式/notifier 消费面冻结)
    assert "zero.jpg: 零字节文件, 跳过上传" in res.failed_files
    assert "bad.mp4: 审核状态未通过 (存在违规风险)" in res.failed_files


@pytest.mark.asyncio
async def test_reject_reason_with_colon_in_name_not_mis_split(pik_env):
    """包1 (K1): 文件名含 ": " 时全前缀匹配剥离不误切 (朴素 split(": ") 会切错)。"""
    raw = [
        {"file_id": "F_COLON", "name": "a: b.jpg", "size_bytes": 0, "parent_id": "R", "audit": {"status": "STATUS_OK"}},
    ]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="polish_f2",
        album_mode=False,
        send_delay_seconds=0,
    )
    assert res.published_count == 0
    assert len(res.failed_details) == 1
    assert res.failed_details[0]["file_name"] == "a: b.jpg"
    assert res.failed_details[0]["reason"] == "零字节文件, 跳过上传"
    assert res.failed_files == ["a: b.jpg: 零字节文件, 跳过上传"]


# ---------------------------------------------------------------------------
# 收尾批次 包4: pikpak 相册组失败归因口径统一 (user_detail 回落 str(exc))
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_album_group_failure_reason_uses_user_detail(pik_env):
    """包4 (K4): 相册组失败三处 (任务日志/b.failed/明细) 同口径 getattr(user_detail) or
    str(exc) —— taxonomy 异常呈现 user_detail 文案。口径钉死用例 (taxonomy str==user_detail
    恒成立, 防未来 __str__/包装策略漂移)。"""
    from app.core.pikpak.taxonomy import FAILURE_DETAIL_TEXT, PikPakPublishRetryableError
    from app.db.database import get_db

    TaskQueueRepository.create("polish_k4", "pikpak_publish", {"target_channel": TARGET})
    pik_env.client.send_file = AsyncMock(side_effect=PikPakPublishRetryableError("link_expired"))
    raw = [_make_image("IMGK41.jpg", parent_id="R"), _make_image("IMGK42.jpg", parent_id="R")]
    res = await PikPakPublisherService.publish_pikpak_files(
        remote_files=raw,
        target_chat_id=TARGET,
        task_id="polish_k4",
        album_mode=True,
        send_delay_seconds=0,
    )
    expected = FAILURE_DETAIL_TEXT["link_expired"]
    assert res.published_count == 0
    assert len(res.failed_files) == 2
    assert len(res.failed_details) == 2
    assert all(d["reason"] == expected for d in res.failed_details)
    assert all(f.endswith(f": {expected}") for f in res.failed_files)
    rows = (
        get_db()
        .execute(
            "SELECT COUNT(*) AS c FROM publish_history WHERE source_type='pikpak' "
            "AND remote_file_id IN ('IMGK41.jpg','IMGK42.jpg')"
        )
        .fetchone()
    )
    assert rows["c"] == 0, "整组失败零入库语义不变"
    logs, _, _ = TaskQueueRepository.get_task_logs("polish_k4")
    assert any(expected in entry["message"] for entry in logs), "任务日志同口径 (user_detail 文案)"


# ---------------------------------------------------------------------------
# 全链路自然语义排序与流媒体播放属性对齐测试
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pikpak_natural_sort_alignment():
    """验证 PikPak 展开器与快照数据源在输入乱序时，能严格按自然语义排序输出。"""
    from app.core.pikpak.expander import ParsedFileItem, expand_remote_files
    from app.core.pipeline.models import PublishContext
    from app.core.pipeline.sources import PikPakSnapshotSource

    # 1. 测试 expand_remote_files 展开乱序子目录与文件
    # 模拟根目录下包含乱序子目录: Season 10, Season 2, Season 1
    # 每个子目录内包含乱序文件: 06.mp4, 01.mp4, 10.mp4, 02.mp4
    folder_files = {
        "D_S10": [
            ParsedFileItem("F_10_02", "10_02.mp4", False, 100, {}, None, None, None, None, "D_S10"),
            ParsedFileItem("F_10_01", "10_01.mp4", False, 100, {}, None, None, None, None, "D_S10"),
        ],
        "D_S2": [
            ParsedFileItem("F_2_06", "06.mp4", False, 100, {}, None, None, None, None, "D_S2"),
            ParsedFileItem("F_2_01", "01.mp4", False, 100, {}, None, None, None, None, "D_S2"),
            ParsedFileItem("F_2_10", "10.mp4", False, 100, {}, None, None, None, None, "D_S2"),
            ParsedFileItem("F_2_02", "02.mp4", False, 100, {}, None, None, None, None, "D_S2"),
        ],
        "D_S1": [
            ParsedFileItem("F_1_02", "02.mp4", False, 100, {}, None, None, None, None, "D_S1"),
            ParsedFileItem("F_1_01", "01.mp4", False, 100, {}, None, None, None, None, "D_S1"),
        ],
    }

    async def fake_list_raw(folder_id, page_token=None):
        return folder_files.get(folder_id, []), None

    raw_dirs = [
        ParsedFileItem("D_S10", "Season 10", True, 0, {}, None, None, None, None, "ROOT"),
        ParsedFileItem("D_S2", "Season 2", True, 0, {}, None, None, None, None, "ROOT"),
        ParsedFileItem("D_S1", "Season 1", True, 0, {}, None, None, None, None, "ROOT"),
    ]

    expanded, dir_tree = await expand_remote_files(raw_dirs, list_files_raw=fake_list_raw)
    # 展开列表应先序遍历: Season 1 -> Season 2 -> Season 10
    names = [f.name for f in expanded]
    expected_names = [
        "01.mp4",
        "02.mp4",  # Season 1
        "01.mp4",
        "02.mp4",
        "06.mp4",
        "10.mp4",  # Season 2
        "10_01.mp4",
        "10_02.mp4",  # Season 10
    ]
    assert names == expected_names

    # 2. 测试 PikPakSnapshotSource 批次与组内条目自然语义排序
    src = PikPakSnapshotSource(remote_files=expanded)
    ctx = PublishContext(
        task_id="t_sort_align",
        target_chat_id=-1,
        is_folder_publish=True,
        folder_name="Show",
        dir_tree=dir_tree,
    )
    batches = await src.get_batches(ctx)
    assert len(batches) == 3
    # 批次顺序必须是 Season 1 < Season 2 < Season 10
    batch_rel_paths = [b.rel_path for b in batches]
    assert batch_rel_paths == ["Season 1", "Season 2", "Season 10"]
    # 验证 Season 2 批次内文件自然排序: 01 < 02 < 06 < 10
    s2_batch = batches[1]
    assert [itm.name for itm in s2_batch.items] == ["01.mp4", "02.mp4", "06.mp4", "10.mp4"]


@pytest.mark.asyncio
async def test_pikpak_stream_transport_engine_streaming_attribute_injection(pik_env):
    """验证 PikPakStreamTransportEngine 在 duration_s=None 时正确注入 DocumentAttributeVideo 及 supports_streaming=True。"""
    from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeVideo

    from app.core.pipeline.engines import PikPakStreamTransportEngine

    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn.fake/test.mp4"]))

    # 1. 单卷视频文件，duration_s 为 None, width/height 为 None
    video_item = PublishItem(
        item_id="F_VID_NO_DURATION",
        name="02.mp4",
        size_bytes=10 * 1024 * 1024,
        source_type="pikpak",
        duration_s=None,
        width=None,
        height=None,
    )
    ctx = PublishContext(task_id="t_stream_attr", target_chat_id=-100123)

    pik_env.client.send_file.reset_mock()
    outcome = await engine.send_item(video_item, ctx)
    assert outcome.message_ids == [4321]

    send_call = pik_env.client.send_file.call_args[1]
    assert send_call["supports_streaming"] is True
    assert "attributes" in send_call
    video_attrs = [a for a in send_call["attributes"] if isinstance(a, DocumentAttributeVideo)]
    assert len(video_attrs) == 1
    assert video_attrs[0].duration == 0
    assert video_attrs[0].w == 0
    assert video_attrs[0].h == 0
    assert video_attrs[0].supports_streaming is True

    # 2. 单卷非视频普通文件 (如 zip 包)，不应注入视频属性且 supports_streaming 为 False
    zip_item = PublishItem(
        item_id="F_ZIP",
        name="data.zip",
        size_bytes=5 * 1024 * 1024,
        source_type="pikpak",
    )
    pik_env.client.send_file.reset_mock()
    await engine.send_item(zip_item, ctx)
    zip_send_call = pik_env.client.send_file.call_args[1]
    assert zip_send_call["supports_streaming"] is False
    assert not any(isinstance(a, DocumentAttributeVideo) for a in zip_send_call.get("attributes", []))

    # 3. 单卷音频文件，注入 DocumentAttributeAudio
    audio_item = PublishItem(
        item_id="F_AUDIO",
        name="song.mp3",
        size_bytes=3 * 1024 * 1024,
        source_type="pikpak",
    )
    pik_env.client.send_file.reset_mock()
    await engine.send_item(audio_item, ctx)
    audio_send_call = pik_env.client.send_file.call_args[1]
    assert audio_send_call["supports_streaming"] is False
    audio_attrs = [a for a in audio_send_call.get("attributes", []) if isinstance(a, DocumentAttributeAudio)]
    assert len(audio_attrs) == 1
    assert audio_attrs[0].title == "song"


@pytest.mark.asyncio
async def test_pikpak_stream_transport_engine_audio_not_misattributed_as_video(pik_env):
    """P0: 音频扩展名 + duration_s>0 不得注入 DocumentAttributeVideo, 音频属性须携带真实时长"""
    from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeVideo

    from app.core.pipeline.engines import PikPakStreamTransportEngine

    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn.fake/song.flac"]))
    ctx = PublishContext(task_id="t_audio_attr", target_chat_id=-100123)

    # 1. 音频 + 媒体时长: 走音频属性, duration 为真实值, supports_streaming 为 False
    audio_item = PublishItem(
        item_id="F_AUDIO_DUR", name="song.flac", size_bytes=8 * 1024 * 1024, source_type="pikpak", duration_s=187
    )
    pik_env.client.send_file.reset_mock()
    outcome = await engine.send_item(audio_item, ctx)
    assert outcome.message_ids == [4321]
    send_call = pik_env.client.send_file.call_args[1]
    assert send_call["supports_streaming"] is False
    assert not any(isinstance(a, DocumentAttributeVideo) for a in send_call.get("attributes", []))
    audio_attrs = [a for a in send_call["attributes"] if isinstance(a, DocumentAttributeAudio)]
    assert len(audio_attrs) == 1
    assert audio_attrs[0].duration == 187

    # 2. 回归锁: 视频扩展名 + duration_s>0 仍注入 DocumentAttributeVideo 且携带时长
    video_item = PublishItem(
        item_id="F_VID_DUR", name="clip.mp4", size_bytes=10 * 1024 * 1024, source_type="pikpak", duration_s=120
    )
    pik_env.client.send_file.reset_mock()
    await engine.send_item(video_item, ctx)
    v_call = pik_env.client.send_file.call_args[1]
    assert v_call["supports_streaming"] is True
    video_attrs = [a for a in v_call["attributes"] if isinstance(a, DocumentAttributeVideo)]
    assert len(video_attrs) == 1
    assert video_attrs[0].duration == 120


@pytest.mark.asyncio
async def test_expander_counts_distinct_file_ids_toward_budget() -> None:
    """目录展开与重复叶子重叠时，仅未保留的唯一 file_id 消耗预算."""
    from app.core.pikpak.expander import ParsedFileItem, expand_remote_files

    def _parsed_item(fid: str, name: str, *, is_dir: bool = False) -> ParsedFileItem:
        return ParsedFileItem(
            file_id=fid,
            name=name,
            is_dir=is_dir,
            size_bytes=0 if is_dir else 1024,
            audit={},
            thumbnail_link=None,
            duration_s=None,
            width=None,
            height=None,
            parent_id="",
        )

    leaves = [_parsed_item(f"L{i}", f"a_leaf_{i}.mp4") for i in range(3)]
    folder = _parsed_item("DIR1", "z_folder", is_dir=True)
    pages_called: list[str | None] = []

    async def paged_list(folder_id: str, page_token=None):
        pages_called.append(page_token)
        if page_token is None:
            return list(leaves), "page2"
        return [_parsed_item(f"N{i}", f"new_{i}.mp4") for i in range(3)], None

    trunc_out: dict = {}
    out_files, _ = await expand_remote_files(
        [*leaves, folder],
        list_files_raw=paged_list,
        max_files=6,
        truncation_out=trunc_out,
    )
    assert pages_called == [None, "page2"]
    assert len(out_files) == 6
    assert {f.file_id for f in out_files} == {"L0", "L1", "L2", "N0", "N1", "N2"}
    assert trunc_out == {}


@pytest.mark.asyncio
async def test_pikpak_source_missing_expander_error_mentions_bound_list_files_raw() -> None:
    """缺少 expander 注入时的报错必须提示绑定 list_files_raw."""
    from app.core.pipeline.sources import PikPakSnapshotSource

    src = PikPakSnapshotSource(remote_files=[{"file_id": "D1", "name": "dir", "is_dir": True}])
    ctx = PublishContext(task_id="t_exp_err", target_chat_id=-1001, source_type="pikpak")
    with pytest.raises(ValueError, match="list_files_raw"):
        await src.get_batches(ctx)


@pytest.mark.asyncio
async def test_prune_empty_descendant_dirs_scopes_to_batch_and_evicts_deleted() -> None:
    """空目录清理仅扫描本批受影响祖先目录，且删后从 context.dir_tree 移除."""
    from app.core.pipeline import cleanup as cleanup_mod

    ctx = PublishContext(
        task_id="t_prune",
        target_chat_id=-10096,
        dir_tree={"d_root": "root", "d_sub1": "root/sub1", "d_sub2": "root/sub2"},
    )
    batch1_items = [
        PublishItem(
            item_id="f1",
            name="a.mp4",
            size_bytes=10,
            parent_folder_id="d_sub1",
            rel_path="root/sub1/a.mp4",
            source_type="pikpak",
        )
    ]
    listed: list[str] = []

    async def fake_list(fid: str, page_token=None):
        listed.append(fid)
        return [], None

    m_del = AsyncMock()
    pruned, _ = await cleanup_mod.prune_empty_descendant_dirs(
        ctx,
        list_files_raw=fake_list,
        delete_files=m_del,
        affected_items=batch1_items,
    )
    assert pruned == 1
    assert listed == ["d_sub1"]
    m_del.assert_awaited_once_with(["d_sub1"])
    assert "d_sub1" not in ctx.dir_tree
    assert "d_sub2" in ctx.dir_tree


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["archive", "delete"])
async def test_final_empty_cleanup_sweeps_deferred_directories(mode):
    from unittest.mock import patch

    from app.core.pipeline import cleanup as cleanup_mod

    ctx = PublishContext(task_id="task", target_chat_id=-1001, auto_cleanup_mode=mode)
    if mode == "archive":
        strategy = cleanup_mod.PikPakStructuredArchiveStrategy(
            archive_files=AsyncMock(),
            resolve_archive_folder=AsyncMock(),
            archive_files_to_folder=AsyncMock(),
            list_files_raw=AsyncMock(),
            delete_files=AsyncMock(),
        )
    else:
        strategy = cleanup_mod.PikPakDeleteCleanupStrategy(delete_files=AsyncMock(), list_files_raw=AsyncMock())
    with patch.object(cleanup_mod, "prune_empty_descendant_dirs", AsyncMock(return_value=(1, []))) as prune:
        await strategy.cleanup([], ctx)
    assert prune.await_args.kwargs["affected_items"] is None


@pytest.mark.asyncio
async def test_expander_truncates_leaf_and_paged_dirs_with_truncation_out():
    """叶子路径与分页目录路径均执行 max_files 截断并填充 truncation_out."""
    from app.core.pikpak.expander import ParsedFileItem, expand_remote_files

    def _parsed(fid: str, name: str, *, is_dir: bool = False, size: int = 100) -> ParsedFileItem:
        return ParsedFileItem(
            file_id=fid,
            name=name,
            is_dir=is_dir,
            size_bytes=size,
            audit={},
            thumbnail_link=None,
            duration_s=None,
            width=None,
            height=None,
            parent_id="",
        )

    leaf_items = [_parsed(f"L{i}", f"f{i}.mp4") for i in range(5)]
    trunc_leaf: dict = {}
    out_leaf, _ = await expand_remote_files(
        leaf_items,
        list_files_raw=AsyncMock(),
        max_files=3,
        truncation_out=trunc_leaf,
    )
    assert len(out_leaf) == 3
    assert trunc_leaf.get("truncated") is True

    pages_called: list[str | None] = []

    async def paged_list(folder_id: str, page_token=None):
        pages_called.append(page_token)
        if page_token is None:
            return [_parsed("A1", "a1.mp4"), _parsed("A2", "a2.mp4"), _parsed("A3", "a3.mp4")], "p2"
        return [_parsed("A4", "a4.mp4")], None

    trunc_dir: dict = {}
    out_dir, _ = await expand_remote_files(
        [_parsed("D1", "folder", is_dir=True)],
        list_files_raw=paged_list,
        max_files=3,
        truncation_out=trunc_dir,
    )
    assert len(out_dir) == 3
    assert pages_called == [None], "达到预算后应立即停止后续分页"
    assert trunc_dir.get("truncated") is True


@pytest.mark.asyncio
async def test_archive_single_item_rejects_zero_moved():
    """moved=0 不可记为已即时归档."""
    strategy = PikPakStructuredArchiveStrategy(
        resolve_archive_folder=AsyncMock(return_value="root_id"),
        archive_files_to_folder=AsyncMock(return_value={"moved": 0}),
        archive_files=AsyncMock(return_value={"moved": 0}),
    )
    ctx = PublishContext(task_id="t_arch0", target_chat_id=-1001, source_type="pikpak", dir_tree={"p1": "sub"})
    ok = await strategy.archive_single_item(
        PublishItem(item_id="f1", name="a.mp4", size_bytes=10, parent_folder_id="p1"), ctx
    )
    assert ok is False
    assert "f1" not in strategy._archived_item_ids


@pytest.mark.asyncio
@pytest.mark.parametrize("already_truncated", [False, True])
async def test_flat_pikpak_snapshot_5001_leaves_capped_preserving_order(already_truncated):
    """5001 个纯叶子文件在 PikPakSnapshotSource 与 runner 均按原序截断至 5000."""
    from unittest.mock import patch

    from app.core.pipeline.sources import PikPakSnapshotSource
    from app.services import publish_runner

    flat_items = [
        {"file_id": f"fid_{i:04d}", "name": f"z_{5001 - i:04d}.mp4", "is_dir": False, "size_bytes": 100}
        for i in range(5001)
    ]
    ctx = PublishContext(
        task_id="t_flat_5001",
        target_chat_id=-1001,
        source_type="pikpak",
        expansion_truncated=already_truncated,
        expansion_limit=5000 if already_truncated else 0,
    )
    src = PikPakSnapshotSource(remote_files=flat_items)
    with patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock) as m_log:
        batches = await src.get_batches(ctx)

    assert len(batches) == 1
    assert len(batches[0].items) == 5000
    kept_ids = [it.item_id for it in batches[0].items]
    # 必须先按输入顺序截断保留前 5000 项 (fid_0000..fid_4999), 再对组内按自然文件名排序 (z_0002 -> z_5001)
    assert kept_ids == [f"fid_{i:04d}" for i in range(4999, -1, -1)]
    assert "fid_5000" not in kept_ids
    assert ctx.expansion_truncated is True
    assert ctx.expansion_limit == 5000
    assert m_log.await_count == (0 if already_truncated else 1)

    TaskQueueRepository.create("t_flat_runner", "pikpak_publish", {})
    trunc_runner: dict = {"truncated": True, "max_files": 5000} if already_truncated else {}
    with (
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock) as runner_log,
        patch.object(TaskQueueRepository, "merge_params", wraps=TaskQueueRepository.merge_params) as merge_params,
    ):
        expanded, _ = await publish_runner._expand_pikpak_remote_files(
            "t_flat_runner",
            flat_items,
            {},
            truncation_out=trunc_runner,
        )
    assert len(expanded) == 5000
    assert expanded[0].file_id == "fid_0000"
    assert expanded[-1].file_id == "fid_4999"
    assert trunc_runner.get("truncated") is True
    assert trunc_runner.get("max_files") == 5000

    assert runner_log.await_count == (0 if already_truncated else 1)
    saved = merge_params.call_args.args[1]
    assert [item["file_id"] for item in saved["remote_files"]] == [item["file_id"] for item in flat_items[:5000]]
    assert saved["expansion_truncated"] is True
    assert saved["expansion_limit"] == 5000

    reloaded_cfg = await publish_runner._resolve_pikpak_run_config(
        "t_flat_runner",
        account_id=None,
        upload_concurrency=None,
        upload_in_flight_per_connection=None,
        prefetch_concurrency=None,
        auto_cleanup_mode="archive",
        oversize_policy="split",
        split_strategy=None,
        send_placeholder_card=None,
        force_republish=False,
        is_folder_publish=False,
        folder_name=None,
        album_mode=False,
        dir_tree=None,
    )
    assert reloaded_cfg["expansion_truncated"] is True
    assert reloaded_cfg["expansion_limit"] == 5000
    reloaded_trunc: dict = (
        {"truncated": True, "max_files": int(reloaded_cfg["expansion_limit"] or 5000)}
        if reloaded_cfg["expansion_truncated"]
        else {}
    )
    with patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock) as retry_log:
        retried, _ = await publish_runner._expand_pikpak_remote_files(
            "t_flat_runner",
            saved["remote_files"],
            {},
            truncation_out=reloaded_trunc,
        )
    assert retried == expanded
    retry_log.assert_not_awaited()


@pytest.mark.asyncio
async def test_pikpak_snapshot_source_dedups_before_5000_cap():
    """PikPakSnapshotSource 平铺载荷必须先过滤/去重再套用 5000 上限, 避免末尾唯一文件被误截."""
    from unittest.mock import patch

    from app.core.pipeline.sources import PikPakSnapshotSource

    raw_files = [{"file_id": f"f{i % 4400}", "name": f"f{i % 4400}.mp4", "is_dir": False} for i in range(5050)]
    raw_files.append({"file_id": "", "name": "empty.mp4", "is_dir": False})
    raw_files.append({"file_id": "tail_unique", "name": "tail_unique.mp4", "is_dir": False})
    ctx = PublishContext(task_id="t_src_dedup", target_chat_id=-1001, source_type="pikpak")

    source = PikPakSnapshotSource(remote_files=raw_files)
    with (
        patch("app.core.pipeline.sources.notify_expansion_truncation", new_callable=AsyncMock) as m_notify,
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock),
    ):
        batches = await source.get_batches(ctx)

    all_ids = [it.item_id for b in batches for it in b.items]
    assert "tail_unique" in all_ids
    assert len(all_ids) == 4401
    assert ctx.expansion_truncated is False
    m_notify.assert_not_awaited()

    many_unique = [{"file_id": f"u{i}", "name": f"u{i}.mp4", "is_dir": False} for i in range(5010)]
    ctx2 = PublishContext(task_id="t_src_cap", target_chat_id=-1001, source_type="pikpak")
    source2 = PikPakSnapshotSource(remote_files=many_unique)
    with (
        patch("app.core.pipeline.sources.notify_expansion_truncation", new_callable=AsyncMock) as m_notify2,
        patch.object(TaskQueueRepository, "append_log_async", new_callable=AsyncMock),
    ):
        batches2 = await source2.get_batches(ctx2)

    all_ids2 = [it.item_id for b in batches2 for it in b.items]
    assert len(all_ids2) == 5000
    assert ctx2.expansion_truncated is True
    assert ctx2.expansion_limit == 5000
    m_notify2.assert_awaited_once()
