"""E2E 捕获模式基建单测: tee 定位写/属性透传/降级语义 + manifest + 源快照 + fMP4 sink。

对应 docs/testing/E2E_GUIDE.md §5.1 D1-D9 与勘误 E1-E9:
- D3/E4: tee 定位写幂等 (重复读覆盖)、构造时属性值拷贝、read_chunk 语义透传- D7: 写盘/快照失败降级不抛 (有日志有 manifest 标记)- D6/E9: manifest 原子写 + 每 item 目录 + 逐卷 upsert- E2: fMP4 sink 乱序定位写 + 每 attempt truncate 重建 + discard 清残留- D7 例外: 目录不可创建整体 skipped; 捕获关闭 inactive 透传 + 目录零创建。
"""

import hashlib
import json
import logging
import os
from unittest.mock import MagicMock

import pytest

import app.core.e2e_capture as e2e_capture_mod
from app.config import settings
from app.core.e2e_capture import (
    E2ECaptureSession,
    TeeChunkProvider,
    _CaptureFileWriter,
    serialize_plan,
)
from app.core.stream_chunk_provider import BaseChunkProvider
from app.core.vsse_provider import VsseVolumePlan


class MemoryProvider(BaseChunkProvider):
    """内存确定性字节 provider (尾块短读语义对齐 LocalFileChunkProvider)。"""

    def __init__(self, data: bytes, file_id: str = "fid", file_name: str = "f.bin"):
        self._data = data
        self.file_id = file_id
        self.file_name = file_name
        self.total_bytes = len(data)
        self.closed = False

    async def read_chunk(self, offset: int, size: int) -> bytes:
        if offset >= self.total_bytes:
            raise ValueError(f"读取偏移越界: offset={offset} >= total_bytes={self.total_bytes}")
        if size <= 0:
            raise ValueError(f"非法读取长度: size={size}")
        return self._data[offset : offset + size]

    async def aclose(self) -> None:
        self.closed = True


class ExplodingProvider(MemoryProvider):
    """读即炸 provider: 模拟源快照中途网络失败。"""

    async def read_chunk(self, offset: int, size: int) -> bytes:
        raise OSError(f"simulated transient failure at offset={offset}")


def _make_plan(part_index: int = 1, o_min: int = 100, payload_len: int = 900, header_len: int = 64) -> VsseVolumePlan:
    return VsseVolumePlan(
        part_index=part_index,
        total_parts=2,
        part_name=f"video.part{part_index:03d}.mp4",
        v_start=1,
        v_end=10,
        a_start=None,
        a_end=None,
        o_min=o_min,
        o_max=o_min + payload_len,
        payload_len=payload_len,
        estimated_header_size=header_len,
        estimated_total_bytes=header_len + payload_len,
        start_sec=0.0,
        end_sec=4.5,
        duration_sec=4.5,
    )


@pytest.fixture
def capture_on(tmp_dir):
    """打开捕获模式 (conftest 已把 E2E_CAPTURE_DIR 沙箱化到 tmp_dir)。"""
    settings.E2E_CAPTURE_MODE = True
    yield
    settings.E2E_CAPTURE_MODE = False


def _open_session(**kwargs) -> E2ECaptureSession:
    kwargs.setdefault("target_chat_id", -1009000000010)
    kwargs.setdefault("split_strategy", "vsse_with_fallback")
    kwargs.setdefault("threshold_bytes", 2044723200)
    task_id = kwargs.pop("task_id", "task1")
    item_id = kwargs.pop("item_id", "item1")
    return E2ECaptureSession.open(task_id, item_id, **kwargs)


def _manifest_on_disk(session: E2ECaptureSession) -> dict:
    with open(os.path.join(session._root, "manifest.json"), encoding="utf-8") as f:
        return json.load(f)


# ---- serialize_plan (E6) ----------------------------------------------------


def test_serialize_plan_includes_property_aliases():
    plan = _make_plan(o_min=512, payload_len=4096)
    data = serialize_plan(plan)
    # dataclass 字段经 asdict 序列化
    assert data["part_name"] == plan.part_name
    assert data["duration_sec"] == 4.5
    # property 别名显式补充 (asdict 取不到)
    assert data["envelope_start"] == 512
    assert data["envelope_end"] == 512 + 4096
    assert data["envelope_length"] == 4096


# ---- TeeChunkProvider -----------------------------------------------


async def test_tee_positional_write_out_of_order_and_idempotent(capture_on):
    data = os.urandom(256 * 1024)
    inner = MemoryProvider(data)
    writer = _CaptureFileWriter(os.path.join(settings.E2E_CAPTURE_DIR, "w1", "parts", "p.mp4.part"))
    tee = TeeChunkProvider(inner, writer)

    # 乱序: 先后半再前半 (fMP4 语义同证; VSSE 预取也可能乱序)
    await tee.read_chunk(128 * 1024, 128 * 1024)
    await tee.read_chunk(0, 128 * 1024)
    # 重复读 (重试/force_fresh 场景) + 越尾短读 (尾块语义)
    await tee.read_chunk(1000, 2000)
    await tee.read_chunk(0, 512 * 1024)
    writer.close()

    with open(writer.path, "rb") as f:
        captured = f.read()
    assert captured == data
    assert tee.bytes_written == len(data)


async def test_tee_attribute_passthrough(capture_on):
    inner = MemoryProvider(b"abcdef", file_id="pk-1", file_name="movie.mkv")
    writer = _CaptureFileWriter(os.path.join(settings.E2E_CAPTURE_DIR, "w2", "p.part"))
    tee = TeeChunkProvider(inner, writer)
    # E4: 构造时值拷贝 (_SpeedMeasuringProvider 先例)
    assert tee.total_bytes == 6
    assert tee.file_id == "pk-1"
    assert tee.file_name == "movie.mkv"


async def test_tee_read_chunk_semantics_passthrough(capture_on):
    inner = MemoryProvider(b"abcdef")
    writer = _CaptureFileWriter(os.path.join(settings.E2E_CAPTURE_DIR, "w3", "p.part"))
    tee = TeeChunkProvider(inner, writer)
    with pytest.raises(ValueError):
        await tee.read_chunk(6, 4)  # offset >= total_bytes 透传
    with pytest.raises(ValueError):
        await tee.read_chunk(0, 0)  # size <= 0 透传
    assert await tee.read_chunk(4, 10) == b"ef"  # 尾块短读透传


async def test_tee_write_failure_warns_not_raises(capture_on, caplog, tmp_dir):
    blocker = os.path.join(tmp_dir, "blocker_file")
    with open(blocker, "wb") as f:
        f.write(b"x")
    # 父路径是普通文件 → makedirs 抛 OSError → 降级停写
    writer = _CaptureFileWriter(os.path.join(blocker, "sub", "p.part"))
    tee = TeeChunkProvider(MemoryProvider(b"abcdef"), writer)
    with caplog.at_level(logging.WARNING):
        data = await tee.read_chunk(0, 6)
    assert data == b"abcdef"  # 读路径不受影响
    assert writer.broken
    assert any("E2E" in r.message for r in caplog.records)


# ---- 会话 manifest ---------------------------------------------------


def test_session_creates_per_item_manifest(capture_on):
    session = _open_session()
    assert session.active
    m = _manifest_on_disk(session)
    assert m["schema_version"] == 1
    assert m["task_id"] == "task1"
    assert m["item_id"] == "item1"
    assert m["force_fresh"] is True  # D8
    assert m["rolled_back"] is False
    assert m["source"]["status"] == "pending"
    assert m["parts"] == []


async def test_vsse_flow_record_part_sent_renames_and_manifest(capture_on):
    session = _open_session()
    plan = _make_plan()
    header_len = plan.estimated_header_size
    payload = os.urandom(plan.payload_len)
    inner = MemoryProvider(payload, file_name=plan.part_name)

    tee = session.make_vsse_tee(plan, inner, header_len=header_len)
    assert isinstance(tee, TeeChunkProvider)
    await tee.read_chunk(0, header_len + plan.payload_len)

    session.record_part_sent(plan.part_name, msg_id=42)

    final_path = os.path.join(session._root, "parts", plan.part_name)
    assert not os.path.exists(final_path + ".part")
    with open(final_path, "rb") as f:
        assert f.read() == payload

    entry = _manifest_on_disk(session)["parts"][0]
    assert entry["part_name"] == plan.part_name
    assert entry["channel"] == "vsse"
    assert entry["status"] == "sent"
    assert entry["msg_id"] == 42
    assert entry["path"] == "parts/" + plan.part_name
    assert entry["bytes_written"] == plan.payload_len
    assert entry["header_len"] == header_len
    assert entry["envelope_start"] == plan.o_min
    assert entry["envelope_length"] == plan.payload_len


async def test_record_part_failed_keeps_part_residue(capture_on):
    session = _open_session()
    plan = _make_plan()
    tee = session.make_vsse_tee(plan, MemoryProvider(os.urandom(64)), header_len=8)
    await tee.read_chunk(0, 64)
    session.record_part_failed(plan.part_name, "send_file failed after 5 retries")

    tmp_path = os.path.join(session._root, "parts", plan.part_name + ".part")
    assert os.path.isfile(tmp_path)  # D3: 残留半成品不被误当有效产物
    entry = _manifest_on_disk(session)["parts"][0]
    assert entry["status"] == "failed"
    assert "send_file failed" in entry["error"]


async def test_record_part_upsert_idempotent(capture_on):
    session = _open_session()
    plan = _make_plan()
    session.make_vsse_tee(plan, MemoryProvider(b"x"), header_len=1)
    session.record_part_sent(plan.part_name, msg_id=1)
    session.record_part_failed(plan.part_name, "second attempt failed")
    session.record_part_sent(plan.part_name, msg_id=9)

    parts = _manifest_on_disk(session)["parts"]
    assert len(parts) == 1  # 同 part_name upsert, 不重复追加
    assert parts[0]["msg_id"] == 9
    assert parts[0]["status"] == "sent"


# ---- sent_attributes 快照 (A5, 属性统一计划包1) ------------------------------


async def test_record_part_sent_writes_sent_attributes_snapshot(capture_on):
    session = _open_session()
    plan = _make_plan()
    session.make_vsse_tee(plan, MemoryProvider(b"x"), header_len=1)

    session.record_part_sent(
        plan.part_name,
        msg_id=11,
        sent_attributes={"duration": 4, "w": 1920, "h": 1080, "supports_streaming": True, "actual_dur": 4.4},
    )

    entry = _manifest_on_disk(session)["parts"][0]
    assert entry["sent_attributes"]["duration"] == 4
    assert entry["sent_attributes"]["w"] == 1920
    assert entry["sent_attributes"]["supports_streaming"] is True
    assert entry["sent_attributes"]["actual_dur"] == 4.4  # fMP4 附加原始实测值


async def test_record_part_sent_without_sent_attributes_omits_key(capture_on):
    # 旧调用面兼容: 不传 sent_attributes 时 manifest 不写键 (A5 对存量退化 SKIP)
    session = _open_session()
    plan = _make_plan()
    session.make_vsse_tee(plan, MemoryProvider(b"x"), header_len=1)
    session.record_part_sent(plan.part_name, msg_id=12)

    entry = _manifest_on_disk(session)["parts"][0]
    assert "sent_attributes" not in entry


# ---- fMP4 sink (E1/E2) -------------------------------------------------------


async def test_fmp4_sink_out_of_order_positional_write(capture_on):
    session = _open_session()
    sink = session.make_fmp4_sink("video.part001.mp4", part_size=4)
    assert callable(sink)
    # 乱序到达: part 0 (sidx patch 后) 与尾块最后入队; 尾块短于 part_size
    sink(2, 3, b"CC")
    sink(1, -1, b"BBBB")
    sink(0, -1, b"AAAA")
    session.record_part_sent("video.part001.mp4", msg_id=7, channel="fmp4")

    with open(os.path.join(session._root, "parts", "video.part001.mp4"), "rb") as f:
        assert f.read() == b"AAAABBBBCC"


async def test_fmp4_sink_truncates_on_new_attempt(capture_on):
    session = _open_session()
    sink1 = session.make_fmp4_sink("video.part001.mp4", part_size=512)
    sink1(0, -1, b"A" * 512)
    sink1(1, -1, b"B" * 512)
    session.discard_part_sink("video.part001.mp4")

    # 新 attempt 重新调工厂 → truncate 重建, 无上次残留 (E2)
    sink2 = session.make_fmp4_sink("video.part001.mp4", part_size=512)
    sink2(0, -1, b"X")
    session.record_part_sent("video.part001.mp4", msg_id=8, channel="fmp4")

    with open(os.path.join(session._root, "parts", "video.part001.mp4"), "rb") as f:
        assert f.read() == b"X"


async def test_discard_part_sink_unlinks_residue(capture_on):
    session = _open_session()
    sink = session.make_fmp4_sink("p.mp4", part_size=512)
    sink(0, -1, b"hi")
    tmp_path = os.path.join(session._root, "parts", "p.mp4.part")
    assert os.path.isfile(tmp_path)
    session.discard_part_sink("p.mp4")
    assert not os.path.exists(tmp_path)


# ---- 源快照 ----------------------------------------------------------


async def test_snapshot_source_ok(capture_on, monkeypatch):
    monkeypatch.setattr(e2e_capture_mod, "SNAPSHOT_CHUNK_SIZE", 64)  # 小块跨多次读
    data = os.urandom(200)
    provider = MemoryProvider(data)
    session = _open_session()

    await session.snapshot_source(lambda: provider, "movie.mkv", len(data))

    src_path = os.path.join(session._root, "source", "movie.mkv")
    with open(src_path, "rb") as f:
        assert f.read() == data
    src = _manifest_on_disk(session)["source"]
    assert src["status"] == "ok"
    assert src["sha256"] == hashlib.sha256(data).hexdigest()
    assert src["path"] == "source/movie.mkv"
    assert provider.closed  # 快照完独立 provider 即关


async def test_snapshot_source_failed_degrades(capture_on, caplog):
    provider = ExplodingProvider(b"whatever")
    session = _open_session()

    with caplog.at_level(logging.WARNING):
        await session.snapshot_source(lambda: provider, "movie.mkv", 1024)  # 不抛

    src = _manifest_on_disk(session)["source"]
    assert src["status"] == "failed"
    assert "simulated transient failure" in src["error"]
    assert provider.closed  # 失败路径也关 provider
    assert any("源快照失败" in r.message for r in caplog.records)


async def test_snapshot_source_idempotent_on_retry(capture_on):
    provider = MemoryProvider(b"hello")
    session = _open_session()
    await session.snapshot_source(lambda: provider, "movie.mkv", 5)
    first = _manifest_on_disk(session)["source"]["sha256"]
    await session.snapshot_source(lambda: MemoryProvider(b"hello"), "movie.mkv", 5)
    assert _manifest_on_disk(session)["source"]["sha256"] == first


# ---- 任务级标记 (E3/熔断降级) --------------------------------------------------


async def test_mark_rolled_back_channel_switch_discarded(capture_on):
    session = _open_session()
    plan = _make_plan()
    tee = session.make_vsse_tee(plan, MemoryProvider(b"x"), header_len=1)
    await tee.read_chunk(0, 1)
    session.record_part_sent(plan.part_name, msg_id=5)

    session.mark_parts_discarded("replan rollback")
    assert _manifest_on_disk(session)["parts"][0]["status"] == "discarded"

    session.record_channel_switch("vsse circuit breaker -> fmp4")
    assert "fmp4" in _manifest_on_disk(session)["channel_switch"]

    session.mark_rolled_back()
    assert _manifest_on_disk(session)["rolled_back"] is True


# ---- inactive / 整体 skipped ---------------------------------------------


async def test_inactive_session_passthrough_and_zero_dirs():
    settings.E2E_CAPTURE_MODE = False
    session = E2ECaptureSession.open("taskX", "itemX")
    assert not session.active

    plan = _make_plan()
    inner = MemoryProvider(b"abc")
    assert session.make_vsse_tee(plan, inner, header_len=4) is inner  # 原样透传
    assert session.make_fmp4_sink("p.mp4", 512) is None

    # 全部记录方法安全 no-op
    await session.snapshot_source(lambda: MemoryProvider(b"ab"), "n", 2)
    session.record_part_sent("p", msg_id=1)
    session.record_part_failed("p", "e")
    session.discard_part_sink("p")
    session.mark_rolled_back()
    session.record_channel_switch("x")

    # 目录零创建 (回归保障: capture 关不产生任何磁盘痕迹)
    assert os.path.isdir(settings.E2E_CAPTURE_DIR)
    assert os.listdir(settings.E2E_CAPTURE_DIR) == []


def test_directory_uncreatable_session_skips(capture_on, tmp_dir, caplog):
    blocker = os.path.join(tmp_dir, "capture_blocker")
    with open(blocker, "wb") as f:
        f.write(b"x")
    settings.E2E_CAPTURE_DIR = os.path.join(blocker, "e2e_capture")

    with caplog.at_level(logging.WARNING):
        session = E2ECaptureSession.open("task1", "item1")

    assert not session.active
    assert any("skipped" in r.message for r in caplog.records)
    settings.E2E_CAPTURE_DIR = os.path.join(tmp_dir, "e2e_capture")


def test_capture_writer_marks_broken_on_close_error():
    """验证 E2E _CaptureFileWriter 在 close 发生 OSError 时标 broken=True"""
    writer = _CaptureFileWriter("dummy.part")
    mock_fh = MagicMock()
    mock_fh.close.side_effect = OSError("Disk flush error")
    writer._fh = mock_fh

    writer.close()
    assert writer.broken is True


async def test_fail_pending_parts_closes_unmanifested_fmp4_writers(capture_on):
    """#1: make_fmp4_sink 尚未落入 manifest 的 writer 在通道中止时也必须关闭并记录 failed。"""
    session = _open_session()
    sink = session.make_fmp4_sink("fmp4_abort.mp4", part_size=8)
    assert callable(sink)
    sink(0, -1, b"1234")
    writer = session._writers["fmp4_abort.mp4"]

    session.fail_pending_parts("fmp4 channel aborted")

    assert writer._fh is None
    assert "fmp4_abort.mp4" not in session._writers
    parts = _manifest_on_disk(session)["parts"]
    assert len(parts) == 1
    assert parts[0]["part_name"] == "fmp4_abort.mp4"
    assert parts[0]["channel"] == "fmp4"
    assert parts[0]["status"] == "failed"
    assert parts[0]["bytes_written"] == 4
    assert "fmp4 channel aborted" in parts[0]["error"]
