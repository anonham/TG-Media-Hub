"""切片双通道真链路 (E2E-LIVE-07 ~ 12, 17; 承 E2E-VSSE-01/02/05,
E2E-FMP4-01/06, E2E-CFG-10 与 E2E-FLOW-01 客观半边)。

前置 (F10 缺失即 skip, 不算失败):
- PikPak logged_in (scratch 克隆库内含真实登录态);
- E2E_LIVE_PIKPAK_REMOTE_DIR 指向网盘测试目录, 内置附录 B 小样本:
  e2e_F1s.mp4 / e2e_F2s.mp4 / e2e_F3s.mkv / e2e_F6s.mp4 (约 12.5 MiB, 制备命令见
  tests/e2e_live/README.md)。harness 以低阈值注入启动 (,
  推荐 E2E_LIVE_MAX_FILE_SIZE_MB=8 / E2E_LIVE_PREMIUM_MAX_FILE_SIZE_MB=12, 默认 64 / 128):
  12.5 MiB 样本按 ~7.8 MiB 切分线稳定切 2 卷 (fMP4 AAC 膨胀可触发动态重规划 2→3 卷, 断言容忍)。

执行期实证勘误 (对 步骤文本, 见附录 A 谱系):
- 分卷 caption 为 "[i/n] <文件名>" (app/core/stream_splitter.format_part_caption),
  非计划书所写 "Part x/y" 标题;
- split 任务 processed_items/total_items 为文件级计数 (engines 分卷进度钩子按
  total_files_count 计), 单文件切 2 卷终值 1/1; "2" 体现在频道分卷消息数与
  分卷成功日志条数;
- raw 通道多分卷消息为裸 document 形态 (build_media_attributes eligible=False,
  无视频属性), VSSE/fMP4 分卷为可播视频形态 (supports_streaming=True)。

E2E-LIVE-17 需要 E2E_LIVE_CAPTURE=1 启动本套件 (服务器子进程注入
E2E_CAPTURE_MODE=true), 磁盘成本 ≈3× 源体积 (E1 源快照 + E2 分卷产物 + E3 镜像)。
"""

import ctypes
import ctypes.wintypes
import json
import re
import shutil
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import pytest

from e2e_live.harness import api_client, env_gate, pikpak_probe
from e2e_live.harness import tg_reader as tg_reader_mod
from e2e_live.harness.tg_reader import is_document_message, is_video_message

pytestmark = [pytest.mark.e2e_live]

# 附录 B 小样本文件名 (网盘测试目录内; 制备命令见 tests/e2e_live/README.md)
F1S = "e2e_F1s.mp4"
F2S = "e2e_F2s.mp4"
F3S = "e2e_F3s.mkv"
F6S = "e2e_F6s.mp4"

_FFMPEG_CAP = 2  # 全局 FFmpeg 并发上限 (E2E-FMP4-06, 承 fMP4 引擎模块级信号量)


@pytest.fixture(scope="module")
def split_stage(live_gate, live_api) -> dict[str, Any]:
    """PikPak 切片族前置: logged_in → 远端测试目录 → 样本索引, 缺项逐级 skip。

    远端样本由使用者经 PikPak 客户端预置并保持原状 (auto_cleanup_mode=none,
    网盘文件零改动); 本夹具只做只读列举建立 file_id 索引。
    """
    state = pikpak_probe.auth_state(live_api)
    if state != "logged_in":
        pytest.skip(f"PikPak 未就绪 (state={state}); 切片族真链路需要真实网盘源")
    remote_dir = env_gate.pikpak_remote_dir()
    if not remote_dir:
        pytest.skip(
            "未配置 E2E_LIVE_PIKPAK_REMOTE_DIR (PikPak 远端测试目录); 样本制备与上传指引见 tests/e2e_live/README.md"
        )
    folder_id = pikpak_probe.resolve_remote_folder(live_api, remote_dir)
    if folder_id is None:
        pytest.skip(f"PikPak 远端测试目录不存在: {remote_dir}")
    files = {i.get("name"): i for i in pikpak_probe.list_remote_page(live_api, folder_id) if not i.get("is_dir")}
    return {"folder_id": folder_id, "files": files, "remote_dir": remote_dir}


def _pick(split_stage: dict[str, Any], name: str) -> dict[str, Any]:
    item = split_stage["files"].get(name)
    if item is None:
        pytest.skip(f"PikPak 测试目录缺样本 {name} (制备命令见 tests/e2e_live/README.md)")
    return item


def _submit_split_publish(
    live_api: api_client.LiveApiClient,
    split_stage: dict[str, Any],
    item: dict[str, Any],
    strategy: str,
    target_chat: str,
    *,
    force_republish: bool = False,
) -> str:
    resp = live_api.post(
        "/api/pikpak/publish",
        json={
            "remote_files": [
                {
                    "file_id": item["file_id"],
                    "name": item["name"],
                    "is_dir": False,
                    "size_bytes": item.get("size_bytes") or 0,
                    "parent_id": item.get("parent_id") or split_stage["folder_id"],
                }
            ],
            "target_channel": target_chat,
            "auto_cleanup_mode": "none",
            "oversize_policy": "split",
            "split_strategy": strategy,
            # 同会话跨用例重发同一样本必须绕过 publish_history 防重, 否则后续用例
            # 全部防重命中 (频道零消息, 断言级联失败)
            "force_republish": force_republish,
            # 上传并发注入: 默认 2×1 在低速网络下 ~0.2 MB/s, 是 live 链路绝对瓶颈;
            # 8×2 (应用契约 1-16 × 1-4 内) 预期数倍提速
            "upload_concurrency": 8,
            "upload_in_flight_per_connection": 2,
            "send_delay_seconds": 0.5,
            "send_placeholder_card": False,
        },
    )
    assert resp.status_code == 200, resp.text
    return str(resp.json()["task_id"])


_TH32CS_SNAPPROCESS = 0x00000002
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value


class _PROCESSENTRY32W(ctypes.Structure):
    _fields_ = [
        ("dwSize", ctypes.wintypes.DWORD),
        ("cntUsage", ctypes.wintypes.DWORD),
        ("th32ProcessID", ctypes.wintypes.DWORD),
        ("th32DefaultHeapID", ctypes.c_size_t),
        ("th32ModuleID", ctypes.wintypes.DWORD),
        ("cntThreads", ctypes.wintypes.DWORD),
        ("th32ParentProcessID", ctypes.wintypes.DWORD),
        ("pcPriClassBase", ctypes.wintypes.LONG),
        ("dwFlags", ctypes.wintypes.DWORD),
        ("szExeFile", ctypes.wintypes.WCHAR * 260),
    ]


def _is_descendant(pid: int, root_pid: int, parent_of: dict[int, int]) -> bool:
    """判断 pid 是否位于 root_pid 进程树下 (带环路保护)。"""
    cur = pid
    seen: set[int] = set()
    while cur and cur not in seen:
        if cur == root_pid:
            return True
        seen.add(cur)
        cur = parent_of.get(cur, 0)
    return False


def _ffmpeg_pids_posix(root_pid: int | None = None) -> set[int]:
    """POSIX ps -eo pid=,ppid=,comm= 采样 ffmpeg PID 集合; 失败抛 RuntimeError 不静默归零。"""
    proc = subprocess.run(["ps", "-eo", "pid=,ppid=,comm="], capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"ps 采样 ffmpeg 进程失败 (rc={proc.returncode}): {proc.stderr.strip()}")
    parent_of: dict[int, int] = {}
    ffmpeg: set[int] = set()
    for raw in proc.stdout.splitlines():
        parts = raw.strip().split(None, 2)
        if len(parts) != 3:
            continue
        try:
            pid, ppid = int(parts[0]), int(parts[1])
        except ValueError:
            continue
        parent_of[pid] = ppid
        exe = Path(parts[2].strip()).name.lower()
        if exe in {"ffmpeg", "ffmpeg.exe"} or exe.startswith("ffmpeg"):
            ffmpeg.add(pid)
    if root_pid is None:
        return ffmpeg
    return {pid for pid in ffmpeg if _is_descendant(pid, root_pid, parent_of)}


def _ffmpeg_pids(root_pid: int | None = None) -> set[int]:
    """跨平台采样运行中的 ffmpeg 进程 PID 集合 (Windows Toolhelp32 / POSIX ps);
    传入 root_pid 时仅统计该进程树子代。"""
    if sys.platform != "win32":
        return _ffmpeg_pids_posix(root_pid)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_snapshot = kernel32.CreateToolhelp32Snapshot
    create_snapshot.argtypes = [ctypes.wintypes.DWORD, ctypes.wintypes.DWORD]
    create_snapshot.restype = ctypes.wintypes.HANDLE
    first_proc = kernel32.Process32FirstW
    first_proc.argtypes = [ctypes.wintypes.HANDLE, ctypes.POINTER(_PROCESSENTRY32W)]
    first_proc.restype = ctypes.wintypes.BOOL
    next_proc = kernel32.Process32NextW
    next_proc.argtypes = [ctypes.wintypes.HANDLE, ctypes.POINTER(_PROCESSENTRY32W)]
    next_proc.restype = ctypes.wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.wintypes.HANDLE]
    close_handle.restype = ctypes.wintypes.BOOL

    snap = create_snapshot(_TH32CS_SNAPPROCESS, 0)
    if not snap or snap == _INVALID_HANDLE_VALUE:
        return set()
    parent_of: dict[int, int] = {}
    ffmpeg: set[int] = set()
    try:
        entry = _PROCESSENTRY32W()
        entry.dwSize = ctypes.sizeof(_PROCESSENTRY32W)
        ok = first_proc(snap, ctypes.byref(entry))
        while ok:
            pid = int(entry.th32ProcessID)
            parent_of[pid] = int(entry.th32ParentProcessID)
            exe = str(entry.szExeFile).lower()
            if exe in {"ffmpeg.exe", "ffmpeg"} or exe.startswith("ffmpeg"):
                ffmpeg.add(pid)
            ok = next_proc(snap, ctypes.byref(entry))
    finally:
        close_handle(snap)
    if root_pid is None:
        return ffmpeg
    return {pid for pid in ffmpeg if _is_descendant(pid, root_pid, parent_of)}


def _server_root_pid(live_server: Any) -> int | None:
    """自起服务器返回进程 PID (Scoped 采样); attach 外部服务器返回 None。"""
    proc = getattr(live_server, "proc", None)
    return int(proc.pid) if proc is not None else None


class _TransportSampler:
    """传输期后台采样器: ffmpeg 并发进程数 + scratch downloads 新增字节峰值。

    - `max_ffmpeg`: 自基线快照后新诞生的 ffmpeg PID 并发峰值 (按 PID 集合做差,
      基线旧进程退出不会掩盖新起进程; 用于 VSSE 零转码 / raw 零进程断言);
    - `max_ffmpeg_absolute`: 被测服务进程树下 (自起服务器按 root_pid 隔离, attach
      形态全主机) 绝对并发 ffmpeg PID 峰值 (不减去基线, 用于 fMP4 全局并发上限断言);
    - `max_downloads_bytes`: 相对基线文件快照的新增/增长字节峰值 (基线旧文件删除
      不会抵消新落盘字节, 用于零盘承诺断言)。
    """

    def __init__(self, downloads_dir: Path, *, root_pid: int | None = None, interval: float = 0.5) -> None:
        self._downloads = downloads_dir
        self._root_pid = root_pid
        self._interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None
        self._baseline_files = pikpak_probe.downloads_snapshot(downloads_dir)
        self._baseline_pids = _ffmpeg_pids(root_pid)
        self.max_ffmpeg = 0
        self.max_ffmpeg_absolute = 0
        self.max_downloads_bytes = 0
        self.samples = 0

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="e2e-live-transport-sampler", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                cur_pids = _ffmpeg_pids(self._root_pid)
                self.max_ffmpeg = max(self.max_ffmpeg, len(cur_pids - self._baseline_pids))
                self.max_ffmpeg_absolute = max(self.max_ffmpeg_absolute, len(cur_pids))
                self.max_downloads_bytes = max(
                    self.max_downloads_bytes,
                    pikpak_probe.downloads_added_bytes(self._downloads, self._baseline_files),
                )
                self.samples += 1
                self._stop.wait(self._interval)
        except BaseException as exc:
            self._error = exc

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                raise TimeoutError("Transport sampler did not stop within 10 seconds")
            self._thread = None
        if self._error is not None:
            raise self._error


def _submit_and_sample(
    live_api: api_client.LiveApiClient,
    live_server: Any,
    split_stage: dict[str, Any],
    item: dict[str, Any],
    strategy: str,
    target_chat: str,
    live_cleanup: Any,
    downloads_dir: Path,
    *,
    force_republish: bool = False,
    timeout: float = 3600.0,
) -> tuple[str, dict[str, Any], _TransportSampler]:
    """提交前捕获基线并启动采样器, 再提交任务并轮询终态 (避免任务提交后早期进程混入基线)。

    3600s 默认预算: 96 MiB × 2 卷在低速网络 (~0.2 MB/s) 下单卷即需 ~16 分钟,
    1800s 会对正常传输误报超时。
    """
    sampler = _TransportSampler(downloads_dir, root_pid=_server_root_pid(live_server))
    sampler.start()
    try:
        task_id = _submit_split_publish(
            live_api, split_stage, item, strategy, target_chat, force_republish=force_republish
        )
        live_cleanup.register_task(task_id)
        final = live_api.wait_terminal(task_id, timeout=timeout)
    finally:
        sampler.stop()
    return task_id, final, sampler


def _logs_text(live_api: api_client.LiveApiClient, task_id: str) -> str:
    return pikpak_probe.logs_text(live_api, task_id)


def _assert_volume_messages(
    msgs: list[Any], sample_name: str, *, expect_video: bool, exact_count: int | None = 2
) -> None:
    """频道分卷形态断言: 分卷为 "[i/n] 文件名" caption 的消息, n 内部一致。

    raw 通道多分卷无视频属性 (裸 document); VSSE/fMP4 分卷带视频属性与
    supports_streaming=True (可播视频形态)。
    exact_count=None 时容忍 fMP4 动态重规划 (PartOversizeError → 2→3 卷), 只要求
    n >= 2 且 caption 集合与实际卷数自洽。
    """
    videos = [m for m in msgs if is_video_message(m)]
    documents = [m for m in msgs if is_document_message(m)]
    pool = videos if expect_video else documents
    n = len(pool)
    if exact_count is not None:
        assert n == exact_count, (
            f"应恰 {exact_count} 条分卷消息 ({'视频' if expect_video else '裸 document'} 形态), 实得 {n}; "
            f"captions={[(m.text or '')[:60] for m in msgs]}"
        )
    else:
        assert n >= 2, (
            f"应 ≥2 条分卷消息 ({'视频' if expect_video else '裸 document'} 形态), 实得 {n}; "
            f"captions={[(m.text or '')[:60] for m in msgs]}"
        )
    got = {(m.text or "").strip() for m in pool}
    expected = {f"[{i}/{n}] {sample_name}" for i in range(1, n + 1)}
    assert got == expected, f"分卷 caption 应为 '[i/n] 文件名' (n={n}): 期望 {expected}, 实得 {got}"
    if expect_video:
        assert not documents or all((m.text or "").strip() not in expected for m in documents), (
            f"分卷不应以裸 document 形态出现: {[(m.text or '')[:60] for m in documents]}"
        )


async def _resolve_chat_or_skip(tg_reader: tg_reader_mod.TgReader, target_chat: str) -> Any:
    """频道实体解析; 不可解析 (多账号不可见) 转 skip。"""
    try:
        return await tg_reader.resolve_chat(target_chat)
    except tg_reader_mod.ChatUnresolvableError as exc:
        pytest.skip(f"无 userbot 会话可解析测试频道 {target_chat}: {exc}")


def _register_messages(live_cleanup: Any, target_chat: str, msgs: list[Any]) -> None:
    for msg in msgs:
        live_cleanup.register_message(target_chat, int(msg.id))


async def test_vsse_faststart_split_publish(
    split_stage, live_api, live_server, tg_reader, target_chat, live_cleanup, scratch_data_dir
) -> None:
    """E2E-LIVE-07 (承 E2E-VSSE-01): VSSE faststart 主通道真发布 (F1s → 2 分卷)。

    断言: 2 条视频形态分卷 ([i/n] caption) + 零转码 (仅封面抽帧新起 ffmpeg ≤ 2) +
    downloads 零新增 (零盘承诺) + VSSE 成功日志 (Part 1/2, 2/2) + 无熔断降级 +
    文件级条目计数 (勘误: 1/1, 分卷数体现在频道消息与日志)。
    """
    item = _pick(split_stage, F1S)
    chat = await _resolve_chat_or_skip(tg_reader, target_chat)
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)

    task_id, final, sampler = _submit_and_sample(
        live_api,
        live_server,
        split_stage,
        item,
        "vsse_with_fallback",
        target_chat,
        live_cleanup,
        scratch_data_dir / "downloads",
        force_republish=True,
    )
    pikpak_probe.completed_or_env_skip(live_api, final)
    progress = final["progress"]
    assert progress["processed_items"] == progress["total_items"] == 1, progress
    assert sampler.max_downloads_bytes == 0, f"零盘承诺被破坏: downloads 峰值 {sampler.max_downloads_bytes} 字节"
    # 远端分卷的元数据探测与封面抽帧会短促 spawn 有界 ffmpeg (≤2):
    # "零 ffmpeg"契约收窄为"零转码"—— fMP4 转码通道峰值≥cap, VSSE 仅封面抽帧 ≤2
    assert sampler.max_ffmpeg <= 2, (
        f"VSSE ffmpeg 进程峰值 {sampler.max_ffmpeg} 超出封面抽帧上界 2 (采样 {sampler.samples} 次)"
    )
    logs = _logs_text(live_api, task_id)
    assert "VSSE 分卷发布成功: Part 1/2" in logs and "VSSE 分卷发布成功: Part 2/2" in logs, logs[-2000:]
    assert "⚡ [CIRCUIT BREAKER]" not in logs, "faststart 主样本不应触发熔断降级"
    msgs = await tg_reader.collect_messages_after(chat, mark)
    _assert_volume_messages(msgs, F1S, expect_video=True)
    _register_messages(live_cleanup, target_chat, msgs)


async def test_vsse_rear_moov_split_publish(
    split_stage, live_api, live_server, tg_reader, target_chat, live_cleanup, scratch_data_dir
) -> None:
    """E2E-LIVE-08 (承 E2E-VSSE-02): VSSE rear-moov 样本真发布 (F2s → 2 分卷)。

    同 LIVE-07 断言族; 额外断言无 preflight 改道日志 (moov 后置由 VSSE 主通道
    原生处理, 不降级 fMP4)。
    """
    item = _pick(split_stage, F2S)
    chat = await _resolve_chat_or_skip(tg_reader, target_chat)
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)

    task_id, final, sampler = _submit_and_sample(
        live_api,
        live_server,
        split_stage,
        item,
        "vsse_with_fallback",
        target_chat,
        live_cleanup,
        scratch_data_dir / "downloads",
        force_republish=True,
    )
    pikpak_probe.completed_or_env_skip(live_api, final)
    assert final["progress"]["processed_items"] == 1, final["progress"]
    assert sampler.max_ffmpeg <= 2 and sampler.max_downloads_bytes == 0, (
        f"VSSE ffmpeg 进程峰值 {sampler.max_ffmpeg} 超出封面抽帧上界 2 / downloads 峰值 {sampler.max_downloads_bytes}"
        f" (采样 {sampler.samples} 次)"
    )
    logs = _logs_text(live_api, task_id)
    assert "VSSE 分卷发布成功: Part 1/2" in logs and "VSSE 分卷发布成功: Part 2/2" in logs, logs[-2000:]
    assert "⚡ [CIRCUIT BREAKER]" not in logs, "rear-moov 样本不应触发熔断降级"
    assert "VSSE pre-flight 校验未通过" not in logs, "rear-moov 样本不应被 preflight 改道 fMP4"
    msgs = await tg_reader.collect_messages_after(chat, mark)
    _assert_volume_messages(msgs, F2S, expect_video=True)
    _register_messages(live_cleanup, target_chat, msgs)


async def test_vsse_hevc_whitelist_publish(
    split_stage, live_api, live_server, tg_reader, target_chat, live_cleanup, scratch_data_dir
) -> None:
    """E2E-LIVE-09 (承 E2E-VSSE-05): VSSE 白名单编码 (HEVC) 真发布 (F6s → 2 分卷)。

    断言 HEVC 走 VSSE 主通道不降级: 无熔断 / 无 preflight 改道 / 无 raw 安全降级。
    """
    item = _pick(split_stage, F6S)
    chat = await _resolve_chat_or_skip(tg_reader, target_chat)
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)

    task_id, final, sampler = _submit_and_sample(
        live_api,
        live_server,
        split_stage,
        item,
        "vsse_with_fallback",
        target_chat,
        live_cleanup,
        scratch_data_dir / "downloads",
        force_republish=True,
    )
    pikpak_probe.completed_or_env_skip(live_api, final)
    assert final["progress"]["processed_items"] == 1, final["progress"]
    assert sampler.max_ffmpeg <= 2 and sampler.max_downloads_bytes == 0, (
        f"VSSE ffmpeg 进程峰值 {sampler.max_ffmpeg} 超出封面抽帧上界 2 / downloads 峰值 {sampler.max_downloads_bytes}"
        f" (采样 {sampler.samples} 次)"
    )
    logs = _logs_text(live_api, task_id)
    assert "VSSE 分卷发布成功: Part 1/2" in logs and "VSSE 分卷发布成功: Part 2/2" in logs, logs[-2000:]
    assert "⚡ [CIRCUIT BREAKER]" not in logs, "HEVC 白名单编码不应触发熔断降级"
    assert "VSSE pre-flight 校验未通过" not in logs, "HEVC/AC3 以外的白名单组合不应被 preflight 改道"
    assert "安全降级至 raw" not in logs, "HEVC 不应触发 raw 物理分卷降级"
    msgs = await tg_reader.collect_messages_after(chat, mark)
    _assert_volume_messages(msgs, F6S, expect_video=True)
    _register_messages(live_cleanup, target_chat, msgs)


async def test_fmp4_mkv_ac3_publish(
    split_stage, live_api, live_server, tg_reader, target_chat, live_cleanup, scratch_data_dir
) -> None:
    """E2E-LIVE-10 (承 E2E-FMP4-01): fMP4 通道 MKV+AC3 真发布 (F3s → 2 分卷)。

    断言: 被测服务绝对 ffmpeg 并发峰值 ≤ 全局上限 (单任务 ≤2, 不减基线) +
    downloads 零新增 + 分卷为可播视频消息。
    """
    item = _pick(split_stage, F3S)
    chat = await _resolve_chat_or_skip(tg_reader, target_chat)
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)

    task_id, final, sampler = _submit_and_sample(
        live_api,
        live_server,
        split_stage,
        item,
        "fmp4_universal",
        target_chat,
        live_cleanup,
        scratch_data_dir / "downloads",
        force_republish=True,
    )
    pikpak_probe.completed_or_env_skip(live_api, final)
    assert final["progress"]["processed_items"] == 1, final["progress"]
    assert sampler.max_downloads_bytes == 0, f"零盘承诺被破坏: downloads 峰值 {sampler.max_downloads_bytes} 字节"
    assert sampler.max_ffmpeg_absolute <= _FFMPEG_CAP, (
        f"单任务绝对 ffmpeg 进程峰值 {sampler.max_ffmpeg_absolute} 超过全局上限 {_FFMPEG_CAP}"
    )
    logs = _logs_text(live_api, task_id)
    # "🎬 fMP4 切片完成: part=..." 与 "🚀 fMP4 分卷发布成功" 只写应用 logger, 从不进
    # 任务 DB 日志 —— 卷级证据以频道消息为准 (切片族用例在
    # 轮因 PikPak 样本未上传而 skip, 本断言从未被真链路执行过);
    # 动态重规划 (PartOversizeError → 2→3 卷) 属设计行为, 卷数容忍。
    assert "⚡ [CIRCUIT BREAKER]" not in logs, "fmp4_universal 直连路由不应出现 VSSE 熔断"
    msgs = await tg_reader.collect_messages_after(chat, mark)
    _assert_volume_messages(msgs, F3S, expect_video=True, exact_count=None)
    _register_messages(live_cleanup, target_chat, msgs)


async def test_raw_split_document_form(
    split_stage, live_api, live_server, tg_reader, target_chat, live_cleanup, scratch_data_dir
) -> None:
    """E2E-LIVE-11 (承 E2E-CFG-10): raw 策略裸分卷真发布 (F1s → 2 分卷)。

    断言: 分卷为裸 document 形态 (多分卷无视频属性) + 日志无 VSSE/fMP4 通道标记
    + 零 ffmpeg (raw 为物理字节切分, 无重封装)。
    """
    item = _pick(split_stage, F1S)
    chat = await _resolve_chat_or_skip(tg_reader, target_chat)
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)

    task_id, final, sampler = _submit_and_sample(
        live_api,
        live_server,
        split_stage,
        item,
        "raw",
        target_chat,
        live_cleanup,
        scratch_data_dir / "downloads",
        force_republish=True,
    )
    pikpak_probe.completed_or_env_skip(live_api, final)
    assert final["progress"]["processed_items"] == 1, final["progress"]
    assert sampler.max_ffmpeg == 0 and sampler.max_downloads_bytes == 0, (
        f"raw 通道应零 ffmpeg/零盘: ffmpeg 峰值 {sampler.max_ffmpeg}, downloads 峰值 {sampler.max_downloads_bytes}"
    )
    logs = _logs_text(live_api, task_id)
    assert "VSSE 分卷发布成功" not in logs, "raw 策略不应走 VSSE 通道"
    assert "fMP4 分卷发布成功" not in logs, "raw 策略不应走 fMP4 通道"
    assert "🎬 fMP4 切片完成" not in logs, "raw 策略不应产生 fMP4 封装日志"
    assert "⚡ [CIRCUIT BREAKER]" not in logs
    msgs = await tg_reader.collect_messages_after(chat, mark)
    _assert_volume_messages(msgs, F1S, expect_video=False)
    _register_messages(live_cleanup, target_chat, msgs)


async def test_ffmpeg_global_concurrency_cap(
    split_stage, live_api, live_server, tg_reader, target_chat, live_cleanup, scratch_data_dir
) -> None:
    """E2E-LIVE-12 (承 E2E-FMP4-06): 全局 FFmpeg 并发上限 — 3 个并发 fMP4 超限任务。

    断言: 3 任务全部完成 (第 3 个排队不报错) + 被测服务进程树内绝对 ffmpeg 并发峰值
    ≤ 2 (不减基线, 模块级信号量跨任务生效) + 频道分卷 ≥6 条 ("[i/n] 文件名" caption,
    容忍动态重规划)。进程峰值 ≥1 属采样窗口运气, 不作硬断言。
    """
    item = _pick(split_stage, F3S)
    chat = await _resolve_chat_or_skip(tg_reader, target_chat)
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)
    sampler = _TransportSampler(
        scratch_data_dir / "downloads",
        root_pid=_server_root_pid(live_server),
        interval=0.5,
    )
    sampler.start()
    task_ids: list[str] = []
    try:
        for _ in range(3):
            task_id = _submit_split_publish(
                live_api, split_stage, item, "fmp4_universal", target_chat, force_republish=True
            )
            live_cleanup.register_task(task_id)
            task_ids.append(task_id)
        for tid in task_ids:
            final = live_api.wait_terminal(tid, timeout=2400)
            pikpak_probe.completed_or_env_skip(live_api, final)
            assert final["progress"]["processed_items"] == 1, final["progress"]
    finally:
        sampler.stop()
    assert sampler.max_ffmpeg_absolute <= _FFMPEG_CAP, (
        f"并发 3 任务绝对 ffmpeg 进程峰值 {sampler.max_ffmpeg_absolute} 超过全局上限 {_FFMPEG_CAP} "
        f"(采样 {sampler.samples} 次)"
    )
    # 卷级证据以频道消息为准 ("🎬 fMP4 切片完成" 只写应用 logger, 见 LIVE-10); 3 任务
    # 各 ≥2 卷 (容忍动态重规划 2→3) → 频道 ≥6 条 "[i/n] e2e_F3s.mkv"
    msgs = await tg_reader.collect_messages_after(chat, mark)
    volumes = [
        m for m in msgs if is_video_message(m) and re.match(r"^\[\d+/\d+\] e2e_F3s\.mkv$", (m.text or "").strip())
    ]
    assert len(volumes) >= 6, (
        f"3 任务应有 ≥6 条 F3s 分卷消息 (容忍重规划), 实得 {len(volumes)}; "
        f"captions={[(m.text or '')[:40] for m in msgs]}"
    )
    for m in msgs:
        live_cleanup.register_message(target_chat, int(m.id))


def _copy_session_bundle(src: Path, dst: Path) -> None:
    """复制 Telethon 会话文件及伴生 journal/wal (mirror 子进程需要完整实体缓存)。"""
    for suffix in ("", "-wal", "-shm", "-journal"):
        source = Path(str(src) + suffix)
        target = Path(str(dst) + suffix)
        if target.exists():
            target.unlink()
        if source.exists():
            shutil.copy2(source, target)


async def test_capture_linkage_vsse_byte_fidelity(
    split_stage, live_api, live_server, tg_reader, target_chat, live_cleanup, scratch_data_dir
) -> None:
    """E2E-LIVE-17 (承 E2E-FLOW-01 客观半边 + E2E-CAP): 超限切片 + capture 取证联动。

    前置: E2E_LIVE_CAPTURE=1 启动套件 (服务器子进程注入 E2E_CAPTURE_MODE=true,
    磁盘成本 ≈3× 源体积)。断言: VSSE 发布完成 → E1 源快照/E2 分卷产物/E3 镜像
    三份证据齐备 → mirror 回下载退出码 0 → compare 退出码 0 且 A1 字节全等 /
    A2 区间完备性 / A4 尺寸双保险均有 PASS 且无 FAIL。
    """
    if not env_gate.capture_mode():
        pytest.skip(
            "E2E-LIVE-17 需要 E2E_LIVE_CAPTURE=1 启动 (服务器注入 E2E_CAPTURE_MODE=true); "
            "capture 磁盘成本 ≈3× 源体积, 见 tests/e2e_live/README.md"
        )
    item = _pick(split_stage, F1S)
    chat = await _resolve_chat_or_skip(tg_reader, target_chat)
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)

    task_id, final, _sampler = _submit_and_sample(
        live_api,
        live_server,
        split_stage,
        item,
        "vsse_with_fallback",
        target_chat,
        live_cleanup,
        scratch_data_dir / "downloads",
        force_republish=True,
    )
    pikpak_probe.completed_or_env_skip(live_api, final)

    capture_root = scratch_data_dir / "e2e_capture" / task_id
    manifests = sorted(capture_root.glob("*/manifest.json"))
    assert len(manifests) == 1, f"单文件任务应恰 1 个 item manifest, 实得 {manifests} (root={capture_root})"
    manifest = manifests[0]

    # E3 回下载: 显式 --manifest/--session/--api-* (settings 路径族 BASE_DIR 派生不读 env,
    # 子进程无法经环境重定向, 会话用 reader 副本束, 不触碰真实 data/)
    mirror_session = scratch_data_dir / "live_mirror.session"
    _copy_session_bundle(tg_reader.session_file, mirror_session)
    from app import config as app_config

    mirror = subprocess.run(
        [
            sys.executable,
            str(env_gate.REPO_ROOT / "scripts" / "e2e_tg_mirror.py"),
            "--manifest",
            str(manifest),
            "--session",
            str(mirror_session),
            "--api-id",
            str(app_config.settings.API_ID),
            "--api-hash",
            str(app_config.settings.API_HASH),
        ],
        cwd=str(env_gate.REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=1800,
        check=False,
    )
    assert mirror.returncode == 0, (
        f"mirror 回下载失败 (rc={mirror.returncode}):\n{mirror.stdout[-2000:]}\n{mirror.stderr[-2000:]}"
    )

    compare = subprocess.run(
        [sys.executable, str(env_gate.REPO_ROOT / "scripts" / "e2e_compare.py"), "--manifest", str(manifest)],
        cwd=str(env_gate.REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    assert compare.returncode == 0, f"compare 存在 FAIL 级断言 (rc={compare.returncode}):\n{compare.stdout[-2000:]}"

    report = json.loads((manifest.parent / "compare_report.json").read_text(encoding="utf-8"))
    assert report.get("ok") is True, report.get("results")
    passed = [r["name"] for r in report.get("results", []) if r.get("status") == "PASS"]
    assert any(n.startswith("A1") for n in passed), f"A1 字节全等无 PASS: {report.get('results')}"
    assert any(n.startswith("A2") for n in passed), f"A2 区间完备性无 PASS: {report.get('results')}"
    assert any(n.startswith("A4") for n in passed), f"A4 尺寸双保险无 PASS: {report.get('results')}"

    msgs = await tg_reader.collect_messages_after(chat, mark)
    _register_messages(live_cleanup, target_chat, msgs)
