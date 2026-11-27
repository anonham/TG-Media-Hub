"""VSSE Audio Sidecar Transcoder (Tier 2: HYBRID_AUDIO_SIDECAR).

利用 LocalPagedRangeProxy 与 FFmpeg 原生解复用能力，将非 AAC 音频
(例如 QuickTime lpcm / sowt / twos, ac-3, eac3, opus, flac) 或非交织 MP4 的音轨
一次性转码/重封装为内存标准 AAC MP4，并解析为可直接用于 VSSE 规划与合成的
SidecarAudioBundle。

核心架构机制:
1. 0 视频流量浪费:
   基于音视频 Chunk 交织拓扑动态生成有界 audio_spans，FFmpeg -vn 仅请求音频 Chunk，
   底层 LocalPagedRangeProxy 绝不拉取任何视频 Chunk 字节。
2. 0 手搓输入容器头:
   完全交由 FFmpeg 原生 libavformat/mov.c 处理复杂的 QuickTime 与非标音频扩展原子，
   免除手搓伪造容器头带来的脆弱性与协议解析风险。
3. 纯内存 / 短生命周期临时文件管理:
   生成的纯音频 MP4 (2 小时电影仅 ~11-14 MiB，远低于 2000 MiB (1.95 GiB) 与 4000 MiB (3.91 GiB) 阈值)
   在解析出 aac_track 及 mdat 净荷后，临时文件在 finally 块中 100% 强制删除回收。
"""

from __future__ import annotations

import array
import asyncio
import bisect
import logging
import os
import secrets
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.core.iso_bmff.box_parser import (
    AAC_ESDS_OBJECT_TYPES,
    Mp4Metadata,
    Track,
    parse_moov,
    scan_top_level_boxes,
)
from app.core.iso_bmff.exceptions import IsoBmffParseError
from app.core.stream_chunk_provider import (
    BaseChunkProvider,
    PikPakLinkExpiredError,
    PikPakNotFoundError,
    PikPakRiskControlError,
    PikPakThrottledError,
)
from app.core.stream_readahead_proxy import LocalPagedRangeProxy, build_pinned_regions, coalesce_byte_spans

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SidecarAudioBundle:
    """封装了混合元数据 (已替换为 AAC 音轨) 与纯 AAC mdat 净荷的侧车包。"""

    hybrid_metadata: Mp4Metadata
    aac_payload: bytes

    @property
    def aac_track(self) -> Track:
        t = self.hybrid_metadata.audio_track
        if t is None:
            raise ValueError("hybrid_metadata has no audio track")
        return t

    @property
    def video_track(self) -> Track | None:
        return self.hybrid_metadata.video_track

    @property
    def payload_size(self) -> int:
        return len(self.aac_payload)

    def __len__(self) -> int:
        return len(self.aac_payload)

    def slice_bytes(self, start: int | None = None, end: int | None = None) -> bytes:
        """安全提取指定区间的 AAC 净荷字节切片。当区间未指定 (start=end=None) 时返回空切片。"""
        if start is None and end is None:
            return b""
        s = 0 if start is None else max(0, start)
        e = len(self.aac_payload) if end is None else min(len(self.aac_payload), end)
        if s >= e:
            return b""
        return self.aac_payload[s:e]


def _append_chunk_spans(
    current_run: list[tuple[int, int]],
    start: int,
    end: int,
    max_span_size: int,
) -> None:
    """将单个音频 Chunk [start, end) 按 max_span_size 切分为连续不重叠片段加入 current_run。"""
    if max_span_size <= 0:
        raise ValueError(f"max_span_size must be positive, got {max_span_size}")
    piece_start = start
    while piece_start < end:
        piece_end = min(piece_start + max_span_size, end)
        current_run.append((piece_start, piece_end))
        piece_start = piece_end


def _compute_last_audio_nominal_end(a_track: Any) -> int | None:
    """计算音轨最后一个 Sample 的理论终点偏移; 对 QuickTime 小采样帧 PCM 返回 None。"""
    try:
        from app.core.iso_bmff.box_synthesizer import get_sample_byte_offset

        total_samples = len(a_track.stbl.stsz)
        if total_samples <= 0:
            return None
        u_size = getattr(a_track.stbl.stsz, "uniform_size", a_track.stbl.default_sample_size)
        entry_fmt = a_track.sample_entry.format if a_track.sample_entry else b""
        if 0 < u_size <= 4 and entry_fmt != b"mp4a":
            return None
        _, nominal_end = get_sample_byte_offset(a_track, total_samples)
        return nominal_end
    except Exception as exc:
        logger.debug("[SidecarAudio] Failed to compute last audio sample byte offset: %s", exc)
        return None


def build_audio_spans(
    metadata: Mp4Metadata,
    total_bytes: int,
    moov_offset: int | None = None,
    max_gap: int = 64 * 1024,
    max_span_size: int = 2 * 1024 * 1024,
) -> list[tuple[int, int]]:
    """根据音频轨与非音频轨的 chunk_offsets 交织拓扑，生成有界音频区间表 (audio_spans)。

    保证:
    1. 相邻连续音频 Chunk (中间无视频 Chunk) 自动合并，单区间上限 max_span_size。
    2. 音频区间严格在下一个视频 Chunk (或非音频 Chunk) 起始处截断，绝不跨越任何视频数据。
    3. 视频区间 100% 排除在外，实现 0 视频流量浪费。
    """
    if max_span_size <= 0:
        raise ValueError(f"max_span_size must be positive, got {max_span_size}")
    if max_gap < 0:
        raise ValueError(f"max_gap cannot be negative, got {max_gap}")
    a_track = metadata.audio_track
    if a_track is None or not a_track.stbl.chunk_offsets:
        return []

    audio_offsets = sorted(set(a_track.stbl.chunk_offsets))
    if not audio_offsets:
        return []

    all_offsets = sorted(set(off for t in metadata.tracks for off in t.stbl.chunk_offsets))
    other_offsets = sorted(set(off for t in metadata.tracks if t != a_track for off in t.stbl.chunk_offsets))

    # 确定文件尾边界或 mdat 终点
    file_limit = total_bytes
    if moov_offset is not None and moov_offset > 0 and max(audio_offsets) < moov_offset:
        file_limit = min(file_limit, moov_offset)

    last_audio_nominal_end = _compute_last_audio_nominal_end(a_track)

    runs: list[list[tuple[int, int]]] = []
    current_run: list[tuple[int, int]] = []

    for off in audio_offsets:
        # 寻找全片后续最近的任意 Chunk 起始 (确保当前音频 Chunk 上界不越界)
        idx = bisect.bisect_right(all_offsets, off)
        if idx < len(all_offsets):
            next_chunk = all_offsets[idx]
        elif last_audio_nominal_end is not None and last_audio_nominal_end > off:
            next_chunk = min(last_audio_nominal_end, file_limit)
        else:
            next_chunk = file_limit

        chunk_end = min(next_chunk, file_limit)
        if chunk_end <= off:
            continue

        _append_chunk_spans(current_run, off, chunk_end, max_span_size)

        # 检查紧随其后的是否为视频或非音频 Chunk
        v_idx = bisect.bisect_right(other_offsets, off)
        next_v = other_offsets[v_idx] if v_idx < len(other_offsets) else file_limit
        if next_v <= chunk_end:
            runs.append(current_run)
            current_run = []

    if current_run:
        runs.append(current_run)

    coalesced: list[tuple[int, int]] = []
    for run in runs:
        merged_run = coalesce_byte_spans(run, max_gap=max_gap, max_span_size=max_span_size)
        coalesced.extend(merged_run)

    return coalesced


def _choose_temp_dir(custom_dir: Path | str | None = None) -> Path:
    if custom_dir is not None:
        p = Path(custom_dir)
        p.mkdir(parents=True, exist_ok=True)
        return p
    # /dev/shm 为 Linux 内核内存盘, os.access(W_OK) 校验后使用, 否则回退 tempfile.gettempdir()
    shm_path = Path("/dev/shm")  # nosec B108
    if shm_path.is_dir() and os.access(shm_path, os.W_OK):
        return shm_path
    return Path(tempfile.gettempdir())


def _raise_if_prioritized_proxy_error(proxy: LocalPagedRangeProxy) -> None:
    provider_err = proxy.last_provider_error
    if isinstance(
        provider_err,
        (PikPakThrottledError, PikPakRiskControlError, PikPakNotFoundError, PikPakLinkExpiredError),
    ):
        raise provider_err


def _raise_sidecar_ffmpeg_failure(
    returncode: int | None,
    stderr_data: bytes,
    proxy: LocalPagedRangeProxy,
) -> None:
    _raise_if_prioritized_proxy_error(proxy)
    err_msg = stderr_data.decode("utf-8", errors="replace").strip()
    raise RuntimeError(f"FFmpeg audio sidecar extraction failed (exit code {returncode}): {err_msg}")


async def build_sidecar_audio_bundle(
    metadata: Mp4Metadata,
    provider: BaseChunkProvider,
    head_bytes: bytes,
    moov_offset: int,
    moov_bytes: bytes,
    *,
    temp_dir: Path | str | None = None,
    max_gap: int = 64 * 1024,
    max_span_size: int = 2 * 1024 * 1024,
    ffmpeg_bin: str = "ffmpeg",
    ffmpeg_timeout_s: float = 300.0,
    is_cancelled: Callable[[], bool] | None = None,
    is_paused: Callable[[], bool] | None = None,
    check_control_fn: Callable[[], None] | None = None,
    on_tick_fn: Callable[[], None] | None = None,
) -> SidecarAudioBundle:
    """利用 LocalPagedRangeProxy 与 FFmpeg 原生转码/重封装提取 AAC 侧车音频包。

    Args:
        metadata: 原片已解析的 Mp4Metadata
        provider: 底层云流式 ChunkProvider (例如 PikPakStreamChunkProvider)
        head_bytes: Stage 0 已下载的文件头前导字节 (通常为 128 KiB)
        moov_offset: moov Box 在原文件中的绝对字节偏移
        moov_bytes: Stage 0 已定位并下载的完整 moov Box 字节
        temp_dir: 可选的受管临时文件目录 (优先使用内存目录 /dev/shm)
        max_gap: 相邻连续音频 Chunk 最大合并间隙 (字节, 默认 64 KiB)
        max_span_size: 单个合并音频活跃区间上限 (字节, 默认 2 MiB)
        ffmpeg_bin: ffmpeg 可执行文件路径或名称
        ffmpeg_timeout_s: ffmpeg 音频转码超时时间 (秒)

    Returns:
        SidecarAudioBundle: 包含替换为 AAC 音轨的 hybrid_metadata 与连续 aac_payload
    """
    a_track = metadata.audio_track
    if a_track is None:
        raise ValueError("Mp4Metadata contains no audio track")
    if not a_track.stbl.chunk_offsets:
        raise ValueError("Audio track contains no chunks in chunk_offsets")

    audio_spans = build_audio_spans(
        metadata=metadata,
        total_bytes=provider.total_bytes,
        moov_offset=moov_offset,
        max_gap=max_gap,
        max_span_size=max_span_size,
    )
    if not audio_spans:
        raise ValueError("No valid audio spans could be generated for audio track")

    pinned_regions = build_pinned_regions(head_bytes, moov_offset, moov_bytes)

    # 判断是否可免重编码直接 copy (原片已为 AAC mp4a 且声道 <= 2)
    can_copy = False
    if a_track.sample_entry:
        entry = a_track.sample_entry
        is_aac = (
            entry.format == b"mp4a"
            and (entry.esds_object_type is None or entry.esds_object_type in AAC_ESDS_OBJECT_TYPES)
        ) or (entry.format != b"mp4a" and entry.codec.strip().lower() == "aac")
        if is_aac and entry.channel_count in (1, 2):
            can_copy = True

    if can_copy:
        codec_args = ["-c:a", "copy"]
    else:
        codec_args = ["-c:a", "aac", "-ac", "2", "-b:a", "128k"]

    out_dir = _choose_temp_dir(temp_dir)
    temp_file = out_dir / f"sidecar_audio_{secrets.token_hex(8)}.mp4"

    # 若底层为超大文件 PikPakStreamChunkProvider (默认 limit_per_host 被钳制为 2)，
    # 在旁路小块音频提取期间临时扩容连接池上限至 8，确保滑动窗口并发预读跑满多路 Keep-Alive 连接。
    inner_prov: Any = getattr(provider, "_inner", provider)
    prev_pool_limit = getattr(inner_prov, "_pool_limit_per_host", None)
    had_pool_attr = hasattr(inner_prov, "_pool_limit_per_host")
    patched_conn: Any = None
    orig_conn_limits: tuple[int, int | None] | None = None

    try:
        if had_pool_attr:
            inner_prov._pool_limit_per_host = max(8, prev_pool_limit or 0)
            old_sess = getattr(inner_prov, "_session", None)
            if old_sess is not None and not old_sess.closed:
                conn = getattr(old_sess, "connector", None)
                if conn is not None and hasattr(conn, "_limit_per_host") and conn._limit_per_host < 8:
                    patched_conn = conn
                    orig_limit = getattr(conn, "_limit", None)
                    orig_conn_limits = (conn._limit_per_host, orig_limit)
                    conn._limit_per_host = 8
                    if orig_limit is not None and orig_limit < 16:
                        conn._limit = 16

        async with LocalPagedRangeProxy(
            provider,
            pinned_regions=pinned_regions,
            audio_spans=audio_spans,
            zero_fill_unmapped=True,
            allowed_channels=("audio",),
        ) as proxy:
            cmd = [
                ffmpeg_bin,
                "-y",
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-reconnect",
                "1",
                "-reconnect_at_eof",
                "1",
                "-reconnect_on_network_error",
                "1",
                "-reconnect_streamed",
                "1",
                "-reconnect_delay_max",
                "5",
                "-vn",
                "-sn",
                "-dn",
                "-i",
                proxy.audio_url,
                "-map",
                "0:a:0",
                *codec_args,
                "-movflags",
                "+faststart",
                str(temp_file),
            ]
            logger.debug(
                "[SidecarAudio] Launching FFmpeg audio extraction: file_id=%s, copy=%s, spans=%d",
                provider.file_id,
                can_copy,
                len(audio_spans),
            )
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            has_control_hooks = any(fn is not None for fn in (check_control_fn, is_cancelled, is_paused, on_tick_fn))
            try:
                if not has_control_hooks:
                    _, stderr_data = await asyncio.wait_for(proc.communicate(), timeout=ffmpeg_timeout_s)
                else:
                    comm_task = asyncio.create_task(proc.communicate())
                    deadline = time.monotonic() + ffmpeg_timeout_s
                    last_tick_time = 0.0
                    try:
                        while not comm_task.done():
                            if check_control_fn is not None:
                                check_control_fn()
                            if is_cancelled is not None and is_cancelled():
                                raise asyncio.CancelledError("Sidecar audio extraction cancelled by caller")
                            if is_paused is not None and is_paused():
                                from app.core.scheduler import UploadPausedError

                                raise UploadPausedError("Sidecar audio extraction paused by caller")
                            now_mono = time.monotonic()
                            if on_tick_fn is not None and (now_mono - last_tick_time) >= 1.0:
                                last_tick_time = now_mono
                                try:
                                    on_tick_fn()
                                except Exception as tick_err:
                                    logger.debug("[SidecarAudio] Ignore on_tick_fn error: %s", tick_err)
                            rem = deadline - now_mono
                            if rem <= 0:
                                raise TimeoutError()
                            done, _ = await asyncio.wait({comm_task}, timeout=min(0.2, rem))
                            if done:
                                break
                        _, stderr_data = await comm_task
                    finally:
                        if not comm_task.done():
                            comm_task.cancel()
                            try:
                                await comm_task
                            except (asyncio.CancelledError, Exception) as comm_err:
                                logger.debug("[SidecarAudio] Ignore cancelled communicate task: %s", comm_err)
            except TimeoutError as exc:
                try:
                    proc.kill()
                    await proc.wait()
                except Exception as kill_err:
                    logger.debug("[SidecarAudio] Failed to kill timed-out ffmpeg process: %s", kill_err)
                raise TimeoutError(f"FFmpeg audio sidecar extraction timed out after {ffmpeg_timeout_s}s") from exc
            except BaseException:
                try:
                    proc.kill()
                    await proc.wait()
                except Exception as kill_err:
                    logger.debug("[SidecarAudio] Failed to kill interrupted ffmpeg process: %s", kill_err)
                raise

            _raise_if_prioritized_proxy_error(proxy)
            if proc.returncode != 0:
                _raise_sidecar_ffmpeg_failure(proc.returncode, stderr_data, proxy)

        # 解析产出的纯音频 MP4
        if not temp_file.exists() or temp_file.stat().st_size == 0:
            raise IsoBmffParseError(f"FFmpeg produced empty audio sidecar file: {temp_file}")

        file_bytes = temp_file.read_bytes()
        top_boxes = scan_top_level_boxes(file_bytes, len(file_bytes))
        moov_box = next((b for b in top_boxes if b.box_type == b"moov"), None)
        mdat_box = next((b for b in top_boxes if b.box_type == b"mdat"), None)
        if moov_box is None:
            raise IsoBmffParseError("FFmpeg sidecar output has no moov box")
        if mdat_box is None:
            raise IsoBmffParseError("FFmpeg sidecar output has no mdat box")

        aac_meta = parse_moov(file_bytes[moov_box.offset : moov_box.offset + moov_box.size])
        aac_track = aac_meta.audio_track
        if aac_track is None:
            raise IsoBmffParseError("FFmpeg sidecar output has no audio track")

        mdat_payload_offset = mdat_box.payload_offset
        mdat_payload_size = mdat_box.payload_size
        aac_payload = file_bytes[mdat_payload_offset : mdat_payload_offset + mdat_payload_size]
        del file_bytes

        # 将 aac_track 的 chunk_offsets 归一化为从 0 开始的相对偏移
        normalized_offsets = array.array("Q", [off - mdat_payload_offset for off in aac_track.stbl.chunk_offsets])
        aac_track.stbl.chunk_offsets = normalized_offsets

        # 同步原片音轨 track_id，避免与视频轨 track_id 冲突
        target_track_id = a_track.track_id
        aac_track.track_id = target_track_id
        aac_track.tkhd.track_id = target_track_id

        # 若 FFmpeg 生成的 movie timescale 与原片 mvhd.timescale 不一致，缩放 tkhd/elst 时长
        if (
            metadata.mvhd.timescale > 0
            and aac_meta.mvhd.timescale > 0
            and metadata.mvhd.timescale != aac_meta.mvhd.timescale
        ):
            scale_ratio = metadata.mvhd.timescale / aac_meta.mvhd.timescale
            aac_track.tkhd.duration = int(round(aac_track.tkhd.duration * scale_ratio))
            if aac_track.edts and aac_track.edts.entries:
                for elst_entry in aac_track.edts.entries:
                    elst_entry.segment_duration = int(round(elst_entry.segment_duration * scale_ratio))

        # 构造替换了原片音轨的 hybrid_metadata
        new_tracks: list[Track] = []
        replaced = False
        for t in metadata.tracks:
            if t.is_audio and not replaced:
                new_tracks.append(aac_track)
                replaced = True
            elif not t.is_audio:
                new_tracks.append(t)
        if not replaced:
            new_tracks.append(aac_track)

        hybrid_metadata = Mp4Metadata(
            mvhd=metadata.mvhd,
            tracks=new_tracks,
            udta_bytes=metadata.udta_bytes,
        )

        return SidecarAudioBundle(
            hybrid_metadata=hybrid_metadata,
            aac_payload=aac_payload,
        )
    finally:
        if had_pool_attr:
            inner_prov._pool_limit_per_host = prev_pool_limit
            if patched_conn is not None and orig_conn_limits is not None:
                orig_per_host, orig_total = orig_conn_limits
                patched_conn._limit_per_host = orig_per_host
                if orig_total is not None and hasattr(patched_conn, "_limit"):
                    patched_conn._limit = orig_total
            cur_sess = getattr(inner_prov, "_session", None)
            if cur_sess is not None and not cur_sess.closed:
                cur_conn = getattr(cur_sess, "connector", None)
                if cur_conn is not None and cur_conn is not patched_conn and hasattr(cur_conn, "_limit_per_host"):
                    from app.config import settings

                    if prev_pool_limit is not None:
                        expected_per_host = max(2, int(prev_pool_limit))
                        expected_total = max(8, expected_per_host * 2)
                    elif getattr(inner_prov, "total_bytes", 0) > settings.MAX_FILE_SIZE_BYTES:
                        pref_c = getattr(inner_prov, "prefetch_concurrency", 1)
                        expected_per_host = max(2, pref_c)
                        expected_total = max(4, expected_per_host * 2)
                    else:
                        expected_per_host = 6
                        expected_total = 8
                    cur_conn._limit_per_host = expected_per_host
                    if hasattr(cur_conn, "_limit"):
                        cur_conn._limit = expected_total
        try:
            if temp_file.exists():
                temp_file.unlink(missing_ok=True)
        except Exception as cleanup_err:
            logger.debug("[SidecarAudio] Failed to remove temp audio file %s: %s", temp_file, cleanup_err)


__all__ = [
    "SidecarAudioBundle",
    "build_audio_spans",
    "build_sidecar_audio_bundle",
]
