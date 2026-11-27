"""MPEG-TS (.ts/.m2ts/.mts) 发布前零落盘重封装策略与元数据共享辅助。"""

import os
from dataclasses import dataclass
from typing import Any

from app.core.media_sniff import TS_AMBIGUOUS_EXTENSIONS
from app.core.pipeline.engines._common import _has_valid_display_dims
from app.core.pipeline.models import PublishContext, PublishItem
from app.core.pipeline.utils import build_media_attributes

REMUX_TARGET_EXTENSIONS: tuple[str, ...] = TS_AMBIGUOUS_EXTENSIONS

MP4_COPY_SAFE_VIDEO_CODECS: frozenset[str] = frozenset(
    {
        "h264",
        "avc",
        "avc1",
        "hevc",
        "h265",
        "hev1",
        "hvc1",
        "mpeg4",
        "mp4v",
        "av1",
        "av01",
    }
)

MP4_COPY_SAFE_AUDIO_CODECS: frozenset[str] = frozenset({"aac", "mp4a"})


@dataclass(frozen=True, slots=True)
class RemuxDecision:
    """MPEG-TS 转封装 MP4 准入判定结果。"""

    should_remux: bool
    copy_audio: bool
    skip_reason: str = ""
    audio_copy_tracks: tuple[int, ...] | None = None


def is_mpegts_remux_candidate(name: str, context: PublishContext) -> bool:
    """判断条目是否命中 MPEG-TS 自动重封装候选（开启开关且后缀为 .ts/.m2ts/.mts，排除 .d.ts）。"""
    if not getattr(context, "auto_remux_mpegts_to_mp4", False):
        return False
    lower = (name or "").strip().lower()
    if not lower or lower.endswith(".d.ts"):
        return False
    ext = os.path.splitext(lower)[1]
    return ext in REMUX_TARGET_EXTENSIONS


def remux_output_filename(name: str) -> str:
    """将 .ts/.m2ts/.mts 展示文件名替换为 .mp4（仅用于 Telegram 上传流标识，不改动源 item）。"""
    raw = (name or "").strip() or "video.ts"
    stem, ext = os.path.splitext(raw)
    if ext.lower() in REMUX_TARGET_EXTENSIONS:
        return f"{stem or 'video'}.mp4"
    return f"{raw}.mp4"


def mpegts_audio_copy_tracks(item: PublishItem) -> tuple[int, ...]:
    """Choose which MPEG-TS audio tracks can be copied into MP4."""
    if item.audio_codecs is None:
        return (0,) if (item.audio_codec or "").strip().lower() in MP4_COPY_SAFE_AUDIO_CODECS else ()
    return tuple(index for index, codec in enumerate(item.audio_codecs) if codec in MP4_COPY_SAFE_AUDIO_CODECS)


def decide_mpegts_remux(
    item: PublishItem,
    *,
    ffmpeg_available: bool,
    require_known_vcodec: bool,
) -> RemuxDecision:
    """结合编解码探测结果与环境能力判定是否执行零落盘 MP4 重封装。"""
    if not ffmpeg_available:
        return RemuxDecision(
            should_remux=False,
            copy_audio=False,
            skip_reason="ffmpeg 不可用，降级为原样 .ts 文档发送",
        )

    vcodec = (getattr(item, "video_codec", None) or "").lower().strip()
    acodec = (getattr(item, "audio_codec", None) or "").lower().strip()

    if vcodec and vcodec not in MP4_COPY_SAFE_VIDEO_CODECS:
        return RemuxDecision(
            should_remux=False,
            copy_audio=False,
            skip_reason=f"视频编码 {vcodec} 不支持 MP4 无损重封装 (-c:v copy)，降级为原样 .ts 文档发送",
        )

    if require_known_vcodec and not vcodec:
        return RemuxDecision(
            should_remux=False,
            copy_audio=False,
            skip_reason="未能探测到有效视频编码，降级为原样 .ts 文档发送",
        )

    copy_audio = acodec in MP4_COPY_SAFE_AUDIO_CODECS
    return RemuxDecision(
        should_remux=True,
        copy_audio=copy_audio,
        audio_copy_tracks=mpegts_audio_copy_tracks(item),
    )


def build_remux_video_attributes(
    item: PublishItem,
    actual_dur: float,
    uploader_w: int,
    uploader_h: int,
) -> list[Any]:
    """为重封装后的 MP4 流构建完整流媒体视频属性（优先 item 有效展示宽高，回落 moov 解析宽高）。"""
    vid_w = int(getattr(item, "width", None) or 0)
    vid_h = int(getattr(item, "height", None) or 0)
    if not _has_valid_display_dims(vid_w, vid_h):
        vid_w = int(uploader_w or 0)
        vid_h = int(uploader_h or 0)
    if not _has_valid_display_dims(vid_w, vid_h):
        vid_w = 0
        vid_h = 0

    item_dur = float(getattr(item, "duration_s", None) or 0.0)
    effective_dur = float(actual_dur or 0.0) if (actual_dur and actual_dur > 0.0) else item_dur
    return build_media_attributes(
        item,
        eligible=True,
        duration_s=effective_dur if effective_dur > 0.0 else None,
        width=vid_w,
        height=vid_h,
    )
