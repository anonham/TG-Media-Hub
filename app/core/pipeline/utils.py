"""标识符解析、文件扫描与通用流水线辅助工具"""

import inspect
import logging
import math
import os
import re
from collections.abc import Callable
from typing import Any

from telethon.errors import FilePartInvalidError, FilePartMissingError, FilePartsInvalidError

from app.config import settings
from app.core.entity_resolver import parse_chat_identifier
from app.core.fast_telethon import SpeedTracker
from app.core.media_sniff import is_supported_local_media
from app.core.path_utils import walk_publish_files

logger = logging.getLogger(__name__)


def natural_sort_key(s: str | None) -> list[Any]:
    """提取字符串的自然语义排序键 (Natural / Human Sort Key)。

    规则：
    1. None 或空字符串安全返回空列表 []；
    2. 连续数字块提取为 (0, int(num), len(num))，支持 1 < 2 < 10 及前导零稳定 tiebreaker；
       数字块判定用 isdecimal()：re 按十进制数字 (Nd 类, 含全角) 切分, 但 No 类字形数字
       ('²','②','¼') 会令 isdigit() 误报 True 而 int() 拒收抛 ValueError, 使目录扫描/相册分组整体崩溃；
    3. 非数字文本块提取为 (1, chunk.casefold())，彻底消除大小写敏感；
    4. 类型标记 0 与 1 彻底避免 Python 3 中 int 与 str 跨类型比较抛出 TypeError。
    """
    if s is None:
        return []
    s = str(s)
    if not s:
        return []

    parts = re.split(r"(\d+)", s)
    key: list[tuple[Any, ...]] = []
    for part in parts:
        if not part:
            continue
        if part.isdecimal():
            key.append((0, int(part), len(part)))
        else:
            key.append((1, part.casefold()))
    return key


def _media_ext(item: Any) -> str:
    """条目扩展名判定 (P0 修复锚): 按 item.name 扩展名判定, name 为空时兜底 item_id。

    禁止按 item_id 判定 —— models.PublishItem.item_id 为双语义字段 (本地=文件绝对路径,
    PikPak=file_id 无扩展名), 按其判扩展名使 PikPak 相册分组恒失效 (实机任务 8da0da5e P0)。
    """
    return os.path.splitext(getattr(item, "name", "") or getattr(item, "item_id", ""))[1].lower()


def is_image_item(item: Any) -> bool:
    """图片条目判定 (本地与 PikPak 两侧统一, 编排层唯一入口 —— 相册分组按本函数收口)。

    注意: 本函数用于判断条目是否参与相册 (Photo Album) 组装。
    Telegram MediaGroup 协议仅支持静态图片 (JPEG/PNG/WebP 等) 作为 Photo 混编入相册；
    GIF 动图在 Telegram 体系中属于动画/文档 (Document/Animation)，与相册 Photo 混发
    会导致 Telegram API 抛出 400 Bad Request 拒收整组媒体。因此 GIF 显式排除在相册之外，
    自动回退至单文件通道逐个正常投递。

    两个门槛谓词是有意不同, 勿"统一" (J2):
    - 相册入组门槛: size >= limit 即拒 —— D1 原文"单文件必须低于分卷阈值才可入组"
      (photo 形态零分卷余地), 见 pipeline._transport_pikpak 与 engines.send_album 二次防御;
    - 单文件超限谓词: size > limit (等值文件可不分卷直传), 见 filtering.filter_batch
      与 pipeline 单文件循环。
    """
    ext = _media_ext(item)
    return ext in settings.IMAGE_EXTENSIONS and ext != ".gif"


def build_media_attributes(
    item: Any,
    *,
    eligible: bool = True,
    duration_s: float | None = None,
    width: int | None = None,
    height: int | None = None,
) -> list[Any]:
    """共享媒体属性构建器: send 与 edit 双路径复用 —— edit 不带 attributes
    会退化为非流媒体文件形态, 故本地引擎与 PikPak 引擎的两份内联副本收口于此。

    - 音频判定优先 (P0 先例, engines.py): 带 duration 的音频绝不误注
      DocumentAttributeVideo (黑屏播放器);
    - video: DocumentAttributeVideo(duration, w, h, supports_streaming=True) ——
      回填 edit 复用同一构建器, "平滑刷新为播放器"才成立;
    - eligible=False (PikPak 多分卷) → 返回空列表 (分卷视频不注流媒体属性, 加固收窄语义);
    - 图片 → 空列表 (photo 形态由 Telethon 自动转换, 无自定义属性);
    - 覆盖参数 (VSSE/fMP4 分卷通道按卷传入): duration_s/width/height 非 None 时优先
      于 item 同名字段, None 回落 —— 仅作用于视频分支三值; 媒体类型判定 (音频优先/
      视频门) 只读 item 字段, 覆盖值不参与判定 (防音频条目因传入时长被误判为视频)。
    """
    if not eligible:
        return []
    from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeVideo

    ext = _media_ext(item)
    is_audio = ext in settings.AUDIO_EXTENSIONS
    is_video = (ext in settings.VIDEO_EXTENSIONS) or (bool(getattr(item, "duration_s", None)) and not is_audio)
    name = getattr(item, "name", "") or os.path.basename(getattr(item, "item_id", "") or "")
    attributes = []
    if is_video:
        raw_duration = duration_s if duration_s is not None else getattr(item, "duration_s", None)
        attributes.append(
            DocumentAttributeVideo(
                duration=_video_duration_value(raw_duration),
                w=safe_int_conversion(width if width is not None else getattr(item, "width", None)),
                h=safe_int_conversion(height if height is not None else getattr(item, "height", None)),
                supports_streaming=True,
            )
        )
    elif is_audio:
        attributes.append(
            DocumentAttributeAudio(
                duration=safe_int_conversion(getattr(item, "duration_s", None)),
                title=os.path.splitext(name)[0],
                performer="",
                voice=False,
            )
        )
    return attributes


def normalize_target_chat_id(target_channel: str | int) -> int | str:
    """将目标频道入参解析归一化：

    规则：
    1. 若为 int (非 bool) 则直接返回；
    2. 若为数字字符串 (如 "-100XXXXXXXXXX", "123456")，强制转换为 int；
    3. 若为私密链接 (t.me/c/<id>/, telegram.me/c/<id>/ 等)，提取并转换为 -100<id> (int)；
    4. 若为公开频道链接 (t.me/<username>, telegram.me/<username>)，提取 username (str)；
    5. 若为邀请链接 (t.me/joinchat/<hash> 或 t.me/+<hash>)，提取 +<hash> (str)；
    6. 若为 Username (@xxx 或 xxx)，保留合法 Username 字符串；
    7. 若为 None / 空白 / bool，安全返回空字符串 ""；
    绝对杜绝纯数字 ID 以 str 形式直传 Telethon get_entity。
    """
    if isinstance(target_channel, bool):
        return ""
    if isinstance(target_channel, int):
        return parse_chat_identifier(target_channel)
    if target_channel is None:
        return ""

    channel_str = str(target_channel).strip()
    if not channel_str:
        return ""

    # 规范化 telegram 别名域名与邀请链接格式
    cleaned = re.sub(r"https?://(?:www\.)?telegram\.(?:me|dog)/", "https://t.me/", channel_str)
    cleaned = re.sub(r"t\.me/joinchat/", "t.me/+", cleaned)

    parsed = parse_chat_identifier(cleaned)
    if isinstance(parsed, bool):
        return ""
    if isinstance(parsed, int):
        return parsed

    parsed_str = str(parsed).strip()
    # 纯负数或正数字符串强制转 int (含 -0 归一化为 0)
    if (parsed_str.startswith("-") and parsed_str[1:].isdigit()) or parsed_str.isdigit():
        return int(parsed_str)

    return parsed_str


def format_folder_tag(folder_path: str) -> str:
    """提取文件夹名称并格式化为 Telegram 检索标签格式：
    1. 自动清洗 #@、@ 前缀，将多博主连写（如 @blogger1@blogger2）自动加空格分隔并转为 # 标签。
    2. 将普通文本词汇统一转换为以 # 开头的合法 Telegram 标签。
    """
    basename = os.path.basename(os.path.normpath(folder_path))
    if not basename:
        basename = folder_path

    s = basename.replace("#@", "@")
    s = re.sub(r"(?<=[^\s])([@#])", r" \1", s)
    s = s.replace("@", "#")

    parts = s.split()
    formatted_parts = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if not part.startswith("#"):
            part = f"#{part}"
        formatted_parts.append(part)

    return " ".join(formatted_parts) if formatted_parts else f"#{basename}"


def scan_folder_grouped(folder_path: str) -> dict[str, list[str]]:
    """按文件夹分组仅扫描有效媒体文件 (图片、视频、原生音频)"""
    grouped_files: dict[str, list[str]] = {}
    if not os.path.exists(folder_path):
        return grouped_files

    for root, files in walk_publish_files(folder_path):
        folder_media = []
        for file in files:
            full_path = os.path.join(root, file)
            is_media, _ = is_supported_local_media(full_path)
            if is_media:
                folder_media.append(full_path)
        if folder_media:
            folder_media.sort(key=lambda p: (natural_sort_key(os.path.basename(p)), p))
            grouped_files[root] = folder_media

    return grouped_files


def safe_invoke_progress(cb: Callable[..., Any] | None, *args: Any, **kwargs: Any) -> Any:
    """向后兼容的进度回调安全分发器 (过滤未声明的参数，防止 TypeError)。"""
    if not cb:
        return None

    try:
        sig = inspect.signature(cb)
        params = sig.parameters
        has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        has_var_positional = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params.values())

        if has_var_keyword:
            filtered_kwargs = dict(kwargs)
        else:
            filtered_kwargs = {
                k: v for k, v in kwargs.items() if k in params and params[k].kind != inspect.Parameter.POSITIONAL_ONLY
            }

        pos_params = [
            p
            for p in params.values()
            if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]

        first_kwarg_index = len(pos_params)
        for i, p in enumerate(pos_params):
            if p.name in filtered_kwargs:
                first_kwarg_index = i
                break

        if first_kwarg_index < len(pos_params):
            passed_args = args[: min(len(args), first_kwarg_index)]
        elif has_var_positional:
            passed_args = args
        else:
            passed_args = args[: min(len(args), len(pos_params))]

        return cb(*passed_args, **filtered_kwargs)
    except (ValueError, TypeError):
        if "sig" not in locals():
            import sys

            for call_args, call_kwargs in [(args, kwargs), (args, {}), (args[:5], {}), (args[:2], {})]:
                try:
                    return cb(*call_args, **call_kwargs)
                except TypeError as err:
                    tb = sys.exc_info()[2]
                    msg = str(err).lower()
                    is_arg_mismatch = (tb is not None and tb.tb_next is None) or any(
                        p in msg for p in ("argument", "keyword", "parameter", "positional")
                    )
                    if not is_arg_mismatch:
                        raise err
            raise
        raise


class UploadSpeedTracker(SpeedTracker):
    """计算上传过程中的实时速率 (MB/s) 与 ETA 预计剩余时间 (基于滑动时间窗口平均)"""

    def __init__(self, file_bytes: int, initial_bytes: int = 0):
        super().__init__(total_bytes=file_bytes, initial_bytes=initial_bytes)
        self.file_bytes = file_bytes


def is_file_parts_invalid(err: Exception) -> bool:
    """Telegram 服务端分块缓存失效签名。"""
    if isinstance(err, (FilePartsInvalidError, FilePartInvalidError, FilePartMissingError)):
        return True
    err_str = str(err).lower()
    return (
        ("file part" in err_str and "missing" in err_str)
        or "file_parts_invalid" in err_str
        or "file_part_invalid" in err_str
    )


def get_meta_path(task_id: str, item_id: str, segment_index: int) -> str:
    """断点 meta 沙箱路径唯一命名助手 (唯一实现在 core/pikpak/meta.py,
    本函数为委托薄层 —— 消除本文件对 META_SUFFIX 字面量的第三处硬编码)。"""
    from app.core.pikpak.meta import meta_path

    return meta_path(task_id, item_id, segment_index)


def _video_duration_value(val: Any) -> int | float:
    """将视频时长归一化为 DocumentAttributeVideo.duration:
    >=1s 保持整型秒 (兼容既有断言); (0, 1) 子秒保留正浮点 (避免被截断为 0); 非正/非法返回 0。
    """
    if val is None:
        return 0
    try:
        fval = float(val)
    except (ValueError, TypeError, OverflowError):
        return 0
    if not math.isfinite(fval) or fval <= 0:
        return 0
    if fval < 1.0:
        return max(0.001, round(fval, 3))
    return int(fval)


def safe_int_conversion(val: Any, default: int = 0) -> int:
    """安全转换为 int，支持 float 字符串 (如 '120.5' -> 120) 及非数字安全兜底。"""
    if val is None:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError, OverflowError):
        return default


# ---------------------------------------------------------------------------
# 本地发布超限分类与失败归因 (自 publisher_service 下沉的纯函数 —— 仅依赖
# settings, 零 IO; publisher_service 保留同名 re-export 兼容既有调用面)
# ---------------------------------------------------------------------------

# 疑似「账号额度不足 / 任务期间 Premium 失效」的服务端 400 错误签名 (小写匹配)
_PREMIUM_LOST_ERROR_PATTERNS = (
    "file_parts_invalid",
    "file_part_too_big",
    "request too large",
    "entity_too_large",
    "entity too large",
)


def classify_oversize(file_size: int, effective_limit_bytes: int, is_premium: bool) -> tuple[str, str | None]:
    """按动态上限对超限文件分类，返回 (oversize_kind, 拒绝理由)；未超限返回 ("", None)。"""
    size_mib = file_size / (1024 * 1024)
    size_gib = file_size / (1024 * 1024 * 1024)

    # 1. 超过全平台单文件最高上限 (4000.00 MiB / 3.91 GiB = 4,194,304,000 字节)
    if file_size > settings.PREMIUM_MAX_FILE_SIZE_BYTES:
        premium_limit_mib = settings.PREMIUM_MAX_FILE_SIZE_BYTES / (1024 * 1024)
        premium_limit_gib = settings.PREMIUM_MAX_FILE_SIZE_BYTES / (1024 * 1024 * 1024)
        return "premium_limit", (
            f"超过 Telegram 平台单文件最高上限 {premium_limit_mib:.0f} MiB ({premium_limit_gib:.2f} GiB / 当前文件 {size_mib:.2f} MiB / {size_gib:.2f} GiB)，任何账号均无法单包直发，必须分卷切片"
        )

    # 2. 超过当前账号有效上限 (非 Premium 账号上限 2000 MiB，或 Premium 账号在特殊门限/自定义限额下)
    if file_size > effective_limit_bytes:
        limit_mib = effective_limit_bytes / (1024 * 1024)
        limit_gib = effective_limit_bytes / (1024 * 1024 * 1024)
        if is_premium:
            # 真实 Premium 账号超出了当前限制 (如 PikPak 3900 MiB 切片门限)，绝不能误导用户“开通会员”
            return "premium_limit", (
                f"超过当前 Premium 账号有效上传上限 {limit_mib:.0f} MiB ({limit_gib:.2f} GiB / 当前文件 {size_mib:.2f} MiB / {size_gib:.2f} GiB)，必须分卷切片"
            )
        # 仅针对真实的非会员提示开通升级
        premium_limit_mib = settings.PREMIUM_MAX_FILE_SIZE_BYTES / (1024 * 1024)
        premium_limit_gib = settings.PREMIUM_MAX_FILE_SIZE_BYTES / (1024 * 1024 * 1024)
        return "non_premium_limit", (
            f"当前账号非 Premium，上传上限 {limit_mib:.0f} MiB ({limit_gib:.2f} GiB / 当前文件 {size_mib:.2f} MiB / {size_gib:.2f} GiB)；"
            f"开通 Telegram Premium 后上限提升至 {premium_limit_mib:.0f} MiB ({premium_limit_gib:.2f} GiB) 即可直发"
        )

    return "", None


def describe_upload_failure(error: Exception, file_size: int, task_is_premium: bool) -> Exception:
    """对疑似账号额度相关的服务端拒绝补充上下文 (FR5)，便于 FailureLogger 与用户归因。

    仅当文件超过非 Premium 上限 (即只有 Premium 有效才可能走到上传) 且错误文本匹配
    服务端额度拒绝签名时包装；其余错误原样返回。
    """
    err_str = str(error).lower()
    if file_size > settings.MAX_FILE_SIZE_BYTES and any(p in err_str for p in _PREMIUM_LOST_ERROR_PATTERNS):
        return Exception(
            f"服务端拒绝上传: 文件 {file_size / (1024 * 1024):.2f} MiB 超过当前账号可用上限"
            f"（任务开始时 Premium={'✅' if task_is_premium else '❌'}，可能已在任务期间到期）；"
            f"原始错误: {error}"
        )
    return error


def _is_premium_lost_failure(error: Exception, task_is_premium: bool) -> bool:
    """判定是否疑似任务期间 Premium 失效导致的服务端拒绝"""
    err_str = str(error).lower()
    return any(p in err_str for p in _PREMIUM_LOST_ERROR_PATTERNS)
