"""PikPak 发布域 core 层: 失败分类学 / 目录展开算法 / meta 命名原语 / 封面链路。

唯一实现层 —— services 侧 (pikpak_service / pikpak_publisher_service) 仅内部消费,
不对外 re-export (pkg 1.4 起, 兼容面已全摘);
依赖方向: app.services.* → 本包, 严禁反转 (本包不得 import app.services.*)。
"""

from app.core.pikpak.expander import ParsedFileItem, expand_remote_files, items_from_snapshot
from app.core.pikpak.meta import META_SUFFIX, meta_path, meta_stem, sanitize_meta_stem
from app.core.pikpak.taxonomy import (
    AUDIT_PASS_STATUSES,
    FAILURE_DETAIL_TEXT,
    PikPakAuthError,
    PikPakCliError,
    PikPakError,
    PikPakNotFoundError,
    PikPakPublishError,
    PikPakPublishFatalError,
    PikPakPublishRetryableError,
    PikPakQuotaError,
    PikPakRiskControlError,
    PikPakTransientError,
    audit_blocked_reason,
    classify_pikpak_failure,
    wrap_pikpak_failure,
)
from app.core.pikpak.thumbnail import (
    THUMBNAIL_TIMEOUT_S,
    extract_frame_ffmpeg,
    fetch_thumbnail,
    make_thumb_provider,
)

__all__ = [
    "AUDIT_PASS_STATUSES",
    "FAILURE_DETAIL_TEXT",
    "META_SUFFIX",
    "THUMBNAIL_TIMEOUT_S",
    "ParsedFileItem",
    "PikPakAuthError",
    "PikPakCliError",
    "PikPakError",
    "PikPakNotFoundError",
    "PikPakPublishError",
    "PikPakPublishFatalError",
    "PikPakPublishRetryableError",
    "PikPakQuotaError",
    "PikPakRiskControlError",
    "PikPakTransientError",
    "audit_blocked_reason",
    "classify_pikpak_failure",
    "expand_remote_files",
    "extract_frame_ffmpeg",
    "fetch_thumbnail",
    "items_from_snapshot",
    "make_thumb_provider",
    "meta_path",
    "meta_stem",
    "sanitize_meta_stem",
    "wrap_pikpak_failure",
]
