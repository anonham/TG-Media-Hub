"""FastTelethon 多通道并行下载/上传引擎子包 (对外外观门面).

业务子模块: primitives (底层原语) / breakpoints (断点续传) / sender_pool (通道池) /
download_engine (下载状态机) / upload_engine (上传状态机) / prefetch (预读流水线)。
公开契约仅含下方 13 个符号; 内部私有符号请直接从所属子模块导入
(app.core.fast_telethon.<submodule>), 子模块对象属性天然可导入, 供 monkeypatch
挂载与跨子模块协作。telethon 网络日志噪音过滤器随 primitives 导入注册
(与原单文件 import 时副作用一致)。
"""

from .breakpoints import (
    UPLOAD_PART_TTL_SECONDS as UPLOAD_PART_TTL_SECONDS,
)
from .breakpoints import (
    discard_upload_meta as discard_upload_meta,
)
from .breakpoints import (
    local_upload_meta_path as local_upload_meta_path,
)
from .breakpoints import (
    resolve_breakpoint_path as resolve_breakpoint_path,
)
from .download_engine import (
    SINGLE_CLIENT_DOWNLOAD_FLOOD_WAIT_MAX_SECONDS as SINGLE_CLIENT_DOWNLOAD_FLOOD_WAIT_MAX_SECONDS,
)
from .download_engine import (
    DownloadIncompleteError as DownloadIncompleteError,
)
from .download_engine import (
    download_file_parallel as download_file_parallel,
)
from .primitives import (
    AdaptivePrefetchQueue as AdaptivePrefetchQueue,
)
from .primitives import (
    SpeedTracker as SpeedTracker,
)
from .primitives import (
    SuppressExpectedCloseFilter as SuppressExpectedCloseFilter,
)
from .sender_pool import (
    FloodPremiumWaitError as FloodPremiumWaitError,
)
from .sender_pool import (
    FloodWaitError as FloodWaitError,
)
from .upload_engine import (
    upload_file_parallel as upload_file_parallel,
)

__all__ = [
    "SINGLE_CLIENT_DOWNLOAD_FLOOD_WAIT_MAX_SECONDS",
    "UPLOAD_PART_TTL_SECONDS",
    "AdaptivePrefetchQueue",
    "DownloadIncompleteError",
    "FloodPremiumWaitError",
    "FloodWaitError",
    "SpeedTracker",
    "SuppressExpectedCloseFilter",
    "discard_upload_meta",
    "download_file_parallel",
    "local_upload_meta_path",
    "resolve_breakpoint_path",
    "upload_file_parallel",
]
