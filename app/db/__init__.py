"""app.db 显式导出面: 各域仓储以具名模块为唯一导入入口 (调用方直翻,
本 __init__ 仅作聚合导出便利面, 不承载逻辑)。

模块地图:
- database        —— 连接缓存 / init_db / db_cursor + db_immediate_tx 事务样板
- write_pool      —— 专用单写者 DB 写线程池 (submit_db_write fire-and-forget)
- task_repository —— TaskQueueRepository (任务全生命周期 + 日志水位 + 级联清理; *_async 门面装饰器生成)
- download_repository —— DownloadHistoryRepository / DownloadBreakpointRepository
- publish_repository  —— PublishHistoryRepository / PlaceholderSlotRepository / norm_file_path
- forward_repository  —— ForwardHistoryRepository / ForwardLedgerRepository (skips/coverage)
- storage_repository  —— StorageRegistryRepository (扫描登记增量落库)
- preset_repository   —— PresetRepository (频道预设)
- account_repository  —— 账号 / Bot / 系统设置仓储 (独立维护)
"""

from .database import (
    close_all_thread_connections,
    close_thread_cached_connection,
    db_cursor,
    db_immediate_tx,
    get_db,
    init_db,
)
from .download_repository import DownloadBreakpointRepository, DownloadHistoryRepository
from .forward_repository import ForwardHistoryRepository, ForwardLedgerRepository
from .preset_repository import PresetRepository
from .publish_repository import (
    SLOT_ACTIVE_STATES_SQL,
    PlaceholderSlotRepository,
    PublishHistoryRepository,
    norm_file_path,
)
from .storage_repository import StorageRegistryRepository
from .task_repository import TaskQueueRepository
from .write_pool import (
    drain_db_write_pool,
    drain_db_write_pool_sync,
    get_db_write_pool,
    shutdown_db_write_pool,
    shutdown_db_write_pool_sync,
    submit_db_write,
)

__all__ = [
    "SLOT_ACTIVE_STATES_SQL",
    "DownloadBreakpointRepository",
    "DownloadHistoryRepository",
    "ForwardHistoryRepository",
    "ForwardLedgerRepository",
    "PlaceholderSlotRepository",
    "PresetRepository",
    "PublishHistoryRepository",
    "StorageRegistryRepository",
    "TaskQueueRepository",
    "close_all_thread_connections",
    "close_thread_cached_connection",
    "db_cursor",
    "db_immediate_tx",
    "drain_db_write_pool",
    "drain_db_write_pool_sync",
    "get_db",
    "get_db_write_pool",
    "init_db",
    "norm_file_path",
    "shutdown_db_write_pool",
    "shutdown_db_write_pool_sync",
    "submit_db_write",
]
