import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 超限治理策略: skip_and_report=跳过并占位报告; strict_fail=严格熔断。
# split 为 PikPak 流式虚拟分卷专属概念 (VirtualStreamSplitter + JIT 索链) —— 本地磁盘
# 传输无虚拟分卷能力, 本地发布 API 的 split 请求值在门面一律映射为 skip_and_report
# (前端不暴露 split 选项; 映射原因必须保留在本注释与本文件常量 docstring)。
# ---------------------------------------------------------------------------
OVERSIZE_POLICY_SKIP_AND_REPORT = "skip_and_report"
OVERSIZE_POLICY_SPLIT = "split"
OVERSIZE_POLICY_STRICT_FAIL = "strict_fail"


@dataclass(frozen=True, slots=True)
class BreathingPolicy:
    """分卷间呼吸策略参数对象"""

    min_floor_s: float = 15.0
    jitter_s: float = 5.0
    poll_s: float = 0.5
    enabled: bool = True
    # fMP4 网络抖动重试退避阶梯 (按 part_attempt 取值, 越界回落末档)
    retry_backoff_s: tuple[float, ...] = (10.0, 25.0)


@dataclass
class PublishItem:
    """归一化发布条目抽象 (跨本地与云端数据源)"""

    item_id: str  # 本地为文件绝对路径, PikPak 为 file_id
    name: str  # 显示文件名 (带扩展名)
    size_bytes: int  # 文件字节数
    rel_path: str = ""  # 相对目录路径 (如 "一字马汁汁/sub1")
    parent_folder_id: str = ""  # 父目录标识 (PikPak 为 parent_id, 本地为 parent dirname)
    mime_type: str | None = None  # MIME 类型
    thumbnail_data: Any | None = None  # 缩略图生成器/URL/字节流
    source_type: str = "local"  # "local" | "pikpak"
    audit: dict[str, Any] = field(default_factory=dict)  # 远端审核状态
    duration_s: float | None = None  # 视频时长 (秒)
    width: int | None = None  # 宽度
    height: int | None = None  # 高度
    is_dir: bool = False  # 是否为目录条目
    audio_codec: str | None = None  # 音频编码器名称 (如 aac, mp4a, ac3, eac3, dts 等)
    video_codec: str | None = None  # 视频编码器名称 (如 h264, hevc, vp8, vp9, av1 等)
    audio_codecs: tuple[str, ...] | None = None  # 按输入轨道顺序排列的探测结果


@dataclass
class PublishBatch:
    """发布批次（本地一个子文件夹或 PikPak 一个目录选择组/散装组）"""

    batch_id: str
    folder_name: str  # 文件夹名称/标头 (如 "一字马汁汁")
    rel_path: str = ""
    is_folder_publish: bool = False  # 本批次是否为文件夹打包形态
    items: list[PublishItem] = field(default_factory=list)

    @property
    def total_size_bytes(self) -> int:
        return sum(item.size_bytes for item in self.items)

    # PublishBatch.total_count 已删除 —— 生产代码零消费者 (直接 len(batch.items));
    # total_size_bytes 保留 (pipeline.execute 全任务字节口径消费)。


@dataclass
class TransportOutcome:
    """引擎传输结果统一契约 (V2 ): 逐条 TG 消息 id + 速率 + 客户端标签。

    message_ids: 分卷/相册多条消息时按序全部携带 (pipeline 落库取末条, 与旧
    last_message_id 语义一致); client_label 用于失败归因 (Bot Pool 路径不再丢失)。
    """

    message_ids: list[int] = field(default_factory=list)
    speed_mb_s: float = 0.0
    client_label: str = ""
    # (J5 扩展): 相册逐项隔离的失败归因 —— message_ids 为空且 error_detail 非空 =
    # 该成员上传/组发失败 (编排层按"部分失败逐条卡 / 整组失败汇总卡"路由, D2/R3);
    # message_ids 为空且 error_detail 为空 = 既有 J5 哨兵语义 (padding/短 sent_list)。
    error_detail: str = ""
    # 传输层遇到 Telethon FloodWaitError 等可由调度器退避重试的错误时标记为 True,
    # 供相册消费者在记录完同一批已送达成员后上抛 RetryableTaskError。
    retryable_error: bool = False
    retry_after_s: int = 0
    flood_marked: bool = False


@dataclass
class DedupQuery:
    """Phase 3 防重查询参数 (source hook 产物; pipeline 单点消费 PublishHistoryRepository)。

    local 语义: (target, local_path, size_bytes, file_hash); pikpak 语义:
    (target, remote_file_id) —— repository 内部按 source_type 分流。
    """

    local_path: str = ""
    size_bytes: int = 0
    file_hash: str = ""
    source_type: str = "local"
    remote_file_id: str = ""


class _DefaultUploadConcurrency(int):
    """区分 PublishContext 默认并发基线 (2) 与调用方显式指定值 (2) 的整型哨兵"""

    _is_default_sentinel: bool = True


_DEFAULT_UPLOAD_CONCURRENCY = _DefaultUploadConcurrency(2)
_DEFAULT_IN_FLIGHT_PER_CONNECTION = _DefaultUploadConcurrency(2)


@dataclass
class PublishContext:
    """贯穿流水线全生命周期的执行上下文"""

    task_id: str
    target_chat_id: int | str  # 强类型规范：纯数字 ID 保证为 int，Username 保留 str
    target_channel_raw: str = ""  # 原始入参字符串 (用于展示与日志)
    is_folder_publish: bool = False  # 全局是否为文件夹打包发布
    folder_name: str = ""  # 全局文件夹名称/标头
    folder_path: str = ""  # 本地文件夹绝对路径 (本地发布使用)
    userbot_client: Any = None  # TelegramClient 实例 (就绪状态)
    account_id: int | None = None
    account_display: str = ""
    is_premium: bool = False  # Premium 账号状态快照
    video_split_threshold_bytes: int = 0  # 视频物理分卷切片门限 (字节, 例如 1950 MiB / 3900 MiB)
    bot_routing_threshold_bytes: int = 0  # Bot 轮询池与 Userbot 路由分流门限 (字节, 默认 30MB)
    split_threshold_bytes: int | None = None  # 构造入参兼容桥接 (在 __post_init__ 中映射至双轨)
    task_limit_bytes: int = 0  # 账号单文件协议硬上限
    upload_concurrency: int | None = _DEFAULT_UPLOAD_CONCURRENCY  # 上传并发通道数
    upload_in_flight_per_connection: int | None = _DEFAULT_IN_FLIGHT_PER_CONNECTION  # 单连接在途分块数 (规范契约)
    prefetch_concurrency: int | None = 3  # 预拉取并发通道数 (PikPak HTTP Range)
    completed_files_count: int = 0
    total_files_count: int = 0
    completed_bytes: int = 0
    total_bytes_sum: int = 0
    delay_seconds: float = 3.0  # 发送间隔延迟
    force_republish: bool = False  # 是否强制重传 (bypass 防重)
    auto_cleanup_mode: str = "archive"  # "none" | "archive" | "delete"
    oversize_policy: str = "skip_and_report"  # "skip_and_report" | "split" | "strict_fail"
    send_placeholder_card: bool = True  # 是否发送超限占位卡
    album_mode: bool = True  # 本地发布图片是否合成相册
    album_pipeline_slots: int = 1  # 包5 (J9): 相册跨图链路预取窗口 (1=串行现行为, 默认关;
    # 上限 3; 仅 PikPak 相册路径消费, 门面传参, 不暴露 API/前端)
    source_type: str = "local"  # 包3 (K3): medium 标识唯一权威 —— 两门面装配显式设置
    # (publisher_service→"local" / pikpak_publisher_service→
    # "pikpak"), 编排层 _execute_impl 唯一消费点; 裸构造默认
    # local (测试/局部场景的安全侧, 本地误判无 pikpak 副作用)
    dir_tree: dict[str, str] = field(default_factory=dict)  # 目录树映射 {folder_id: rel_path}
    is_cancelled: Callable[[], bool] | None = None
    is_paused: Callable[[], bool] | None = None
    progress_callback: Callable[..., Any] | None = None
    pause_drain: bool = False  # 暂停排水模式: 暂停信号已收到, 队列剩余占位卡以风控地板间隔
    # 快速发完后再暂停 (card_throttle_wait 置位, 下一个常规检查点收口 raise);
    # 仅暂停触发, 取消不排水照常立即中断。上下文每次执行新建, resume 自然复位。
    sent_placeholder_keys: set[str] = field(default_factory=set)  # 已发送占位卡特征键账本 (任务级发卡幂等)
    split_strategy: str | None = None  # 超限大文件流式切片策略 ("fmp4_universal" | "raw")
    auto_remux_mpegts_to_mp4: bool = (
        False  # 由发布门面从 system_settings 注入 (生产默认 True; 裸构造保底 False 不破坏纯 raw 单测)
    )
    breathing: BreathingPolicy = field(default_factory=BreathingPolicy)  # 分卷间呼吸调度策略
    expansion_truncated: bool = False  # 目录展开是否触达单任务文件数上限而截断
    expansion_limit: int = 0  # 触发截断时的单任务上限值
    cleanup_had_failures: bool = field(default=False, init=False, repr=False)
    cleanup_defer_unrelated_dirs: bool = field(default=False, init=False, repr=False)
    placeholder_cards_sent: int = field(default=0, init=False, repr=False)
    placeholder_overflow_sent: bool = field(default=False, init=False, repr=False)

    # 动态并发调谐内部缓存与单调时钟状态
    _cached_upload_concurrency: int | None = field(default=None, init=False, repr=False)
    _last_upload_concurrency_check: float = field(default=0.0, init=False, repr=False)
    _cached_in_flight: int | None = field(default=None, init=False, repr=False)
    _last_in_flight_check: float = field(default=0.0, init=False, repr=False)
    _cached_prefetch_concurrency: int | None = field(default=None, init=False, repr=False)
    _last_prefetch_check: float = field(default=0.0, init=False, repr=False)
    _cached_send_delay: float | None = field(default=None, init=False, repr=False)
    _last_send_delay_check: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.split_threshold_bytes is not None:
            if not self.video_split_threshold_bytes:
                self.video_split_threshold_bytes = self.split_threshold_bytes
            if not self.bot_routing_threshold_bytes:
                self.bot_routing_threshold_bytes = self.split_threshold_bytes
        if (not self.target_channel_raw) and (self.target_chat_id is not None and self.target_chat_id != ""):
            self.target_channel_raw = str(self.target_chat_id)
        if self.target_chat_id is not None and self.target_chat_id != "":
            from app.core.pipeline.utils import normalize_target_chat_id

            self.target_chat_id = normalize_target_chat_id(self.target_chat_id)

    def reset_probe_cache(self) -> None:
        """重置动态探针缓存 (在启动新上传项或分卷时调用)"""
        self._cached_upload_concurrency = None
        self._last_upload_concurrency_check = 0.0
        self._cached_in_flight = None
        self._last_in_flight_check = 0.0
        self._cached_prefetch_concurrency = None
        self._last_prefetch_check = 0.0
        self._cached_send_delay = None
        self._last_send_delay_check = 0.0

    def get_current_upload_concurrency(self) -> int:
        """获取当前上传并发通道数 (2.0s TTL 缓存, 优先读取 DB 任务最新参数, 防御性限制 [1, 16])"""
        now = time.monotonic()
        if self._cached_upload_concurrency is not None and (now - self._last_upload_concurrency_check < 2.0):
            return self._cached_upload_concurrency

        val = None
        from_db = False
        if self.task_id:
            try:
                from app.db.task_repository import TaskQueueRepository

                task_rec = TaskQueueRepository.get_by_id(self.task_id)
                if task_rec:
                    p_params = task_rec.get("params") or {}
                    val = p_params.get("upload_concurrency") or task_rec.get("concurrency_total")
                    if val is not None:
                        from_db = True
            except Exception as t_err:
                logger.debug("动态获取任务并发参数异常 (非致命): %s", t_err)

        if val is None:
            val = self.upload_concurrency
        if val is None:
            try:
                from app.config import settings
                from app.db.account_repository import SystemSettingsRepository

                val = SystemSettingsRepository.get_int(
                    "upload_parallel_connections",
                    getattr(settings, "UPLOAD_PARALLEL_CONNECTIONS", 2),
                )
            except Exception:
                val = 2

        try:
            val = max(1, min(int(val), 16))
        except (ValueError, TypeError):
            val = 2

        self._cached_upload_concurrency = val
        self._last_upload_concurrency_check = now
        if from_db or self.upload_concurrency is not None:
            self.upload_concurrency = val
        return val

    def get_current_in_flight(self) -> int:
        """获取当前单连接在途分块数 (2.0s TTL 缓存, 优先读取 DB, 防御性限制 [1, 4])"""
        now = time.monotonic()
        if self._cached_in_flight is not None and (now - self._last_in_flight_check < 2.0):
            return self._cached_in_flight

        val = None
        from_db = False
        if self.task_id:
            try:
                from app.db.task_repository import TaskQueueRepository

                task_rec = TaskQueueRepository.get_by_id(self.task_id)
                if task_rec:
                    p_params = task_rec.get("params") or {}
                    val = p_params.get("upload_in_flight_per_connection")
                    if val is not None:
                        from_db = True
            except Exception as t_err:
                logger.debug("动态获取任务在途请求参数异常 (非致命): %s", t_err)

        if val is None:
            val = self.upload_in_flight_per_connection
        if val is None:
            val = 2

        try:
            val = max(1, min(int(val), 4))
        except (ValueError, TypeError):
            val = 2

        self._cached_in_flight = val
        self._last_in_flight_check = now
        if from_db or self.upload_in_flight_per_connection is not None:
            self.upload_in_flight_per_connection = val
        return val

    def get_current_prefetch_concurrency(self) -> int:
        """获取当前流式预取通道数 (2.0s TTL 缓存, 优先读取 DB, 防御性限制 [1, 16])"""
        now = time.monotonic()
        if self._cached_prefetch_concurrency is not None and (now - self._last_prefetch_check < 2.0):
            return self._cached_prefetch_concurrency

        val = None
        from_db = False
        if self.task_id:
            try:
                from app.db.task_repository import TaskQueueRepository

                task_rec = TaskQueueRepository.get_by_id(self.task_id)
                if task_rec:
                    p_params = task_rec.get("params") or {}
                    val = p_params.get("pikpak_prefetch_concurrency") or p_params.get("prefetch_concurrency")
                    if val is not None:
                        from_db = True
            except Exception as t_err:
                logger.debug("动态获取任务预取并发参数异常 (非致命): %s", t_err)

        if val is None:
            val = self.prefetch_concurrency
        if val is None:
            try:
                from app.config import settings
                from app.db.account_repository import SystemSettingsRepository

                val = SystemSettingsRepository.get_int(
                    "pikpak_prefetch_concurrency",
                    getattr(settings, "PIKPAK_PREFETCH_CONCURRENCY", 3),
                )
            except Exception:
                val = 3

        try:
            val = max(1, min(int(val), 16))
        except (ValueError, TypeError):
            val = 3

        self._cached_prefetch_concurrency = val
        self._last_prefetch_check = now
        if from_db or self.prefetch_concurrency is not None:
            self.prefetch_concurrency = val
        return val

    async def get_current_send_delay_seconds(self) -> float:
        """获取当前发送间隔延迟 (2.0s TTL 缓存, 优先读取 DB 任务最新参数, 防御性限制 [0, 60])。

        steady 降档把 send_delay_seconds 落库后, 运行中执行器经本活取值在下一个发送节拍即时
        生效 —— 与 upload/prefetch 的活取值路径对齐, 不再依赖 dispatch 期固化的 delay_seconds。"""
        now = time.monotonic()
        if self._cached_send_delay is not None and (now - self._last_send_delay_check < 2.0):
            return self._cached_send_delay

        val: float | None = None
        from_db = False
        if self.task_id:
            try:
                from app.db.task_repository import TaskQueueRepository

                task_rec = await TaskQueueRepository.get_by_id_async(self.task_id)
                if task_rec:
                    p_params = task_rec.get("params") or {}
                    raw_delay = p_params.get("send_delay_seconds")
                    if raw_delay is not None:
                        val = float(raw_delay)
                        from_db = True
            except Exception as t_err:
                logger.debug("动态获取任务发送延迟参数异常 (非致命): %s", t_err)

        if val is None:
            val = float(self.delay_seconds or 0.0)

        try:
            val = max(0.0, min(val, 60.0))
        except (ValueError, TypeError):
            val = 0.0

        self._cached_send_delay = val
        self._last_send_delay_check = now
        if from_db:
            self.delay_seconds = val
        return val


class PublishResult(int):
    """统一发布执行结果容器类，继承自 int 保持对历史 count: int 调用的 100% 向后兼容"""

    published_count: int  # 总已发布数 (含历史防重跳过数，保证进度 100% 与向后兼容)
    newly_published_count: int  # 本次任务实际新发送文件数 (用于抑制重复总结卡)
    total_files_count: int
    failed_files: list[str]
    skipped_files: list[Any]
    archive_summary: str | None
    is_folder_publish: bool
    folder_name: str
    archive_failed: bool
    album_published_count: int  # 相册形态成功张数 (批次总结卡 "📷 相册" 行; 非相册批次为 0)
    failed_details: list[dict[str, Any]]  # 包4 (J7): 结构化失败明细 {file_name, reason, client_label}
    dedup_skipped: list[dict[str, Any]]  # 包4 (J7): 防重命中明细 {file_name, size_bytes}

    def __new__(
        cls,
        published_count: int,
        total_files_count: int,
        failed_files: list[str] | None = None,
        skipped_files: list[Any] | None = None,
        archive_summary: str | None = None,
        is_folder_publish: bool = False,
        folder_name: str = "",
        newly_published_count: int | None = None,
        archive_failed: bool = False,
        album_published_count: int | None = None,
        failed_details: list[dict[str, Any]] | None = None,
        dedup_skipped: list[dict[str, Any]] | None = None,
    ) -> "PublishResult":
        return super().__new__(cls, published_count)

    def __init__(
        self,
        published_count: int,
        total_files_count: int,
        failed_files: list[str] | None = None,
        skipped_files: list[Any] | None = None,
        archive_summary: str | None = None,
        is_folder_publish: bool = False,
        folder_name: str = "",
        newly_published_count: int | None = None,
        archive_failed: bool = False,
        album_published_count: int | None = None,
        failed_details: list[dict[str, Any]] | None = None,
        dedup_skipped: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__()
        self.published_count = published_count
        self.total_files_count = total_files_count
        self.newly_published_count = published_count if newly_published_count is None else newly_published_count
        self.failed_files = list(failed_files or [])
        self.skipped_files = list(skipped_files or [])
        self.archive_summary = archive_summary
        self.is_folder_publish = is_folder_publish
        self.folder_name = folder_name
        self.archive_failed = archive_failed
        self.album_published_count = album_published_count or 0
        self.failed_details = list(failed_details or [])
        self.dedup_skipped = list(dedup_skipped or [])

    @property
    def has_failures(self) -> bool:
        return bool(self.failed_files)

    # PublishResult.is_success 已删除 —— 生产终态公式唯一位于
    # publish_executor_service (本地 :114 / pikpak :368), 与该属性语义并不一致,
    # 双实现徒增漂移风险; 唯一消费者为测试 (已随本包改写为 has_failures 断言)。
