import logging
import mimetypes
import os
from typing import ClassVar


def _parse_dotenv_value(raw_value: str) -> str:
    """解析 .env 值：支持带引号值内保留 #、剥离闭引号后的行尾 # 注释及无引号行尾注释"""
    value = raw_value.strip()
    if not (value and value[0] in ('"', "'")):
        return value.split("#")[0].strip()

    quote = value[0]
    idx = 1
    escaped = False
    end_idx = -1
    while idx < len(value):
        ch = value[idx]
        if escaped:
            escaped = False
        elif quote == '"' and ch == "\\":
            escaped = True
        elif ch == quote:
            end_idx = idx
            break
        idx += 1

    if end_idx != -1:
        trailing = value[end_idx + 1 :].strip()
        if not trailing or trailing.startswith("#"):
            return value[1:end_idx]
    return value.strip('"').strip("'")


def _load_dotenv(path: str) -> None:
    """极简 .env 加载器(支持注释过滤与引号去除), 已存在的环境变量优先"""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, raw_value = line.partition("=")
            key = key.strip()
            value = _parse_dotenv_value(raw_value)
            if key and key not in os.environ:
                os.environ[key] = value


_load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))


def _optional_str(name: str) -> str | None:
    """读取可选凭证配置: 缺失/空白返回 None, 缺失项由启动期 Settings.validate_required 统一校验"""
    value = os.getenv(name, "").strip()
    return value or None


def _optional_int(name: str) -> int | None:
    """读取可选整数配置: 缺失返回 None; 提供但非法时保留 import 期快速失败"""
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"配置 {name} 不是合法整数: {raw!r}") from None


class Settings:
    # Telegram API 凭证(从 .env / 环境变量读取, 不再硬编码进代码)
    # 缺失时置 None/空列表, import 期不报错 (fresh clone 无 .env 时 import/pytest 不崩)
    # 由启动期 validate_required() 一次性收集全部缺失键后快速失败
    API_ID: int | None = _optional_int("TG_API_ID")
    API_HASH: str | None = _optional_str("TG_API_HASH")
    DEFAULT_PHONE: str = os.getenv("TG_DEFAULT_PHONE", "")
    USERBOT_SESSION_NAME: str = os.getenv("USERBOT_SESSION_NAME", "userbot_session")

    # Web 服务端口配置 (默认避开 Windows Hyper-V 排除端口段 7995-8094)
    HOST: str = os.getenv("HOST", "127.0.0.1")
    PORT: int = int(os.getenv("PORT", os.getenv("TG_PORT", "8500")))
    DEBUG: bool = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")

    # 4-Bot 轮询 Token 池(逗号分隔, 支持覆盖环境变量 TG_BOT_TOKENS)
    BOT_TOKENS: ClassVar[list[str]] = [t.strip() for t in os.getenv("TG_BOT_TOKENS", "").split(",") if t.strip()]

    def validate_required(self) -> None:
        """启动期一次性校验必填 Telegram 凭证: 收集全部缺失键后统一抛出, 而不是命中第一个即中断"""
        missing = [
            key
            for key, value in (
                ("TG_API_ID", self.API_ID),
                ("TG_API_HASH", self.API_HASH),
                ("TG_BOT_TOKENS", self.BOT_TOKENS),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(
                f"缺少 Telegram 凭证配置: {', '.join(missing)}。"
                "请在项目根目录创建 .env 文件并填写以上配置, 可参考 .env.example 与 README.md 中的配置说明。"
            )
        # PR1 迁移守卫: TG_API_KEY 向后兼容别名已移除 —— 旧部署残留该变量而未显式设置 API_KEY
        # 会使鉴权静默降级为免认证, 必须快速失败强制改名 (两者并存时仅告警, 见 app/main.py lifespan)
        if os.getenv("TG_API_KEY", "").strip() and not self.API_KEY:
            raise RuntimeError(
                "检测到已弃用的 TG_API_KEY 环境变量且未设置 API_KEY: 自本版本起 TG_API_KEY 不再生效,"
                "请将 .env 中的 TG_API_KEY 改名为 API_KEY 后重启 (防止鉴权静默降级为免认证)"
            )
        if self.SECRET_KEY and self.SECRET_KEY == self.API_KEY:
            raise RuntimeError("SECRET_KEY 与 API_KEY 必须独立配置")
        if self.SPLIT_STRATEGY not in self.ALLOWED_SPLIT_STRATEGIES:
            raise RuntimeError(f"非法切片策略: {self.SPLIT_STRATEGY}，允许的选项: {self.ALLOWED_SPLIT_STRATEGIES}")
        if self.WAL_CHECKPOINT_INTERVAL_SECONDS <= 0:
            raise RuntimeError(
                f"WAL_CHECKPOINT_INTERVAL_SECONDS 必须为正数 (秒): {self.WAL_CHECKPOINT_INTERVAL_SECONDS!r}"
            )
        if self.WAL_SIZE_WARNING_THRESHOLD_BYTES <= 0:
            raise RuntimeError(
                f"WAL_SIZE_WARNING_THRESHOLD_BYTES 必须为正数 (字节): {self.WAL_SIZE_WARNING_THRESHOLD_BYTES!r}"
            )
        if self.TASK_LOGS_RETENTION_PER_TASK <= 0:
            raise RuntimeError(
                f"TASK_LOGS_RETENTION_PER_TASK 必须为正整数 (条/任务): {self.TASK_LOGS_RETENTION_PER_TASK!r}"
            )

    # 默认转存私有频道 ID
    DEFAULT_TARGET_CHANNEL: str = os.getenv("DEFAULT_TARGET_CHANNEL", "-1009000000010")

    # 路径与存储配置
    BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DOWNLOAD_DIR: str = os.path.join(BASE_DIR, "downloads")
    DATA_DIR: str = os.path.join(BASE_DIR, "data")
    DB_PATH: str = os.path.join(DATA_DIR, "tg_service.db")
    LOCK_PATH: str = os.path.join(DATA_DIR, "tg_service.lock")
    LOG_DIR: str = os.path.join(DATA_DIR, "logs")
    STATIC_DIR: str = os.path.join(BASE_DIR, "static")
    # 失败占位卡静态海报 (1280×720 16:9 JPEG ≤40KB, 随应用分发)。
    # 禁放 static/ (前端构建 emptyOutDir 清仓); 资产缺失时发卡降级纯文本, 永不致命。
    PLACEHOLDER_ASSET_PATH: str = os.getenv(
        "PLACEHOLDER_ASSET_PATH",
        os.path.join(BASE_DIR, "app", "assets", "placeholder_poster.jpg"),
    )

    # SQLite WAL 自保参数 (由 app/db/database.py 消费; 基础设施级, 不入运行时设置表)
    WAL_CHECKPOINT_INTERVAL_SECONDS: float = float(os.getenv("WAL_CHECKPOINT_INTERVAL_SECONDS", "300.0"))
    WAL_SIZE_WARNING_THRESHOLD_BYTES: int = int(os.getenv("WAL_SIZE_WARNING_THRESHOLD_BYTES", str(50 * 1024 * 1024)))
    # 任务日志保留策略: 启动期按任务修剪, 每任务保留最新 N 条 (防长任务日志无界增长)
    TASK_LOGS_RETENTION_PER_TASK: int = int(os.getenv("TASK_LOGS_RETENTION_PER_TASK", "5000"))

    # FastTelethon 下载与发布引擎配置
    CHUNK_SIZE: int = 512 * 1024  # 512 KiB (Telegram MTProto 协议单分块硬性上限 512 KiB = 524,288 字节)
    DOWNLOAD_PARALLEL_CONNECTIONS: int = int(
        os.getenv("DOWNLOAD_PARALLEL_CONNECTIONS", "8")
    )  # 8通道并发稳态下载 (兼顾账号安全与高吞吐量)
    UPLOAD_PARALLEL_CONNECTIONS: int = int(os.getenv("UPLOAD_PARALLEL_CONNECTIONS", "2"))  # 2通道并发上传 (推荐默认 2)
    UPLOAD_IN_FLIGHT_PER_CONNECTION: int = int(
        os.getenv("UPLOAD_IN_FLIGHT_PER_CONNECTION", "2")
    )  # 单连接在途请求数 (Pipelining 深度，默认 2，范围 1-4)
    PARALLEL_CONNECTIONS: int = DOWNLOAD_PARALLEL_CONNECTIONS  # 向后兼容旧别名
    GRACEFUL_STOP_TIMEOUT: float = float(os.getenv("GRACEFUL_STOP_TIMEOUT", "10.0"))
    # [历史兼容别名] 早期代码定义的 2048 MiB 常量；注意 Telegram 非会员门限实为 2000 MiB (1.95 GiB)（对应 4,000 块 × 512 KiB）
    TWO_GB: int = 2 * 1024 * 1024 * 1024  # 历史 2048 MiB 常量，非 Telegram 2000 MiB (1.95 GiB) 上限
    # Telegram MTProto 单文件限制本质是分块计数上限：
    # 普通账号：最多 4,000 块 × 512 KiB = 恰好 2000 MiB (1.95 GiB)（2,097,152,000 字节）(保留 MAX_FILE_SIZE_MB 环境变量向后兼容)
    MAX_FILE_SIZE_MIB: int = int(os.getenv("MAX_FILE_SIZE_MB", "2000"))
    MAX_FILE_SIZE_BYTES: int = MAX_FILE_SIZE_MIB * 1024 * 1024
    # Premium 账号：最多 8,000 块 × 512 KiB = 恰好 4000 MiB (3.91 GiB)（4,194,304,000 字节）(保留 PREMIUM_MAX_FILE_SIZE_MB 环境变量向后兼容)
    PREMIUM_MAX_FILE_SIZE_MIB: int = int(os.getenv("PREMIUM_MAX_FILE_SIZE_MB", "4000"))
    PREMIUM_MAX_FILE_SIZE_BYTES: int = PREMIUM_MAX_FILE_SIZE_MIB * 1024 * 1024
    # Telegram 协议绝对硬顶 4096 MiB（4,294,967,296 字节），任何账号与分卷切片不可逾越
    PROTOCOL_MAX_FILE_SIZE_BYTES: int = 4 * 1024 * 1024 * 1024
    # 发布分流阈值 (MB): 超过该体积的文件自动切换 Userbot 上传, 否则走 Bot 池轮询。
    # 单位为 MB; 仅当设置中心动态项 userbot_upload_threshold_mb 缺失时兜底, 设置中心值优先生效。
    USERBOT_UPLOAD_THRESHOLD_MB: int = int(os.getenv("USERBOT_UPLOAD_THRESHOLD", "30"))

    # 媒体文件后缀分类与限制
    IMAGE_EXTENSIONS: tuple = (".jpg", ".jpeg", ".png", ".gif", ".webp")
    VIDEO_EXTENSIONS: tuple = (".mp4", ".m4v", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".ts", ".m2ts", ".mts")
    AUDIO_EXTENSIONS: tuple = (".mp3", ".m4a", ".flac", ".ogg", ".opus", ".wav", ".aac")
    VALID_MEDIA_EXTENSIONS: tuple = IMAGE_EXTENSIONS + VIDEO_EXTENSIONS + AUDIO_EXTENSIONS
    SUPPORTED_MEDIA_EXTENSIONS: tuple = IMAGE_EXTENSIONS + VIDEO_EXTENSIONS
    SUPPORTED_ALL_EXTENSIONS: tuple = VALID_MEDIA_EXTENSIONS + (".zip", ".rar", ".7z", ".pdf", ".txt")

    # 安全与鉴权配置
    # 仅认 API_KEY (禁用旧别名 TG_API_KEY; 残留旧变量由 validate_required 快速失败强制改名)
    API_KEY: str = os.getenv("API_KEY", "").strip()
    # 公网入口显式禁用本机豁免。无法识别的配置值也按要求认证处理。
    CONSOLE_REQUIRE_API_KEY: bool = os.getenv("CONSOLE_REQUIRE_API_KEY", "false").strip().lower() not in (
        "false",
        "0",
        "no",
    )
    # 首次部署/受限恢复专用高熵值部署者引导密钥 (与正常业务凭据 API_KEY 严格分离)
    BOOTSTRAP_TOKEN: str = os.getenv("TG_BOOTSTRAP_TOKEN", "").strip()
    # Web 2FA 会话 JWT 签名密钥 (HS256); 留空时自动生成并持久化到 DATA_DIR/.web_2fa_secret
    # (绝不从 API_KEY 派生 —— API_KEY 会分发给控制台用户, 派生链可被第一因子
    # 持有者离线伪造第二因子); 显式设置后与持久密钥解耦
    SECRET_KEY: str = os.getenv("SECRET_KEY", "").strip()
    # 可信反向代理网段白名单 (X-Forwarded-For 解析, 支持 CIDR), 透传给 uvicorn forwarded_allow_ips; 禁止设为 "*"
    TRUSTED_PROXIES: str = os.getenv("TRUSTED_PROXIES", "172.16.0.0/12,127.0.0.1")
    CORS_ORIGINS: ClassVar[list[str]] = [
        o.strip()
        for o in os.getenv("CORS_ORIGINS", "http://localhost:8500,http://127.0.0.1:8500").split(",")
        if o.strip()
    ]
    RATE_LIMIT_PER_MINUTE: int = int(os.getenv("RATE_LIMIT_PER_MINUTE", "120"))

    # 调度器自愈保护配置: 服务启动时是否自动恢复执行任务 (默认 False，安全暂停保护)
    AUTO_RESUME_TASKS_ON_STARTUP: bool = os.getenv("AUTO_RESUME_TASKS_ON_STARTUP", "false").lower() in (
        "true",
        "1",
        "yes",
    )

    # 多 Userbot 联合并发下载与缓存配置
    ENABLE_MULTI_USERBOT_DOWNLOAD: bool = os.getenv("ENABLE_MULTI_USERBOT_DOWNLOAD", "true").lower() in (
        "true",
        "1",
        "yes",
    )
    USERBOT_ACCESS_CACHE_TTL: float = float(os.getenv("USERBOT_ACCESS_CACHE_TTL", "60.0"))

    # 静默中转群桥接与多 Bot 借壳加速配置
    ENABLE_TRANSIT_ACCELERATION: bool = os.getenv("ENABLE_TRANSIT_ACCELERATION", "true").lower() in ("true", "1", "yes")
    TRANSIT_CHAT_ID: str = os.getenv("TRANSIT_CHAT_ID", "")
    TRANSIT_MIN_FILE_SIZE_MB: int = int(os.getenv("TRANSIT_MIN_FILE_SIZE_MB", "20"))

    # ==== PikPak 流式发布配置 ====
    # 红线: PIKPAK_TOKEN 只从环境变量读取, 永不写入日志/数据库/API 响应
    PIKPAK_TOKEN: str | None = _optional_str("PIKPAK_TOKEN")
    # 流式预读窗口提示值 (MB): 按 blocks = MB*1024*1024 // (512*1024) 映射消费
    PIKPAK_STREAM_PREFETCH_MB: int = int(os.getenv("PIKPAK_STREAM_PREFETCH_MB", "32"))
    # L1 环形容灾缓存开关 (1C1G 降级档置 false, 直接走 L2 Range 补拉)
    PIKPAK_ENABLE_RING_BUFFER: bool = os.getenv("PIKPAK_ENABLE_RING_BUFFER", "true").lower() in ("true", "1", "yes")
    # L1 环形容灾缓存字节预算 (MB): 覆盖预读(≤64块)+在途(≤32块) 峰值窗口尚有余量
    # FloodPremiumWait 15s 冷却期新增读取 ~3.6MB 不会挤出刚发送块
    PIKPAK_RING_BUFFER_MB: int = int(os.getenv("PIKPAK_RING_BUFFER_MB", "50"))
    # pikpak CLI 可执行文件 (默认取 PATH; 单测指向 fake CLI)
    PIKPAK_CLI_BIN: str = os.getenv("PIKPAK_CLI_BIN", "pikpak")
    # PikPak 发布默认并发档 (稳健档: 2 通道 x 单连接 1 在途
    # 实测非 Premium 每 ~20MB 触发 FloodPremiumWaitError 15s 冷却 + 直链下行 0.24~0.64 MB/s 为链路瓶颈
    # 存量本地发布路径默认并发 UPLOAD_PARALLEL_CONNECTIONS=2 不动)
    PIKPAK_UPLOAD_CONCURRENCY: int = int(os.getenv("PIKPAK_UPLOAD_CONCURRENCY", "2"))
    PIKPAK_UPLOAD_IN_FLIGHT: int = int(os.getenv("PIKPAK_UPLOAD_IN_FLIGHT", "1"))
    PIKPAK_PREFETCH_CONCURRENCY: int = int(os.getenv("PIKPAK_PREFETCH_CONCURRENCY", "3"))
    PIKPAK_PROXY_URL: str | None = _optional_str("PIKPAK_PROXY_URL")
    # 断点元数据沙箱根 (零媒体落盘约束的 meta 唯一常量源, 命名助手与 GC 共用)。
    # 本地默认随 DATA_DIR; 容器经 env 覆盖为 /app/data/upload_meta (Dockerfile/compose 声明)。
    # 本地断点 meta 亦收拢于此 (local_ 前缀命名空间隔离, 键 = sha256(路径)[:16]_size)
    PIKPAK_META_DIR: str = _optional_str("PIKPAK_META_DIR") or os.path.join(DATA_DIR, "upload_meta")
    # 超限大文件流式切片策略: "fmp4_universal" (全能保底可播切片) | "vsse_with_fallback" (虚拟样本切片极速主通道+保底熔断) | "raw" (存量二进制硬切)
    # 与运行时 DB 键 pikpak_default_split_strategy 缺省一致, 引擎直读 settings, 不再旁读 env
    ALLOWED_SPLIT_STRATEGIES: tuple = ("raw", "fmp4_universal", "vsse_with_fallback")
    SPLIT_STRATEGY: str = os.getenv("SPLIT_STRATEGY", "vsse_with_fallback")
    # E2E 捕获模式 (取证, 默认关闭): 开启后为超限流式切片发布留存三份证据
    # (源快照/切分产物/TG 镜像) 到 E2E_CAPTURE_DIR, 磁盘成本 ~3x 源体积/item
    # 生产模式语义与零盘承诺不变。改开关需重启服务 (有意的强隔离)。
    # 详见 docs/testing/E2E_GUIDE.md
    E2E_CAPTURE_MODE: bool = os.getenv("E2E_CAPTURE_MODE", "false").lower() in ("true", "1", "yes")
    # 捕获证据落盘根目录 (本地默认随 DATA_DIR 派生; 容器可经 env 覆盖), 惰性创建
    E2E_CAPTURE_DIR: str = os.getenv("E2E_CAPTURE_DIR", os.path.join(DATA_DIR, "e2e_capture"))
    # E2E 强制分页代理 (默认关闭): fmp4_universal 直连通道经 use_paged_proxy 改由
    # LocalPagedRangeProxy 供流 (生产下代理仅由 VSSE 熔断降级透传触发), 仅供 E2E
    # 真链路验证代理路径, 生产语义不变。改开关需重启服务。
    E2E_FORCE_PAGED_PROXY: bool = os.getenv("E2E_FORCE_PAGED_PROXY", "false").lower() in ("true", "1", "yes")
    # MPEG-TS (.ts/.m2ts/.mts) 发布前自动零落盘重封装为 MP4 (Telegram 服务端剥离 mpegts 视频属性)
    AUTO_REMUX_MPEGTS_TO_MP4: bool = os.getenv("AUTO_REMUX_MPEGTS_TO_MP4", "true").lower() in ("true", "1", "yes")


settings = Settings()


def secure_file_permissions(path: str) -> None:
    """对敏感文件 (如 .session, tg_service.db) 设置非公开读写权限 (POSIX 0600)"""
    if os.path.exists(path) and hasattr(os, "chmod"):
        try:
            os.chmod(path, 0o600)
        except Exception as e:
            if os.name == "posix":
                logging.warning("设置敏感文件权限失败 (%s): %s", path, e)
            else:
                logging.debug("设置敏感文件权限失败 (可能是非 POSIX 环境): %s", e)


# 固化 MIME 类型映射，防止宿主机操作系统注册表将媒体后缀污染为文本或代码类型
mimetypes.add_type("video/mp4", ".m4v")
mimetypes.add_type("video/mp2t", ".ts")
mimetypes.add_type("video/mp2t", ".m2ts")
mimetypes.add_type("video/mp2t", ".mts")

# 确保核心目录存在
os.makedirs(settings.DOWNLOAD_DIR, exist_ok=True)
os.makedirs(settings.DATA_DIR, exist_ok=True)
os.makedirs(settings.LOG_DIR, exist_ok=True)
os.makedirs(settings.STATIC_DIR, exist_ok=True)
