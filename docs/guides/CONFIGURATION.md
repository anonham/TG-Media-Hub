# ⚙️ 系统全量环境变量与配置参考手册 (Configuration Reference)

本文档面向系统管理员与后端开发者，提供 TG-Media-Hub 全量环境变量、默认值、必填性、对应实现文件及配置陷阱的完整技术参考。

系统绝大部分环境变量在 `app/config.py` 中集中解析，部分模块自管变量（如 WAL 检查点参数）在其所属模块内按需读取。服务启动时优先读取宿主机已注入的环境变量；若缺失，则从项目根目录下的 `.env` 文件加载。

---

## 1. Telegram 核心凭据与账号连接池 (Telegram Credentials)

| 环境变量名 | 类型 | 必填 | 默认值 | 作用位置 / 对应文件 | 说明 |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `TG_API_ID` | `int` | **是** | `None` | `app/core/client_manager.py` | Telegram 官方 API 编号（在 [my.telegram.org](https://my.telegram.org) 申请） |
| `TG_API_HASH` | `str` | **是** | `None` | `app/core/client_manager.py` | Telegram 官方 API 密匙 |
| `TG_BOT_TOKENS` | `str` | **是** | `""` | `app/core/client_manager.py` | 辅助 Bot 令牌列表，逗号分隔（推荐配置 4 个 Bot）。用于正式媒体发布轮换与小文件分流 |
| `TG_DEFAULT_PHONE` | `str` | 否 | `""` | `app/core/client_manager.py` | 默认 Userbot 登录手机号（国际格式，如 `+XX XXXXXXXXX`）。配置后首绑锁定该号码；留空时由已认证部署者在引导页指定 |
| `USERBOT_SESSION_NAME` | `str` | 否 | `userbot_session` | `app/core/client_manager.py` | 主 Userbot 的 `.session` 数据库文件名前缀 |
| `DEFAULT_TARGET_CHANNEL` | `str` | 否 | `-1009000000010` | `app/core/pipeline/pipeline.py` | 默认转存的目标私有/公开频道 ID 或 Username（占位值，请改为自己的频道） |

> [!IMPORTANT]
> 启动校验红线：若 `TG_API_ID`、`TG_API_HASH` 或 `TG_BOT_TOKENS` 缺失，`Settings.validate_required` 会在启动期快速抛出 `RuntimeError` 并中断启动。

---

## 2. Web 服务、网关与安全模型 (Web & Security)

| 环境变量名 | 类型 | 必填 | 默认值 | 作用位置 / 对应文件 | 说明 |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `HOST` | `str` | 否 | `127.0.0.1` | `run.py` / `app/main.py` | FastAPI 监听地址。官方 Compose 显式设为 `0.0.0.0`。非回环绑定且空 Key 时，回环请求也会锁定 |
| `PORT` / `TG_PORT` | `int` | 否 | `8500` | `run.py` / `app/main.py` | Web 端口。本地启动若被占用自动顺延（最多 +20） |
| `DEBUG` | `bool` | 否 | `false` | `app/main.py` | 调试日志模式开关 (`true`/`false`) |
| `TG_BOOTSTRAP_TOKEN` | `str` | 否 | `""` | `app/api/auth.py` | 部署者受限引导密钥。使用独立的随机值，至少 16 个字符且包含至少 6 种不同字符，不能复用 `API_KEY`。受限态支持首次绑定或恢复已绑定但未通过验证的默认 Userbot，恢复时须匹配已绑定身份 |
| `API_KEY` | `str` | 公网部署 | `""` | `app/api/auth.py` | 控制台访问口令。空值仅允许符合本机豁免条件的请求访问业务。程序化访问受保护的 `/metrics` 与 `/openapi.json` 只需要 `X-API-Key`。默认 Userbot 验证通过前，业务不开放 |
| `CONSOLE_REQUIRE_API_KEY` | `bool` | 公网反代 | `false` | `app/config.py` | `true` 对整个实例禁用本机豁免，回环对端和缺少来源头的反代也执行公网认证。官方 Compose 显式开启。`false`、`0`、`no` 表示关闭，其余配置值均启用 |
| `SECRET_KEY` | `str` | 否 | `""` | `app/api/auth.py` | Web 2FA 会话 JWT (HS256) 签名密钥。留空时使用自动生成并保存到 `DATA_DIR/.web_2fa_secret` 的服务器密钥 |
| `TRUSTED_PROXIES` | `str` | 否 | `172.16.0.0/12,127.0.0.1` | `run.py` | 可信反向代理 CIDR 白名单，用于解析 `X-Forwarded-For`。严禁设为 `*` |
| `CORS_ORIGINS` | `str` | 否 | `http://localhost:8500,...` | `app/main.py` | 允许跨域的 Origin 列表，逗号分隔 |
| `RATE_LIMIT_PER_MINUTE` | `int` | 否 | `120` | `app/api/auth.py` | 单 IP 滑动窗口限流阈值（次/分钟），$\le 0$ 表示禁用限流 |

> [!WARNING]
> **`TG_API_KEY` 别名已移除**：`.env` 中残留 `TG_API_KEY` 且未设置 `API_KEY` 时启动快速失败并提示改名。两者并存时启动成功并输出告警。

---

## 3. 传输协议栈与发布引擎 (FastTelethon & Pipeline)

| 环境变量名 | 类型 | 必填 | 默认值 | 作用位置 / 对应文件 | 说明 |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `DOWNLOAD_PARALLEL_CONNECTIONS` | `int` | 否 | `8` | `app/core/fast_telethon/download_engine.py` | 极速下载并发 TCP 通道数（推荐 8） |
| `UPLOAD_PARALLEL_CONNECTIONS` | `int` | 否 | `2` | `app/core/fast_telethon/upload_engine.py` | 并发上传 TCP 通道数（推荐默认 2，兼顾吞吐与风控） |
| `UPLOAD_IN_FLIGHT_PER_CONNECTION` | `int` | 否 | `2` | `app/core/fast_telethon/upload_engine.py` | 单连接在途请求数（Pipelining 深度，推荐 2，范围 1~4） |
| `GRACEFUL_STOP_TIMEOUT` | `float` | 否 | `10.0` | `app/core/scheduler.py` | 传输通道优雅停止超时（秒） |
| `MAX_FILE_SIZE_MB` | `int` | 否 | `2000` | `app/config.py` | 普通 Userbot 账号单文件上传门限（变量名沿用 MB 向后兼容，底层实为 MiB 计算 `* 1024 * 1024`：4,000 分块 × 512 KiB = 恰好 2000 MiB (1.95 GiB / 2097.15 MB = 2,097,152,000 字节)） |
| `PREMIUM_MAX_FILE_SIZE_MB` | `int` | 否 | `4000` | `app/config.py` | Telegram Premium 账号单文件上传硬顶（变量名沿用 MB 向后兼容，底层实为 MiB 计算 `* 1024 * 1024`：8,000 分块 × 512 KiB = 恰好 4000 MiB (3.91 GiB / 4194.30 MB = 4,194,304,000 字节)） |
| `USERBOT_UPLOAD_THRESHOLD` | `int` | 否 | `30` | `app/core/pipeline/pipeline.py` | **分流阈值（MB）**：小文件走 Bot 池轮询，超过此体积自动切换 Userbot |
| `AUTO_RESUME_TASKS_ON_STARTUP` | `bool` | 否 | `false` | `app/core/scheduler.py` | 服务重启时是否自动恢复执行遗留任务（默认安全暂停） |
| `SPLIT_STRATEGY` | `str` | 否 | `vsse_with_fallback` | `app/config.py` | 超限分卷策略：`fmp4_universal`（全能 fMP4 保底）/ `vsse_with_fallback`（纯 Python VSSE v2 极速主通道，缺省）/ `raw`（裸硬切）；与运行时设置键 `pikpak_default_split_strategy` 缺省一致 |
| `AUTO_REMUX_MPEGTS_TO_MP4` | `bool` | 否 | `true` | `app/config.py` | 发布 `.ts/.m2ts/.mts` 视频前自动零落盘重封装为 MP4，避免 Telegram 剥离视频属性导致无法流式播放；与设置中心动态项 `auto_remux_mpegts_to_mp4` 缺省一致 |
| `PLACEHOLDER_ASSET_PATH` | `str` | 否 | `app/assets/placeholder_poster.jpg` | `app/core/pipeline/engines.py` | 占位卡暗底静态海报路径（16:9 JPEG $\le 40$KB） |

---

## 4. 多 Userbot 与静默中转群借壳加速 (Multi-Client & Transit)

| 环境变量名 | 类型 | 必填 | 默认值 | 作用位置 / 对应文件 | 说明 |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `ENABLE_MULTI_USERBOT_DOWNLOAD` | `bool` | 否 | `true` | `app/services/downloader_service.py` | 是否组装多客户端（主账号 + 就绪 Bot 池）联合并发下载受限媒体 |
| `USERBOT_ACCESS_CACHE_TTL` | `float` | 否 | `60.0` | `app/core/client_manager.py` | 私有频道账号可达性探针结果缓存时间（秒） |
| `ENABLE_TRANSIT_ACCELERATION` | `bool` | 否 | `true` | `app/services/transit_bridge_service.py` | 静默中转群借壳加速总开关 |
| `TRANSIT_CHAT_ID` | `str` | 否 | `""` | `app/services/transit_bridge_service.py` | 中转群组/频道 ID（格式 `-100XXXXXXXXXX`）。留空则不开启中转借壳加速 |
| `TRANSIT_MIN_FILE_SIZE_MB` | `int` | 否 | `20` | `app/services/transit_bridge_service.py` | 触发借壳加速的最小文件体积阈值（MB） |

---

## 5. PikPak 云端流式发布与容灾 (PikPak Cloud Streaming)

| 环境变量名 | 类型 | 必填 | 默认值 | 作用位置 / 对应文件 | 说明 |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `PIKPAK_TOKEN` | `str` | 否 | `None` | `app/services/pikpak_service.py` | PikPak Personal Access Token。**最高优先级**，仅从环境变量读取，绝不落库或写入日志 |
| `PIKPAK_DIR` | `str` | 否 | `/app/data/.pikpak` | `app/services/pikpak_service.py` | PikPak CLI 状态与凭证目录（Docker 必须挂载至持久卷） |
| `PIKPAK_META_DIR` | `str` | 否 | `data/upload_meta` | `app/core/stream_chunk_provider.py` | 流式 Provider 断点元数据沙箱根目录 |
| `PIKPAK_NO_UPDATE_NOTIFIER` | `int` | 否 | `1` | `app/services/pikpak_service.py` | 禁用 PikPak CLI 启动时的版本更新检查提示 |
| `PIKPAK_NO_LOG` | `int` | 否 | `1` | `app/services/pikpak_service.py` | 禁用 PikPak CLI 冗余调试日志文件输出 |
| `PIKPAK_CLI_BIN` | `str` | 否 | `pikpak` | `app/services/pikpak_service.py` | PikPak CLI 可执行二进制文件路径或命令名 |
| `PIKPAK_STREAM_PREFETCH_MB` | `int` | 否 | `32` | `app/core/pipeline/engines.py` | HTTP Range 流式预读滑动窗口提示值（MB） |
| `PIKPAK_ENABLE_RING_BUFFER` | `bool` | 否 | `true` | `app/core/pipeline/engines.py` | L1 环形内存容灾缓存开关（低内存环境可置 `false`） |
| `PIKPAK_RING_BUFFER_MB` | `int` | 否 | `50` | `app/core/pipeline/engines.py` | L1 环形内存容灾缓存字节预算（MB，默认 50MB） |
| `PIKPAK_UPLOAD_CONCURRENCY` | `int` | 否 | `2` | `app/core/pipeline/engines.py` | PikPak 发布默认并发连接数（稳健档：2） |
| `PIKPAK_UPLOAD_IN_FLIGHT` | `int` | 否 | `1` | `app/core/pipeline/engines.py` | PikPak 单连接在途未确认分块数（稳健档：1） |
| `PIKPAK_PREFETCH_CONCURRENCY` | `int` | 否 | `3` | `app/core/pipeline/engines.py` | HTTP Range 预拉取并发协程数 |
| `PIKPAK_PROXY_URL` | `str` | 否 | `None` | `app/services/pikpak_service.py`、`app/core/cdn_transport.py` | PikPak CLI 和控制面 CDN 探测的可选 HTTP/SOCKS5 代理地址。发布时的原样上传和 MPEG-TS 整文件重封装均直连 CDN 读取媒体，并忽略环境代理。 |
| `E2E_CAPTURE_MODE` | `bool` | 否 | `false` | `app/core/e2e_capture.py` | E2E 捕获/取证模式：开启后为超限流式切片发布逐条目留存三份证据（源快照 / 切分产物 / TG 镜像）到 `E2E_CAPTURE_DIR`，供离线对比（`scripts/e2e_tg_mirror.py` + `scripts/e2e_compare.py`）。生产模式语义与零盘承诺完全不变；改开关需重启服务；磁盘成本约 3 倍源体积/条目，手动清理 |
| `E2E_CAPTURE_DIR` | `str` | 否 | `data/e2e_capture` | `app/core/e2e_capture.py` | 捕获证据根目录，布局 `<task_id>/<item_id>/`（manifest.json + source/ + parts/ + tg_mirror/） |
| `E2E_FORCE_PAGED_PROXY` | `bool` | 否 | `false` | `app/core/pipeline/engines.py` | E2E 强制分页代理：开启后 `fmp4_universal` 直连切片通道改由 `LocalPagedRangeProxy` 供流，用于真链路验证代理路径 |
| `TEST_CHANNEL_LINK` | `str` | 否 | `https://t.me/telegram/1` | `tests/e2e_live/` / `tests/e2e_ui/` | E2E 测试专用目标频道链接（运行时不参与业务） |
| `TEST_SAMPLE_FOLDER` | `str` | 否 | `""` | `tests/` | 本地测试专用样本目录路径（运行时不参与业务） |

---

## 6. 数据库与持久化治理 (Database & Storage)

| 环境变量名 | 类型 | 必填 | 默认值 | 作用位置 / 对应文件 | 说明 |
| :--- | :---: | :---: | :--- | :--- | :--- |
| `WAL_CHECKPOINT_INTERVAL_SECONDS` | `float` | 否 | `300.0` | `app/db/database.py` | SQLite WAL 定时主动检查点执行周期（秒，默认 5 分钟） |
| `WAL_SIZE_WARNING_THRESHOLD_BYTES` | `int` | 否 | `52428800` (50 MiB) | `app/db/database.py` | WAL 文件积压大小告警阈值（字节，50 × 1024 × 1024 字节），超出门限记录 WARNING 日志 |
| `TASK_LOGS_RETENTION_PER_TASK` | `int` | 否 | `5000` | `app/config.py` / `app/main.py` | 启动期任务日志修剪上限（每个任务最多保留最新 N 条日志，防止长周期任务日志无限膨胀） |

---

## 7. 已知历史配置差异与避坑指南 (Known Gotchas)

1. **`USERBOT_UPLOAD_THRESHOLD` 单位历史陷阱**：
   早期版本中该环境变量在个别说明中曾被误标为字节单位。**当前版本统一定义为兆字节（MB）**。例如 `USERBOT_UPLOAD_THRESHOLD=30` 代表 30MB。
2. **设置中心动态值 vs 环境变量优先级**：
   - 系统首次初始化建库时，环境变量的值会作为 `settings` 动态配置表的初始种子值（Seed）   - 在控制台「系统设置」中修改的动态配置项具有最高优先级，修改后直接持久化至数据库，不再受 `.env` 环境变量覆盖。
3. **`SECRET_KEY` 与 2FA 会话失效**：
   若未配置 `SECRET_KEY`，系统使用 `DATA_DIR/.web_2fa_secret` 中的持久化密钥。轮换 `API_KEY` 后，已签发的 Web 2FA JWT 仍在原 24 小时有效期内有效。轮换显式签名密钥，或更换或删除持久化密钥文件使服务改用新密钥后，旧 JWT 将无法通过验签，需重新完成 2FA。
