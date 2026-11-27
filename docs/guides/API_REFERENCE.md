# 📡 TG-Media-Hub API 与协议参考手册 (API & Protocol Reference)

本文档面向前后端开发者与第三方系统集成人员，系统性描述 TG-Media-Hub 提供的全量 RESTful API 端点规范、安全鉴权模型以及 WebSocket 全双工实时通信协议。

后端基于 FastAPI 构建，全量 RESTful HTTP 操作均挂载显式 Pydantic `response_model` 契约锁，并通过 `tests\api\openapi_snapshot.json` 固化结构快照。

---

## 1. 鉴权协议 (Authentication)

1. **默认 Userbot 受限准入门禁**：当严格指定的默认 Userbot 尚未完成绑定或身份核验时，服务以受限引导模式运行，拒绝全量业务路由、媒体流、`POST /api/auth/media-ticket`、WebSocket `/ws/progress` 及 `/metrics`。所有 `X-Bootstrap-Token` 口令校验共享按 IP 计算的尝试额度。持续轮询请先通过 `POST /api/telegram/bootstrap/auth` 获取短期 `HttpOnly` 引导 Cookie，会话轮询不占用口令尝试额度。默认主账号核验通过后自动解除受限态，引导凭证随即失去权限。
2. **公网入口控制台锁定 (`console_locked`)**：公网反代与官方 Compose 启用 `CONSOLE_REQUIRE_API_KEY=true` 后，整个实例禁用本机豁免。默认 Userbot 已准入且公网入口缺少配置 `API_KEY` 时，业务 API、媒体票据、预览与下载返回 `503`，错误码为 `console_locked`。WebSocket 拒绝握手。受保护的首次绑定、`GET /api/auth/status`、首页、静态资源与健康检查保持可达。
3. **业务双层安全网关 (`app/api/auth.py`)**：当正常准入且生产环境变量配置了 `API_KEY` 时：
   - **API Key 请求头**：`X-API-Key: <your_api_key>`
   - **Web 2FA 会话令牌头**：`X-2FA-Token: <jwt_token>`（通过 `POST /api/telegram/web-2fa/verify` 获取，有效期 24 小时）
   - **短时效媒体与 WebSocket 票据 (`POST /api/auth/media-ticket`)**：携带 `X-API-Key` 与 `X-2FA-Token` 请求头签发 60 秒 TTL 票据，供 `wss://<host>:<port>/ws/progress?ticket=<ticket>`、`/api/storage/preview/{file_path}?ticket=<ticket>` 与 `/api/storage/download/{file_path}?ticket=<ticket>` 使用（本地 HTTP 部署将 `wss://` 换为 `ws://`）。受限期内，或配置了 `API_KEY` 且未携带有效票据时，WebSocket 在接受连接前以 HTTP `403 Forbidden` 拒绝握手。默认 Userbot 已准入且处于本机免认证模式时可免票握手。
4. **指标与文档入口可达性**：配置 `API_KEY` 后，`/metrics` 只接受有效 `X-API-Key`，认证成功的抓取可豁免限流。`/docs` 和 `/redoc` 仅本机直连可达，公网 `/openapi.json` 需有效 `X-API-Key`。通过默认 Userbot 与空 Key 两道前置门禁后，匿名公网文档请求返回 `404`。前置门禁未通过时返回 `503`。

---

## 2. 核心 REST API 规范 (共 14 组业务模块)

### 2.1 鉴权与安全网关 (`/api/auth` & `/api/telegram`)

| 方法 | 路径 | 描述 | 关键参数 / 载荷 |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/auth/status` | 查询 API Key 启用状态与受限引导准入状态（公开免鉴权，不返回手机号或业务数据） | - |
| `GET` | `/api/auth/verify` | 校验 `X-API-Key` 请求头有效性 | 请求头 `X-API-Key` |
| `POST` | `/api/auth/media-ticket` | 签发 60s TTL 短时效票据（供媒体流与 `/ws/progress` 握手使用） | 请求头 `X-API-Key` + `X-2FA-Token` |
| `GET` | `/api/telegram/status` | 查询 Telegram 客户端就绪态、受限引导状态与 Userbot 能力 | `probe_premium=false` 只读已知 Premium 档位，不发起探测 |
| `POST` | `/api/telegram/bootstrap/auth` | 受限期校验部署者 `TG_BOOTSTRAP_TOKEN` 并签发短期 `HttpOnly` 引导 Cookie | 请求头 `X-Bootstrap-Token` |
| `POST` | `/api/telegram/bootstrap/logout` | 清除部署者引导会话与浏览器挑战上下文 Cookie | - |
| `POST` | `/api/telegram/web-2fa/send` | 向绑定的主 Userbot「收藏夹」发送 6 位 Web 登录验证码（独立限流 5 次/分） | - |
| `POST` | `/api/telegram/web-2fa/verify` | 提交 6 位验证码，签发 24 小时 HS256 Web JWT Token | `{ "code": "123456" }` |
| `POST` | `/api/telegram/native/send-code` | 原生 Userbot 发起登录验证码挑战（独立限流 5 次/分，服务端托管 `phone_code_hash`） | `{ "phone": "+XX XXXXXXXXX" }` |
| `POST` | `/api/telegram/native/resend-code` | 按 Telegram `next_type` 与冷却时间重发原生登录验证码（独立限流 5 次/分） | `{ "challenge_id": "..." }` |
| `POST` | `/api/telegram/native/verify-code` | 原生 Userbot 提交验证码与可选 2FA 密码完成挑战授权 | `{ "challenge_id": "...", "code": "...", "password": "..." }` |
| `POST` | `/api/telegram/native/password` | 原生 Userbot 提交 2FA 云端密码续接登录挑战 | `{ "challenge_id": "...", "password": "..." }` |
| `POST` | `/api/telegram/qr/start` | 发起或刷新默认主账号 Telegram QR 扫码登录挑战（独立限流 5 次/分） | 首次生成用 `{ "phone": "+XX XXXXXXXXX" }`，刷新用 `{ "refresh_challenge_id": "..." }` |
| `GET` | `/api/telegram/qr/status` | 轮询默认主账号 QR 扫码登录状态 | `challenge_id` |
| `POST` | `/api/telegram/qr/password` | 提交 2FA 云端密码完成默认主账号 QR 扫码登录 | `{ "challenge_id": "...", "password": "..." }` |
| `POST` | `/api/telegram/challenge/cancel` | 取消默认主账号挂起的登录挑战并释放临时客户端 | `{ "challenge_id": "..." }` |

刷新须由同一浏览器及引导会话提交上一轮最新扫码挑战 ID，服务端沿用已确认的目标并使旧码失效。生成扫码挑战后，`target_phone_hint` 提供目标手机号掩码。首绑扫码状态会通过 `identity_phone_mismatch` 和 `identity_phone_unavailable` 区分账号号码不符与缺少可核验手机号。

具备控制台业务权限时，`startup` 返回 `initializing`、`ready` 或 `degraded`，以及调度器和启用 Bot 的就绪状态。后台初始化结束时，`/ws/progress` 的 `system_event` 事件 `telegram_startup_changed` 通知控制台刷新状态与 Bot 列表。

### 2.2 任务中心与生命周期管理 (`/api/tasks`)

| 方法 | 路径 | 描述 | 关键参数 / 载荷 |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/tasks` | 分页查询任务列表并返回全库级 `status_counts` 统计与嵌套 `progress` | `status`, `task_type`, `account_id`, `q`, `page`, `page_size` |
| `POST` | `/api/tasks/batch` | 批量操作任务（暂停 / 恢复 / 重试 / 取消 / 删除） | `{ "action": "pause", "task_ids": [...] }` |
| `POST` | `/api/tasks/precheck-access` | 任务提交前预检目标频道的可达性与管理员写权限 | `{ "channel": "-100...", "account_id": 1 }` |
| `POST` | `/api/tasks/queue/pause` | 全局暂停任务派发调度队列 | - |
| `POST` | `/api/tasks/queue/resume` | 全局恢复任务派发调度队列。同键下载冲突的任务保持暂停，并在 `skipped_conflicts` 中返回任务 ID 和冲突详情 | - |
| `DELETE` | `/api/tasks` | 清理所有已完成、已失败或已取消的历史任务 | - |
| `GET` | `/api/tasks/{task_id}` | 获取指定任务的详细元数据与单轨嵌套 `progress` | 路径参数 `task_id` |
| `DELETE` | `/api/tasks/{task_id}` | 删除指定任务（手动级联清理日志与非活跃占位槽，解绑保留转发防重账本） | 路径参数 `task_id` |
| `POST` | `/api/tasks/{task_id}/pause` | 暂停正在排队或执行中的任务 | - |
| `POST` | `/api/tasks/{task_id}/resume` | 恢复任务。下载重新入队时如已有其他活跃任务处理相同目标，返回 HTTP `409` 与 `detail.code=duplicate_download_task` | - |
| `POST` | `/api/tasks/{task_id}/cancel` | 取消任务并清理对应执行上下文 | - |
| `POST` | `/api/tasks/{task_id}/retry` | 重试失败、取消或暂停的任务（`BEGIN IMMEDIATE` 原子重置状态与重试计数）。重试下载时如已有其他活跃任务处理相同目标，返回 HTTP `409` 与 `detail.code=duplicate_download_task` | - |
| `PATCH` | `/api/tasks/{task_id}/concurrency` | 运行时动态调整该任务的并发通道数与单连接在途块数 | `{ "concurrency": 8, "upload_in_flight_per_connection": 2 }` |
| `POST` | `/api/tasks/{task_id}/concurrency` | 动态调整任务并发参数（`PATCH` 兼容别名） | `{ "concurrency": 8, "upload_in_flight_per_connection": 2 }` |
| `GET` | `/api/tasks/{task_id}/logs` | 基于自增 `seq` 游标拉取增量执行日志 | `after_seq=0`, `limit=200` |

### 2.3 账号连接池与辅助 Bot 管理 (`/api/accounts` & `/api/bots`)

| 方法 | 路径 | 描述 | 关键参数 / 载荷 |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/accounts` | 查询所有配置的 Userbot 账号状态与配额信息 | - |
| `POST` | `/api/accounts` | 注册新 Userbot 账号，名称与手机号必填 | `{ "account_name": "发布账号", "phone": "+XX XXXXXXXXX", "max_concurrency": 2 }` |
| `PUT` | `/api/accounts/{account_id}` | 更新账号名称、并发上限、启用状态或默认账号标记 | `{ "max_concurrency": 3, "is_default": true }` |
| `DELETE` | `/api/accounts/{account_id}` | 注销并移除指定 Userbot 账号 | 路径参数 `account_id` |
| `POST` | `/api/accounts/{account_id}/send-code` | 向已绑定号码发起验证码登录挑战，返回 `challenge_id` 与投递元数据 | `{}` |
| `POST` | `/api/accounts/{account_id}/resend-code` | 按 Telegram `next_type` 与冷却期重发该账号登录验证码 | `{ "challenge_id": "..." }` |
| `POST` | `/api/accounts/{account_id}/verify-code` | 提交 `challenge_id` 与验证码（及可选 2FA 密码）完成账号授权 | `{ "challenge_id": "...", "code": "12345" }` |
| `POST` | `/api/accounts/{account_id}/password` | 提交 2FA 云端密码续接该账号的挂起登录挑战 | `{ "challenge_id": "...", "password": "..." }` |
| `POST` | `/api/accounts/{account_id}/qr/start` | 发起或刷新指定账号的 Telegram QR 扫码登录挑战 | - |
| `GET` | `/api/accounts/{account_id}/qr/status` | 轮询指定账号的 QR 扫码登录挑战状态 | `challenge_id` |
| `POST` | `/api/accounts/{account_id}/qr/password` | 提交 2FA 云端密码完成指定账号 QR 扫码登录 | `{ "challenge_id": "...", "password": "..." }` |
| `POST` | `/api/accounts/{account_id}/challenge/cancel` | 取消指定账号挂起的登录挑战并释放临时会话 | `{ "challenge_id": "..." }` |
| `POST` | `/api/accounts/{account_id}/refresh` | 主动探活并刷新账号有效性与 Premium 标识 | - |
| `GET` | `/api/bots` | 查询辅助 Bot 轮询池状态与健康指标 | - |
| `POST` | `/api/bots` | 注册新辅助 Bot Token 并进行可用性校验 | `{ "token": "8000000001:AA..." }` |
| `POST` | `/api/bots/ping` | 对 Bot 池全量发起并发健康探活与网络延迟测算 | - |
| `DELETE` | `/api/bots/{bot_id}` | 从轮询池中移除指定 Bot | 路径参数 `bot_id` |

创建账号时请填写带国际区号的手机号。账号已授权并绑定手机号后，更新请求只能沿用原号码或省略该字段。验证码挑战从首次发码成功起最多有效 5 分钟，重发不会延长挑战。若验证码或挑战已过期，请重新发起登录，并提交新验证码与该次发码返回的 `challenge_id`。

探活返回的 `auth_state` 为 `UNAUTHENTICATED` 时，可以稍后再点「探活」。返回 `INVALID` 时，会话已确认失效，请用绑定手机号重新授权。

### 2.4 受限内容极速下载 (`/api/download`)

| 方法 | 路径 | 描述 | 关键参数 / 载荷 |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/download/link` | 提交单个或批量 Telegram 消息直链下载任务 | `{ "links": "https://t.me/c/[频道ID]/[消息ID]", "threads": 16, "account_id": 1 }` |
| `POST` | `/api/download/range` | 按消息 ID 区间批量抓取频道媒体文件 | `{ "channel_identifier": "-100...", "start_id": 1, "end_id": 100, "threads": 16 }` |
| `POST` | `/api/download/inspect-range` | 预检指定区间的消息媒体分布、防重命中与磁盘空间 | `{ "channel_identifier": "-100...", "start_id": 1, "end_id": 100 }` |

下载相册时，如果无法确认相册成员，系统会报告该条下载失败。确认账号访问权限与网络状态后可重试。

提交同一组消息链接，或同一频道与起止 ID 的范围时，如原任务仍在等待、排队、运行或暂停，接口优先返回 HTTP `409`。响应的 `detail.code` 为 `duplicate_download_task`，并提供活跃任务的 `task_id`、`status` 和提示 `message`。

普通消息链接提交的全部目标均有有效的本地文件时，接口返回 HTTP `409`，其中 `detail.code` 为 `already_downloaded`，附带 `message`、`file_count`、`file_names`，以及可唯一确定的历史 `task_id`。包含相册链接的提交会入队展开相册成员，再逐一检查已有文件。这类提交仍受活跃任务防重约束。范围提交只查询本地数据库与磁盘，区间内每个消息 ID 都需要有有效文件才会返回 `already_downloaded`，其中相册成员的历史任务还需已完成。清理任务历史后仍保留该完成状态的证据。范围预检返回 `history_check_complete`，标识全部消息批次及本地下载历史是否检查完成。两种提交均接受 `force`（默认 `false`）。设为 `true` 时忽略其他任务的下载历史，活跃任务防重仍然生效。同一强制任务恢复或重试时会复用该任务已完成且仍有效的本地文件。

范围预检的 `already_downloaded_count` 按有效本地文件逐条统计。`all_media_downloaded` 为 `true` 表示预检已完整检查，范围内有媒体，且每条媒体都有有效文件。相册成员还需要历史任务完成的证据，界面据此提示全量已下载。

### 2.5 云端消息转发与覆盖账本 (`/api/forward`)

| 方法 | 路径 | 描述 | 关键参数 / 载荷 |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/forward/channel` | 提交云端频道消息转发任务（支持二分隔离容错与试转；源与目标解析为同一频道时，任务在执行阶段拒绝转发，防自我复制循环） | `{ "source_channel": "...", "target_channel": "...", "start_id": 1, "end_id": 200, "dry_run": false }` |
| `POST` | `/api/forward/inspect` | 探测源与目标频道的互通性与权限状态 | `{ "source_channel": "...", "target_channel": "..." }` |
| `POST` | `/api/forward/estimate` | 预估待转发区间与未覆盖 Gap | `{ "source_channel": "...", "target_channel": "...", "start_id": 1, "end_id": 50 }` |
| `GET` | `/api/forward/coverage` | 查询源频道到目标频道的已转发覆盖区间账本 | `source_channel`, `target_channel` |
| `GET` | `/api/forward/coverage/gaps` | 分析覆盖账本并返回遗漏的断档区间（Gaps） | `source_channel`, `target_channel`, `start_id`, `end_id` |
| `GET` | `/api/forward/history` | 查询历史转发条目与去重记录 | `limit` |
| `GET` | `/api/forward/receipt/{task_id}` | 获取指定转发任务的执行回执与失败隔离报告 | 路径参数 `task_id` |

### 2.6 本地媒体发布 (`/api/publish`)

| 方法 | 路径 | 描述 | 关键参数 / 载荷 |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/publish/folder` | 提交本地文件夹批量发布任务 | `{ "folder_path": "...", "target_channel": "...", "upload_concurrency": 2, "upload_in_flight_per_connection": 2 }` |
| `POST` | `/api/publish/inspect` | 扫描本地目录，探测账号上传上限和写权限，返回不可读文件数与明细（`unavailable_files_count` / `unavailable_files`） | `{ "folder_path": "...", "target_channel": "..." }` |

### 2.7 PikPak 云端免落盘流式发布 (`/api/pikpak`)

| 方法 | 路径 | 描述 | 关键参数 / 载荷 |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/pikpak/auth/status` | 查询 PikPak 凭据状态（`"logged_in"` / `"not_bound"` / `"unreachable"`） | `force: bool` |
| `POST` | `/api/pikpak/auth/device/start` | 发起 PikPak 官方 OAuth2 设备码登录流程 | - |
| `POST` | `/api/pikpak/auth/device/poll` | 轮询设备码登录授权状态 | `{ "device_code": "...", "interval_s": 5, "max_wait_s": 30 }` |
| `GET` | `/api/pikpak/files/list` | 浏览远程网盘目录结构 | `folder_id`, `page_token`, `refresh` |
| `POST` | `/api/pikpak/inspect` | 探针探测目标频道写权限与网盘选中文件的审核状态/总体积 | `{ "remote_files": [...], "target_channel": "..." }` |
| `POST` | `/api/pikpak/publish` | 提交免落地云端直链流式发布任务 | `{ "remote_files": [...], "target_channel": "...", "dir_tree": {...}, "split_strategy": "vsse_with_fallback" }` |

### 2.8 存储资源与 206 流式预览 (`/api/storage`)

| 方法 | 路径 | 描述 | 关键参数 / 载荷 |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/storage/overview` | 获取存储空间配额、可用容量（`effective_free_bytes`）与统计 | - |
| `POST` | `/api/storage/scan` | 触发存储资产增量扫描与索引同步 | - |
| `GET` | `/api/storage/files` | 分页浏览本地已下载媒体资产列表 | `channel`, `media_type`, `status`, `q`, `page`, `page_size` |
| `DELETE` | `/api/storage/files` | 批量物理删除指定媒体文件（含三重守卫校验） | `{ "file_ids": [...], "paths": [...] }` |
| `POST` | `/api/storage/delete` | 批量安全删除文件（`DELETE /api/storage/files` 兼容入口） | `{ "file_ids": [...], "paths": [...] }` |
| `GET` | `/api/storage/preview/{file_path}` | **HTTP 206 Range 流式在线播放预览**（需 `?ticket=<ticket>`） | 支持标准 `Range: bytes=x-y` 标头 |
| `GET` | `/api/storage/download/{file_path}` | 浏览器下载本地媒体文件（需 `?ticket=<ticket>`） | 路径参数 `file_path` |
| `GET` | `/api/storage/breakpoints` | 查看当前系统的在途断点碎片与元数据文件 | - |
| `POST` | `/api/storage/clean-orphans` | 清理无活跃任务关联的孤儿断点碎片文件（`.part` / `.meta`） | - |
| `GET` | `/api/storage/metrics` | 获取存储治理运行统计指标 | - |
| `POST` | `/api/storage/quota` | 设置存储空间配额上限（GB，`0` 表示不设限） | `{ "quota_gb": 100.0 }` |
| `PUT` | `/api/storage/quota` | 更新存储空间配额上限（与 `POST` 同构） | `{ "quota_gb": 100.0 }` |

### 2.9 频道预设管理 (`/api/presets`)

| 方法 | 路径 | 描述 | 关键参数 / 载荷 |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/presets` | 获取已保存的频道预设列表 | `preset_type` |
| `POST` | `/api/presets` | 创建新的频道预设 | `{ "name": "...", "chat_identifier": "-100...", "preset_type": "target" }` |
| `POST` | `/api/presets/inspect` | 探针校验目标频道标识符的可达性与元数据 | `{ "chat_identifier": "-100..." }` |
| `DELETE` | `/api/presets/{preset_id}` | 删除指定预设 | 路径参数 `preset_id` |

### 2.10 失败审计账本 (`/api/audit`)

| 方法 | 路径 | 描述 | 关键参数 / 载荷 |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/audit/forward-skips` | 审计转发清洗跳过的重复或坏消息清单 | `task_id`, `source_channel`, `reason`, `page`, `page_size` |
| `GET` | `/api/audit/publish-failures` | 审计发布流程中的失败记录（`publish_failures.jsonl`） | `type`, `task_id`, `chat_id`, `page`, `page_size` |
| `GET` | `/api/audit/download-failures` | 审计下载链路中的失败记录（`download_failures.jsonl`） | `type`, `task_id`, `chat_id`, `page`, `page_size` |

### 2.11 系统运行日志与监控 (`/api/logs` & `/metrics`)

| 方法 | 路径 | 描述 | 关键参数 / 载荷 |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/logs/system` | 获取服务端内存环形缓冲区运行日志 | `limit=100`, `level` |
| `GET` | `/api/logs/tasks/{task_id}` | 获取指定任务的执行日志 | 路径参数 `task_id`, `limit=200` |
| `GET` | `/metrics` | 输出 Prometheus 格式的实时性能与任务统计指标 | - |

### 2.12 系统设置中心 (`/api/settings`)

| 方法 | 路径 | 描述 | 关键参数 / 载荷 |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/settings` | 获取全部系统动态设置项分类字典、`all_settings` 与权威 `app_version` | - |
| `PUT` | `/api/settings` | 校验并持久化动态配置，整数设置收到布尔值或浮点数时返回 `400` | `{ "settings": { "max_global_concurrency": 4 } }` |
| `POST` | `/api/settings/verify-transit` | 检查已配置中转群的权限与 Bot 可达性。Userbot 侧检查通过时，空 Bot 池判为 `degraded` | - |

---

## 3. WebSocket 实时事件规范 (`/ws/progress`)

客户端先通过 `POST /api/auth/media-ticket` 获取短期票据，随后在公网 HTTPS 部署中连接 `wss://<host>:<port>/ws/progress?ticket=<ticket>`（本地 HTTP 部署使用 `ws://`），服务端以 JSON 格式推送实时运行事件：

### 3.1 任务进度事件 (`type: "progress"`)
```json
{
  "type": "progress",
  "task_id": "pub_20261030_001",
  "progress": {
    "processed_bytes": 104857600,
    "total_bytes": 524288000,
    "processed_items": 1,
    "total_items": 3,
    "percent": 20.0,
    "speed_mb_s": 15.4,
    "eta_s": 27.2,
    "current_item": "[1/3] movie_part1.mp4",
    "concurrency": 2,
    "concurrency_total": 2,
    "upload_in_flight_per_connection": 1,
    "dc_id": 5
  }
}
```

### 3.2 任务状态流转事件 (`type: "task_update"`)
```json
{
  "type": "task_update",
  "task_id": "pub_20261030_001",
  "status": "completed"
}
```

### 3.3 增量日志广播事件 (`type: "log"`)
```json
{
  "type": "log",
  "task_id": "pub_20261030_001",
  "seq": 15,
  "time": "12:02:10",
  "level": "info",
  "message": "分卷 [1/2] 上传成功，消息 ID: 1045"
}
```

---

## 4. 错误响应格式 (Error Responses)

系统保持 FastAPI 原生错误响应约定：
- **业务与鉴权异常（`400` / `401` / `403` / `404` / `409` / `429` / `500`）**：返回标准 `{"detail": "具体错误原因描述"}`；
- **请求体校验失败（`422 Unprocessable Entity`）**：返回 Pydantic 标准字段级校验错误列表 `{"detail": [{"loc": [...], "msg": "...", "type": "..."}]}`。
