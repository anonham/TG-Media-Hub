# 🛠️ 生产故障排查与风控应对手册 (Troubleshooting & Risk Control Runbook)

本文档汇总 TG-Media-Hub 在 VPS / Docker 生产环境中常见的 MTProto 会话冲突、Telegram 限频风控、私有频道实体解析、容器权限与低配服务器调优方案。

---

## 1. MTProto Session 跨 IP 冲突与注销恢复 (`AUTH_KEY_DUPLICATED`)

### 1.1 现象与底层原理
- **现象**：当本地电脑与远端 VPS **共用同一份 `.session` 文件** 并同时启动时，两端几乎瞬间报错下线，随后再次启动均提示 `AUTH_KEY_UNREGISTERED (401)`。
- **MTProto 规范原理**：
  1. `.session` 文件中存储的是客户端与 Telegram 数据中心协商的唯一认证密钥（`AuthKey`）与单调递增消息序列号（`msg_seqno`）；
  2. Telegram 协议严格规定：**一个 `AuthKey` 仅代表一个独立的物理设备端点**；
  3. 当服务端检测到同一个 `AuthKey` 在极短时间内从两个不同的公网 IP 发送序列号冲突的加密报文时，立即触发 `AUTH_KEY_DUPLICATED` 并将该 `AuthKey` **永久注销（Revoke）**。

### 1.2 恢复与规范操作
1. **清理失效的旧 Session 文件并重启**：
   ```bash
   docker exec tg-media-hub bash -c "rm -f /app/data/*.session* /app/*.session*"
   docker compose up -d
   ```
2. **重新获取验证码登录**：访问 Web 控制台，重新输入手机号和验证码生成全新的 `AuthKey`。
3. **铁律**：同一 `.session` 迁移切换时，必须先在旧节点执行 `docker compose down` 确认停机后，再启动新节点；若需双端同时在线，请分别执行独立验证码登录生成各自的 Session。

---

## 2. Telegram API `FloodWait` 限频风控与熔断避坑

### 2.1 触发原因剖析
1. **安全惩罚与冷静期**：账号若刚经历跨 IP 冲突注销并立即重登，或新创建的 Session 在前 30~60 分钟冷启动期内高频请求，会被 Telegram 下发 ~30 分钟（`1800s+`）的全局观察期；
2. **风控挂载维度**：Telegram 的写操作（如 `messages.forwardMessages` / `sendMedia`）限频挂载在 **用户账号 (User ID) 与目标频道** 维度，单纯更换 `.session` 文件无法绕过服务端配额。

### 2.2 系统自愈机制与排障准则
- **内置冷却隔离与故障转移**：`ClientManager` 与自适应节流器（`AdaptivePacer`）捕获 `FloodWaitError` 后，会自动将该客户端置入冷却隔离（休眠 `e.seconds + 2` 秒），并将后续流量自动切换至健康备用 Userbot 或辅助 Bot 轮询池；
- **切勿强行杀容器重试**：遭遇 `FloodWait` 时**绝对不要重启容器强行重发**，静待控制台倒计时横幅归零即可自动恢复，防止风控升级为 24 小时封禁；
- **调优建议**：配置 4 个以上辅助 Bot Token 分担小文件与借壳中转负载；在发布设置中将 `delay_seconds` 调至 `1.5s ~ 3.0s`；
- **本地 0-RPC 清洗免风控**：云端转发的 `MessagePureFilter.is_forwardable` 过滤属于纯本地内存计算（0 RPC），绝不消耗 Telegram API 配额。

---

## 3. 私有频道实体解析失败与 IP 版权封锁鉴别 (`Access Hash` vs `Geo-blocking`)

当远端 VPS 无法访问特定私密群组/频道（如报错 `Could not find the input entity for PeerChannel`），而本地设备能正常访问时，请按下表定性排查：

| 错误类型与特征 | 根本原因 | 排查方向与解决对策 |
| :--- | :--- | :--- |
| **`ValueError: Could not find the input entity for PeerChannel(...)`** | **Telethon Session 缺少 `access_hash` 索引**（非 IP 封锁） | Telethon 访问私有频道必须持有 `access_hash`。新挂载的 `.session` 若未缓存该群组，代码会自动调用 `client.get_dialogs(limit=50)` 刷新近期对话。若该群组长期无消息排在 50 名之外或位于「归档 (Archived)」夹内，在官方客户端点开该频道浏览一下或运行下方全量同步脚本即可。 |
| **`ChannelPrivateError`**（伴随 `restriction_reason: copyright` / `legal`） | **特定国家/机房 IP 地理版权封锁 (Geo-blocking)** | 部分欧洲/特定地区机房 IP 受司法版权管制，需为容器配置代理或切换出口 IP。 |
| **`ChatForbiddenError` / `UserBannedInChannelError`** | **账号被移出频道或无发帖权限** | 在 Telegram 客户端确认当前登录的 Userbot 是否仍在频道内且具备管理员 `post_messages` 权限。 |

### 3.1 容器内全量会话（含归档夹）`Access Hash` 强制同步脚本
当目标冷门频道不在最近 50 个对话中时，可在宿主机执行以下命令遍历全部普通会话与归档夹（`folder=1`），将所有频道的 `access_hash` 写入 `.session` 数据库：

```bash
docker exec -it tg-media-hub python -c "
import asyncio
from app.core.client_manager import client_manager

async def sync_and_check():
    await client_manager.initialize()
    userbot = client_manager.userbot
    if not userbot:
        print('❌ Userbot 未就绪')
        return
    count = 0
    for folder in (0, 1):
        async for _ in userbot.iter_dialogs(limit=None, folder=folder):
            count += 1
    print(f'✅ 已全量扫描并持久化同步 {count} 个会话（含归档夹）的 Access Hash！')

asyncio.run(sync_and_check())
"
```

---

## 4. 公网入口控制台锁定 (`503 console_locked`) 与免认证边界

### 4.1 现象与判定规则
- **现象**：浏览器打开控制台显示「控制台已锁定」页，业务 API 返回 `503` 且 `error_code=console_locked`；WebSocket 握手被拒（关闭码 `1008`）。
- **判定规则**：本机豁免要求 `CONSOLE_REQUIRE_API_KEY=false`、回环绑定、数字回环对端且无转发标记。公网反代和官方 Compose 使用 `CONSOLE_REQUIRE_API_KEY=true`，即使漏传代理头也执行公网规则。受保护的首次绑定、`/api/auth/status`、首页、静态资源与健康检查仍可访问。

### 4.2 排查与解决
1. **容器/公网部署**：在 `.env` 中设置强随机 `API_KEY` 后重启（`docker compose up -d`，勿用 `restart`），随后用 API Key 登录；
2. **本机直跑期望免认证**：确认 `.env` 中 `HOST=127.0.0.1`（而非 `0.0.0.0`）、`CONSOLE_REQUIRE_API_KEY=false`（尤其是曾部署公网反代时），且从 `http://127.0.0.1:8500` 访问；绑定 `0.0.0.0` 的实例即使本机访问也会被锁定（来源不明 fail-closed）；修改后重启服务；
3. **同机公网反代**：设置 `CONSOLE_REQUIRE_API_KEY=true` 和 `API_KEY`。即使后端监听 `127.0.0.1` 也需要这项配置。反代透传 `X-Forwarded-For` 供来源识别和限流使用；
4. **历史别名残留**：启动报错提示 `TG_API_KEY` 已弃用时，将 `.env` 中的 `TG_API_KEY` 改名为 `API_KEY` 后重启。

---

## 5. 容器权限、`docker cp` 注入与环境变量排查

### 5.1 使用 `docker cp` 手动注入 Session 的权限陷阱 (`UID 10001`)
Docker 容器根路径 `/app` 属于临时层（重建即丢），所有 `.session` 必须存放在持久卷目录 `/app/data/` 下。此外，`docker cp` 从宿主机拷入的文件默认继承 `root (UID 0)` 属主，会导致容器内无特权用户 `appuser (UID 10001)` 无法写入 SQLite Session 而报错。**注入后必须修正属主与权限**：

```bash
# 1. 拷贝至持久化 data 目录
docker cp my_userbot.session tg-media-hub:/app/data/userbot_session.session

# 2. 修正为 appuser (UID 10001) 属主与 0600 权限
docker exec -u 0 tg-media-hub chown appuser:appuser /app/data/userbot_session.session
docker exec -u 0 tg-media-hub chmod 600 /app/data/userbot_session.session

# 3. 重启生效
docker compose restart
```

### 5.2 `.env` 重复定义覆盖陷阱与实时生效值核查
- **重复键覆盖**：在编辑 `.env` 时若首尾出现同名变量，Docker Compose 与 `python-dotenv` 遵循**后定义覆盖先定义**规则；且若某项已在 Web 控制台「系统设置」修改过，运行时以 SQLite `system_settings` 表中的动态值为最高优先级；
- **核查容器内真实生效的配置与 Bot 池健康度**：
  ```bash
  # 打印容器内 Python 实际加载的配置值
  docker exec -it tg-media-hub python -c "from app.config import settings; print('HOST=', settings.HOST, 'SPLIT=', settings.SPLIT_STRATEGY)"

  # 触发 Bot 池全量并发 Ping 探活与延迟测算
  docker exec -it tg-media-hub python -c "
  import asyncio
  from app.core.client_manager import client_manager
  async def main():
      await client_manager.initialize()
      for r in await client_manager.ping_bots_detailed():
          print(r)
  asyncio.run(main())
  "
  ```

---

## 6. 低配 VPS（如 2C2G 机械盘）内存与磁盘防爆调优

- **零落盘架构保护**：PikPak 云端流式发布与超限视频双轨切片（`vsse_with_fallback` / `fmp4_universal`）全程通过内存有界队列与 `pipe:1` 传输，媒体字节 0 落盘，避免廉价机械盘 I/O Wait 飙升导致 SQLite 锁死；
- **低内存环境调优**：
  1. 若空闲物理内存不足 512MB，可在 `.env` 中将 `PIKPAK_RING_BUFFER_MB` 从默认的 `50` 调低至 `16`（或设 `PIKPAK_ENABLE_RING_BUFFER=false`）；
  2. 全局并发 FFmpeg 切片进程已由代码级 `asyncio.Semaphore(2)` 硬性封顶，建议将全局并发任务数 `max_global_concurrency` 维持在 `2` 以内，避免多路并发榨干双核 CPU。

---

## 7. Telegram 视频在线播放与拖动进度说明

- **客户端与编码兼容性**：视频发布后会按既有策略保留流式传输属性并补齐时长与展示分辨率元数据，但 Telegram 客户端是否支持在线内联播放（inline playback）以及任意位置拖动进度（seek），取决于终端设备、Telegram 客户端版本、容器封装格式（如 MP4/M4V/MOV/MKV/WebM/AVI/WMV/FLV/TS）以及音视频编码格式（例如部分设备不支持 HEVC/VP9/AV1 硬解或非 MP4 容器的原生流式拖动）。
- **元数据探测与可选 `ffprobe` 依赖**：MP4/MOV/M4V/MKV/WebM 本地发布优先走内置有界容器头解析；AVI/WMV/FLV/TS/M2TS/MTS 及缺失字段补齐会调用本地可选的 `ffprobe`（PikPak 远端非 MP4 仅对已通过严格 HTTP 206 Range 拉取的 128 KiB 有界前缀通过 `pipe:0` 调用，绝不对远端直链直接调用 `ffprobe`）。若宿主机未安装 `ffprobe`，探测自动 fail-open 跳过，不影响正常上传。
- **上传行为说明**：发布链路保持原有无损直传 / 既有分卷策略，不会自动对普通视频执行重封装或转码；若某格式在特定客户端无法直接拖动播放，可下载到本地播放器查看。
