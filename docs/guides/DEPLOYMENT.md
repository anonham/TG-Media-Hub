# 🚀 TG-Media-Hub 生产部署与容器日常运维指南 (Deployment & Operations Guide)

本文档提供 TG-Media-Hub 的容器化部署（Docker Compose）、裸机虚拟环境部署、HTTPS 反向代理配置（Caddy / Nginx）以及容器数据卷备份、热更新与优雅停机等日常运维指引。

---

## 1. 推荐部署方式：Docker Compose

Docker 是运行 TG-Media-Hub 最推荐的方式，项目提供了多阶段构建的生产级镜像、非特权用户隔离及自动健康检查配置。

### 1.1 准备环境与配置文件

1. 确认已安装 Docker 24+ 与 Docker Compose v2+；
2. 获取项目代码（公开仓库直接 HTTPS 克隆即可）：
   ```bash
   git clone https://github.com/anonham/TG-Media-Hub tg-media-hub
   cd tg-media-hub
   ```
3. 创建生产环境变量文件：
   ```bash
   cp .env.example .env
   ```
4. 编辑 `.env` 文件，填入核心配置项（完整字段参考 [环境变量配置手册](CONFIGURATION.md)）：
   ```ini
   # Telegram API 凭证 (在 https://my.telegram.org 申请)
   TG_API_ID=12345678
   TG_API_HASH=abcdef0123456789abcdef0123456789
   TG_DEFAULT_PHONE=  # 可选，填写自己的国际格式手机号

   # 4 个辅助 Bot Token (逗号分隔，通过 @BotFather 申请)
   TG_BOT_TOKENS=8000000001:AA...,8000000002:AA...,8000000003:AA...,8000000004:AA...

   # 首次绑定或恢复默认 Userbot 前，填入独立生成的随机密钥。
   # 至少 16 个字符、包含至少 6 种不同字符，不能复用 API_KEY。
   TG_BOOTSTRAP_TOKEN=

   # 控制台安全访问保护 (容器/公网/反代入口必须设置: 留空时控制台整体锁定并提示配置;
   # 历史别名 TG_API_KEY 已移除, 残留该变量且未设置 API_KEY 时启动直接报错要求改名)
   API_KEY=your_secure_password_here
   CONSOLE_REQUIRE_API_KEY=true
   SECRET_KEY=your_jwt_signing_key_here

   # Compose 显式设置容器内 HOST=0.0.0.0 和 CONSOLE_REQUIRE_API_KEY=true。
   PORT=8500

   # 超限大文件流式切片策略 (可选: fmp4_universal | vsse_with_fallback，缺省 vsse_with_fallback)
   SPLIT_STRATEGY=vsse_with_fallback
   ```

### 1.2 启动与常用运维指令速查

| 操作需求 | 对应执行命令 |
| :--- | :--- |
| **构建并后台启动/更新服务** | `docker compose up -d --build` |
| **修改 `.env` 后重新注入生效** | `docker compose up -d` *(切勿用 `restart`，`restart` 不会重读 `.env`)* |
| **查看容器健康状态与端口** | `docker compose ps` |
| **查看实时滚动日志 (后 50 行)** | `docker compose logs -f --tail=50 tg-media-hub` |
| **安全停止并退出容器** | `docker compose down` |
| **进入容器内部终端排查** | `docker exec -it tg-media-hub /bin/bash` |
| **查看容器资源占用 (CPU/内存)** | `docker stats tg-media-hub` |
| **清理废弃旧镜像层释放磁盘** | `docker image prune -f` |

---

## 2. 裸机直接部署 (Bare-Metal / Virtual Environment)

适用于已有 Python 环境的 VPS（如 Debian 12 / Ubuntu 22.04 / Rocky Linux 9）或本地开发机。

### 2.1 环境要求
- **操作系统**：Linux (推荐) 或 Windows 10/11
- **Python**：3.12+ (推荐使用 `uv` 自动管理 `.venv`)
- **Node.js**：24+ 与 pnpm 11+ (如需自行从源码重新编译前端)
- **FFmpeg（裸机部署按需安装）**：FFmpeg 5.0+。`fmp4_pipe_uploader.py` 的全能保底分卷切片与封面自适应抽帧依赖 FFmpeg。**官方 Docker 镜像（`Dockerfile` 第三阶段）已内置预装 `ffmpeg` 与 `pikpak` CLI 二进制，开箱即用无需额外修改**；仅在裸机直接部署且需启用 fMP4 切片/抽帧时，需确保宿主机 `ffmpeg` 命令在 `PATH` 中可用。

### 2.2 部署步骤

1. **同步 Python 虚拟环境**：
   ```bash
   # 依赖由 uv 管理 (pyproject.toml + uv.lock 为唯一真源), 自动创建 .venv
   uv sync
   ```
2. **构建前端静态资源 (可选)**：
   ```bash
   cd frontend
   pnpm install
   pnpm run build
   cd ..
   ```
   构建产物将根据 `frontend/vite.config.js` 自动输出至根目录的 `static/` 目录，并由 FastAPI 托管静态路由。
3. **初始化配置并启动**：
   ```bash
   cp .env.example .env
   uv run python run.py
   ```
   服务就绪后，默认监听在 `http://127.0.0.1:8500`。

   **升级已有裸机实例**：如果旧配置保留了 `HOST=0.0.0.0` 或 `HOST=::`，`API_KEY` 留空时连本机回环访问也会锁定。仅在本机使用时，将 `HOST` 改为 `127.0.0.1` 或 `::1`，并将 `CONSOLE_REQUIRE_API_KEY` 设为 `false`（若此前设置为 `true`）；要继续对外监听或使用公网反代则保持该开关为 `true` 并配置强随机 `API_KEY`。修改后重启服务。默认 Userbot 未准入时，部署者仍可先完成受保护的绑定。

---

## 3. 反向代理与 HTTPS 配置 (Caddy / Nginx)

由于 Telegram 会话密钥、Web 2FA 登录凭据在公网传输高度敏感，生产环境**必须挂载 HTTPS 反向代理**，同时配置正确的 IP 透传标头以支持防刷限流。

公网反代部署请在后端 `.env` 启用下列策略。官方 Compose 已显式启用，裸机后端可以继续绑定回环。

```ini
CONSOLE_REQUIRE_API_KEY=true
API_KEY=your_secure_password_here
```

启用后，该实例的所有入口均禁用本机豁免。即使同机反代漏传来源头，业务 API、媒体、WebSocket 和文档仍执行公网策略。`API_KEY` 留空时锁定业务，并保留受保护的首次绑定。`/docs` 和 `/redoc` 仅在本机直连模式开放，公网调用 `/openapi.json` 需带有效 `X-API-Key`。反代仍应透传 `X-Forwarded-For` 等来源头并配置 `TRUSTED_PROXIES`，供客户端 IP 识别和限流使用。

### 3.1 Caddy 示例 (推荐，自动签发 Let's Encrypt 证书)

```caddy
hub.yourdomain.com {
    encode gzip zstd

    # WebSocket 与 REST API 统一反向代理
    reverse_proxy 127.0.0.1:8500 {
        header_up X-Real-IP {remote_host}
        header_up X-Forwarded-For {remote_host}
        header_up X-Forwarded-Proto {scheme}
    }
}
```

### 3.2 Nginx 示例

```nginx
server {
    listen 80;
    server_name hub.yourdomain.com;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl http2;
    server_name hub.yourdomain.com;

    ssl_certificate /etc/letsencrypt/live/hub.yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/hub.yourdomain.com/privkey.pem;

    client_max_body_size 500M;

    location / {
        proxy_pass http://127.0.0.1:8500;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket 协议升级支持
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # 针对长连接与大文件传输调整超时
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
    }
}
```

---

## 4. 容器安全隔离、命名卷备份与跨机迁移

### 4.1 非 Root 安全容器与命名卷 (`Named Volume`) 设计
- **DevSecOps 权限隔离**：`Dockerfile` 内创建了专用无特权用户 `appuser`（`UID/GID = 10001`）并启用 `no-new-privileges:true`；
- **为何推荐命名卷而非宿主机路径直挂**：在 Linux / WSL2 下直接挂载 `./data:/app/data` 会继承宿主机 UID（通常为 `1000` 或 `0`），导致容器内 `UID 10001` 写入报 `PermissionError`。采用命名卷（`tg-media-hub_data:/app/data`）可由 Docker 自动初始化属主权限，且在 WSL2 下直接驻留于 Linux `ext4` 虚拟磁盘，避免跨 NTFS 边界的 SQLite I/O 损耗；
- **持久化目录矩阵**：

| 挂载卷 / 宿主机路径 | 容器内路径 | 描述 | 备份建议 |
| :--- | :--- | :--- | :--- |
| `tg-media-hub_data` (`./data`) | `/app/data` | 核心数据库 `tg_service.db`、所有 `.session` 会话、`.pikpak` 凭证与日志 | 每日定点增量或快照打包备份 |
| `data/upload_meta` | `/app/data/upload_meta` | 分块上传断点记录沙箱（2 小时 TTL） | 崩溃自愈目录，无需长期归档 |
| `tg-media-hub_downloads` (`./downloads`) | `/app/downloads` | 受限频道下载与本地转存媒体文件 | 按业务存储空间规划挂载高速磁盘 |

> [!WARNING]
> SQLite 会话与 `.session` 凭证具有最高账号权限。系统启动时会自动将 `data/` 下所有 `.session` 加固为 `0600` 权限，严禁赋予外部不可信用户读取权限。

### 4.2 全量数据与多账号 Session 无损备份与跨机迁移
利用临时 Alpine 容器直接以只读模式挂载命名卷 `tg-media-hub_data` 即可在线快照打包，**无需停止正在运行的生产容器**：

```bash
# 1. 在源 VPS 宿主机当前目录下生成带时间戳的备份压缩包 (含 tg_service.db 与全部 Userbot/Bot session)
docker run --rm \
  -v tg-media-hub_data:/data:ro \
  -v "$(pwd):/backup" \
  alpine tar czf /backup/tg_media_hub_backup_$(date +%Y%m%d_%H%M%S).tar.gz -C /data .
```

在新服务器还原并自动修正 `appuser (UID 10001)` 属主权限：
```bash
# 1. 在新 VPS 创建空命名卷并解压还原，同时修复 UID 10001 与 0600 权限
docker volume create tg-media-hub_data
docker run --rm \
  -v tg-media-hub_data:/data \
  -v "$(pwd):/backup" \
  alpine sh -c "tar xzf /backup/tg_media_hub_backup_*.tar.gz -C /data && chown -R 10001:10001 /data && chmod 600 /data/*.session /data/*.db"

# 2. 启动新服务器容器（务必先在旧 VPS 执行 docker compose down 防止跨 IP 冲突！）
docker compose up -d
```

---

## 5. 代码平滑升级、动态热扩容与优雅停机

### 5.1 代码热更新与镜像版本管理
1. **拉取远程更新并无损重建**：
   ```bash
   git pull
   docker compose up -d --build
   docker image prune -f
   ```
2. **彻底销毁挂载卷并从零重构 (Clean Rebuild)**（仅在需要清空所有历史库与 Session 时使用）：
   ```bash
   docker compose down -v
   docker compose up -d --build --force-recreate
   ```

直接使用 ASGI 服务器启动 `app.main:app` 时，应用也会在启动阶段校验 Telegram 凭证并锁定数据目录。停机时，应用完成清理后再释放锁。

### 5.2 Web 控制台动态增删 Bot 的零停机热生效
连接池 `ClientManager` 具备内存状态与 SQLite `tg_bots` 表的实时双向绑定：
- 在控制台【账号与 Bot】点击【添加 Bot】后，后端即时完成 `TelegramBotClient` 实例化、`connect()` 握手与 `get_me()` 校验，自动挂载入内存轮询队列；
- 删除 Bot 时先优雅断开连接并移出轮询池，在途任务自动平滑转移至备用 Bot 或 Userbot，**全程无需重启 Docker 容器**。

### 5.3 四段式优雅退出与容器停止超时 (`stop_grace_period`)
系统在收到 `SIGINT`/`SIGTERM` 时按严格顺序收割：
1. 向活跃任务发出让位安全暂停（`paused`）通知并保留断点；
2. 调度器收割存活任务协程（预算 `GRACEFUL_STOP_TIMEOUT` 默认 10s，加 5s 兜底 cancel）；
3. 排空专用 DB 写线程池（`drain_db_write_pool`），确保未决进度全部落盘；
4. 执行最终 SQLite WAL 检查点刷盘并断开所有客户端。

`docker-compose.yml` 已配置 `stop_grace_period: 30s`（大于 Uvicorn 优雅停机预算 18s），避免 Docker 默认 10 秒 `SIGKILL` 腰斩 WAL 刷盘。若使用 `docker stop` 手动停止容器，请指定 `-t 30`：
```bash
docker stop -t 30 tg-media-hub
```
