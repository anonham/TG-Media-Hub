# 安全策略 (Security Policy)

## 支持版本

| 版本 | 支持状态 |
| :--- | :--- |
| 1.0.x | ✅ 正在支持 |

## 如何报告漏洞

**请勿以公开 Issue 报告安全问题。**

主渠道: GitHub 私密安全建议 —— 仓库页 **Security → Advisories → Report a vulnerability**。

备用渠道: 发送邮件至 [Kabiruham@proton.me](mailto:Kabiruham@proton.me)（如无法使用 GitHub 私密建议）。

上报内容建议包含: 影响面描述、最小化复现步骤、涉及版本或 commit、部署方式 (Docker Compose / 本地 Python)、已脱敏的日志片段。请勿附带真实 API ID/Hash、Bot Token、手机号或 `.session` 文件。

## 响应承诺

- **72 小时内**确认收到报告
- **7 天内**给出评估结论 (接受 / 拒绝 / 需补充信息) 与初步影响分级
- 修复发布后在对应 Security Advisory 中说明; 是否公开致谢由报告者决定。

## 安全设计基线

以下为本项目已有的安全机制, 评估报告时可作为参照 (均可在代码中溯源):

- **双层鉴权与短时效票据**: 配置 `API_KEY` 后业务路由强制 `X-API-Key` + Web 2FA 会话 JWT (`X-2FA-Token`)；媒体流播放与 WebSocket 握手全面切换为 `POST /api/auth/media-ticket` 签发的 60s TTL 可重复消费票据（兼容 `<video>` 分段播放），消除 URL 明文凭据传输 (`app/api/auth.py`)。
- **本机免认证边界与公网锁定**: 公网反代须设置 `CONSOLE_REQUIRE_API_KEY=true`，官方 Compose 已启用。开启后整个实例按公网策略认证，覆盖回环对端与缺失来源头的请求。空 Key 锁定业务，保留受保护的首次绑定和健康检查。`/metrics` 与公网 `/openapi.json` 须携带 `X-API-Key`。`/docs` 和 `/redoc` 仅在本机直连模式开放。
- **可信反代白名单**: `TRUSTED_PROXIES` 限制哪些代理可提供 `X-Forwarded-For`，显式拒绝设为 `*` (`run.py`)。来源头用于识别客户端 IP 和限流。本机豁免要求数字回环对端，存在转发标记即取消豁免。
- **SQL 参数绑定**: 数据层全部使用占位符参数查询, 无字符串拼接 SQL (`app/db/`)。
- **路径沙箱**: 发布源目录拦截 `..` 逃逸、系统敏感目录与凭据目录 (`app/api/publish.py`); 存储端点仅允许 `downloads/` 内路径 (`app/services/storage_service.py`)。
- **非特权容器**: `appuser` (UID/GID 10001) 运行并启用 `no-new-privileges` (`Dockerfile`、`docker-compose.yml`)。
- **CI 六道安全门**: Gitleaks / Semgrep / Bandit / pip-audit / pnpm audit / Hadolint (`.github/workflows/security.yml`)。

## 不受理范围

- 需要持有**你本人有效 Telegram 凭证** (API ID/Hash、`.session`、Bot Token) 才能实施的「账号级风险」(如凭证泄露后他人操作账号) 属账号安全与 [Telegram ToS](https://telegram.org/tos) 范畴, 不在本项目漏洞响应流程内; 请立即前往 Telegram 官方设置终止全部会话并更换凭证。
- 针对自部署实例的拒绝服务 (DoS), 以及需要物理访问或已完全控制宿主机的攻击链。
- Telegram 服务端协议行为本身的问题。
