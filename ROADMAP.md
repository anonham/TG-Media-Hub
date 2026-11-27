# 🗺️ TG-Media-Hub Open Source Roadmap

> **Target Version**: v1.0.0 (Current Release) & Beyond
> **Repository**: [TG-Media-Hub](https://github.com/anonham/TG-Media-Hub)
>
> **License**: MIT License

---

## 🎯 Project Vision & Mission

**TG-Media-Hub** 是一个专为海量媒体内容打造的高性能 Telegram 资源中转、备份、下载与流式发布基础设施。
核心设计理念是**“吞吐极致、安全隔离、零内存泄漏、高可用容灾”**：
- **混合通道加速**：自研 FastTelethon 协议栈，结合多 Userbot 联合并发与辅助 Bot 借壳中转技术，突破 Telegram 单连接限速瓶颈
- **全格式与大文件**：原生支持 2000 MiB (1.95 GiB) (标准账号) 与 4000 MiB (3.91 GiB) (Telegram Premium) 超大文件切片传输与自动占位槽自愈
- **云端直通**：深度集成 PikPak 云盘，实现免下载落地（Zero-Disk Storage）的 CDN 直链实时转存与分发
- **企业级高可用**：SQLite WAL + 线程级单写连接池，保证高并发读写零锁表，配合 Web 2FA 登录态与滑动窗口限流。

---

## 📅 Version Milestones

### ✅ v1.0.0 - Initial Open-Source Release (Current Release)
- **核心架构与并发加速**：
  - [x] 基于 FastAPI + Telethon 的统一异步驱动底座与 `TaskScheduler` 核心调度引擎（状态机：`pending` -> `queued` -> `running` -> `completed` / `failed` / `paused` / `cancelled`）
  - [x] 多账号管理与协同加速：支持主 Userbot、多 Userbot 联合并发、辅助 Bot 轮询池与静默中转群借壳加速架构
  - [x] **FastTelethon 并发重构**：多 DC 并发 TCP 链接池与局部断点续传沙箱（`.part` / `.meta` / `.upload.meta`）
  - [x] 受限频道媒体批量抓取、静态目录本地发布与零落盘云端频道转存（含二分故障隔离与区间覆盖账本）。
- **零落盘流媒体与可播分卷切片**：
  - [x] **PikPak 云端流式发布**：零磁盘占用边下边传架构（Zero-Disk Direct Streaming）、自适应 HTTP Range 预拉取队列、L1 环形缓存与 CDN 直链 JIT 续期
  - [x] **超限大文件流式可播放切片引擎**：`ZeroDiskFmp4StreamUploader` 全能保底通道（全局 `sidx` 回填）+ `MkvCuesProber` 关键帧探测，以及纯 Python VSSE 极速主通道（`stss` 重映射与 `co64` 替换）
  - [x] 统一 `PublishPipeline` 编排引擎、`StrategyRegistry` 策略注册中心与 `PlaceholderSlotCoordinator` 占位海报卡原地编辑自愈（详见 [流媒体中继与可播分卷切片引擎](docs/architecture/STREAMING_AND_SPLITTING.md)）。
- **数据库治理与网关安全**：
  - [x] SQLite WAL 模式、三层写并发治理（单写线程池 + `BEGIN IMMEDIATE` 事务）、`task_logs` 独立分表与 Baseline-Only Schema 规范（`LATEST_VERSION = 1`）
  - [x] 双层安全鉴权（`X-API-Key` + Web 2FA HS256 JWT）、`POST /api/auth/media-ticket` 短时效媒体票据、可信反代白名单与非特权容器隔离
  - [x] `run.py` CLI 参数解析（`--host`、`--port`、`-V/--version`）与拟人化设备指纹管理。
- **前端工程化与自动化测试体系**：
  - [x] 基于 Vue 3 + Pinia + Naive UI + Vite 的现代控制台，集成 `@hey-api/openapi-ts` 强类型 SDK 与 `oxlint` / `oxfmt` / `vue-tsc` / `vitest` 质量门禁
  - [x] 全套沙箱隔离单测、Python Playwright UI 测试（`tests/e2e_ui/`，`E2E_UI=1`）与真链路 Live 套件（`tests/e2e_live/`，`E2E_LIVE=1`）。

---

## 🚀 Future Roadmap

### 🔮 v1.1.0 - Storage Quota & Automation Pipeline (In Active Planning)
- [ ] **存储配额硬性拦截与自动驱逐 (Storage Quota Hard Enforcement)**:
  - 调度器与下载/发布执行器前置校验磁盘已用空间与配额配置，超额任务前置阻断并抛出结构化超额异常
  - 提供可配置的 LRU 历史缓存与已发布归档文件自动清理策略。
- [ ] **Webhook 与即时通知**：支持任务完成/失败推送到 Telegram Bot、Discord、Webhook、Bark 等渠道
- [ ] **RSS / 频道监听自动抓取**：指定频道新消息监听自动加入转存队列
- [ ] **多云存储后端扩展**：
  - 支持阿里云盘、115 网盘、Google Drive 的 Direct Stream 插件化接入
- [ ] **多节点集群与分布式 Worker**：
  - 控制台与下载/发布 Worker 节点分离，支持多机器横向扩展
- [ ] **高级媒体预处理管线**：
  - 集成 FFmpeg 硬件加速自动转码（AV1 / HEVC）、自动提取精彩片段视频缩略图（Video Preview Thumbnails）。

### 🌌 v2.0.0 - Unified Cloud Media Orchestrator (Vision)
- [ ] **WebRTC 实时在线视频预览**：无需下载即在控制台在线点播 Telegram / PikPak 媒体
- [ ] **智能去重与媒体指纹**：基于感知哈希（pHash）与音频指纹的多频道跨来源智能去重
- [ ] **开放插件生态系统（Plugin SDK）**：允许社区自定义抓取解析器与发布模板。

---

## 🤝 Contributing & Feedback

我们欢迎社区参与贡献！
- 如果您有新的功能需求或架构改进建议，请在 [GitHub Issues](https://github.com/anonham/TG-Media-Hub/issues) 中提交 Feature Request
- 参与代码贡献请阅读 [贡献与开发指南](CONTRIBUTING.md) 与 [端到端验证指南](docs/testing/E2E_GUIDE.md)。
