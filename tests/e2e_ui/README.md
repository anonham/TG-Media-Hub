# E2E UI 自动化回归套件 (Playwright + Python)

本目录为 tg-media-hub Web 控制台的端到端自动化测试套件，基于 Python `playwright.async_api` 与 `pytest-asyncio` 驱动。

## 1. 运行纪律与门禁说明

- **门禁控制**: 本套件默认不进入 CI 流程。执行 `uv run pytest` 时，收集阶段会自动将本目录全量用例标记为 `skip`，避免对常规单元测试造成耗时与环境污染。
- **单实例串行纪律**: UI 测试依赖独占的本地端口（8500 / 5173）与全局单实例锁，严禁通过 `pytest-xdist`（`-n` 参数）并发执行。
- **离线与缓存优先**: 浏览器内核优先复用本地已缓存的 Chromium 二进制（Windows: `%LOCALAPPDATA%\ms-playwright`，Linux: `~/.cache/ms-playwright`，macOS: `~/Library/Caches/ms-playwright`），不触发网络下载。

## 2. 触发方式与环境变量

在仓库根目录下运行：

```bash
# 1. 执行全量 UI 测试 (默认无头 Headless 模式)
E2E_UI=1 uv run pytest tests/e2e_ui/ -v

# 2. 运行单模块 (例如核心导航与表单保持)
E2E_UI=1 uv run pytest tests/e2e_ui/test_ui_core.py -v

# 3. 开启浏览器可视化窗口 (有头模式，便于实时调试观察)
E2E_UI=1 E2E_UI_HEADLESS=0 uv run pytest tests/e2e_ui/ -v

# 4. 指定外部前端/后端地址或显式指定测试频道 (默认从 .env 读取 TEST_CHANNEL_LINK)
E2E_UI=1 E2E_UI_BASE_URL=http://127.0.0.1:5173 E2E_UI_TARGET_CHAT=-100xxxxxxxxxx uv run pytest tests/e2e_ui/ -v
```

## 3. 测试用例矩阵与映射关系

所有用例断言语义与选择器严格以 `docs/testing/E2E_GUIDE.md` §4 为真源：

| 用例编号 | 测试文件 | 目标场景 | 环境分级 |
|---|---|---|---|
| E2E-UI-01 | `test_ui_core.py` | 全局导航与 8 大页面健康冒烟 | 本地-dummy |
| E2E-UI-02 | `test_ui_tasks.py` | 任务中心双量纲进度 (条目+字节单调性) | 本地-live |
| E2E-UI-03 | `test_ui_tasks.py` | 速度/ETA/通道矩阵/DC 徽标 + 并发调节与 REST 复核 | 本地-live |
| E2E-UI-04 | `test_ui_pikpak.py` | FloodWait 倒计时横幅闭环 | 固定 SKIP (自然不可触发) |
| E2E-UI-05 | `test_ui_pikpak.py` | PikPak 双速率仪表盘与抽屉双曲线 | 固定 SKIP (网盘隐私红线) |
| E2E-UI-06 | `test_ui_tasks.py` | current_item 滚动与多任务抽屉状态隔离 | 本地-live |
| E2E-UI-07 | `test_ui_tasks.py` | 任务抽屉三 Tab 懒加载与保活 | 本地-live |
| E2E-UI-08 | `test_ui_tasks.py` | 日志终端关键词过滤/级别过滤与导出防重入 | 本地-dummy |
| E2E-UI-09 | `test_ui_tasks.py` | 创建页错误面契约与表单保持 | 本地-dummy |
| E2E-UI-10 | `test_ui_storage_settings.py` | 存储页跨页勾选/覆盖条/批量删除对话框取消安全 | 本地-dummy |
| E2E-UI-11 | `test_ui_storage_settings.py` | 设置中心六标签保存流、REST 复核与非法值钳制 | 本地-dummy |
| E2E-UI-12 | `test_ui_pikpak.py` | PikPak 工作台策略选择器显隐与提交载荷契约 | 固定 SKIP (网盘隐私红线) |
| E2E-UI-13 | `test_ui_core.py` | 创建表单 KeepAlive 与预设/账号选项一致性 | 本地-dummy |
| E2E-UI-14 | `test_ui_core.py` | 主题切换与侧栏折叠状态刷新持久化 | 本地-dummy |
| E2E-UI-15 | `test_ui_mocked.py` | 默认 Userbot 引导准入 OTP 闭环（引导屏 → 引导密钥 → OTP → 准入） | Mocked |
| E2E-UI-16 | `test_ui_mocked.py` | 公网控制台锁定双通道（503 触发 / 重检保持 / 修复解锁） | Mocked |
| E2E-UI-17 | `test_ui_mocked.py` | 扫码登录通道（QR 手机号起挑战 / 本地 SVG / 2FA 密码 / 准入） | Mocked |
| E2E-UI-18 | `test_ui_mocked.py` | localStorage 记忆凭据注入 `X-API-Key` + `X-2FA-Token` 双 Header | Mocked |
| E2E-UI-19 | `test_ui_tasks.py` | 真实后端移动端 (≤768px) 任务中心卡片流/抽屉/Dashboard 零溢出 | 本地-live |

## 4. 失败现场与调试证据

当测试用例执行失败时，夹具将自动收集失败现场并归档至：
`tests/.tmp/playwright_ui/<test_name>_<timestamp>/`
- `failure.png`: 失败发生时的全屏渲染截图。
- `trace.zip`: Playwright 完整的执行轨迹追踪包（可通过 `uv run python -m playwright show-trace trace.zip` 可视化复盘）。
- 该临时目录受 `.gitignore` 保护，并在每次执行 pytest 前被自动化夹具清扫，零污染 Git 工作区。
