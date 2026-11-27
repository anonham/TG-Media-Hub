# 贡献指南 (Contributing)

感谢关注 tg-media-hub! 在提交 Issue 或 Pull Request 之前, 请先阅读本指南。

## 行为准则与安全上报

- 参与社区即表示同意 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) (含项目附加条款: 禁止传授或协助违反 Telegram ToS / 版权法规的用法)。
- 安全漏洞请**勿**以公开 Issue 报告, 请走 [SECURITY.md](SECURITY.md) 的私密上报渠道。

## 开发环境

| 组件 | 要求 |
| :--- | :--- |
| Python | 3.12+ (Docker 镜像使用 3.12, `python:3.12-slim`) |
| Node.js | 24+ (前端构建; Docker 构建阶段为 `node:24-alpine`) |
| Telegram 凭证 | API ID / API Hash ([my.telegram.org](https://my.telegram.org) 申请) 与至少 1 个 Bot Token |

```bash
# 1. 前端构建 (产物输出至 ../static, 见 frontend/vite.config.js)
cd frontend
pnpm install
pnpm run build
cd ..

# 2. 后端依赖 (uv 管理, 唯一真源 pyproject.toml + uv.lock)
uv sync

# 3. 配置
cp .env.example .env # 填写 TG_API_ID / TG_API_HASH / TG_BOT_TOKENS

# 4. 启动 (默认 127.0.0.1:8500, Swagger 见 /docs)
uv run python run.py
```

## 运行测试

```bash
uv run pytest -q -n auto -m "not benchmark"
```

> 与 CI 完全同构 (xdist 并行约 50s，串行约 8 分钟)。e2e_ui / e2e_live 门禁套件保持串行——有状态真实后端运行，xdist 会掩蔽事件环死锁。

- pytest 配置统一收拢至 `pyproject.toml`；`tests/conftest.py` 为每个用例创建独立临时 SQLite 数据库（隔离于 `tests/.tmp/` 沙箱），不会污染真实 `data/tg_service.db`。**严禁任何测试在仓库根目录遗留 `*.session`、`*.session-journal` 或 `*.db` 文件**。
- 缺少 Telegram 凭证时 `import app.config` 不再报错 (缺失字段置 None/空列表), 但 pytest 全量用例需要凭证 (既有用例依赖默认凭证回退链): 请先完成上面的配置步骤, 或按 CI 的做法在环境变量注入哑凭证 (`TG_API_ID=1`、`TG_API_HASH=dummy`、`TG_BOT_TOKENS=1:dummy`、`API_KEY=testkey`)。凭证校验已延迟到启动期 (`run.py` / lifespan 调用 `Settings.validate_required`)。
- **CI 已包含完整的后端与前端自动化硬门禁** (`.github/workflows/test.yml`，push / PR 到 main 触发) —— 测试必须在本地全量通过后再提交。

## 分层架构与编码红线

1. **单向依赖红线**：严格遵循 `app/api/` → `app/services/` → `app/core/` → `app/db/` 单向导入（`app/core/` 绝不导入 `app/services/` 或 `app/api/`；`app/services/` 绝不导入 `app/api/`），由 `scripts/lint_standards.py` 强制校验。
2. **禁止裸 `except` 静默吞错**：`app/` 与 `run.py` 中严禁出现未记录日志或未处理的 `except ...: pass` / `...`（由 `scripts/lint_bare_except.py` 强制校验）。

## 提交前检查清单

- [ ] 后端测试与规范守卫全量通过：
  - `uv run pytest -q -n auto -m "not benchmark"`
  - `uv run ruff check .`
  - `uv run ruff format --check .`
  - `uv run mypy`
  - `uv run python scripts/lint_standards.py`
  - `uv run python scripts/lint_bare_except.py`
  - `uv run python scripts/verify_docs_consistency.py`
  - `uv run bandit -r app/ -ll -s B608`
- [ ] 前端改动时五连门禁全量通过：
  - `cd frontend && pnpm run openapi-ts && pnpm run lint && pnpm run format:check && pnpm run typecheck && pnpm run test && pnpm run build`
- [ ] README 与技术文档同步: API 变更更新「API 接口索引」, 环境变量变更更新「环境变量配置说明」; 文档中的可验证声明 (默认值/路径/上限) 须能在代码中找到出处
- [ ] 不引入硬编码凭据 (API ID/Hash、Bot Token、手机号、`.session` 文件)
- [ ] 数据层新增查询一律使用参数绑定占位符, 禁止字符串拼接 SQL

CI 门禁矩阵 (push / PR 到 main 触发): `.github/workflows/test.yml` 全量测试与五连门禁; `.github/workflows/security.yml` 六道安全门 —— Gitleaks 密钥扫描 / Semgrep SAST / Bandit / pip-audit / pnpm audit (≥high 阻断) / Hadolint。

## 数据库 Schema 管理规范 (Baseline-Only 政策)

本项目采用 **Baseline-Only** 架构管理数据库表结构（代码位于 `app/db/database.py`）：
1. **单一事实源**：`app/db/database.py` 中的 `_create_baseline_tables` 是数据库 Schema 的唯一真源，当前基线结构不依赖历史迁移链。
2. **基线版本锁定**：服务启动时校验 SQLite `PRAGMA user_version`，必须恒等于 `LATEST_VERSION` (=1)，否则 `init_db` 会拒绝启动并抛出错误。
3. **开发期变更规范**：修改表结构或索引时，可直接在 `_create_baseline_tables` 中调整基线 DDL，并备份 `data/tg_service.db` 后重新初始化本地数据库。
4. **事务与写入安全**：所有数据操作严禁直接执行裸 SQL 写入，必须通过 `db_cursor(commit=True)`、`db_immediate_tx` 或异步写池 `submit_db_write` 执行。

## Commit 与 Pull Request 规范

- Commit 信息遵循 [Conventional Commits](https://www.conventionalcommits.org/zh-hans/): `feat|fix|docs|style|refactor|perf|test|chore(可选 scope): 描述`, 例如 `fix(publisher): ...`。
- 一个 PR 聚焦一件事; 关联 Issue 请在描述中写 `Closes #N`。
- 中英文均可, 但需清晰描述动机与影响面。
