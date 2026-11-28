# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development, Build & Test Commands

### Backend (FastAPI + Telethon, Python 3.12)
```bash
# Sync environment dependencies via uv (automatically manages .venv from pyproject.toml and uv.lock)
uv sync

# Initialize environment configuration (requires TG_API_ID, TG_API_HASH, TG_BOT_TOKENS)
cp .env.example .env

# Start the server (defaults to 127.0.0.1:8500, auto-probes up to +20 ports, acquires data/tg_service.lock)
uv run python run.py
uv run python run.py --host 0.0.0.0 --port 8500
uv run python run.py --version
```

### Frontend (Vue 3 + Pinia + Naive UI + Vite, Node 24+, pnpm 11+)
```bash
cd frontend
pnpm install

# Start HMR dev server on http://127.0.0.1:5173 (proxies /api and /ws to 127.0.0.1:8500)
pnpm run dev

# Regenerate TypeScript SDK from OpenAPI snapshot (tests/api/openapi_snapshot.json)
pnpm run openapi-ts

# Frontend checks and tests
pnpm run lint # oxlint static analysis
pnpm run format:check # oxfmt format check (or pnpm run format to auto-format)
pnpm run typecheck # vue-tsc --noEmit with strict template checks
pnpm run test # vitest unit tests (taskNormalizer, taskHelpers, formatters, stores)

# Production build (outputs directly to ../static with emptyOutDir: true)
pnpm run build

# Return to the repository root (backend command blocks below use root-relative paths)
cd ..
```

### Running Tests (pytest, `asyncio_mode = auto`)
```bash
# Full test suite (executed via uv; mirror CI exactly with xdist — ~50s parallel vs ~8min serial)
uv run pytest -q -n auto -m "not benchmark"

# Run a single test file or specific test function
uv run pytest tests/pipeline/test_vsse_engine_contract.py
uv run pytest tests/core/test_scheduler.py -k "test_scheduler"

# Run tests without a local .env file (CI dummy credentials)
TG_API_ID=1 TG_API_HASH=dummy TG_BOT_TOKENS="1:dummy" API_KEY=testkey uv run pytest -q -n auto -m "not benchmark"

# E2E UI tests (Python Playwright, requires E2E_UI=1; default skips in 0.03s; run serially — no -n, xdist masks event-loop deadlocks)
E2E_UI=1 uv run pytest tests/e2e_ui/ -v

# E2E Live tests against real Telegram/PikPak targets (requires E2E_LIVE=1 and .env.e2e; run serially — no -n)
E2E_LIVE=1 uv run pytest tests/e2e_live/ -v
```

### Static Guards & Linters (Run Before Committing)
```bash
# Code style and linting (E, F, W, I, B, UP)
uv run ruff check .
uv run ruff format --check .

# Type checking on definition layers
uv run mypy

# Normative contract guard: unit terminology, concurrency kwargs, config parity, banned symbols
uv run python scripts/lint_standards.py

# AST guard: forbids bare `except ...: pass` or `...` without logging/handling in app/ and run.py
uv run python scripts/lint_bare_except.py

# Documentation & OpenAPI consistency guards (also executed as part of pytest)
uv run python scripts/verify_docs_consistency.py
uv run pytest tests/test_docs_consistency.py

# OpenAPI contract snapshot test
uv run pytest tests/api/test_openapi_snapshot.py

# Security checks matching CI (.github/workflows/security.yml)
uv run bandit -r app/ -ll -s B608
cd frontend && pnpm audit --audit-level=high
```

---

## High-Level Architecture

### 1. Monotonic Layering
- **Dependency direction**: Maintain strictly monotonic downward dependencies (`app/api/` → `app/services/` → `app/core/` → `app/db/`, enforced by `scripts/lint_standards.py`): never import upward, keep `app/api/` from importing `app/db.*` directly (delegate persistence through `app/services/`), and keep `app/db/database` primitives (`get_db`, `db_cursor`, `db_immediate_tx`) encapsulated inside `app/db/*_repository.py`.
- **Domain Repositories & Models**: Database queries are split by domain under `app/db/*_repository.py` (`task_repository`, `publish_repository`, `preset_repository`, `account_repository`, `download_repository`, `forward_repository`, `storage_repository`) alongside `write_pool.py` and `database.py`. Pydantic models are modularized by domain under `app/models/` (`task`, `forward`, `publish`, `pikpak`, `storage`, `auth`, `common`, `logs`, `settings`). All HTTP API routes enforce typed responses via `response_model` locks.
- **Task Strategy Inversion**: `TaskTypeStrategy` and `StrategyRegistry` are defined in `app/core/task_strategy.py` with zero upward dependencies. Concrete task strategies (`DownloadLinkStrategy`, `DownloadRangeStrategy`, `ForwardChannelStrategy`, `PublishFolderStrategy`, `PikPakPublishStrategy`) are implemented in `app/services/task_strategies/` and wired into both `StrategyRegistry` and `TaskScheduler` at startup via `register_default_executors` in `app/services/executor_registry.py`.

### 2. Scheduling, Connection Pool & MTProto Transfer
- **`ClientManager` (`app/core/client_manager.py`)**: Manages multiple Userbot sessions (primary/secondary tiers, stable device fingerprints) and a round-robin Bot pool (`TG_BOT_TOKENS`). Handles per-client `FloodWaitError` cooldown isolation, channel read/write permission probing (cached via `USERBOT_ACCESS_CACHE_TTL`), and runtime account failover. New Userbot accounts require a bound normalized phone number; interactive login challenges (`native_send_code` / `native_verify_code`) are serialized per account via `_verify_lock`, and restored sessions verify identity (`user_id` match, or canonical phone match when `user_id` is unset) before entering `AUTHENTICATED`.
- **`TaskScheduler` (`app/core/scheduler.py`)**: Two-level concurrency scheduler enforcing global concurrency limits, per-account quotas, and a global 32-thread download watergate (`MAX_GLOBAL_DOWNLOAD_THREADS = 32`). Distinguishes `RetryableTaskError` (backoff retry) from `FatalTaskError`. All task creation dispatches are unified through `task_queue` and scheduler polling with immediate `notify_new_task` wakeups, eliminating unmanaged execution bypasses. On startup, `recover_orphaned_tasks` transitions interrupted `running`/`pending`/`queued` tasks to `paused` (or `queued` when `AUTO_RESUME_TASKS_ON_STARTUP` is enabled).
- **`FastTelethon` (`app/core/fast_telethon/` subpackage: `primitives` / `breakpoints` / `sender_pool` / `download_engine` / `upload_engine` / `prefetch` behind a slim facade `__init__.py` exposing the 13-symbol public contract)**: Multi-connection MTProto transfer engine (default 8 download connections, 2 upload connections, per-connection pipelining controlled by `upload_in_flight_per_connection`). Persists chunk-completion bitmaps to `.part`/`.meta` for downloads and `.upload.meta` (under `data/upload_meta/` with 2-hour TTL self-healing) for uploads.
- **`TransitBridgeService` (`app/services/transit_bridge_service.py`)**: Optional silent transit group acceleration (`ENABLE_TRANSIT_ACCELERATION`). Forwards eligible media to `TRANSIT_CHAT_ID` so `1 Userbot + N Bots` can pull chunks concurrently, then deletes the temporary transit messages in `finally`.

### 3. Unified Publish Pipeline & Zero-Disk Stream Splitting
- **`PublishPipeline` (`app/core/pipeline/pipeline.py`)**: Template Method executing a 6-stage lifecycle (pre-flight probe → source normalization → dedup/oversize filtering → transfer loop → batch summary card → post-cleanup/archive) over four orthogonal interfaces in `app/core/pipeline/interfaces.py`:
  - `ItemSource`: `LocalFolderSource` vs `PikPakSnapshotSource`
  - `MediaTransportEngine`: `LocalDiskTransportEngine` vs `PikPakStreamTransportEngine`
  - `CleanupStrategy`: `NoopCleanupStrategy` vs `PikPakStructuredArchiveStrategy` (preserves relative `dir_tree` hierarchy)
  - `NotificationStrategy`: `TelegramChannelNotifier`
- **`PlaceholderSlotCoordinator` (`app/core/pipeline/placeholder.py`)**: Preserves episode ordering when an item fails mid-batch by sending a 16:9 placeholder card (`app/assets/placeholder_poster.jpg` — kept in `app/assets/` because Vite wipes `static/` on build) and later healing it in place via `editMessageMedia` guarded by a CAS slot lock (`placeholder_sent` → `editing`).
- **Dual-Track Playable Stream Splitting (`SPLIT_STRATEGY`)**: Media exceeding Telegram's MTProto part-count limits (standard accounts: 4,000 chunks × 512 KiB = `2000 MiB (1.95 GiB)`; Premium accounts: 8,000 chunks × 512 KiB = `4000 MiB (3.91 GiB)`; split thresholds reserve a 2.5% margin at 1,950 MiB and 3,900 MiB) is split with zero disk writes:
  - **`vsse_with_fallback` (`app/core/vsse_provider.py`, `app/core/iso_bmff/`)**: Pure-Python ISO-BMFF box parser and synthesizer using compact `array.array('I')` tables (<15 MB RAM, 0% CPU) to map HTTP Byte Ranges and synthesize valid 32-byte `ftyp` and `moov` (with audio priming `elst` compensation) headers per volume. Raises `UnsupportedMp4LayoutError` on multi-`mdat`, `mvex`, or non-AAC tracks to automatically fall back to `fmp4_universal`.
  - **`fmp4_universal` (`app/core/fmp4_pipe_uploader.py`, `app/core/keyframe_prober.py`, `app/core/iso_bmff/fragmented.py`, `app/core/stream_splitter.py`)**: `ZeroDiskFmp4StreamUploader` streams FFmpeg (`-c:v copy`, audio to AAC, bounded by `asyncio.Semaphore(2)`) to `pipe:1` and uploads 512 KiB chunks out-of-order via `saveBigFilePart` (`file_total_parts=-1`, keeping a 1-chunk lookahead buffer so the final chunk declares total parts). Reserves 64 KiB in `file_part=0` to backfill the global `sidx` index and updated durations (`mvhd`, `tkhd`, `mdhd`, `mvex.mehd`). Cut points are snapped to probed keyframes ($S = K$ via `MkvCuesProber` / `Mp4StssProber`) with `-noaccurate_seek` and $+0.18\text{s}$ demuxer overshoot for A/V sync.

### 4. Cloud Forwarding Engine
- **`ForwardService` (`app/services/forward_service.py`)**: Zero-disk cloud-to-cloud message forwarding with 0-RPC purity filtering, album-aware batching (`grouped_id`), $O(\log N)$ recursive bisect fault isolation when a batch hits deleted/invalid message IDs, and an interval coverage ledger (`app/core/intervals.py`) stored in SQLite to compute unforwarded gaps and prevent duplicate forwards.

### 5. SQLite Persistence & Single-Writer Pool
- **Write Serialization & Transactions (`app/db/write_pool.py`, `app/db/database.py`)**: SQLite runs in WAL mode with `PRAGMA foreign_keys = ON`, `PRAGMA busy_timeout = 5000`, and thread-local `_CachedSQLiteConnection`. Write operations use a three-tier model: single-statement repository writes via `db_cursor(commit=True)`, multi-statement atomic transactions via `db_immediate_tx` (`BEGIN IMMEDIATE`), and high-frequency fire-and-forget heartbeat/progress writes via `submit_db_write` (`ThreadPoolExecutor(max_workers=1, thread_name_prefix="db-write")`).
- **Baseline-Only Schema Policy (`app/db/database.py`)**: Schema policy is baseline-only. `_create_baseline_tables` is the sole schema source; `PRAGMA user_version` must equal `LATEST_VERSION` (=1) at startup or `init_db` refuses startup. Schema changes update the baseline DDL directly with a fresh database initialization; an incremental migration chain will be introduced in future releases.
- **Settings Precedence**: Dynamic rows in SQLite `system_settings` (such as `userbot_upload_threshold_mb`, `max_global_concurrency`, `max_storage_quota_gb`) are the runtime source of truth and override `.env` fallback defaults.

### 6. Gateway Security & Frontend Integration
- **Dual-Layer Auth & Short-TTL Media Tickets (`app/api/__init__.py`, `app/api/auth.py`)**: When `API_KEY` is set, all business routers require both `X-API-Key` header and `X-2FA-Token` (24h HS256 JWT issued after verifying a 6-digit code sent to the Userbot's Saved Messages). Plaintext query-string credentials (`token=`, `api_key=`) are strictly rejected; native browser `<video>` streaming/download (`/api/storage/{preview,download}`) and WebSocket handshakes (`/ws/progress`) authenticate via 60s short-TTL tickets issued by `POST /api/auth/media-ticket` (`?ticket=<ticket>`).
- **Frontend (`frontend/src/`)**: Vue 3 SPA using hash routing (`frontend/src/router/index.ts`), 11 Pinia stores + 1 realtime collaborator (`frontend/src/stores/`), and a single WebSocket multiplex bus (`/ws/progress`) for task progress, speed curves, and incremental task logs (`seq` cursor).

---

## Enforced Repository Guardrails

Automated tests (`tests/test_docs_consistency.py`) and static lint scripts (`scripts/lint_standards.py`, `scripts/lint_bare_except.py`) enforce the following rules across code and markdown files (including root `*.md` files):

1. **Test Sandbox Cleanliness**: `tests/conftest.py` redirects `DATA_DIR`, `DOWNLOAD_DIR`, `PIKPAK_META_DIR`, `LOG_DIR`, and Telethon session creation into `tests/.tmp/`. Tests must never leave `*.session`, `*.session-journal`, or `*.db` files in the repository root.
2. **Environment Variable Parity**: Every environment variable read via `os.getenv` / `_optional_*` in `app/config.py` must be documented in `.env.example` (and vice versa).
3. **Upload Limit Terminology**: In any `.md`, `.py`, `.vue`, `.js`, `.ts`, or `.env.example` file, never write decimal-MB or bare-GB/GiB upload limit shorthands (rejected by `check_upload_limit_precision` in `scripts/lint_standards.py`). Always use exact binary terminology: `2000 MiB (1.95 GiB)` for standard accounts and `4000 MiB (3.91 GiB)` for Premium accounts. Do not reference the legacy file-size constant outside `app/config.py` (use `MAX_FILE_SIZE_MIB` / `PREMIUM_MAX_FILE_SIZE_MIB`).
4. **Upload Concurrency Naming**: Always use the parameter/attribute name `upload_in_flight_per_connection` (never the deprecated `upload_in_flight` or `threads_for_display`).
5. **No Silent Exception Suppression**: Never write `except ...: pass` or `except ...: ...` in `app/` or `run.py`; always log at an appropriate level (`logger.debug`/`warning`/`error`) or handle the error explicitly.
6. **Documentation Integrity**: Do not include `file.py:line` (or `.vue`/`.js`/`.ts`/`.css:line`) line-number references in `README.md`, `CONTRIBUTING.md`, `SECURITY.md`, or `docs/**/*.md`. Every `/api/...` route, HTTP method, and backticked architectural class name (`*Service`, `*Engine`, `*Strategy`, `*Provider`, `*Manager`, `*Scheduler`, `*Coordinator`, etc.) mentioned in `docs/` must exist in the actual FastAPI OpenAPI schema and `app/` AST.
