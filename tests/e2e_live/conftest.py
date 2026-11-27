"""live harness 会话夹具 : 门控 skip + scratch 克隆 + 服务器生命周期 +
只读三通道 (REST / DB / Telethon) + 清理账本。

门控: E2E_LIVE 未设置时本目录用例收集即 skip; xdist worker 内一律 skip
。手动触发流程见 tests/e2e_live/README.md。
"""

import asyncio
import os
import shutil
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from e2e_live.harness import api_client, cleanup, db_readonly, env_gate, scratch_env, server_proc
from e2e_live.harness import tg_reader as tg_reader_mod

_PACKAGE_DIR = Path(__file__).resolve().parent


def pytest_collection_modifyitems(config, items):
    """门控: E2E_LIVE 未设置 → 本目录全部用例收集即 skip; xdist worker 内一律 skip。"""
    if env_gate.is_enabled() and not os.environ.get("PYTEST_XDIST_WORKER"):
        return
    skip_mark = pytest.mark.skip(reason="E2E live 需要 E2E_LIVE=1 且禁用 xdist; 触发流程见 tests/e2e_live/README.md")
    for item in items:
        if Path(str(item.fspath)).is_relative_to(_PACKAGE_DIR):
            item.add_marker(skip_mark)


@pytest.fixture(scope="session")
def live_gate() -> Iterator[None]:
    """双重门禁兜底: 即使绕过收集钩子强制执行, 夹具仍拒绝启动。"""
    if not env_gate.is_enabled():
        pytest.skip("E2E_LIVE 未设置 (默认 skip)")
    if os.environ.get("PYTEST_XDIST_WORKER"):
        pytest.skip("live 套件禁止 xdist")
    yield


@pytest.fixture(scope="session")
def scratch_data_dir(live_gate) -> Iterator[Path]:
    """scratch DATA_DIR: 完整克隆真实 data/ (§4.3); 跑后清理 (F7), KEEP 开启时保留。"""
    dest = env_gate.resolve_scratch_dir()
    scratch_env.clone_source_data(dest)
    yield dest
    if not env_gate.keep_scratch():
        shutil.rmtree(dest, ignore_errors=True)


@pytest.fixture(scope="session")
def live_server(live_gate, scratch_data_dir) -> Iterator[server_proc.LiveServer | server_proc.ExternalServer]:
    """真服务器进程: 冲突守卫 → 自起 → 就绪探测 → 关停; attach 形态见 env_gate.base_url。"""
    external = env_gate.base_url()
    if external is not None:
        server = server_proc.ExternalServer(external)
        server.wait_ready()
        yield server
        return

    host = env_gate.server_host()
    try:
        scratch_env.ensure_no_real_server(host, env_gate.real_server_port())
    except RuntimeError as exc:
        pytest.skip(str(exc))

    server = server_proc.LiveServer(scratch_data_dir, server_proc.find_free_port(host), host)
    server.start()
    try:
        server.wait_ready()
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="session")
def live_api(live_server) -> Iterator[api_client.LiveApiClient]:
    client = api_client.LiveApiClient(live_server.base_url)
    yield client
    client.close()


@pytest.fixture(scope="session")
def live_db(scratch_data_dir) -> Iterator[sqlite3.Connection]:
    """scratch 库只读连接 (F8: 仅 SELECT/PRAGMA, 写操作一律走应用 API)。"""
    conn = db_readonly.connect_ro(scratch_data_dir / "tg_service.db")
    yield conn
    conn.close()


@pytest.fixture
def cleanup_ledger() -> Iterator[cleanup.CleanupLedger]:
    """函数级清理账本 : 目录清理由本夹具兜底; 消息/任务删除由异步用例
    在 finally 中 `await ledger.flush_async(api, reader)` 承担 (断言失败也执行)。"""
    ledger = cleanup.CleanupLedger(keep_messages=env_gate.keep_messages(), keep_dirs=env_gate.keep_scratch())
    yield ledger
    ledger.flush_sync()


def _reader_creds() -> tuple[int, str] | None:
    from app import config as app_config

    s = app_config.settings
    if not s.API_ID or not s.API_HASH:
        return None
    return s.API_ID, s.API_HASH


_WARM_SESSION_NAME = "_live_reader_warm.session"


def _userbot_session_candidates(scratch_data_dir: Path) -> list[Path]:
    """reader 会话候选链: warmed → 默认 userbot 会话 → 其余非 Bot 池 userbot 会话
    (多账号部署下测试频道可能仅对第二账号可见)。"""
    candidates: list[Path] = []
    warm = scratch_data_dir / _WARM_SESSION_NAME
    if warm.exists():
        candidates.append(warm)
    default = scratch_env.find_session_file(scratch_data_dir)
    if default is not None:
        candidates.append(default)
    reserved = {p.name for p in candidates} | {
        "live_reader.session",
        "live_cleanup.session",
        "live_stage.session",
        _WARM_SESSION_NAME,
    }
    for item in sorted(scratch_data_dir.glob("*.session")):
        if item.name in reserved or item.name.startswith("bot_pool"):
            continue
        candidates.append(item)
    return candidates


def _reader_session_source(scratch_data_dir: Path) -> Path | None:
    """reader 会话副本的克隆源: 候选链首项 (warm 优先)。"""
    candidates = _userbot_session_candidates(scratch_data_dir)
    return candidates[0] if candidates else None


@pytest.fixture(scope="session")
def target_chat(live_gate) -> str:
    """测试频道引用 (chat_ref): E2E_LIVE_TARGET_CHAT 优先, 回退 .env TEST_CHANNEL_LINK。"""
    chat_ref = env_gate.chat_ref_from_link(env_gate.target_chat() or "")
    if not chat_ref:
        pytest.skip("未配置测试频道 (E2E_LIVE_TARGET_CHAT 或 .env TEST_CHANNEL_LINK)")
    return chat_ref


@pytest.fixture
async def tg_reader(live_gate, scratch_data_dir):
    """函数级 Telethon 只读端 (§4.5): 会话文件副本独占打开 (避免与服务器进程争
    session SQLite 锁); 每用例独立实例——pytest-asyncio 函数级事件循环间不可复用
    同一 Telethon 客户端。"""
    session_src = _reader_session_source(scratch_data_dir)
    if session_src is None:
        pytest.skip("scratch 无可用 session 文件")
    creds = _reader_creds()
    if creds is None:
        pytest.skip("TG_API_ID/TG_API_HASH 缺失, Telethon 只读端不可用")
    session_copy = scratch_data_dir / "live_reader.session"
    shutil.copy2(session_src, session_copy)
    reader = tg_reader_mod.TgReader(session_copy, *creds)
    yield reader
    await reader.aclose()


@pytest.fixture(scope="session")
def live_cleanup(live_gate, live_api, scratch_data_dir, target_chat) -> Iterator[cleanup.CleanupLedger]:
    """会话级清理账本: setup 消息与各用例产物统一登记, 会话结束时一次性回收
    (消息 → 任务, §3.2 清理口径; scratch 目录由 scratch_data_dir 夹具负责)。"""
    ledger = cleanup.CleanupLedger(keep_messages=env_gate.keep_messages(), keep_dirs=env_gate.keep_scratch())
    yield ledger
    if env_gate.keep_messages():
        ledger.flush_sync()
        return
    session_src = _reader_session_source(scratch_data_dir)
    creds = _reader_creds()
    if session_src is None or creds is None:
        ledger.flush_sync()
        return
    session_copy = scratch_data_dir / "live_cleanup.session"
    shutil.copy2(session_src, session_copy)
    reader = tg_reader_mod.TgReader(session_copy, *creds)

    async def _flush() -> None:
        try:
            await ledger.flush_async(api=live_api, reader=reader)
        finally:
            await reader.aclose()

    asyncio.run(_flush())


@pytest.fixture(scope="session")
def channel_stage(live_gate, scratch_data_dir, target_chat, live_cleanup) -> dict:
    """频道搭台 (session 级, D3 最小发送量): 独立 reader 实例投放 2 个样本媒体 +
    1 条非媒体消息, 供 download_link / download_range / forward 三用例共用;
    消息 id 登记进会话清理账本, 会话结束统一删除。"""
    from e2e_live.harness import samples

    candidates = _userbot_session_candidates(scratch_data_dir)
    if not candidates:
        pytest.skip("scratch 无可用 session 文件 (频道搭台需要真实登录态)")
    creds = _reader_creds()
    if creds is None:
        pytest.skip("TG_API_ID/TG_API_HASH 缺失 (频道搭台需要真实凭据)")
    stage_dir = scratch_data_dir / "stage"
    size1 = samples.write_sample_jpeg(stage_dir / "e2e_live_sample_a.jpg")
    samples.write_sample_jpeg(stage_dir / "e2e_live_sample_b.jpg")

    async def _stage(session_copy: Path) -> dict:
        reader = tg_reader_mod.TgReader(session_copy, *creds)
        try:
            await reader.connect()
            chat = await reader.resolve_chat(target_chat)
            media1 = await reader.send_media(chat, stage_dir / "e2e_live_sample_a.jpg")
            media2 = await reader.send_media(chat, stage_dir / "e2e_live_sample_b.jpg")
            note = await reader.send_text(chat, "E2E-LIVE 非媒体消息 (range 跳过计数锚点)")
            return {
                "chat_ref": target_chat,
                "media1_id": int(media1.id),
                "media2_id": int(media2.id),
                "text_id": int(note.id),
                "media_size": size1,
            }
        finally:
            await reader.aclose()

    from telethon import errors as tg_errors

    stage = None
    chosen_copy = None
    unresolvable = 0
    forbidden: str | None = None
    for candidate in candidates:
        session_copy = scratch_data_dir / "live_stage.session"
        shutil.copy2(candidate, session_copy)
        try:
            stage = asyncio.run(_stage(session_copy))
            chosen_copy = session_copy
            break
        except tg_reader_mod.ChatUnresolvableError:
            # 裸 ID 解析失败 = 该会话的账号不可见测试频道 (多账号部署常态), 试下一个
            unresolvable += 1
            continue
        except tg_errors.ForbiddenError as exc:
            # 该账号可见但无发帖权: 记录后仍试下一账号 (服务器侧可能路由到别的账号)
            forbidden = str(exc)
            continue
    if stage is None or chosen_copy is None:
        if forbidden is not None:
            pytest.skip(
                f"测试频道 {target_chat} 对 userbot 无发帖权限: {forbidden}; "
                "请将 TEST_CHANNEL_LINK / E2E_LIVE_TARGET_CHAT 指向 userbot 为拥有者/管理员的频道 "
                "(见 docs/testing/E2E_GUIDE.md 环境前置)"
            )
        pytest.skip(
            f"无任何 userbot 会话可解析测试频道 {target_chat} (尝试 {unresolvable} 个会话); "
            "请确认 TEST_CHANNEL_LINK 指向 userbot 账号可见的频道"
        )
    # 解析成功即完成实体缓存预热: 留存 warmed 会话供后续 reader/cleanup 副本免重扫
    shutil.copy2(chosen_copy, scratch_data_dir / _WARM_SESSION_NAME)
    for msg_id in (stage["media1_id"], stage["media2_id"], stage["text_id"]):
        live_cleanup.register_message(target_chat, msg_id)
    return stage
