import asyncio
import hmac
import logging
import os
import random
import re
import secrets
import shutil
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telethon import TelegramClient
from telethon.sessions import MemorySession, SQLiteSession

from app.config import secure_file_permissions, settings
from app.core.account_selection import preferred_channel_account, probe_account_access, ranked_candidates
from app.core.connection_manager import connection_manager
from app.core.entity_resolver import is_private_channel_identifier, parse_chat_identifier, resolve_entity
from app.core.session_errors import is_session_invalid_error
from app.db.account_repository import AccountRepository, BotRepository

logger = logging.getLogger(__name__)

PREMIUM_STATUS_TTL_SECONDS = 300.0
CONNECT_VERIFY_TIMEOUT_SECONDS = 15.0
_LOGIN_CHALLENGE_TTL_SECONDS = 300.0
_PHONE_RE = re.compile(r"^\+?\d{5,20}$")


class SessionHandoverRecoveryError(RuntimeError):
    """A pending session handover must be recovered before the account can be removed."""


def normalize_phone_number(raw: str | None) -> str:
    """规范化并校验手机号 (去除空格、连字符、括号，将 '00' 前缀转为 '+' 后必须为可选 '+' 前缀的 5~20 位数字)."""
    if raw is None:
        raise ValueError("手机号不能为空")
    cleaned = str(raw).strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if cleaned.startswith("00"):
        cleaned = "+" + cleaned[2:]
    if not cleaned or not _PHONE_RE.match(cleaned):
        raise ValueError("手机号格式无效，请提供包含国际区号的有效数字号码 (如 +XX XXXXXXXXX)")
    return cleaned


def canonical_phone_number(raw: str | None) -> str:
    """Compare full international phone numbers independently of their prefix formatting."""
    return normalize_phone_number(raw).lstrip("+")


@dataclass(frozen=True)
class LoginBinding:
    user_id: int | None
    phone: str
    session_file: str

    @classmethod
    def from_record(cls, row: dict[str, Any]) -> "LoginBinding":
        user_id = row.get("user_id")
        return cls(
            int(user_id) if user_id is not None else None,
            str(row.get("phone") or ""),
            str(row.get("session_file") or ""),
        )


DEVICE_FINGERPRINT_PROFILES = [
    {
        "device_model": "Desktop",
        "system_version": "Windows 11 Pro 64bit",
        "app_version": "5.4.1 x64",
        "lang_code": "en",
    },
    {
        "device_model": "Desktop",
        "system_version": "Windows 10 Enterprise",
        "app_version": "5.2.2 x64",
        "lang_code": "en",
    },
    {
        "device_model": "MacBook Pro (Apple Silicon)",
        "system_version": "macOS 14.5 Sonoma",
        "app_version": "5.3.0",
        "lang_code": "en",
    },
    {
        "device_model": "MacBook Air",
        "system_version": "macOS 13.6.7 Ventura",
        "app_version": "5.1.4",
        "lang_code": "en",
    },
    {"device_model": "PC 64bit", "system_version": "Ubuntu 22.04.4 LTS", "app_version": "5.0.1", "lang_code": "en"},
    {
        "device_model": "Desktop",
        "system_version": "Windows 11 Home",
        "app_version": "5.1.0 x64",
        "lang_code": "zh-hans",
    },
]


def generate_device_fingerprint(seed: int | str | None = None) -> dict[str, str]:
    """生成专属且合规的拟人化设备指纹档案"""
    if seed is not None:
        idx = abs(hash(str(seed))) % len(DEVICE_FINGERPRINT_PROFILES)
        return dict(DEVICE_FINGERPRINT_PROFILES[idx])
    return dict(random.choice(DEVICE_FINGERPRINT_PROFILES))


def _matches_channel_permission_error(e: BaseException) -> bool:
    name = type(e).__name__
    err_s = str(e).lower()
    return (
        "ChannelPrivateError" in name
        or "ChatAdminRequiredError" in name
        or "ChannelPublicGroupNaError" in name
        or "ChatWriteForbiddenError" in name
        or ("cannot find any entity" in err_s and "peer" in err_s)
        or ("could not find the input entity" in err_s)
        or ("channel is private" in err_s)
        or ("you have been kicked" in err_s)
    )


def is_channel_permission_error(e: BaseException) -> bool:
    """判断是否为频道私有、无权或未加入的权限异常 (沿 __cause__ 因果链穿透包装异常)"""
    visited: set[int] = set()
    cur: BaseException | None = e
    while cur is not None and id(cur) not in visited:
        visited.add(id(cur))
        if _matches_channel_permission_error(cur):
            return True
        inner_cause = getattr(cur, "cause", None)
        if isinstance(inner_cause, BaseException) and id(inner_cause) not in visited:
            cur = inner_cause
            continue
        cur = cur.__cause__
    return False


def _resolve_session_target_sync(raw_session_file: str) -> str:
    """同步解析并沙箱化 session 路径 (提取为纯同步辅助函数，规避协程内 ASYNC240)"""
    session_base = os.path.basename(str(raw_session_file).replace("\\", "/"))
    if session_base.endswith(".session"):
        session_base = session_base[:-8]
    data_session = os.path.join(settings.DATA_DIR, f"{session_base}.session")
    base_session = os.path.join(settings.BASE_DIR, f"{session_base}.session")
    if (
        os.path.exists(data_session)
        or _handover_path(data_session, ".pending").exists()
        or not (os.path.exists(base_session) or _handover_path(base_session, ".pending").exists())
    ):
        return os.path.join(settings.DATA_DIR, session_base)
    return os.path.join(settings.BASE_DIR, session_base)


def _remove_session_files_sync(account_id: int, session_file_name: str) -> None:
    """同步清理本地 session 及其附属文件"""
    clean_name = os.path.basename(session_file_name)
    if not clean_name.endswith(".session"):
        clean_name = f"{clean_name}.session"
    for target_dir in (settings.DATA_DIR, settings.BASE_DIR):
        session_path = os.path.join(target_dir, clean_name)
        if os.path.exists(session_path):
            os.remove(session_path)
            for suffix in ("-journal", "-wal", "-shm"):
                extra = f"{session_path}{suffix}"
                if os.path.exists(extra):
                    os.remove(extra)
            logger.info("已物理清理账号 [%d] 的 session 文件: %s", account_id, session_path)
        _handover_path(session_path, ".pending").unlink(missing_ok=True)
        _clear_handover_files_sync(session_path)


def _stage_session_file_sync(
    session_base: str,
    dc_id: int,
    server_address: str,
    port: int,
    auth_key: Any,
    takeout_id: Any = None,
) -> str:
    """将已验证会话写入同目录隔离 staging 文件并收紧权限，失败时清理临时文件."""
    official_path = f"{session_base}.session"
    staged = _handover_path(official_path, ".staged.session")
    staged.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    staged_path = str(staged)
    _create_private_file_sync(staged_path)
    tmp_sess: SQLiteSession | None = None
    try:
        tmp_sess = SQLiteSession(staged_path)
        tmp_sess.set_dc(dc_id, server_address, port)
        tmp_sess.auth_key = auth_key
        tmp_sess.takeout_id = takeout_id
        tmp_sess.save()
        tmp_sess.close()
        tmp_sess = None
        secure_file_permissions(staged_path)
        return staged_path
    except Exception:
        if tmp_sess is not None:
            try:
                tmp_sess.close()
            except Exception as close_err:
                logger.debug("关闭失败的 staging session 异常: %s", type(close_err).__name__)
        _remove_path_with_sidecars_sync(staged_path)
        raise


def _remove_path_with_sidecars_sync(path: str) -> None:
    if not path:
        return
    for candidate in (path, f"{path}-journal", f"{path}-wal", f"{path}-shm"):
        if os.path.exists(candidate):
            try:
                os.remove(candidate)
            except OSError as rm_err:
                logger.debug("清理临时 session 文件失败 (%s): %s", os.path.basename(candidate), type(rm_err).__name__)


def _create_private_file_sync(path: str) -> None:
    os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))


def _handover_path(official_path: str, suffix: str) -> Path:
    path = Path(official_path)
    # Account session filenames live in the parent directory and cannot collide with internal files.
    return path.parent / ".telegram-handover" / (path.name + suffix)


def _copy_private_file_sync(source: str, destination: str) -> None:
    _create_private_file_sync(destination)
    with open(source, "rb") as src, open(destination, "wb") as dst:
        shutil.copyfileobj(src, dst)
        dst.flush()
        os.fsync(dst.fileno())


def _clear_handover_files_sync(official_path: str) -> None:
    for suffix in (".staged.session", ".pending.tmp", ".backup", ".restore"):
        _remove_path_with_sidecars_sync(str(_handover_path(official_path, suffix)))


def _unlink_session_sidecars_sync(official_path: str) -> None:
    for suffix in ("-journal", "-wal", "-shm"):
        Path(f"{official_path}{suffix}").unlink(missing_ok=True)


def _swap_with_backup_sync(official_path: str, staged_path: str, handover_id: str) -> None:
    """Record rollback state before atomically replacing the official entry."""
    if any(os.path.exists(f"{official_path}{suffix}") for suffix in ("-journal", "-wal", "-shm")):
        raise OSError("Official Telegram session still has SQLite sidecars")
    had_backup = os.path.exists(official_path)
    if had_backup:
        _copy_private_file_sync(official_path, str(_handover_path(official_path, ".backup")))
    journal_tmp = str(_handover_path(official_path, ".pending.tmp"))
    _create_private_file_sync(journal_tmp)
    with open(journal_tmp, "w", encoding="ascii") as journal:
        journal.write(f"{int(had_backup)} {handover_id}")
        journal.flush()
        os.fsync(journal.fileno())
    os.replace(journal_tmp, _handover_path(official_path, ".pending"))
    os.replace(staged_path, official_path)


def _recover_session_handover_sync(official_path: str, account_id: int) -> None:
    """Reconcile a process interruption before opening the official SQLite session."""
    journal = _handover_path(official_path, ".pending")
    try:
        record = journal.read_text(encoding="ascii")
    except FileNotFoundError:
        _clear_handover_files_sync(official_path)
        return
    match = re.fullmatch(r"([01]) ([0-9a-f]{32})", record)
    if match is None:
        raise ValueError("Telegram session handover journal is invalid")
    had_backup, handover_id = match.groups()
    if AccountRepository.get_committed_handover(account_id) == handover_id:
        if not os.path.isfile(official_path):
            raise FileNotFoundError("Committed Telegram session file is missing")
    elif not _handover_path(official_path, ".staged.session").exists():
        _unlink_session_sidecars_sync(official_path)
        if had_backup == "1":
            restore_path = _handover_path(official_path, ".restore")
            restore_path.unlink(missing_ok=True)
            _copy_private_file_sync(str(_handover_path(official_path, ".backup")), str(restore_path))
            os.replace(restore_path, official_path)
        else:
            Path(official_path).unlink(missing_ok=True)
    # Keep the backup available until recovery is repeatable without the journal.
    journal.unlink()
    _clear_handover_files_sync(official_path)


def _finish_session_handover_sync(official_path: str, account_id: int) -> None:
    try:
        _handover_path(official_path, ".pending").unlink()
    except OSError as exc:
        logger.warning("账号 [%d] 将在下次启动清理交接记录 (%s)", account_id, type(exc).__name__)
    else:
        _clear_handover_files_sync(official_path)


def _delete_account_record_sync(account_id: int, session_file: str | None, *, retain_session: bool) -> None:
    if retain_session and session_file:
        try:
            session_base = _resolve_session_target_sync(session_file)
            _recover_session_handover_sync(f"{session_base}.session", account_id)
        except (OSError, ValueError, sqlite3.Error) as exc:
            logger.warning("账号 [%d] 删除前恢复保留的会话失败 (%s)", account_id, type(exc).__name__)
            raise SessionHandoverRecoveryError("无法恢复待保留的 Telegram 会话，请修复会话后重试删除") from exc
    AccountRepository.delete(account_id)


def _open_sqlite_session_sync(session_base: str, expected_raw_key: bytes) -> SQLiteSession:
    """打开已落盘的官方 SQLiteSession 并校验证书 AuthKey 一致性."""
    sess = SQLiteSession(session_base)
    try:
        loaded_key = getattr(sess.auth_key, "key", None) if sess.auth_key is not None else None
        if not isinstance(loaded_key, (bytes, bytearray)) or bytes(loaded_key) != expected_raw_key:
            raise ValueError("交接后的正式 session 文件密钥校验失败")
        return sess
    except Exception:
        try:
            sess.close()
        except Exception as close_err:
            logger.debug("关闭校验失败的 SQLiteSession 异常: %s", type(close_err).__name__)
        raise


class TelegramAccountClient:
    """Userbot 账号客户端封装 (状态机、并发记账、FloodWait 熔断与 Premium 缓存)"""

    def __init__(
        self,
        account_id: int,
        account_name: str,
        phone: str,
        session_file: str,
        api_id: int,
        api_hash: str,
        max_concurrency: int = 1,
        is_active: bool = True,
        is_default: bool = False,
        dc_id: int = 0,
        tier: str = "primary",
        device_model: str = "",
        system_version: str = "",
        app_version: str = "",
        lang_code: str = "en",
        user_id: int | None = None,
    ):
        self.account_id = account_id
        self.account_name = account_name
        self.phone = phone
        self.session_file = session_file
        self.api_id = int(api_id)
        self.api_hash = str(api_hash)
        self.max_concurrency = int(max_concurrency)
        self.is_active = bool(is_active)
        self.is_default = bool(is_default)
        self.tier = str(tier or "primary")
        self.device_model = str(device_model or "")
        self.system_version = str(system_version or "")
        self.app_version = str(app_version or "")
        self.lang_code = str(lang_code or "en")

        # 运行时状态
        self.client: TelegramClient | None = None
        self.auth_state: str = "UNAUTHENTICATED"
        self._authenticated_before_flood: bool = False
        self.is_premium: bool = False
        self.premium_checked_at: float = 0.0
        self.flood_wait_until: float = 0.0
        self.dc_id: int = int(dc_id or 0)
        self.user_id: int | None = user_id
        self.username: str | None = None
        self.first_name: str | None = None

        self._lock: asyncio.Lock | None = None
        self._verify_lock: asyncio.Lock | None = None
        self._premium_lock: asyncio.Lock | None = None
        self._pending_2fa_codes: dict[str, dict] = {}
        self._web_2fa_locks: dict[str, dict] = {}
        self._pending_login: dict[str, Any] | None = None
        self._handover_recovery_task: asyncio.Task[None] | None = None

    @property
    def active_task_count(self) -> int:
        """单一可信源: 调度器运行期读取权威计数; 非运行期恒 0 (无任务可存在)"""
        try:
            from app.core.scheduler import task_scheduler

            if getattr(task_scheduler, "is_running", False):
                return task_scheduler.get_account_task_count(self.account_id)
        except Exception as exc:
            logger.debug("读取调度器账号任务计数异常: %s", exc)
        return 0

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _get_verify_lock(self) -> asyncio.Lock:
        if self._verify_lock is None:
            self._verify_lock = asyncio.Lock()
        return self._verify_lock

    def _get_premium_lock(self) -> asyncio.Lock:
        if self._premium_lock is None:
            self._premium_lock = asyncio.Lock()
        return self._premium_lock

    def is_flood_locked(self) -> bool:
        if self.flood_wait_until > time.time():
            return True
        if self.flood_wait_until > 0 and time.time() >= self.flood_wait_until:
            # 自动解封
            self.flood_wait_until = 0.0
            if self.auth_state == "FLOOD_WAIT":
                if self._authenticated_before_flood:
                    self.auth_state = "AUTHENTICATED"
                    AccountRepository.update_status(self.account_id, "authenticated")
                else:
                    self.auth_state = "UNAUTHENTICATED"
                    AccountRepository.update_status(self.account_id, "unauthenticated")
                self._authenticated_before_flood = False
        return False

    def is_admitted(self) -> bool:
        """业务准入判定：仅依据活跃默认账号在运行期的真实已验证授权态 (FloodWait/重连不丢失身份)."""
        return (
            self.is_active
            and self.is_default
            and type(self.user_id) is int
            and self.user_id > 0
            and (
                self.auth_state == "AUTHENTICATED"
                or (self.auth_state == "FLOOD_WAIT" and self._authenticated_before_flood)
            )
        )

    def is_ready(self) -> bool:
        return (
            self.is_active
            and not self.is_flood_locked()
            and self.auth_state == "AUTHENTICATED"
            and self.client is not None
            and self.client.is_connected()
        )

    def mark_flood_wait(self, wait_seconds: int):
        self._authenticated_before_flood = self.auth_state == "AUTHENTICATED" or (
            self.auth_state == "FLOOD_WAIT" and self._authenticated_before_flood
        )
        self.flood_wait_until = time.time() + wait_seconds
        self.auth_state = "FLOOD_WAIT"
        AccountRepository.mark_flood_wait(self.account_id, wait_seconds)
        logger.warning("账号 [%s] 已被标记 FloodWait 熔断, 冷却时长: %ds", self.account_name, wait_seconds)

    def invalidate_session(self, reason: str = ""):
        logger.warning("账号 [%s] 会话失效: %s", self.account_name, reason)
        self.auth_state = "UNAUTHENTICATED"
        self._authenticated_before_flood = False
        self.is_premium = False
        self.premium_checked_at = 0.0
        AccountRepository.update_status(self.account_id, "invalid")

    @staticmethod
    def _phones_equivalent(bound_phone: str, session_phone: str) -> bool:
        try:
            return canonical_phone_number(bound_phone) == canonical_phone_number(session_phone)
        except ValueError:
            return False

    @staticmethod
    def _normalize_remote_phone(me: Any, fallback_phone: str | None = None) -> str:
        me_phone = getattr(me, "phone", None)
        if isinstance(me_phone, str) and me_phone.strip():
            cleaned = me_phone.strip()
            if not cleaned.startswith("+") and cleaned.isdigit():
                cleaned = f"+{cleaned}"
            try:
                return normalize_phone_number(cleaned)
            except ValueError:
                logger.debug("Telegram 返回的手机号无法规范化")
        if fallback_phone and str(fallback_phone).strip():
            return normalize_phone_number(fallback_phone)
        return ""

    @staticmethod
    def validate_remote_identity(
        me: Any,
        *,
        expected_user_id: int | None,
        expected_phone: str | None,
        detailed_phone_errors: bool = False,
    ) -> tuple[bool, str, str]:
        """校验远程 Telegram get_me() 身份是否有效且与预期绑定目标一致。"""
        remote_id = getattr(me, "id", None) if me is not None else None
        if me is None or type(remote_id) is not int or remote_id <= 0:
            msg = (
                "未能获取 Telegram 账号身份信息，请稍后重试"
                if me is None
                else "Telegram 返回的账号 ID 无效，已拒绝授权"
            )
            return False, "identity_unavailable" if detailed_phone_errors else "identity_mismatch", msg

        if expected_user_id is not None:
            if remote_id != expected_user_id:
                return False, "identity_mismatch", "登录的 Telegram 账号身份与原绑定账号不一致，已拒绝覆盖"
            return True, "", ""

        me_phone = getattr(me, "phone", None)
        error_code = "identity_mismatch"
        reason = ""
        if not expected_phone or not str(expected_phone).strip():
            reason = "缺少已绑定的手机号基线，已拒绝授权"
        elif (
            not isinstance(me_phone, str)
            or not me_phone.strip()
            or (detailed_phone_errors and not TelegramAccountClient._normalize_remote_phone(me))
        ):
            error_code = "identity_phone_unavailable" if detailed_phone_errors else error_code
            reason = (
                "Telegram 未提供可核验的账号手机号，请刷新二维码后重试"
                if detailed_phone_errors
                else "未能确认 Telegram 会话手机号，已拒绝授权"
            )
        elif not TelegramAccountClient._phones_equivalent(expected_phone, me_phone):
            error_code = "identity_phone_mismatch" if detailed_phone_errors else error_code
            reason = (
                "扫码账号与本次确认的手机号不一致，请在手机 Telegram 中切换账号后重新扫码"
                if detailed_phone_errors
                else "登录的 Telegram 账号手机号与绑定手机号不一致，已拒绝覆盖"
            )
        if reason:
            return False, error_code, reason
        return True, "", ""

    async def _reject_connect_identity(self, client: Any, reason: str) -> None:
        self.invalidate_session(reason)
        await self._close_client_and_session(client)
        if self.client is client:
            self.client = None

    async def _defer_unverified_connect(self, client: Any, dc_id: int, log_msg: str) -> None:
        logger.warning(log_msg, self.account_name)
        self.auth_state = "UNAUTHENTICATED"
        self._authenticated_before_flood = False
        self.dc_id = dc_id
        AccountRepository.update_status(self.account_id, "unauthenticated", dc_id=self.dc_id)
        await self._close_client_and_session(client)
        if self.client is client:
            self.client = None

    def _resolve_startup_phone_baseline(self) -> tuple[bool, str | None]:
        """为尚未持久化 user_id 的会话解析可信号码基线 (默认账号优先采用有效 TG_DEFAULT_PHONE)."""
        if self.is_default:
            raw_cfg = (getattr(settings, "DEFAULT_PHONE", "") or "").strip()
            if raw_cfg:
                try:
                    return True, normalize_phone_number(raw_cfg)
                except ValueError:
                    return False, None
        raw_saved = (self.phone or "").strip()
        if raw_saved:
            try:
                return True, normalize_phone_number(raw_saved)
            except ValueError:
                return False, None
        return True, None

    async def _handle_authorized_connect(self) -> None:
        """连接鉴权通过后的身份连续性校验与元信息同步 (拒绝跨账号、跨手机号或身份未知 session)."""
        client = self.client
        if client is None:
            return
        dc_id = getattr(client.session, "dc_id", 0) or self.dc_id
        me = None
        try:
            me = await client.get_me()
        except Exception as e:
            if is_session_invalid_error(e):
                raise
            logger.debug("获取账号 me 信息失败: %s", e)

        if me is None:
            await self._defer_unverified_connect(
                client, dc_id, "账号 [%s] 连接时未能确认 Telegram 身份，保持未认证状态并保留会话待重试"
            )
            return

        expected_phone = None
        if self.user_id is None:
            baseline_ok, expected_phone = self._resolve_startup_phone_baseline()
            if not baseline_ok or not expected_phone:
                await self._defer_unverified_connect(client, dc_id, "账号 [%s] 缺少有效的手机号基线，拒绝自动加载会话")
                return
            me_phone = getattr(me, "phone", None)
            if not isinstance(me_phone, str) or not me_phone.strip():
                await self._defer_unverified_connect(
                    client, dc_id, "账号 [%s] 首次连接未获取到会话手机号，暂不持久化身份并保留会话待重试"
                )
                return
        valid_identity, _, reason = self.validate_remote_identity(
            me, expected_user_id=self.user_id, expected_phone=expected_phone
        )
        if not valid_identity:
            await self._reject_connect_identity(client, reason)
            return
        verified_phone = self._normalize_remote_phone(me, expected_phone) if expected_phone else None
        username = getattr(me, "username", None)
        first_name = getattr(me, "first_name", "")
        is_premium = bool(getattr(me, "premium", False))
        AccountRepository.commit_verified_login(
            self.account_id,
            expected_user_id=self.user_id,
            expected_phone=self.phone,
            expected_session_file=self.session_file,
            require_default=self.is_default,
            phone=verified_phone,
            user_id=me.id,
            username=username,
            first_name=first_name,
            dc_id=dc_id,
            is_premium=is_premium,
        )
        if verified_phone and (self.is_default or not self.phone):
            self.phone = verified_phone
        self.auth_state = "AUTHENTICATED"
        self._authenticated_before_flood = True
        self.dc_id = dc_id
        self.user_id = me.id
        self.username = username
        self.first_name = first_name
        self.is_premium = is_premium
        self.premium_checked_at = time.monotonic()

    def _open_session_client(self, session_base: str, *, require_session_file: bool) -> TelegramClient | None:
        official_path = f"{session_base}.session"
        try:
            _recover_session_handover_sync(official_path, self.account_id)
        except (OSError, ValueError, sqlite3.Error) as exc:
            self._mark_session_unavailable()
            logger.error("账号 [%d] 会话交接恢复失败 (%s)", self.account_id, type(exc).__name__)
            return None

        if require_session_file and not os.path.isfile(official_path):
            self.auth_state = "UNAUTHENTICATED"
            self._authenticated_before_flood = False
            AccountRepository.update_status(self.account_id, "unauthenticated")
            logger.info("默认 Userbot 正式 session 文件不存在，跳过 Telegram 连接")
            return None

        try:
            return TelegramClient(
                session_base,
                self.api_id,
                self.api_hash,
                device_model=self.device_model,
                system_version=self.system_version,
                app_version=self.app_version,
                lang_code=self.lang_code,
                system_lang_code=self.lang_code,
            )
        except (OSError, ValueError, sqlite3.Error) as exc:
            self._mark_session_unavailable()
            logger.warning("账号 [%d] 无法打开 Telegram session (%s)", self.account_id, type(exc).__name__)
            return None

    async def connect(self, force_reconnect: bool = False, *, require_session_file: bool = False):
        async with self._get_lock():
            session_base = _resolve_session_target_sync(self.session_file)

            # 确保拥有真实拟人化设备指纹
            if not self.device_model or not self.system_version or not self.app_version:
                fp = generate_device_fingerprint(self.account_id)
                self.device_model = fp["device_model"]
                self.system_version = fp["system_version"]
                self.app_version = fp["app_version"]
                self.lang_code = fp.get("lang_code", "en")
                AccountRepository.update_device_fingerprint(
                    self.account_id, self.device_model, self.system_version, self.app_version, self.lang_code
                )

            if not self.client or force_reconnect:
                if self.client is not None:
                    await self._close_client_and_session(self.client)
                self.client = self._open_session_client(session_base, require_session_file=require_session_file)

            active_client = self.client
            if active_client is None:
                return
            try:
                async with asyncio.timeout(CONNECT_VERIFY_TIMEOUT_SECONDS if require_session_file else None):
                    if not active_client.is_connected():
                        await active_client.connect()

                    if await active_client.is_user_authorized():
                        await self._handle_authorized_connect()
                    else:
                        self.auth_state = "UNAUTHENTICATED"
                        self._authenticated_before_flood = False
                        AccountRepository.update_status(self.account_id, "unauthenticated")
                        await self._close_client_and_session(active_client)
                        if self.client is active_client:
                            self.client = None
            except Exception as e:
                logger.warning("账号 [%s] 连接或授权校验异常: %s", self.account_name, e)
                await asyncio.shield(self._close_client_and_session(active_client))
                if self.client is active_client:
                    self.client = None
                if is_session_invalid_error(e):
                    self.invalidate_session(str(e))
                else:
                    self.auth_state = "UNAUTHENTICATED"
                    self._authenticated_before_flood = False
                    AccountRepository.update_status(self.account_id, "unauthenticated")

    async def disconnect(self):
        async with self._get_lock():
            await self._disconnect_locked()

    async def _disconnect_locked(self) -> None:
        if self._handover_recovery_task is not None:
            self._handover_recovery_task.cancel()
            self._handover_recovery_task = None
        if self.client and self.client.is_connected():
            try:
                await self.client.disconnect()
            except Exception as e:
                logger.debug("断开账号 [%s] 客户端异常: %s", self.account_name, e)

    async def remove_record(self, *, retain_session: bool) -> None:
        """Serialize removal with session handover until the database record is gone."""
        async with self._get_lock():
            await self._disconnect_locked()
            try:
                _delete_account_record_sync(self.account_id, self.session_file, retain_session=retain_session)
            except SessionHandoverRecoveryError:
                self.client = None
                self._mark_session_unavailable()
                raise
            self.is_active = False

    async def probe_premium(self, force: bool = False) -> bool:
        if not force and time.monotonic() - self.premium_checked_at < PREMIUM_STATUS_TTL_SECONDS:
            return self.is_premium

        async with self._get_premium_lock():
            if not force and time.monotonic() - self.premium_checked_at < PREMIUM_STATUS_TTL_SECONDS:
                return self.is_premium
            if not self.client or not self.client.is_connected() or self.auth_state != "AUTHENTICATED":
                return self.is_premium

            try:
                me = await self.client.get_me()
                self.is_premium = bool(getattr(me, "premium", False))
                self.premium_checked_at = time.monotonic()
                self.dc_id = getattr(self.client.session, "dc_id", 0) or self.dc_id
                AccountRepository.update_status(self.account_id, "authenticated", dc_id=self.dc_id)
                AccountRepository.update_premium(self.account_id, self.is_premium)
            except Exception as e:
                logger.debug("账号 [%s] Premium 状态探测失败: %s", self.account_name, e)

            return self.is_premium

    def effective_max_file_size_bytes(self) -> int:
        """返回当前账号生效的最大单文件大小 (字节，普通号 2000 MiB (1.95 GiB = 2,097,152,000 字节)，Premium 号 4000 MiB (3.91 GiB = 4,194,304,000 字节))"""
        tier_limit = settings.PREMIUM_MAX_FILE_SIZE_BYTES if self.is_premium else settings.MAX_FILE_SIZE_BYTES
        return min(tier_limit, settings.PROTOCOL_MAX_FILE_SIZE_BYTES)

    _MAX_WEB_2FA_LOCKS = 5000

    def _prune_web_2fa_locks(self, now: float, client_ip: str | None = None) -> None:
        """清理过期的 Web 2FA 锁定记录，并在超出容量上限时按最旧过期时间驱逐。"""
        self._web_2fa_locks = {
            ip: rec
            for ip, rec in self._web_2fa_locks.items()
            if rec.get("locked_until", 0) > now or rec.get("reset_at", 0) > now
        }
        if len(self._web_2fa_locks) >= self._MAX_WEB_2FA_LOCKS:
            overflow = len(self._web_2fa_locks) - self._MAX_WEB_2FA_LOCKS + 1
            oldest = sorted(
                (ip for ip in self._web_2fa_locks if ip != client_ip),
                key=lambda ip: max(
                    self._web_2fa_locks[ip].get("locked_until", 0),
                    self._web_2fa_locks[ip].get("reset_at", 0),
                ),
            )
            for ip in oldest[:overflow]:
                self._web_2fa_locks.pop(ip, None)

    # --- 🔒 Web 2FA 动态验证码 ---
    async def send_web_2fa_code(self, client_ip: str) -> bool:
        if self.auth_state != "AUTHENTICATED" or not self.client:
            return False
        now = time.time()
        # 1. 独立维护 IP 锁定状态 (锁定 30 分钟不因挑战码过期而抹除)
        self._prune_web_2fa_locks(now, client_ip)
        lock_rec = self._web_2fa_locks.get(client_ip)
        if lock_rec and lock_rec.get("locked_until", 0) > now:
            raise Exception("尝试过于频繁，已锁定，请稍后再试。")

        # 2. 清理过期验证码 (3 分钟生命周期)
        self._pending_2fa_codes = {k: v for k, v in self._pending_2fa_codes.items() if v["expires"] > now}

        code = f"{secrets.randbelow(900000) + 100000:06d}"
        self._pending_2fa_codes[client_ip] = {"code": code, "expires": now + 180}
        message_text = f"🔒 **[tg-media-hub] Web 2FA 登录请求**\n\n来自 IP: `{client_ip}`\n\n您的动态解锁验证码是: **`{code}`**\n*(验证码 3 分钟内有效)*"
        try:
            await self.client.send_message("me", message_text)
            return True
        except Exception as e:
            logger.warning("发送 2FA 验证码失败: %s", e)
            return False

    def verify_web_2fa_code(self, client_ip: str, code: str) -> bool:
        now = time.time()
        self._prune_web_2fa_locks(now, client_ip)
        existing_lock = self._web_2fa_locks.get(client_ip)
        if existing_lock and existing_lock.get("locked_until", 0) > now:
            raise Exception("尝试过于频繁，已锁定，请稍后再试。")

        record = self._pending_2fa_codes.get(client_ip)
        if not record:
            return False
        if record["expires"] < now:
            self._pending_2fa_codes.pop(client_ip, None)
            raise Exception("验证码已过期，请重新获取。")

        expected_bytes = str(record["code"]).encode("utf-8")
        provided_bytes = str(code).strip().encode("utf-8")
        if hmac.compare_digest(expected_bytes, provided_bytes):
            self._pending_2fa_codes.pop(client_ip, None)
            self._web_2fa_locks.pop(client_ip, None)
            return True

        lock_rec = self._web_2fa_locks.setdefault(
            client_ip, {"attempts": 0, "locked_until": 0.0, "reset_at": now + 600}
        )
        lock_rec["attempts"] += 1
        lock_rec["reset_at"] = max(lock_rec.get("reset_at", 0), now + 600)
        if lock_rec["attempts"] >= 3:
            lock_rec["locked_until"] = now + 1800
            raise Exception("连续输入错误 3 次，IP已被锁定 30 分钟。")
        return False

    # --- 🌐 原生 Telegram 验证码登录 ---
    async def native_send_code(self, phone: str) -> str:
        async with self._get_verify_lock():
            target_phone = normalize_phone_number(phone)
            if self.phone and normalize_phone_number(self.phone) != target_phone:
                raise ValueError("请求手机号与该账号已绑定的手机号不一致")

            if not self.client:
                await self.connect()
            if not self.client:
                raise RuntimeError("Telegram client failed to initialize")
            if not self.client.is_connected():
                await self.client.connect()
            result = await self.client.send_code_request(target_phone)
            self._pending_login = {
                "phone": target_phone,
                "phone_code_hash": result.phone_code_hash,
                "expires_at": time.monotonic() + _LOGIN_CHALLENGE_TTL_SECONDS,
                "awaiting_password": False,
            }
            return result.phone_code_hash

    def _resolve_verify_challenge(self, phone: str, phone_code_hash: str) -> tuple[str, str]:
        """校验验证请求与绑定手机及挂起登录挑战的一致性，返回实际用于 sign_in 的 (phone, hash)。"""
        req_phone = normalize_phone_number(phone)
        if self.phone and normalize_phone_number(self.phone) != req_phone:
            raise ValueError("验证手机号与该账号已绑定的手机号不一致")

        pending = self._pending_login
        if not isinstance(pending, dict):
            return req_phone, phone_code_hash
        if time.monotonic() > float(pending.get("expires_at", 0.0)):
            self._pending_login = None
            raise ValueError("登录验证码挑战已过期，请重新获取验证码")
        if req_phone != pending["phone"]:
            raise ValueError("验证手机号与最近一次下发验证码的手机号不一致")
        if (phone_code_hash or "") != pending["phone_code_hash"]:
            raise ValueError("验证码 Hash 与最近一次下发记录不匹配，请重新获取验证码")
        return pending["phone"], pending["phone_code_hash"]

    async def _reject_login_session(self, client: Any, reason: str, *, mark_invalid: bool = False) -> None:
        """拒绝并清理不符合绑定约束的登录会话。"""
        self._pending_login = None
        try:
            await client.log_out()
        except Exception as lo_err:
            logger.debug("拒绝登录会话后登出失败 (非致命): %s", lo_err)
        await self.disconnect()
        if mark_invalid:
            self.invalidate_session(reason)
        raise ValueError(reason)

    async def _finalize_authorized_login(self, target_phone: str, *, had_pending: bool = False) -> None:
        """登录授权成功后的身份校验、并发一致性守卫、手机号持久化与状态同步。"""
        client = self.client
        if client is None:
            raise RuntimeError("Telegram client failed to initialize")
        me = None
        try:
            me = await client.get_me()
        except Exception as e:
            logger.debug("获取账号 me 信息失败: %s", type(e).__name__)

        if me is None:
            raise RuntimeError("登录校验阶段未能获取 Telegram 账号身份，请稍后重试")

        expected_phone = self.phone or target_phone
        ok, _err_code, reason = self.validate_remote_identity(
            me,
            expected_user_id=self.user_id,
            expected_phone=expected_phone,
        )
        if not ok:
            if self.auth_state != "AUTHENTICATED":
                await self._reject_login_session(
                    client,
                    reason,
                    mark_invalid=(self.user_id is not None),
                )
            raise ValueError(reason)

        if (had_pending and not isinstance(self._pending_login, dict)) or (
            self.phone and normalize_phone_number(self.phone) != normalize_phone_number(target_phone)
        ):
            await self._reject_login_session(client, "登录期间账号绑定手机号发生变更，已拒绝本次授权")

        self._pending_login = None
        dc_id = getattr(client.session, "dc_id", 0) or self.dc_id
        username = getattr(me, "username", None)
        first_name = getattr(me, "first_name", "") or ""
        is_premium = bool(getattr(me, "premium", False))

        if not self.phone and target_phone:
            AccountRepository.update(self.account_id, phone=target_phone)
            self.phone = target_phone

        AccountRepository.update_status(
            self.account_id,
            "authenticated",
            user_id=me.id,
            username=username,
            first_name=first_name,
            dc_id=dc_id,
        )
        AccountRepository.update_premium(self.account_id, is_premium)

        self.auth_state = "AUTHENTICATED"
        self._authenticated_before_flood = True
        self.dc_id = dc_id
        self.user_id = me.id
        self.username = username
        self.first_name = first_name
        self.is_premium = is_premium
        self.premium_checked_at = time.monotonic()

        await self.probe_premium(force=True)
        try:
            await client_manager.recover_startup_degraded()
        except Exception as rec_err:
            logger.warning("登录后触发调度器恢复失败 (账号持久化已完成): %s", rec_err)

    def _prepare_device_fingerprint_fields(self) -> tuple[str, str, str, str]:
        if self.device_model and self.system_version and self.app_version:
            return self.device_model, self.system_version, self.app_version, self.lang_code or "en"
        fp = generate_device_fingerprint(self.account_id)
        return (
            fp["device_model"],
            fp["system_version"],
            fp["app_version"],
            fp.get("lang_code", "en"),
        )

    @staticmethod
    def _validate_source_session_for_handover(source_client: Any) -> tuple[Any, int, str, int, bytes, Any]:
        source_session = getattr(source_client, "session", None)
        source_auth_key = getattr(source_session, "auth_key", None)
        raw_key = getattr(source_auth_key, "key", None) if source_auth_key is not None else None
        dc_id = getattr(source_session, "dc_id", None)
        server_address = getattr(source_session, "server_address", None)
        port = getattr(source_session, "port", None)
        if (
            source_session is None
            or not isinstance(raw_key, (bytes, bytearray))
            or not raw_key
            or type(dc_id) is not int
            or dc_id <= 0
            or not isinstance(server_address, str)
            or not server_address.strip()
            or type(port) is not int
            or port <= 0
        ):
            raise ValueError("临时登录会话缺少有效的 DC 或 AuthKey 信息，拒绝交接正式会话")
        return (
            source_auth_key,
            dc_id,
            server_address,
            port,
            bytes(raw_key),
            getattr(source_session, "takeout_id", None),
        )

    @staticmethod
    async def _close_client_and_session(client: Any) -> None:
        if client is None:
            return
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception as disc_err:
            logger.debug("断开客户端异常 (非致命): %s", type(disc_err).__name__)
        sess = getattr(client, "session", None)
        close_fn = getattr(sess, "close", None)
        if callable(close_fn):
            try:
                close_fn()
            except Exception as close_err:
                logger.debug("关闭 session 句柄异常 (非致命): %s", type(close_err).__name__)

    async def _adopt_or_connect_handover_client(
        self,
        source_client: Any,
        session_base: str,
        raw_key: bytes,
        fp_fields: tuple[str, str, str, str],
        expected_user_id: int,
    ) -> tuple[Any, SQLiteSession | None]:
        """优先复用已验证连接的客户端绑定落盘 session，否则在 DB 提交前建立连接验证."""
        source_session = getattr(source_client, "session", None)
        if source_client.is_connected():
            persisted_session = _open_sqlite_session_sync(session_base, raw_key)
            close_src = getattr(source_session, "close", None)
            if callable(close_src) and source_session is not persisted_session:
                try:
                    close_src()
                except Exception as close_err:
                    logger.debug("释放临时 MemorySession 异常: %s", type(close_err).__name__)
            source_client.session = persisted_session
            return source_client, persisted_session

        await self._close_client_and_session(source_client)
        dm, sv, av, lc = fp_fields
        new_client = TelegramClient(
            session_base,
            self.api_id,
            self.api_hash,
            device_model=dm,
            system_version=sv,
            app_version=av,
            lang_code=lc,
            system_lang_code=lc,
        )
        try:
            await new_client.connect()
            replacement_identity = await new_client.get_me() if await new_client.is_user_authorized() else None
            ok, _, reason = self.validate_remote_identity(
                replacement_identity, expected_user_id=expected_user_id, expected_phone=None
            )
            if not ok:
                raise ValueError(reason)
        except BaseException:
            await asyncio.shield(self._close_client_and_session(new_client))
            raise
        return new_client, None

    async def _rollback_handover_state(
        self,
        *,
        old_client: Any,
        binding: LoginBinding,
        official_path: str,
        activated_client: Any,
        persisted_session: SQLiteSession | None,
        source_client: Any,
    ) -> None:
        if activated_client is not None:
            await self._close_client_and_session(activated_client)
        if persisted_session is not None:
            close_fn = getattr(persisted_session, "close", None)
            if callable(close_fn):
                try:
                    close_fn()
                except Exception as close_err:
                    logger.debug("回滚关闭持久化 session 异常: %s", type(close_err).__name__)
        if source_client is not activated_client:
            await self._close_client_and_session(source_client)

        try:
            _recover_session_handover_sync(official_path, self.account_id)
        except Exception as exc:
            self.client = None
            self._mark_session_unavailable()
            logger.error("账号 [%d] 会话回滚失败 (%s)", self.account_id, type(exc).__name__)
            return

        if old_client is not None:
            await self._restore_previous_connection(old_client, binding)

    async def _resume_previous_client(self, old_client: Any, binding: LoginBinding) -> bool:
        try:
            async with asyncio.timeout(10.0):
                if not old_client.is_connected():
                    await old_client.connect()
                me = await old_client.get_me() if await old_client.is_user_authorized() else None
                ok, _, reason = self.validate_remote_identity(
                    me, expected_user_id=binding.user_id, expected_phone=binding.phone
                )
            if not ok:
                logger.warning("账号 [%d] 无法恢复原会话: %s", self.account_id, reason)
                return False
            row = AccountRepository.get_by_id(self.account_id)
            return bool(row and row["is_active"] and LoginBinding.from_record(row) == binding)
        except Exception as exc:
            logger.warning("账号 [%d] 恢复原连接失败 (%s)", self.account_id, type(exc).__name__)
            return False

    async def _restore_previous_connection(self, old_client: Any, binding: LoginBinding) -> None:
        if self.auth_state != "AUTHENTICATED":
            return
        if self.is_active and await self._resume_previous_client(old_client, binding):
            return
        await self._close_client_and_session(old_client)
        self._mark_session_unavailable()
        if self.is_active:
            if self._handover_recovery_task is not None:
                self._handover_recovery_task.cancel()
            self._handover_recovery_task = asyncio.create_task(self._retry_previous_connection(old_client, binding))

    def _mark_session_unavailable(self) -> None:
        self.auth_state = "UNAUTHENTICATED"
        self._authenticated_before_flood = False
        try:
            AccountRepository.update_status(self.account_id, "unauthenticated")
        except Exception as exc:
            logger.warning("账号 [%d] 无法保存会话不可用状态 (%s)", self.account_id, type(exc).__name__)

    async def _retry_previous_connection(self, old_client: Any, binding: LoginBinding) -> None:
        try:
            await asyncio.sleep(1)
            async with self._get_lock():
                if not self.is_active or self.client is not old_client or self.is_ready():
                    return
                try:
                    if await self._resume_previous_client(old_client, binding):
                        AccountRepository.update_status(self.account_id, "authenticated")
                        self.auth_state = "AUTHENTICATED"
                        self._authenticated_before_flood = True
                        return
                except Exception as exc:
                    logger.warning("账号 [%d] 重试原会话连接失败 (%s)", self.account_id, type(exc).__name__)
                await self._close_client_and_session(old_client)
        finally:
            if self._handover_recovery_task is asyncio.current_task():
                self._handover_recovery_task = None

    def _resolve_effective_phone_for_handover(self, me: Any, target_phone: str | None) -> str:
        # 首绑 (user_id is None) 时目标手机号优先覆盖未验证旧预填; 已绑定账号以既有绑定为准
        if self.user_id is None:
            effective = (target_phone or self.phone or "").strip()
        else:
            effective = (self.phone or target_phone or "").strip()
        if effective:
            return effective
        return self._normalize_remote_phone(me)

    def _validate_handover_db_snapshot(
        self,
        db_snapshot: dict[str, Any] | None,
        *,
        target_phone: str | None,
        require_default: bool,
        expected_binding: LoginBinding | None = None,
    ) -> LoginBinding:
        if db_snapshot is None or not bool(db_snapshot.get("is_active", 1)):
            raise ValueError("账号已删除或已禁用，拒绝激活登录会话")
        if require_default and not bool(db_snapshot.get("is_default", 0)):
            raise ValueError("默认主账号绑定已变更，拒绝完成登录")

        binding = LoginBinding.from_record(db_snapshot)
        if binding.user_id != self.user_id:
            raise ValueError("登录期间账号绑定身份与数据库快照不一致，已拒绝本次授权")

        expected_target = (target_phone or self.phone or "").strip()
        if binding.user_id is not None:
            if (
                binding.phone.strip()
                and expected_target
                and not self._phones_equivalent(binding.phone, expected_target)
            ):
                raise ValueError("账号数据库记录的手机号与当前目标手机号不一致，已拒绝本次授权")
        elif expected_binding is None and not require_default:
            # 未传入挑战期快照的非默认首绑直调场景，仍守卫 DB phone 未在期间被外部改写
            if binding.phone.strip() and self.phone.strip() and not self._phones_equivalent(binding.phone, self.phone):
                raise ValueError("账号数据库记录的手机号与当前目标手机号不一致，已拒绝本次授权")

        if binding.session_file and self.session_file and binding.session_file != self.session_file:
            raise ValueError("账号数据库记录的会话文件目标已变更，已拒绝本次授权")
        return binding

    def _check_handover_target(
        self, me: Any, target_phone: str | None, require_default: bool, expected_binding: LoginBinding | None
    ) -> tuple[str, LoginBinding]:
        if not self.is_active:
            raise ValueError("账号已禁用，拒绝激活登录会话")
        if require_default and not self.is_default:
            raise ValueError("默认主账号已变更，拒绝完成登录")

        effective_phone = self._resolve_effective_phone_for_handover(me, target_phone)
        ok, _, reason = self.validate_remote_identity(me, expected_user_id=self.user_id, expected_phone=effective_phone)
        if not ok:
            raise ValueError(reason)
        if (
            self.user_id is not None
            and self.phone
            and target_phone
            and not self._phones_equivalent(self.phone, target_phone)
        ):
            raise ValueError("登录期间账号绑定手机号发生变更，已拒绝本次授权")

        binding = self._validate_handover_db_snapshot(
            AccountRepository.get_by_id(self.account_id),
            target_phone=target_phone,
            require_default=require_default,
            expected_binding=expected_binding,
        )
        if expected_binding is not None and binding != expected_binding:
            raise ValueError("登录期间账号绑定已变更，拒绝完成登录")
        committed_phone = self._normalize_remote_phone(me, effective_phone) or effective_phone
        return committed_phone, binding

    async def apply_verified_login_session(
        self,
        source_client: Any,
        me: Any,
        *,
        target_phone: str | None = None,
        require_default: bool = False,
        expected_binding: LoginBinding | None = None,
    ) -> None:
        """在确认远程身份后，经同步无间断 staging -> 原子替换 -> 单事务守卫提交 交接会话，任一步失败均完整回滚."""
        async with self._get_lock():
            committed_phone, binding = self._check_handover_target(me, target_phone, require_default, expected_binding)
            write_phone = (require_default and binding.user_id is None) or not self.phone
            auth_key_obj, dc_id, server_addr, port, raw_key, takeout_id = self._validate_source_session_for_handover(
                source_client
            )
            fp_fields = self._prepare_device_fingerprint_fields()
            session_base = _resolve_session_target_sync(self.session_file)
            official_path = f"{session_base}.session"

            old_client = self.client
            old_client_closed = False
            handover_id = secrets.token_hex(16)
            activated_client: Any = None
            persisted_session: SQLiteSession | None = None

            _recover_session_handover_sync(official_path, self.account_id)
            try:
                staged_path = _stage_session_file_sync(session_base, dc_id, server_addr, port, auth_key_obj, takeout_id)

                if old_client is not None and old_client is not source_client:
                    old_client_closed = True
                    await self._close_client_and_session(old_client)

                _swap_with_backup_sync(official_path, staged_path, handover_id)

                activated_client, persisted_session = await self._adopt_or_connect_handover_client(
                    source_client, session_base, raw_key, fp_fields, me.id
                )

                username = getattr(me, "username", None)
                first_name = getattr(me, "first_name", "") or ""
                is_premium = bool(getattr(me, "premium", False))
                dm, sv, av, lc = fp_fields

                AccountRepository.commit_verified_login(
                    self.account_id,
                    phone=committed_phone if write_phone else None,
                    expected_user_id=binding.user_id,
                    expected_phone=binding.phone,
                    expected_session_file=binding.session_file,
                    require_default=require_default,
                    handover_id=handover_id,
                    device_model=dm,
                    system_version=sv,
                    app_version=av,
                    lang_code=lc,
                    user_id=me.id,
                    username=username,
                    first_name=first_name,
                    dc_id=dc_id,
                    is_premium=is_premium,
                )
            except BaseException:
                await asyncio.shield(
                    self._rollback_handover_state(
                        old_client=old_client if old_client_closed else None,
                        binding=binding,
                        official_path=official_path,
                        activated_client=activated_client,
                        persisted_session=persisted_session,
                        source_client=source_client,
                    )
                )
                raise

            _finish_session_handover_sync(official_path, self.account_id)

            self.device_model, self.system_version, self.app_version, self.lang_code = fp_fields
            if committed_phone and write_phone:
                self.phone = committed_phone
            self.client = activated_client
            self._pending_login = None
            self.auth_state = "AUTHENTICATED"
            self._authenticated_before_flood = True
            self.dc_id = dc_id
            self.user_id = me.id
            self.username = username
            self.first_name = first_name
            self.is_premium = is_premium
            self.premium_checked_at = time.monotonic()

    async def _run_verify_sign_in(
        self, target_phone: str, clean_code: str, target_hash: str, clean_password: str | None
    ) -> None:
        """执行 Telegram 验证码或 2FA 密码登录调用并归一异常语义。"""
        client = self.client
        if client is None:
            raise RuntimeError("Telegram client failed to initialize")

        if clean_password:
            try:
                await client.sign_in(password=clean_password)
                return
            except Exception as e:
                if "PasswordHashInvalidError" in str(type(e).__name__):
                    raise Exception("2FA 两步验证密码错误，请核对后重试") from e
                logger.debug("尝试直接以 2FA 密码登录未命中挂起状态，走验证码主流程: %s", e)

        try:
            await client.sign_in(phone=target_phone, code=clean_code, phone_code_hash=target_hash)
        except Exception as e:
            err_name = str(type(e).__name__)
            if "SessionPasswordNeededError" in err_name:
                if isinstance(self._pending_login, dict):
                    self._pending_login["awaiting_password"] = True
                if not clean_password:
                    raise Exception("该账号启用了两步验证 (2FA)，请输入密码后再次点击完成授权。") from e
                await client.sign_in(password=clean_password)
            else:
                if "PhoneCodeExpiredError" in err_name:
                    self._pending_login = None
                raise e

    async def native_verify_code(
        self, phone: str, code: str, phone_code_hash: str, password: str | None = None
    ) -> bool:
        # 使用独立于 _get_lock() 的验证锁串行化同账号挑战校验，避免与 connect()/disconnect() 锁重入死锁
        # 同时防止并发校验同时通过挑战比对并在后到请求中误注销首个请求刚建立的会话。
        caller_saw_pending = isinstance(self._pending_login, dict)
        async with self._get_verify_lock():
            if caller_saw_pending and not isinstance(self._pending_login, dict):
                raise ValueError("登录验证码挑战已失效或已被使用，请重新获取验证码")

            had_pending = isinstance(self._pending_login, dict)
            target_phone, target_hash = self._resolve_verify_challenge(phone, phone_code_hash)

            if not self.client:
                await self.connect()
            if not self.client:
                raise RuntimeError("Telegram client failed to initialize")
            if not self.client.is_connected():
                await self.client.connect()

            clean_code = str(code).strip().replace(" ", "").replace("-", "") if code else ""
            clean_password = password if password and password.strip() else None

            # 核心防死锁优化：若传入了密码，优先尝试直接 sign_in(password=clean_password)
            # （针对用户第一次提交验证码被提示 2FA，第二次补填密码提交时，验证码已在前次被消耗的场景）
            await self._run_verify_sign_in(target_phone, clean_code, target_hash, clean_password)

            if await self.client.is_user_authorized():
                await self._finalize_authorized_login(target_phone, had_pending=had_pending)
                return True
            return False


class TelegramBotClient:
    """Bot 令牌客户端封装 (轮询分发、健康检查与 FloodWait 状态隔离)"""

    def __init__(
        self,
        bot_id: int,
        bot_token: str,
        session_name: str,
        api_id: int,
        api_hash: str,
        is_active: bool = True,
        bot_name: str = "",
        dc_id: int = 0,
    ):
        self.bot_id = bot_id
        self.bot_token = bot_token
        self.session_name = session_name
        self.api_id = int(api_id)
        self.api_hash = str(api_hash)
        self.is_active = bool(is_active)
        self.bot_name = bot_name

        self.client: TelegramClient | None = None
        self.status: str = "unauthenticated"
        self.flood_wait_until: float = 0.0
        self.dc_id: int = int(dc_id or 0)
        self.bot_username: str | None = None
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def is_flood_locked(self) -> bool:
        if self.flood_wait_until > time.time():
            return True
        if self.flood_wait_until > 0 and time.time() >= self.flood_wait_until:
            self.flood_wait_until = 0.0
            if self.status == "flood_wait":
                self.status = "ready"
                BotRepository.update_status(self.bot_id, "ready")
        return False

    def is_ready_snapshot(self) -> bool:
        """读取调度就绪状态，不更新内存或数据库。"""
        now = time.time()
        return (
            self.is_active
            and self.flood_wait_until <= now
            and (self.status == "ready" or (self.status == "flood_wait" and self.flood_wait_until > 0))
            and self.client is not None
            and self.client.is_connected()
        )

    def is_ready(self) -> bool:
        ready = self.is_ready_snapshot()
        if self.is_active:
            self.is_flood_locked()
        return ready

    def mark_flood_wait(self, wait_seconds: int):
        self.flood_wait_until = time.time() + wait_seconds
        self.status = "flood_wait"
        BotRepository.mark_flood_wait(self.bot_id, wait_seconds)
        logger.warning("Bot [%s] 已被标记 FloodWait 熔断, 冷却时长: %ds", self.session_name, wait_seconds)

    async def connect(self, *, probe: bool = False) -> str | None:
        """连接或探活，短暂探活失败时保留已有连接。"""
        async with self._get_lock():
            if not self.is_active or self.is_flood_locked():
                return "Bot 处于 FloodWait 冷却" if self.is_active else "Bot 未启用或已移除"
            if not probe and self.is_ready_snapshot():
                return None
            client = self.client
            keep_connection = client is not None and client.is_connected() and self.status != "invalid"
            ready = False
            try:
                if client is not None and self.status == "invalid":
                    await self._disconnect_locked()
                    client = None
                if client is None:
                    session_target = _resolve_session_target_sync(self.session_name)
                    client = TelegramClient(session_target, self.api_id, self.api_hash)
                    self.client = client
                if not client.is_connected():
                    await client.start(bot_token=self.bot_token)
                me = await client.get_me()
                if not self.is_active:
                    return "Bot 已移除"
                if not me:
                    keep_connection = False
                    self.status = "unauthenticated"
                    BotRepository.update_status(self.bot_id, self.status)
                    return "Bot 未授权或身份信息为空"
                if getattr(me, "bot", False) is not True or str(me.id) != self.bot_token.partition(":")[0]:
                    keep_connection = False
                    self.status = "invalid"
                    BotRepository.update_status(self.bot_id, self.status)
                    return "Bot 会话与配置的 Bot 身份不一致"
                self.dc_id = getattr(client.session, "dc_id", 0) or self.dc_id
                self.bot_username = getattr(me, "username", None)
                BotRepository.update_status(self.bot_id, "ready", bot_username=self.bot_username, dc_id=self.dc_id)
                self.status = "ready"
                ready = True
                return None
            except asyncio.CancelledError:
                self.status = "invalid" if self.status == "invalid" else "unauthenticated"
                raise
            except Exception as e:
                logger.warning("Bot [%s] 激活异常: %s", self.session_name, e)
                self.status = (
                    "invalid" if self.status == "invalid" or is_session_invalid_error(e) else "unauthenticated"
                )
                if self.is_active:
                    BotRepository.update_status(self.bot_id, self.status)
                return str(e) or "Bot 连接失败"
            finally:
                # 就绪状态可能被上一次探活更新，已有连接仍可能正被传输使用。
                if not ready and (not keep_connection or not self.is_active or self.status == "invalid"):
                    await self._disconnect_locked()

    async def _disconnect_locked(self) -> None:
        client, self.client = self.client, None
        if client is not None:
            try:
                await client.disconnect()
            except Exception as e:
                logger.debug("断开 Bot [%s] 客户端异常: %s", self.session_name, e)

    async def disconnect(self):
        async with self._get_lock():
            await self._disconnect_locked()


class ClientManager:
    """全局 Telegram 动态客户端连接池管理器 (多 Userbot 与 Bot 池托管)"""

    def __init__(self):
        self._accounts: dict[int, TelegramAccountClient] = {}
        self._bots: dict[int, TelegramBotClient] = {}
        self._bot_index: int = 0
        self._lock: asyncio.Lock | None = None
        self._recovery_lock: asyncio.Lock | None = None
        self._pool_init_task: asyncio.Task[None] | None = None
        self._startup_initializing = False
        self._shutting_down: bool = False
        self._chat_accessible_cache: dict[str, tuple[float, list[int]]] = {}
        self._access_cache_epoch: int = 0
        self.startup_degraded: bool = False

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _get_recovery_lock(self) -> asyncio.Lock:
        if self._recovery_lock is None:
            self._recovery_lock = asyncio.Lock()
        return self._recovery_lock

    def is_default_account_admitted(self) -> bool:
        """业务准入总判定：仅依据严格默认活跃 Userbot 的真实已验证授权态."""
        acc = self.get_default_account_strict()
        return bool(acc is not None and acc.is_admitted())

    def get_startup_status(self) -> dict[str, Any]:
        """从现有连接状态读取快照，无需探活 RPC 或数据库写入。"""
        from app.core.scheduler import task_scheduler

        active_bots = [bot for bot in self._bots.values() if bot.is_active]
        unready_bots = [bot.bot_name or f"Bot #{bot.bot_id}" for bot in active_bots if not bot.is_ready_snapshot()]
        scheduler_ready = task_scheduler.is_running and not task_scheduler.startup_degraded
        if self._startup_initializing:
            phase = "initializing"
        elif self.startup_degraded or not scheduler_ready or unready_bots:
            phase = "degraded"
        else:
            phase = "ready"
        return {
            "phase": phase,
            "scheduler_ready": scheduler_ready,
            "ready_bots": len(active_bots) - len(unready_bots),
            "total_bots": len(active_bots),
            "unready_bots": unready_bots,
        }

    def _sync_pool_records_from_db(self) -> None:
        """从 DB 幂等装载活跃账号与 Bot 记录，绝不重建或覆盖已存在的客户端实例."""
        db_accounts = AccountRepository.get_all(only_active=True)
        for acc in db_accounts:
            if acc["id"] not in self._accounts:
                self._register_account_from_record(acc)

        db_bots = BotRepository.get_all(only_active=True)
        for b in db_bots:
            if b["id"] not in self._bots:
                bot_client = TelegramBotClient(
                    bot_id=b["id"],
                    bot_token=b["bot_token"],
                    session_name=b["session_name"],
                    api_id=b["api_id"],
                    api_hash=b["api_hash"],
                    is_active=bool(b.get("is_active", 1)),
                    bot_name=b.get("bot_name", ""),
                    dc_id=b.get("dc_id", 0),
                )
                self._bots[b["id"]] = bot_client

    async def _connect_remaining_pool(self, *, skip_account_id: int | None = None) -> None:
        """在默认主账号就绪后幂等连接其余辅助账号与 Bot 池，绝不覆盖已绑定的客户端."""
        self._sync_pool_records_from_db()

        for acc_client in list(self._accounts.values()):
            if self._shutting_down:
                return
            if skip_account_id is not None and acc_client.account_id == skip_account_id:
                continue
            if not acc_client.is_active or acc_client.client is not None or acc_client.auth_state != "UNAUTHENTICATED":
                continue
            try:
                await acc_client.connect()
                await asyncio.sleep(random.uniform(0.15, 0.35))
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                logger.warning("Userbot [%s] 初始化连接失败: %s", acc_client.account_name, ex)

        for bot in list(self._bots.values()):
            if self._shutting_down:
                return
            if self._bots.get(bot.bot_id) is not bot or not bot.is_active or bot.is_ready_snapshot():
                continue
            try:
                await asyncio.wait_for(bot.connect(), timeout=CONNECT_VERIFY_TIMEOUT_SECONDS)
                await asyncio.sleep(random.uniform(0.1, 0.25))
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                logger.warning("Bot [%s] 初始化连接失败: %s", bot.session_name, ex)

        logger.info(
            "ClientManager 就绪: %d 个 Userbot 账号 (已鉴权: %d), %d 个 Bot (就绪: %d)",
            len(self._accounts),
            sum(1 for a in self._accounts.values() if a.auth_state == "AUTHENTICATED"),
            len(self._bots),
            sum(1 for b in self._bots.values() if b.status == "ready"),
        )

    async def recover_startup_degraded(self):
        """仅当严格默认账号验证并提交成功后，幂等、可清理地继续其余账号池和调度器初始化。"""
        from app.core.scheduler import task_scheduler

        async with self._get_recovery_lock():
            default_account = self.get_default_account_strict()
            if self._shutting_down or default_account is None or not default_account.is_admitted():
                self._startup_initializing = False
                return
            if not self.startup_degraded and task_scheduler.is_running:
                return

            self._startup_initializing = True
            try:
                if self.startup_degraded:
                    await self._connect_remaining_pool(skip_account_id=default_account.account_id)
                if self._shutting_down or not self.is_default_account_admitted():
                    return
                if not task_scheduler.is_running:
                    await task_scheduler.start()
                    logger.info("⚡ 登录自愈联动: 默认账号就绪，已成功拉起后台任务调度器")
                self.startup_degraded = False
                task_scheduler.startup_degraded = False
            except Exception as e:
                self.startup_degraded = True
                task_scheduler.startup_degraded = True
                logger.warning("登录自愈拉起调度器异常: %s", e)
            finally:
                self._startup_initializing = False

            await connection_manager.broadcast({"type": "system_event", "event": "telegram_startup_changed"})

    def schedule_startup_recovery(self) -> None:
        """验证成功后继续连接账号池，避免验证码或扫码响应等待其他 Telegram 连接。"""
        from app.core.scheduler import task_scheduler

        if self._shutting_down or self._startup_initializing or not self.is_default_account_admitted():
            return
        if not self.startup_degraded and task_scheduler.is_running:
            return
        if self._pool_init_task is None or self._pool_init_task.done():
            self._startup_initializing = True
            self._pool_init_task = asyncio.create_task(self.recover_startup_degraded())

    async def initialize(self):
        """从 DB 加载账号与 Bot；先严格验证默认主账号，未通过则保持受限态不阻塞绑定页."""
        logger.info("正在初始化 ClientManager 动态连接池...")
        self._shutting_down = False
        self._sync_pool_records_from_db()

        default_acc = self.get_default_account_strict()
        if default_acc is None:
            self.startup_degraded = True
            logger.warning("未找到活跃且标记默认的 Userbot 账号，跳过其余账号池初始化并进入受限引导状态")
            return

        try:
            await default_acc.connect(require_session_file=True)
        except Exception as ex:
            logger.warning("默认 Userbot [%s] 初始化连接失败: %s", default_acc.account_name, ex)

        if not self.is_default_account_admitted():
            self.startup_degraded = True
            logger.warning("默认 Userbot [%s] 尚未完成授权验证，跳过其余账号与 Bot 初始化", default_acc.account_name)
            return

        await self._connect_remaining_pool(skip_account_id=default_acc.account_id)
        self.startup_degraded = False

    async def stop_startup_recovery(self) -> None:
        """在调度器停机前结束恢复，避免停机期间重新拉起调度器。"""
        self._shutting_down = True
        self._startup_initializing = False
        pool_task = self._pool_init_task
        self._pool_init_task = None
        if pool_task is not None and not pool_task.done():
            pool_task.cancel()
            try:
                await pool_task
            except (asyncio.CancelledError, Exception) as exc:
                logger.debug("退出时等待池初始化任务终止: %s", type(exc).__name__)

    async def disconnect_all(self):
        """优雅关闭所有 Telegram 连接与后台恢复任务"""
        await self.stop_startup_recovery()
        tasks = []
        for acc in self._accounts.values():
            tasks.append(acc.disconnect())
        for bot in self._bots.values():
            tasks.append(bot.disconnect())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("ClientManager 所有客户端连接已关闭")

    def get_account(self, account_id: int | None = None) -> TelegramAccountClient | None:
        """获取指定账号，若 account_id 为 None 或 0 则返回默认主账号；若指定了明确的账号 ID 但不存在则返回 None"""
        effective_id = None if account_id in (None, 0) else account_id
        if effective_id is not None and effective_id in self._accounts:
            return self._accounts[effective_id]

        if not self._accounts or (effective_id is not None and effective_id not in self._accounts):
            try:
                db_accs = AccountRepository.get_all(only_active=True)
                for a in db_accs:
                    if a["id"] not in self._accounts:
                        self._accounts[a["id"]] = TelegramAccountClient(
                            account_id=a["id"],
                            account_name=a["account_name"],
                            phone=a.get("phone", ""),
                            session_file=a["session_file"],
                            api_id=a["api_id"],
                            api_hash=a["api_hash"],
                            max_concurrency=a.get("max_concurrency", 1),
                            is_active=bool(a.get("is_active", 1)),
                            is_default=bool(a.get("is_default", 0)),
                            dc_id=a.get("dc_id", 0),
                            tier=a.get("tier", "primary"),
                            device_model=a.get("device_model", ""),
                            system_version=a.get("system_version", ""),
                            app_version=a.get("app_version", ""),
                            lang_code=a.get("lang_code", "en"),
                            user_id=a.get("user_id"),
                        )
            except Exception as load_err:
                logger.debug("按需从数据库载入账号信息异常 (非致命): %s", load_err)

        if effective_id is not None:
            return self._accounts.get(effective_id)

        # 查找 is_default=True 的活跃账号
        for acc in self._accounts.values():
            if acc.is_default and acc.is_active:
                return acc

        # 回退第一个活跃账号
        for acc in self._accounts.values():
            if acc.is_active:
                return acc
        return None

    def get_default_account_strict(self) -> TelegramAccountClient | None:
        """严格获取指定的默认主账号 (要求 is_default=True 且 is_active=True，绝不回退至其他账号)."""
        if not self._accounts:
            try:
                db_accs = AccountRepository.get_all(only_active=True)
                for a in db_accs:
                    if a["id"] not in self._accounts:
                        self._register_account_from_record(a)
            except Exception as load_err:
                logger.debug("按需从数据库载入默认账号信息异常 (非致命): %s", load_err)

        for acc in self._accounts.values():
            if acc.is_default and acc.is_active:
                return acc
        return None

    def create_isolated_login_client(self, acc: TelegramAccountClient) -> TelegramClient:
        """为交互式 OTP/QR 登录创建独立隔离的临时 TelegramClient (MemorySession, 不触碰官方 .session)."""
        if not acc.device_model or not acc.system_version or not acc.app_version:
            fp = generate_device_fingerprint(acc.account_id)
            acc.device_model = fp["device_model"]
            acc.system_version = fp["system_version"]
            acc.app_version = fp["app_version"]
            acc.lang_code = fp.get("lang_code", "en")
            AccountRepository.update_device_fingerprint(
                acc.account_id, acc.device_model, acc.system_version, acc.app_version, acc.lang_code
            )
        return TelegramClient(
            MemorySession(),
            acc.api_id,
            acc.api_hash,
            device_model=acc.device_model,
            system_version=acc.system_version,
            app_version=acc.app_version,
            lang_code=acc.lang_code,
            system_lang_code=acc.lang_code,
        )

    async def get_available_account(
        self,
        task_type: str = "download",
        target_chat: str | int | None = None,
        exclude_account_ids: set[int] | None = None,
    ) -> TelegramAccountClient | None:
        """按负载、账号分级和目标频道权限选择账号。"""
        active_accounts = [
            a
            for a in self._accounts.values()
            if a.is_active
            and a.is_ready()
            and not a.is_flood_locked()
            and (not exclude_account_ids or a.account_id not in exclude_account_ids)
        ]
        if not active_accounts:
            default_acc = self.get_account()
            if default_acc and exclude_account_ids and default_acc.account_id in exclude_account_ids:
                return None
            return default_acc

        require_write = task_type in ("publish_folder", "pikpak_publish")
        if target_chat and (require_write or is_private_channel_identifier(target_chat)):
            matched = await preferred_channel_account(
                active_accounts, task_type, target_chat, require_write=require_write
            )
            if matched is not None:
                return matched

        # 频道探测期间并发占用可能变化，回退时按当前负载重新排序。
        available = ranked_candidates(active_accounts, task_type)
        return available[0] if available else self.get_account()

    async def probe_chat_access(
        self,
        chat_identifier: str | int,
        account_id: int | None = None,
        require_write: bool = False,
        timeout: float = 5.0,
    ) -> tuple[TelegramAccountClient | None, Any]:
        """探测指定账号或并发探测账号池，按池顺序返回有权限的账号和已解析的实体。"""
        chat = parse_chat_identifier(chat_identifier)
        if account_id is not None and account_id > 0:
            account = self.get_account(account_id)
            if account is None:
                return None, None
            return await probe_account_access(account, chat, require_write=require_write, rpc_timeout=timeout)

        accounts = [a for a in self._accounts.values() if a.is_active and not a.is_flood_locked()]
        if not accounts:
            default_account = self.get_account()
            if default_account is None:
                return None, None
            accounts = [default_account]

        results = await asyncio.gather(
            *(probe_account_access(a, chat, require_write=require_write, rpc_timeout=timeout) for a in accounts),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, tuple) and result[0] is not None:
                return result
        return None, None

    async def find_failover_account(
        self, exclude_account_ids: set[int], target_chat: str | int, task_type: str = "download"
    ) -> TelegramAccountClient | None:
        """为权限受限任务探寻替代执行账号 (排除已知无权限账号)"""
        active_accounts = [
            a
            for a in self._accounts.values()
            if a.account_id not in exclude_account_ids
            and a.is_active
            and a.is_ready()
            and not a.is_flood_locked()
            and a.active_task_count < a.max_concurrency
        ]
        if not active_accounts:
            # 降级尝试：即使达到单账号并发上限但仍健康的账号 (应急让位)
            active_accounts = [
                a
                for a in self._accounts.values()
                if a.account_id not in exclude_account_ids and a.is_active and a.is_ready() and not a.is_flood_locked()
            ]

        if not active_accounts:
            return None

        if task_type in ("download", "download_link", "download_range"):
            active_accounts.sort(
                key=lambda a: (0 if getattr(a, "tier", "primary") == "secondary" else 1, a.active_task_count)
            )
        else:
            active_accounts.sort(key=lambda a: a.active_task_count)

        parsed_chat = parse_chat_identifier(target_chat)

        for cand in active_accounts:
            try:
                if cand.client and cand.client.is_connected():
                    entity = await asyncio.wait_for(resolve_entity(cand.client, parsed_chat), timeout=3.0)
                    if entity:
                        logger.info(
                            "故障转移成功匹配: 账号 [%s] 具备私有频道 [%s] 权限", cand.account_name, target_chat
                        )
                        return cand
            except Exception as e:
                logger.debug("故障转移探测账号 [%s] 失败: %s", cand.account_name, e)

        return None

    def invalidate_accessible_cache(self, target_chat: str | int | None = None, account_id: int | None = None):
        """精准清除或刷新私有频道在群账号可达性缓存"""
        self._access_cache_epoch = getattr(self, "_access_cache_epoch", 0) + 1
        if target_chat is not None:
            try:
                parsed = parse_chat_identifier(target_chat)
                self._chat_accessible_cache.pop(str(parsed), None)
            except Exception:
                self._chat_accessible_cache.pop(str(target_chat), None)
            return

        if account_id is not None:
            keys_to_del = [k for k, (ts, aids) in self._chat_accessible_cache.items() if account_id in aids]
            for k in keys_to_del:
                self._chat_accessible_cache.pop(k, None)
            return

        self._chat_accessible_cache.clear()

    async def get_accessible_userbots(
        self, target_chat: str | int, exclude_account_ids: set[int] | None = None
    ) -> list[TelegramAccountClient]:
        """获取所有具备目标私密群组/频道访问权限的健康活跃 Userbot 账号 (带防雪崩缓存与并发探针，100% 排除 Bot 池)"""
        parsed_chat = parse_chat_identifier(target_chat)
        cache_key = str(parsed_chat)
        now = time.time()
        ttl = getattr(settings, "USERBOT_ACCESS_CACHE_TTL", 60.0)
        probe_epoch = getattr(self, "_access_cache_epoch", 0)

        # 1. 检查防雪崩缓存
        if cache_key in self._chat_accessible_cache:
            ts, acc_ids = self._chat_accessible_cache[cache_key]
            if now - ts < ttl:
                cached_accs = [
                    self._accounts[aid]
                    for aid in acc_ids
                    if aid in self._accounts
                    and self._accounts[aid].is_ready()
                    and (not exclude_account_ids or aid not in exclude_account_ids)
                ]
                if cached_accs:
                    return cached_accs

        # 2. 筛选健康活跃候选 Userbot (绝对红线：仅针对 _accounts，杜绝 Bot)
        candidates = [
            a
            for a in self._accounts.values()
            if a.is_active
            and a.is_ready()
            and a.client
            and a.client.is_connected()
            and not a.is_flood_locked()
            and (not exclude_account_ids or a.account_id not in exclude_account_ids)
        ]
        if not candidates:
            return []

        # 3. 并发轻量探针探测
        async def _probe(acc: TelegramAccountClient) -> TelegramAccountClient | None:
            try:
                entity = await asyncio.wait_for(resolve_entity(acc.client, parsed_chat), timeout=3.0)
                if entity:
                    return acc
            except Exception as ex:
                logger.debug("账号 [%s] 探测私密频道 [%s] 权限失败: %s", acc.account_name, target_chat, ex)
            return None

        results = await asyncio.gather(*[_probe(c) for c in candidates], return_exceptions=True)
        accessible = [r for r in results if isinstance(r, TelegramAccountClient)]

        # 4. 仅在无排除过滤且探针期间未发生缓存失效时回填全局防雪崩缓存
        if not exclude_account_ids and getattr(self, "_access_cache_epoch", 0) == probe_epoch:
            self._chat_accessible_cache[cache_key] = (now, [a.account_id for a in accessible])
        return accessible

    def get_all_accounts(self) -> list[TelegramAccountClient]:
        return list(self._accounts.values())

    def get_all_bots(self) -> list[TelegramBotClient]:
        return list(self._bots.values())

    def get_bot(self, bot_id: int) -> TelegramBotClient | None:
        return self._bots.get(bot_id)

    def get_bot_count(self) -> int:
        return len(self._bots)

    async def get_client_for_task(self, account_id: int | None = None, mode: str = "auto") -> TelegramClient | None:
        """获取可用于执行任务的 Telegram 客户端"""
        acc = self.get_account(account_id)
        if mode == "userbot":
            if acc and acc.client:
                if not acc.client.is_connected():
                    await acc.connect()
                return acc.client
            return None

        if mode == "bot_round_robin":
            bot = self.get_next_available_bot()
            if bot:
                return bot
            return acc.client if acc else None

        # auto 模式: 默认返回 userbot
        if acc and acc.client:
            if not acc.client.is_connected():
                await acc.connect()
            return acc.client
        return None

    def get_next_available_bot(self) -> TelegramClient | None:
        """从健康 Bot 池中轮询获取下一个可用 Bot 客户端"""
        ready_bots = [b for b in self._bots.values() if b.is_ready()]
        if not ready_bots:
            # 回退到默认账号
            default_acc = self.get_account()
            return default_acc.client if default_acc else None

        self._bot_index = (self._bot_index + 1) % len(ready_bots)
        bot = ready_bots[self._bot_index]
        return bot.client

    def get_all_clients(self) -> list[TelegramClient]:
        """获取所有可用客户端列表 (主 Userbot + 全部 Bot)"""
        clients = []
        if self.userbot:
            clients.append(self.userbot)
        if self.bots:
            for b in self.bots:
                if b not in clients:
                    clients.append(b)
        if clients:
            return clients
        return self.get_all_active_clients()

    def get_all_active_clients(self, account_id: int | None = None) -> list[TelegramClient]:
        """获取多通道并发加速下载所需的所有活跃 Client 实例池 (主 Userbot + 活跃 Bot 池)"""
        clients = []
        acc = self.get_account(account_id)
        if acc and acc.is_ready() and acc.client:
            clients.append(acc.client)

        for bot in self._bots.values():
            if bot.is_ready() and bot.client:
                clients.append(bot.client)

        # 降级: 若无完全 ready 的，返回已有 client 实例
        if not clients:
            if acc and acc.client:
                clients.append(acc.client)
            for bot in self._bots.values():
                if bot.client:
                    clients.append(bot.client)
        return clients

    def mark_flood_wait(self, client_or_id: TelegramClient | int | str, wait_seconds: int):
        """精准标记受限账号或 Bot 的 FloodWait 状态"""
        # 0. 规范化数字字符串 ID
        if isinstance(client_or_id, str) and (
            client_or_id.isdigit() or (client_or_id.startswith("-") and client_or_id[1:].isdigit())
        ):
            client_or_id = int(client_or_id)

        # 1. 尝试按账号 ID 匹配
        if isinstance(client_or_id, int) and client_or_id in self._accounts:
            self._accounts[client_or_id].mark_flood_wait(wait_seconds)
            self.invalidate_accessible_cache(account_id=client_or_id)
            try:
                from app.core.metrics import floodwait_total

                floodwait_total.labels(scope="account").inc()
            except Exception as m_err:
                logger.debug("更新 account FloodWait 指标异常 (非致命): %s", m_err)
            return

        # 2. 尝试按 Bot ID 匹配
        if isinstance(client_or_id, int) and client_or_id in self._bots:
            self._bots[client_or_id].mark_flood_wait(wait_seconds)
            try:
                from app.core.metrics import floodwait_total

                floodwait_total.labels(scope="bot").inc()
            except Exception as m_err:
                logger.debug("更新 bot FloodWait 指标异常 (非致命): %s", m_err)
            return

        # 3. 按 TelegramClient 实例反查匹配
        for acc in self._accounts.values():
            if acc.client == client_or_id:
                acc.mark_flood_wait(wait_seconds)
                self.invalidate_accessible_cache(account_id=acc.account_id)
                try:
                    from app.core.metrics import floodwait_total

                    floodwait_total.labels(scope="account").inc()
                except Exception as m_err:
                    logger.debug("更新 account FloodWait 指标异常 (非致命): %s", m_err)
                return

        for bot in self._bots.values():
            if bot.client == client_or_id:
                bot.mark_flood_wait(wait_seconds)
                try:
                    from app.core.metrics import floodwait_total

                    floodwait_total.labels(scope="bot").inc()
                except Exception as m_err:
                    logger.debug("更新 bot FloodWait 指标异常 (非致命): %s", m_err)
                return

        # 4. 保底 fallback: 未在池内注册的 client (如临时 client 或单测 mock) 默认按 account 记账
        try:
            from app.core.metrics import floodwait_total

            floodwait_total.labels(scope="account").inc()
        except Exception as m_err:
            logger.debug("更新 fallback FloodWait 指标异常 (非致命): %s", m_err)

    async def probe_premium(self, account_id: int | None = None, force: bool = False) -> bool:
        """探测指定账号或默认主账号的 Premium 身份"""
        acc = self.get_account(account_id)
        if not acc:
            return False
        return await acc.probe_premium(force=force)

    async def ping_bots(self) -> dict[int, bool]:
        """批量探活 Bot 池并更新状态与 DC ID"""
        return {item["bot_id"]: item["online"] for item in await self.ping_bots_detailed()}

    async def ping_bots_detailed(self) -> list[dict[str, Any]]:
        """批量详细探活 Bot 池，记录毫秒级延迟与错误明细"""
        before = self.get_startup_status()
        results = []
        for b_id, bot in list(self._bots.items()):
            if self._bots.get(b_id) is not bot:
                continue
            t0 = time.perf_counter()
            try:
                error = await asyncio.wait_for(bot.connect(probe=True), timeout=CONNECT_VERIFY_TIMEOUT_SECONDS)
            except TimeoutError:
                error = "Bot 连接或探活超时"
            if self._bots.get(b_id) is not bot:
                continue
            online = error is None and bot.is_ready_snapshot()
            results.append(
                {
                    "bot_id": b_id,
                    "bot_name": bot.bot_name,
                    "bot_username": bot.bot_username,
                    "online": online,
                    "latency_ms": round((time.perf_counter() - t0) * 1000, 2) if online else None,
                    "dc_id": bot.dc_id,
                    "status": bot.status,
                    "error": None if online else error or "Bot 未就绪",
                }
            )
        if before != self.get_startup_status():
            await connection_manager.broadcast({"type": "system_event", "event": "telegram_startup_changed"})
        return results

    async def add_account(
        self,
        account_name: str,
        phone: str,
        session_file: str,
        api_id: int,
        api_hash: str,
        is_active: int = 1,
        is_default: int = 0,
        max_concurrency: int = 1,
    ) -> TelegramAccountClient:
        """动态添加并挂载 Userbot 账号"""
        async with self._get_lock():
            record = AccountRepository.create(
                account_name=account_name,
                phone=phone,
                session_file=session_file,
                api_id=api_id,
                api_hash=api_hash,
                is_active=is_active,
                is_default=is_default,
                max_concurrency=max_concurrency,
            )
            acc_client = TelegramAccountClient(
                account_id=record["id"],
                account_name=record["account_name"],
                phone=record.get("phone", ""),
                session_file=record["session_file"],
                api_id=record["api_id"],
                api_hash=record["api_hash"],
                max_concurrency=record.get("max_concurrency", 1),
                is_active=bool(record.get("is_active", 1)),
                is_default=bool(record.get("is_default", 0)),
                dc_id=record.get("dc_id", 0),
            )
            self._accounts[record["id"]] = acc_client
            if is_default:
                for aid, a in self._accounts.items():
                    if aid != record["id"]:
                        a.is_default = False
            await acc_client.connect()
            if acc_client.is_default and acc_client.is_ready():
                await self.recover_startup_degraded()
            return acc_client

    def _sync_existing_account_fields(
        self,
        acc: TelegramAccountClient,
        account_id: int,
        account_name: str | None,
        phone: str | None,
        max_concurrency: int | None,
        is_active: bool | None,
        is_default: bool | None,
    ) -> None:
        if account_name is not None:
            acc.account_name = account_name
        if phone is not None:
            acc.phone = phone
            acc._pending_login = None
        if max_concurrency is not None:
            acc.max_concurrency = int(max_concurrency)
        if is_active is not None:
            acc.is_active = bool(is_active)
        if is_default is not None:
            acc.is_default = bool(is_default)
            if is_default:
                for aid, a in self._accounts.items():
                    if aid != account_id:
                        a.is_default = False

    def _register_account_from_record(self, record: dict[str, Any]) -> TelegramAccountClient:
        account_id = record["id"]
        d_model = record.get("device_model")
        s_ver = record.get("system_version")
        a_ver = record.get("app_version")
        l_code = record.get("lang_code") or "en"
        if not d_model or not s_ver or not a_ver:
            fp = generate_device_fingerprint(account_id)
            d_model = fp["device_model"]
            s_ver = fp["system_version"]
            a_ver = fp["app_version"]
            l_code = fp.get("lang_code", "en")
            AccountRepository.update_device_fingerprint(account_id, d_model, s_ver, a_ver, l_code)
        acc_client = TelegramAccountClient(
            account_id=account_id,
            account_name=record["account_name"],
            phone=record.get("phone", ""),
            session_file=record["session_file"],
            api_id=record["api_id"],
            api_hash=record["api_hash"],
            max_concurrency=record.get("max_concurrency", 1),
            is_active=bool(record.get("is_active", 1)),
            is_default=bool(record.get("is_default", 0)),
            dc_id=record.get("dc_id", 0),
            tier=record.get("tier", "primary"),
            device_model=d_model,
            system_version=s_ver,
            app_version=a_ver,
            lang_code=l_code,
            user_id=record.get("user_id"),
        )
        self._accounts[account_id] = acc_client
        if acc_client.is_default:
            for aid, a in self._accounts.items():
                if aid != account_id:
                    a.is_default = False
        return acc_client

    async def update_account(
        self,
        account_id: int,
        account_name: str | None = None,
        phone: str | None = None,
        max_concurrency: int | None = None,
        is_active: bool | None = None,
        is_default: bool | None = None,
    ) -> dict[str, Any] | None:
        """动态更新 Userbot 账号配置并同步运行时实例"""
        target_activation: TelegramAccountClient | None = None
        activated = False
        async with self._get_lock():
            updated = AccountRepository.update(
                account_id=account_id,
                account_name=account_name,
                phone=phone,
                max_concurrency=max_concurrency,
                is_active=is_active,
                is_default=is_default,
            )
            if updated and account_id in self._accounts:
                existing = self._accounts[account_id]
                activated = bool(updated.get("is_active")) and not existing.is_active
                self._sync_existing_account_fields(
                    existing,
                    account_id,
                    account_name,
                    phone,
                    max_concurrency,
                    is_active,
                    is_default,
                )
                if updated.get("is_active") and not existing.is_ready():
                    target_activation = existing
            elif updated and account_id not in self._accounts and updated.get("is_active"):
                activated = True
                target_activation = self._register_account_from_record(updated)
        if activated:
            self.invalidate_accessible_cache()
        if target_activation is not None:
            await target_activation.connect()
            if target_activation.is_ready():
                self.invalidate_accessible_cache()
                if target_activation.is_default:
                    await self.recover_startup_degraded()
        return updated

    async def remove_account(self, account_id: int, delete_session_file: bool = False):
        """动态移除并卸载 Userbot 账号，支持可选清理本地 Session 文件"""
        async with self._get_lock():
            session_file_name: str | None
            acc = self._accounts.get(account_id)
            if acc is not None:
                session_file_name = acc.session_file
                await acc.remove_record(retain_session=not delete_session_file)
                self._accounts.pop(account_id, None)
            else:
                db_acc = AccountRepository.get_by_id(account_id)
                session_file_name = db_acc.get("session_file") if db_acc else None
                _delete_account_record_sync(account_id, session_file_name, retain_session=not delete_session_file)

            if delete_session_file and session_file_name:
                try:
                    await asyncio.to_thread(_remove_session_files_sync, account_id, session_file_name)
                except Exception as e:
                    logger.warning("清理账号 [%d] session 文件异常: %s", account_id, e)

    async def add_bot(
        self, bot_token: str, bot_name: str, session_name: str, api_id: int, api_hash: str, is_active: int = 1
    ) -> TelegramBotClient:
        """动态添加并挂载 Bot"""
        async with self._get_lock():
            record = BotRepository.create(
                bot_token=bot_token,
                bot_name=bot_name,
                session_name=session_name,
                api_id=api_id,
                api_hash=api_hash,
                is_active=is_active,
            )
            bot_client = TelegramBotClient(
                bot_id=record["id"],
                bot_token=record["bot_token"],
                session_name=record["session_name"],
                api_id=record["api_id"],
                api_hash=record["api_hash"],
                is_active=bool(record.get("is_active", 1)),
                bot_name=bot_name,
                dc_id=record.get("dc_id", 0),
            )
            self._bots[record["id"]] = bot_client
            await bot_client.connect()
            return bot_client

    async def remove_bot(self, bot_id: int):
        """动态移除并卸载 Bot"""
        async with self._get_lock():
            BotRepository.delete(bot_id)
            bot = self._bots.pop(bot_id, None)
            if bot is not None:
                bot.is_active = False
                await bot.disconnect()

    # =========================================================================
    # 派生读写视图 (真实账号状态; 无 _fallback 影子双写 —— setter/deleter 直写 _accounts
    # 内的默认账号对象, 缺失时自动建号。测试 patch("...userbot", mock) 形态经此生效)
    # =========================================================================

    def _ensure_default_account(self) -> TelegramAccountClient:
        acc = self.get_account()
        if acc is None:
            acc = TelegramAccountClient(
                account_id=1,
                account_name="default_userbot",
                phone="",
                session_file="default_userbot.session",
                api_id=0,
                api_hash="",
                is_active=True,
                is_default=True,
            )
            self._accounts[acc.account_id] = acc
        return acc

    @property
    def userbot(self) -> TelegramClient | None:
        """默认主账号 Userbot 客户端 (派生自 get_account())"""
        acc = self.get_account()
        return acc.client if acc else None

    @userbot.setter
    def userbot(self, client: TelegramClient | None):
        self._ensure_default_account().client = client

    @userbot.deleter
    def userbot(self):
        self._ensure_default_account().client = None

    @property
    def bots(self) -> list[TelegramClient]:
        """全部活跃 Bot 客户端列表 (派生自真实 Bot 池)"""
        return [b.client for b in self.get_all_bots() if b.client is not None]

    @bots.setter
    def bots(self, clients: list[TelegramClient]):
        self._bots.clear()
        for i, c in enumerate(clients or [], start=1):
            bot = TelegramBotClient(
                bot_id=i,
                bot_token=f"{i}:runtime_seed",
                session_name=f"bot_runtime_{i}",
                api_id=0,
                api_hash="",
            )
            bot.client = c
            if c is not None:
                bot.status = "ready"
            self._bots[i] = bot

    @bots.deleter
    def bots(self):
        self._bots.clear()

    @property
    def bot_index(self) -> int:
        """当前 Bot 轮询索引 (真实轮询游标)"""
        return self._bot_index

    @bot_index.setter
    def bot_index(self, val: int):
        self._bot_index = val

    @bot_index.deleter
    def bot_index(self):
        self._bot_index = 0

    @property
    def auth_state(self) -> str:
        """主账号鉴权状态 (严格派生自默认活跃 Userbot，不回退至其他账号)"""
        acc = self.get_default_account_strict()
        return acc.auth_state if acc else "UNAUTHENTICATED"

    @auth_state.setter
    def auth_state(self, val: str):
        acc = self._ensure_default_account()
        acc.auth_state = val
        if val == "AUTHENTICATED":
            acc._authenticated_before_flood = True
        elif val == "UNAUTHENTICATED":
            acc._authenticated_before_flood = False

    @auth_state.deleter
    def auth_state(self):
        acc = self._ensure_default_account()
        acc.auth_state = "UNAUTHENTICATED"
        acc._authenticated_before_flood = False

    @property
    def is_premium(self) -> bool:
        """主账号 Premium 状态 (派生自默认账号; 粘性语义在账号层 probe_premium —— 探测失败保持旧值)"""
        acc = self.get_account()
        return bool(acc.is_premium) if acc else False

    @is_premium.setter
    def is_premium(self, val: bool):
        self._ensure_default_account().is_premium = bool(val)

    @is_premium.deleter
    def is_premium(self):
        self._ensure_default_account().is_premium = False

    @property
    def premium_checked_at(self) -> float:
        """主账号 Premium 探测时间戳 (派生自默认账号)"""
        acc = self.get_account()
        return acc.premium_checked_at if acc else 0.0

    @premium_checked_at.setter
    def premium_checked_at(self, val: float):
        self._ensure_default_account().premium_checked_at = val

    @premium_checked_at.deleter
    def premium_checked_at(self):
        self._ensure_default_account().premium_checked_at = 0.0

    async def disconnect(self):
        """向后兼容别名：断开所有 Telegram 客户端连接"""
        await self.disconnect_all()

    async def refresh_premium(self, force: bool = False) -> bool:
        """刷新默认主账号 Premium 状态 (TTL 内幂等; 探测失败保持旧值 —— 粘性语义在账号层)"""
        return await self.probe_premium(force=force)

    def effective_max_file_size_bytes(self, account_id: int | None = None) -> int:
        """返回生效的最大单文件大小 (字节，普通号 2000 MiB (1.95 GiB = 2,097,152,000 字节)，Premium 号 4000 MiB (3.91 GiB = 4,194,304,000 字节))"""
        if account_id is not None:
            acc = self.get_account(account_id)
            if acc:
                return acc.effective_max_file_size_bytes()
        tier_limit = settings.PREMIUM_MAX_FILE_SIZE_BYTES if self.is_premium else settings.MAX_FILE_SIZE_BYTES
        return min(tier_limit, settings.PROTOCOL_MAX_FILE_SIZE_BYTES)

    def resolve_client_account_id(self, client: Any) -> int | None:
        """根据 TelegramClient 实例反查所属 Userbot 账号 ID"""
        if client is None:
            return None
        for acc in self._accounts.values():
            if acc.client is not None and acc.client == client:
                return acc.account_id
        return None

    def is_bot_client(self, client: Any) -> bool:
        """判定 TelegramClient 实例是否属于 Bot 池"""
        if client is None:
            return False
        return any(bot.client is not None and bot.client == client for bot in self._bots.values())

    def invalidate_bot_by_client(self, client: Any, reason: str = "") -> bool:
        """将指定 Bot 客户端标记为 invalid (避免会话失效跨账号污染 Userbot)"""
        if client is None:
            return False
        for bot in self._bots.values():
            if bot.client is not None and bot.client == client:
                logger.warning("Bot [%s] 会话失效: %s", bot.session_name, reason)
                bot.status = "invalid"
                BotRepository.update_status(bot.bot_id, "invalid")
                return True
        return False

    def invalidate_session(self, reason: str = "", account_id: int | None = None):
        """会话失效统一处置: 重置指定账号状态 (未指定时回退至默认主账号)"""
        acc = self.get_account(account_id) if account_id is not None else self.get_account()
        if acc:
            acc.invalidate_session(reason)

    # --- 🔒 Web 2FA 动态验证码 ---
    async def send_web_2fa_code(self, client_ip: str) -> bool:
        acc = self.get_default_account_strict()
        if not acc:
            return False
        return await acc.send_web_2fa_code(client_ip)

    def verify_web_2fa_code(self, client_ip: str, code: str) -> bool:
        acc = self.get_default_account_strict()
        if not acc:
            return False
        return acc.verify_web_2fa_code(client_ip, code)

    # --- 🌐 原生 Telegram 验证码登录 ---
    async def native_send_code(self, phone: str) -> str:
        acc = self.get_account()
        if not acc:
            raise RuntimeError("Telegram 客户端未初始化或缺少可用 Userbot 账号")
        return await acc.native_send_code(phone)

    async def native_verify_code(
        self, phone: str, code: str, phone_code_hash: str, password: str | None = None
    ) -> bool:
        acc = self.get_account()
        if not acc:
            raise RuntimeError("Telegram 客户端未初始化或缺少可用 Userbot 账号")
        if isinstance(acc, TelegramAccountClient) and not isinstance(getattr(acc, "_pending_login", None), dict):
            raise ValueError("请先请求下发登录验证码")
        success = await acc.native_verify_code(phone, code, phone_code_hash, password=password)
        if success:
            await self.recover_startup_degraded()
        return success


client_manager = ClientManager()
