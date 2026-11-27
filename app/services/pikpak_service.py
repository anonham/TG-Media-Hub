"""PikPak CLI 服务包装。

设计要点 (docs/architecture/STREAMING_AND_SPLITTING.md §1):
- 子进程: asyncio.create_subprocess_exec(settings.PIKPAK_CLI_BIN, *args[, -F json]);
  全局 spawn 信号量限流 (2C2G 防树浏览器高频 ls 打爆); 单调用超时 + kill 兜底。
- 子进程 env: os.environ 复制 + PIKPAK_TOKEN (settings 有值才注入, 保证 token_source
  探测与 CLI 实际行为一致——CLI 凭据优先级 env > PAT > OAuth) + PIKPAK_NO_UPDATE_NOTIFIER
  + PIKPAK_NO_LOG; PIKPAK_DIR 不新增 config 字段, 由容器 ENV/用户环境透传。
- 错误映射: CLI 统一 stderr schema (-F json 时 {"error":{code,...}}, 退出码 1/2, 错误码
  大写归一) → PikPakError 体系; message 全部经 redact_text, 不含凭据/直链。
- 凭据恢复单飞: 并发 PikPakAuthError 经 asyncio.Lock 只触发一次恢复探针; 探针结局
  分类 (ok/auth/transient/cli) 供 auth/status 三态交叉归因, 仅探针成功置恢复窗。
- 日志红线: 直链/Token 永不落日志 (公共脱敏器 redact_text 全量兜底)。

命名冲突注记: 本模块 PikPakRiskControlError (CLI 控制面) 与
app/core/stream_chunk_provider.py 的同名类 (CDN Content-Type 数据面) 不同层,
import 时不得混用。
"""

import asyncio
import collections
import json
import logging
import os
import re
import sys
import time
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from app.config import settings
from app.core.pikpak.expander import ParsedFileItem
from app.core.pikpak.taxonomy import (
    PikPakAuthError,
    PikPakCliError,
    PikPakCliTimeoutError,
    PikPakError,
    PikPakNotFoundError,
    PikPakQuotaError,
    PikPakRiskControlError,
    PikPakTransientError,
)
from app.core.redact import redact_text

logger = logging.getLogger(__name__)

DEFAULT_CLI_TIMEOUT_S = 60.0
THUMBNAIL_LOOKUP_TIMEOUT_S = 2.0
# spawn 限流上限
_CLI_SPAWN_LIMIT = 3
# 强杀后的管道排水超时 (秒): taskkill 失败/超时 (suppress 吞掉) 时孙进程会孤儿化并继续
# 持有 stdout/stderr 管道 —— 无界 communicate 将永不返回并占死 spawn 信号量槽, 3 次即
# 堵死全部 CLI 操作; 超时即放弃排水并留日志
_CLI_DRAIN_TIMEOUT_S = 10.0
# 恢复探针成功后的时间窗: 窗口内的并发恢复请求直接复用 (不重复探针)
_RECOVERY_GUARD_S = 30.0
# 探针失败结局 memo 复用窗: 持续断网下并发/高频请求复用失败结局, 防探针风暴
# (成功窗只挡成功复用 —— 失败路径原实现无任何复用, N 并发会产生 N 次探针)
_PROBE_REUSE_S = 15.0


@dataclass
class PikPakServiceState:
    """PikPak CLI 服务运行态容器 (替代裸 global 变量修改)"""

    spawn_semaphore: asyncio.Semaphore | None = None
    refresh_lock: asyncio.Lock | None = None
    refresh_success_at: float = 0.0
    last_probe: tuple = (0.0, "")


pikpak_service_state = PikPakServiceState()
_spawn_semaphore: asyncio.Semaphore | None = None
_refresh_lock: asyncio.Lock | None = None
_refresh_success_at: float = 0.0
# 最近一次恢复探针结局 (monotonic, ok|auth|transient|cli): 供 classify_auth_failure 交叉归因复用
_last_probe: tuple = (0.0, "")


def _set_pikpak_attr(name: str, val: Any) -> None:
    setattr(sys.modules[__name__], name, val)


# ---------------------------------------------------------------------------
# 错误体系 (唯一实现在 core/pikpak/taxonomy.py, 本模块仅内部消费;
# _map_cli_error 映射表不变; isinstance 语义不变 —— 同一类对象)
# ---------------------------------------------------------------------------


def _get_spawn_semaphore() -> asyncio.Semaphore:
    sem = _spawn_semaphore
    if sem is None:
        sem = asyncio.Semaphore(_CLI_SPAWN_LIMIT)
        pikpak_service_state.spawn_semaphore = sem
        _set_pikpak_attr("_spawn_semaphore", sem)
    return sem


def _get_refresh_lock() -> asyncio.Lock:
    lock = _refresh_lock
    if lock is None:
        lock = asyncio.Lock()
        pikpak_service_state.refresh_lock = lock
        _set_pikpak_attr("_refresh_lock", lock)
    return lock


def reset_runtime_state_for_tests() -> None:
    """测试辅助: 重置事件循环绑定的模块级原语 (pytest-asyncio 每用例新 loop, 复用会跨 loop 报错)"""
    pikpak_service_state.spawn_semaphore = None
    pikpak_service_state.refresh_lock = None
    pikpak_service_state.refresh_success_at = 0.0
    pikpak_service_state.last_probe = (0.0, "")
    _set_pikpak_attr("_spawn_semaphore", None)
    _set_pikpak_attr("_refresh_lock", None)
    _set_pikpak_attr("_refresh_success_at", 0.0)
    _set_pikpak_attr("_last_probe", (0.0, ""))
    _auth_status_cache.clear()


# ---------------------------------------------------------------------------
# CLI 进程包装与错误映射
# ---------------------------------------------------------------------------

_ERROR_CODE_RE = re.compile(r"Code:\s*([A-Za-z0-9_]+)")

_AUTH_CODES = {
    "UNAUTHENTICATED",
    "UNAUTHORIZED",
    "TOKEN_EXPIRED",
    "TOKEN_REFRESH_FAILED",
    "CREDENTIALS_ERROR",
    "ACCESS_DENIED",
}
_QUOTA_CODES = {"CONNECTED_APPS_QUOTA_EXCEEDED", "FILE_SPACE_NOT_ENOUGH", "TRANSFER_QUOTA_EXCEEDED"}
_NOT_FOUND_CODES = {"FILE_NOT_FOUND", "INVALID_FILE_ID", "INVALID_PARENT_ID"}
_TRANSIENT_CODES = {"NETWORK_ERROR", "TIMEOUT", "REQUEST_FAILED"}
_RISK_HINTS = ("risk", "verify", "captcha")


def _extract_error_code(stderr_text: str) -> str | None:
    """优先 stderr JSON 的 error.code, 回退文本行 `Code: XXX`; 错误码大写归一"""
    text = stderr_text.strip()
    if text:
        with suppress(json.JSONDecodeError):
            data = json.loads(text)
            code = (data.get("error") or {}).get("code") if isinstance(data, dict) else None
            if code:
                return str(code).upper()
    match = _ERROR_CODE_RE.search(stderr_text)
    if match:
        return match.group(1).upper()
    return None


def _map_cli_error(stderr_text: str, returncode: int) -> PikPakError:
    """CLI 失败 → PikPakError 子类; message 只含 code/摘要, 全文经 redact_text (安全红线)"""
    code = _extract_error_code(stderr_text)
    lowered = stderr_text.lower()
    if code in _AUTH_CODES:
        return PikPakAuthError(f"PikPak 认证/授权失败 ({code})")
    if code in _QUOTA_CODES:
        return PikPakQuotaError(f"PikPak 配额受限 ({code})")
    if code in _NOT_FOUND_CODES:
        return PikPakNotFoundError(f"PikPak 资源不存在 ({code})")
    if code in _TRANSIENT_CODES:
        return PikPakTransientError(f"PikPak 网络瞬断 ({code})")
    if any(h in lowered for h in _RISK_HINTS):
        return PikPakRiskControlError(f"PikPak 风控拦截 (rc={returncode})")
    return PikPakCliError(redact_text(f"PikPak CLI 调用失败 (rc={returncode}, code={code or 'UNKNOWN'})"))


async def _kill_process_tree(proc: asyncio.subprocess.Process) -> None:
    """超时兜底: 强杀进程树。Windows 下 launcher (.cmd) 会派生 cmd.exe→CLI 孙进程,
    TerminateProcess 只杀直接子进程, 孙进程会被孤儿化并继续持有 stdout/stderr 管道
    (communicate 永不返回) —— 用 taskkill /T 连树杀; POSIX kill 直杀单进程即可。"""
    with suppress(Exception):
        if sys.platform == "win32" and proc.returncode is None:
            killer = await asyncio.create_subprocess_exec(
                "taskkill",
                "/T",
                "/F",
                "/PID",
                str(proc.pid),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(killer.wait(), timeout=5.0)
    with suppress(Exception):
        proc.kill()
    with suppress(Exception):
        # 有界排水: 上方 taskkill 失败/超时被吞时, 孙进程仍持管道,
        # 无界 communicate 会占死 spawn 信号量槽 —— 超时即放弃并留日志
        await asyncio.wait_for(proc.communicate(), timeout=_CLI_DRAIN_TIMEOUT_S)
        return
    logger.warning("PikPak CLI 强杀后管道排水超时 (%.1fs), 放弃等待孤儿管道关闭", _CLI_DRAIN_TIMEOUT_S)


# Windows 批处理启动器场景下参数中的 shell 元字符 (CWE-78): cmd.exe 解释 .bat/.cmd 命令行时,
# 未加引号的 & | < > ^ 是命令/重定向分隔符, % ! 触发变量/延迟展开, 引号破坏 list2cmdline 配对
_BATCH_LAUNCHER_SUFFIXES = (".cmd", ".bat")
_BATCH_METACHAR_RE = re.compile(r'[&|<>^%!"\r\n]')


def _reject_batch_launcher_metachars(argv: list[str]) -> None:
    """PIKPAK_CLI_BIN 指向 Windows .cmd/.bat 时, 子进程经 shell 解释执行, Python 对批处理
    参数不做任何转义防护 (list2cmdline 仅按空格/引号决定是否加引号) —— 远端目录/文件名可携带
    shell 元字符注入命令。批处理启动器场景下含元字符的参数一律拒绝, 引导改用直连可执行文件;
    非 batch 启动器 (.exe/无后缀) 无 shell 介入, 参数按字面传递, 不受影响。"""
    launcher = str(settings.PIKPAK_CLI_BIN or "")
    if not launcher.lower().endswith(_BATCH_LAUNCHER_SUFFIXES):
        return
    for a in argv:
        if _BATCH_METACHAR_RE.search(a):
            raise PikPakCliError(
                f"PIKPAK_CLI_BIN 为 Windows 批处理启动器, 参数含 shell 元字符已拒绝执行 "
                f"(请改用直接可执行文件): {redact_text(a)}"
            )


async def _communicate_with_deadline(
    proc: asyncio.subprocess.Process,
    cmd_name: str,
    deadline: float,
    timeout_s: float,
) -> tuple[bytes, bytes]:
    """按剩余总体截止预算等待子进程退出，超时或取消时强杀整棵进程树。"""
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            await _kill_process_tree(proc)
            logger.warning("PikPak CLI 调用超时已强杀: %s", cmd_name)
            raise PikPakCliTimeoutError(f"PikPak CLI 调用超时 ({timeout_s}s), 进程已终止")
        return await asyncio.wait_for(proc.communicate(), timeout=remaining)
    except TimeoutError:
        await _kill_process_tree(proc)
        logger.warning("PikPak CLI 调用超时已强杀: %s", cmd_name)
        raise PikPakCliTimeoutError(f"PikPak CLI 调用超时 ({timeout_s}s), 进程已终止") from None
    except asyncio.CancelledError:
        # 调用方取消: communicate 被取消不会终止进程 —— 先杀树再放行
        # 信号量槽, 否则孤儿 CLI 继续存活且 spawn 并发上限被绕过; 控制流异常原样上抛
        await _kill_process_tree(proc)
        raise


async def _run_cli(args: Sequence[str], *, timeout_s: float = DEFAULT_CLI_TIMEOUT_S, json_mode: bool = True) -> Any:
    """执行 `PIKPAK_CLI_BIN <args...> [-F json]`, 返回解析后的 JSON (json_mode) 或 stdout 文本。

    spawn 信号量内执行; 超时 kill 兜底; 非零退出 → _map_cli_error; JSON 解析失败 → PikPakCliError。
    Windows 注记: PIKPAK_CLI_BIN 可指向 .cmd launcher (CreateProcess 空应用名语义支持批处理);
    Python 对 .bat/.cmd 参数不做 shell 转义防护, 批处理启动器场景下含 shell 元字符的参数
    在 spawn 前被 _reject_batch_launcher_metachars 拒绝。"""
    argv = list(args)
    if json_mode and "-F" not in argv:
        argv += ["-F", "json"]
    _reject_batch_launcher_metachars(argv)
    env = dict(os.environ)
    env.pop("TG_BOOTSTRAP_TOKEN", None)
    if settings.PIKPAK_TOKEN:
        env["PIKPAK_TOKEN"] = settings.PIKPAK_TOKEN
    env["PIKPAK_NO_UPDATE_NOTIFIER"] = "1"
    env["PIKPAK_NO_LOG"] = "1"

    # 控制面代理隔离: 仅在 CLI 子进程 env 副本显式注入代理, 主进程与 aiohttp 绝对不受污染
    proxy_url = None
    try:
        from app.db.account_repository import SystemSettingsRepository

        proxy_url = SystemSettingsRepository.get_str(
            "pikpak_proxy_url", getattr(settings, "PIKPAK_PROXY_URL", "") or ""
        )
    except Exception:
        proxy_url = getattr(settings, "PIKPAK_PROXY_URL", "") or ""

    if proxy_url and str(proxy_url).strip():
        clean_proxy = str(proxy_url).strip()
        env["HTTP_PROXY"] = clean_proxy
        env["HTTPS_PROXY"] = clean_proxy
        env["http_proxy"] = clean_proxy
        env["https_proxy"] = clean_proxy
        env["all_proxy"] = clean_proxy

    cmd_name = str(argv[0]) if argv else "<empty>"
    deadline = time.monotonic() + max(0.01, float(timeout_s))
    sem = _get_spawn_semaphore()
    try:
        await asyncio.wait_for(sem.acquire(), timeout=max(0.01, deadline - time.monotonic()))
    except TimeoutError:
        logger.warning("PikPak CLI 等待并发槽超时: %s", cmd_name)
        raise PikPakCliTimeoutError(f"PikPak CLI 调用超时 ({timeout_s}s)") from None

    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PikPakCliTimeoutError(f"PikPak CLI 调用超时 ({timeout_s}s)")
        proc = await asyncio.create_subprocess_exec(
            settings.PIKPAK_CLI_BIN,
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout_b, stderr_b = await _communicate_with_deadline(proc, cmd_name, deadline, timeout_s)
    finally:
        sem.release()

    stderr_text = stderr_b.decode("utf-8", "replace")
    if proc.returncode is not None and proc.returncode != 0:
        raise _map_cli_error(stderr_text, proc.returncode)

    stdout_text = stdout_b.decode("utf-8", "replace")
    if not json_mode:
        return stdout_text
    try:
        return json.loads(stdout_text)
    except json.JSONDecodeError:
        raise PikPakCliError(f"PikPak CLI 输出 JSON 解析失败 ({cmd_name})") from None


# ---------------------------------------------------------------------------
# 凭据刷新单飞
# ---------------------------------------------------------------------------


async def _credential_recovery_barrier() -> str:
    """恢复探针 (`auth status`, 让 CLI 内部自然刷新并保存凭据): asyncio.Lock 串行 +
    双重复用窗——30s 成功窗 (既有) 与 15s 失败结局 memo (防探针风暴)。

    返回探针结局: "ok"=探针成功 (唯一置 _refresh_success_at 的路径) | "ok_window"=成功窗
    内复用 | "auth"=探针报认证错误 (真未绑定/凭据失效) | "transient"=探针网络瞬断 |
    "cli"=探针 CLI 异常。缺陷③修复 (tristate plan §1.1): 仅探针成功才置成功时间戳——
    探针非认证失败 (网络瞬断/CLI 异常) 不再伪占 30s 恢复窗; 探针任何失败不再上抛,
    结局经返回值分类, 供 _with_auth_recovery 与 auth/status 交叉归因消费。"""
    async with _get_refresh_lock():
        now = time.monotonic()
        if now - _refresh_success_at < _RECOVERY_GUARD_S:
            return "ok_window"
        last_ts, last_outcome = _last_probe
        if last_outcome in ("auth", "transient", "cli") and now - last_ts < _PROBE_REUSE_S:
            return last_outcome
        try:
            await _run_cli(["auth", "status"])
        except PikPakAuthError:
            probe_val = (time.monotonic(), "auth")
            pikpak_service_state.last_probe = probe_val
            _set_pikpak_attr("_last_probe", probe_val)
            return "auth"
        except PikPakTransientError:
            probe_val = (time.monotonic(), "transient")
            pikpak_service_state.last_probe = probe_val
            _set_pikpak_attr("_last_probe", probe_val)
            return "transient"
        except PikPakError:
            probe_val = (time.monotonic(), "cli")
            pikpak_service_state.last_probe = probe_val
            _set_pikpak_attr("_last_probe", probe_val)
            return "cli"
        success_ts = time.monotonic()
        probe_ok = (success_ts, "ok")
        pikpak_service_state.refresh_success_at = success_ts
        pikpak_service_state.last_probe = probe_ok
        _set_pikpak_attr("_refresh_success_at", success_ts)
        _set_pikpak_attr("_last_probe", probe_ok)
        return "ok"


async def _with_auth_recovery[T](coro_factory: Callable[[], Awaitable[T]]) -> T:
    """执行 CLI 调用; PikPakAuthError → 经单飞恢复屏障分类: 探针结局 ok/ok_window
    (恢复有望) 重试一次; 探针证实仍失败 (auth/transient/cli) → 不做必然失败的无效
    重试, 直接上抛原认证错误。"""
    try:
        return await coro_factory()
    except PikPakAuthError:
        outcome = await _credential_recovery_barrier()
        if outcome in ("ok", "ok_window"):
            return await coro_factory()
        raise


# ---------------------------------------------------------------------------
# LRU 缓存原语
# ---------------------------------------------------------------------------


class _LruCache:
    """容量上限与可选 TTL LRU (OrderedDict; 存储 (expire_at, value); 命中移队尾, 超容或超时淘汰)"""

    def __init__(self, max_entries: int, default_ttl_s: float | None = None):
        self._max = max(1, int(max_entries))
        self._default_ttl_s = float(default_ttl_s) if default_ttl_s is not None else None
        self._store: collections.OrderedDict[tuple, tuple[float | None, Any]] = collections.OrderedDict()

    def get(self, key, now: float | None = None):
        if key not in self._store:
            return None
        expire_at, value = self._store[key]
        curr = time.monotonic() if now is None else now
        if expire_at is not None and curr >= expire_at:
            self._store.pop(key, None)
            return None
        self._store.move_to_end(key)
        return value

    def put(self, key, value, ttl_s: float | None = None, now: float | None = None) -> None:
        eff_ttl = ttl_s if ttl_s is not None else self._default_ttl_s
        curr = time.monotonic() if now is None else now
        expire_at = (curr + eff_ttl) if eff_ttl is not None else None
        self._store[key] = (expire_at, value)
        self._store.move_to_end(key)
        while len(self._store) > self._max:
            self._store.popitem(last=False)

    def remove_prefix(self, prefix) -> int:
        """移除键首段 == prefix 的全部条目, 返回移除数"""
        doomed = [k for k in self._store if k[0] == prefix]
        for k in doomed:
            self._store.pop(k, None)
        return len(doomed)

    def clear(self) -> None:
        self._store.clear()

    def __len__(self) -> int:
        return len(self._store)


# 鉴权状态 60s 内存 TTL 缓存 (默认命中快照免除 6s 冷启动)
_auth_status_cache = _LruCache(4, default_ttl_s=60.0)


# ---------------------------------------------------------------------------
# 鉴权域
# ---------------------------------------------------------------------------


async def auth_status(force: bool = False, *, timeout_s: float = DEFAULT_CLI_TIMEOUT_S) -> dict:
    """`auth status -F json` → 账号/空间摘要 + token_source 探测。

    支持 60s 内存 TTL 缓存 (force=False 默认命中快照, force=True 强制刷新)。
    token_source: 'env' if settings.PIKPAK_TOKEN else 'stored' (本服务推断口径;
    CLI 凭据优先级 env > PAT > OAuth, 注入行为见 _run_cli)。未登录 → PikPakAuthError。"""
    cache_key = ("status", settings.PIKPAK_TOKEN)
    if not force:
        cached = _auth_status_cache.get(cache_key)
        if cached is not None:
            return dict(cached)

    data = await _with_auth_recovery(lambda: _run_cli(["auth", "status"], timeout_s=timeout_s))
    if not isinstance(data, dict) or not data.get("logged_in"):
        raise PikPakAuthError("PikPak 未登录 (CLI 凭据缺失或已失效)")
    res = {
        "logged_in": True,
        "user_id": data.get("user_id", ""),
        "name": data.get("name", ""),
        "email": data.get("email", ""),
        "vip": data.get("vip") or {},
        "storage": data.get("storage") or {},
        "additional_quotas": data.get("additional_quotas") or {},
        "token_source": "env" if settings.PIKPAK_TOKEN else "stored",
    }
    _auth_status_cache.put(cache_key, res)
    return dict(res)


async def classify_auth_failure() -> str:
    """认证域失败交叉归因 (tristate plan §2.3): 区分「真未绑定/凭据失效」与「够不到 PikPak」。

    返回 "not_bound" | "transient"。优先复用 _last_probe 新鲜结局 —— 认证错误走
    _with_auth_recovery 时屏障必然刚执行过探针, memo 必新鲜, 本函数通常零额外 spawn;
    无新鲜结局 (防御性, 理论不达) → 经屏障单飞补一次探针。"""
    last_ts, last_outcome = _last_probe
    if last_outcome in ("auth", "transient", "cli") and time.monotonic() - last_ts < _PROBE_REUSE_S:
        return "transient" if last_outcome in ("transient", "cli") else "not_bound"
    outcome = await _credential_recovery_barrier()
    return "transient" if outcome in ("transient", "cli") else "not_bound"


async def device_login_start(*, timeout_s: float = DEFAULT_CLI_TIMEOUT_S) -> dict:
    """`auth login -m device -F json` → {device_code, verification_uri, user_code, expires_in,
    interval} (CLI 输出后即退出, 不自动轮询); 轮询节奏/终止判定由调用方 驱动。"""
    data = await _run_cli(["auth", "login", "-m", "device"], timeout_s=timeout_s)
    for key in ("device_code", "verification_uri", "user_code"):
        if not isinstance(data, dict) or not data.get(key):
            raise PikPakCliError(f"设备码登录启动响应缺少字段: {key}")
    return data


async def device_login_poll(device_code: str, *, timeout_s: float = DEFAULT_CLI_TIMEOUT_S) -> dict:
    """`auth login --continue <device_code> -F json` 单次轮询。

    status=='success' → 完成 (含账号摘要); 非 success 状态原样上呈 (授权等待/过期/拒绝;
    非 success 状态值属「待校准」输入缺口, 不得写死到 UI 文案)。"""
    data = await _run_cli(["auth", "login", "--continue", device_code], timeout_s=timeout_s)
    if not isinstance(data, dict) or not data.get("status"):
        raise PikPakCliError("设备码轮询响应缺少 status 字段")
    if data.get("status") == "success":
        _auth_status_cache.clear()
        _files_lru.clear()
        _meta_cache.clear()
        _archive_folder_lru.clear()
    return data


# ---------------------------------------------------------------------------
# 文件域: ls 分页 LRU / JIT 直链 / mv / rm / audit / 封面
# ---------------------------------------------------------------------------

# auto_cleanup 默认 archive 归档模式 (
# 不立即释放配额、mv 不变); delete (rm) 为显式可选模式
ARCHIVE_FOLDER_NAME = "已发布归档"


# ParsedFileItem 实现在 core/pikpak/expander.py (顶部 import);
# `ls --raw` 解析 (_parse_ls_item) 仍留在本模块 —— 它是 CLI 包装层的 IO 域逻辑。


def _parse_ls_item(raw: dict) -> ParsedFileItem:
    params = raw.get("params") or {}
    medias = raw.get("medias") or []
    audio_codec = None
    for m in medias:
        if isinstance(m, dict) and "video" in m and isinstance(m["video"], dict):
            audio_codec = m["video"].get("audio_codec")
            if audio_codec:
                break
    if not audio_codec and isinstance(params, dict) and "audio" in params and isinstance(params["audio"], dict):
        audio_codec = params["audio"].get("audio_codec") or params["audio"].get("codec")

    video_codec = None
    for m in medias:
        if isinstance(m, dict) and "video" in m and isinstance(m["video"], dict):
            video_codec = m["video"].get("video_codec") or m["video"].get("codec")
            if video_codec:
                break
    if not video_codec and isinstance(params, dict) and "video" in params and isinstance(params["video"], dict):
        video_codec = params["video"].get("video_codec") or params["video"].get("codec")

    return ParsedFileItem(
        file_id=str(raw.get("id", "")),
        name=str(raw.get("name", "")),
        is_dir=(raw.get("kind") == "drive#folder"),
        size_bytes=int(raw.get("size") or 0),  # raw 的 size 为字符串字节 ("392927574")
        audit=raw.get("audit") or {},
        thumbnail_link=raw.get("thumbnail_link") or None,
        duration_s=params.get("duration"),
        width=params.get("width"),
        height=params.get("height"),
        parent_id=str(raw.get("parent_id") or ""),
        audio_codec=audio_codec,
        video_codec=video_codec,
    )


DEFAULT_FILES_CACHE_TTL_S = 60.0

# 模块级单例: ls 分页缓存 (键 (folder_id, page_token), 容量默认 128, 默认 60s TTL) 与
# audit/thumbnail 内存缓存 (键 file_id, 容量 256; 仅供 封面注入与 audit
# 拦截消费——禁落盘/禁 DB/禁 WS), 以及目录查找 LRU 缓存 (键 (parent_id, name))
_files_lru = _LruCache(128, default_ttl_s=DEFAULT_FILES_CACHE_TTL_S)
_meta_cache = _LruCache(256)
_archive_folder_lru = _LruCache(128)


def reset_files_cache_for_tests() -> None:
    """测试辅助: 清空文件域缓存与鉴权状态缓存 (跨用例隔离)"""
    _files_lru.clear()
    _meta_cache.clear()
    _archive_folder_lru.clear()
    _auth_status_cache.clear()


def invalidate_folder_cache(folder_id: str) -> int:
    """mv/rm/mkdir 等变更操作后按目录失效分页缓存 (供上层/后续包调用)。

    解耦设计: 绝不清理 _archive_folder_lru。移动或增删文件仅改变目录内文件列表,
    归档目录本身的 ID 永不变更。
    """
    return _files_lru.remove_prefix(folder_id)


async def list_files_raw(
    folder_id: str = "",
    page_token: str | None = None,
    *,
    use_cache: bool = True,
    timeout_s: float = DEFAULT_CLI_TIMEOUT_S,
) -> tuple:
    """`ls --raw` 单页列表: 返回 (ParsedFileItem 列表, next_page_token)。

    分页严格按返回的 next_page_token 透传 (`-l` 小于 10 会被服务端抬到 10, 以实际
    token 为准; 本封装不传 -l); 命中 LRU 不 spawn CLI (配合 spawn 限流)。
    不提供任何 search 封装——服务端已禁用 search, 树浏览只能逐级 ls。
    mv/rm/mkdir 等变更操作后必须 invalidate_folder_cache(folder_id)。"""
    key = (folder_id, page_token)
    if use_cache:
        cached = _files_lru.get(key)
        if cached is not None:
            return cached
    args = ["ls", "--raw"]
    if folder_id:
        args += ["-p", folder_id]
    if page_token:
        args += ["-t", page_token]
    data = await _with_auth_recovery(lambda: _run_cli(args, timeout_s=timeout_s))
    items = [_parse_ls_item(raw) for raw in (data.get("files") or [])]
    next_token = data.get("next_page_token") or None
    for item in items:
        if not item.is_dir:
            _meta_cache.put(
                item.file_id,
                {
                    "audit": item.audit,
                    "thumbnail_link": item.thumbnail_link,
                    "duration_s": item.duration_s,
                    "width": item.width,
                    "height": item.height,
                    "audio_codec": item.audio_codec,
                    "video_codec": item.video_codec,
                },
            )
    result = (items, next_token)
    _files_lru.put(key, result)
    return result


async def get_download_links(
    file_ids: Sequence[str],
    *,
    timeout_s: float = DEFAULT_CLI_TIMEOUT_S,
) -> list:
    """`download <ids...> --url-only` 单次 spawn 批量 JIT 直链 (禁止 get --url-type
    download, 省一次探活请求); 输出为纯文本直链逐行 (非 JSON)。

    红线: 直链只存内存、只传给 Provider, 禁止写日志/DB/WS;
    本函数不产生任何含 URL 的日志。"""
    if not file_ids:
        return []
    args = ["download", *file_ids, "--url-only"]
    text = await _with_auth_recovery(lambda: _run_cli(args, timeout_s=timeout_s, json_mode=False))
    links = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(links) != len(file_ids):
        raise PikPakCliError(f"直链数量不符: 请求 {len(file_ids)} 实得 {len(links)}")
    return links


async def get_thumbnail_link(
    file_id: str,
    *,
    timeout_s: float = THUMBNAIL_LOOKUP_TIMEOUT_S,
) -> str | None:
    """`get <file_id> --raw -F json` 单次有界回查官方封面链接 (不走鉴权恢复与重试)。

    仅返回顶层合法 http(s) 字符串, 缺失或非法返回 None; 失败降级由传输引擎按 best-effort 管理。
    红线: 封面 URL 只存内存, 禁止写日志/DB/WS。"""
    if not file_id or not str(file_id).strip():
        return None
    data = await _run_cli(["get", str(file_id).strip(), "--raw"], timeout_s=timeout_s)
    if not isinstance(data, dict):
        return None
    link = data.get("thumbnail_link")
    if isinstance(link, str):
        cleaned = link.strip()
        if cleaned.lower().startswith(("http://", "https://")):
            return cleaned
    return None


async def resolve_archive_folder(
    parent_id: str = "",
    name: str = ARCHIVE_FOLDER_NAME,
    *,
    timeout_s: float = DEFAULT_CLI_TIMEOUT_S,
) -> str:
    """mkdir 幂等: 逐页 ls 父目录找同名 drive#folder → 命中返回其 id (不 spawn mkdir);
    未命中 → `mkdir <name> [-p parent] -F json` 取新 folder_id (输出形状待校准, §9-8)。
    依赖 _archive_folder_lru 避免重复查找; list_files_raw 自身负责单页 LRU 缓存。"""
    cached_id = _archive_folder_lru.get((parent_id, name))
    if cached_id is not None:
        return cached_id

    next_page_token: str | None = None
    while True:
        items, next_page_token = await list_files_raw(parent_id, page_token=next_page_token, timeout_s=timeout_s)
        for item in items:
            if item.is_dir and item.name == name:
                _archive_folder_lru.put((parent_id, name), item.file_id)
                return item.file_id
        if not next_page_token:
            break

    args = ["mkdir", name]
    if parent_id:
        args += ["-p", parent_id]
    data = await _with_auth_recovery(lambda: _run_cli(args, timeout_s=timeout_s))
    folder_id = data.get("id") if isinstance(data, dict) else None
    if not folder_id:
        raise PikPakCliError("mkdir 响应缺少 id 字段")
    _archive_folder_lru.put((parent_id, name), folder_id)
    _files_lru.remove_prefix(parent_id)
    return folder_id


async def archive_files(
    file_ids: Sequence[str],
    *,
    source_parent_id: str = "",
    timeout_s: float = DEFAULT_CLI_TIMEOUT_S,
) -> dict:
    """auto_cleanup 默认模式 = archive 归档: 建目录取 folder_id → `mv <ids...> <folder_id>`
    → 失效源目录与归档目录的 LRU。"""
    folder_id = await resolve_archive_folder(source_parent_id, timeout_s=timeout_s)
    await _run_cli(["mv", *file_ids, folder_id], json_mode=False, timeout_s=timeout_s)
    invalidate_folder_cache(source_parent_id)
    invalidate_folder_cache(folder_id)
    logger.info("PikPak 归档完成: moved=%d", len(file_ids))  # 只记数量, 不记路径/直链
    return {"archive_folder_id": folder_id, "moved": len(file_ids)}


async def archive_files_to_folder(
    file_ids: Sequence[str],
    *,
    folder_id: str,
    source_parent_id: str = "",
    timeout_s: float = DEFAULT_CLI_TIMEOUT_S,
) -> dict:
    """结构化归档 (包D): mv file_ids 到调用方已解析的归档子目录 (目录树由 dir_tree 重建);
    失效源目录与目标子目录的 LRU。"""
    await _run_cli(["mv", *file_ids, folder_id], json_mode=False, timeout_s=timeout_s)
    invalidate_folder_cache(source_parent_id)
    invalidate_folder_cache(folder_id)
    logger.info("PikPak 结构化归档完成: moved=%d target=%s", len(file_ids), redact_text(folder_id))
    return {"archive_folder_id": folder_id, "moved": len(file_ids)}


async def delete_files(
    file_ids: Sequence[str],
    *,
    parent_id: str = "",
    timeout_s: float = DEFAULT_CLI_TIMEOUT_S,
) -> None:
    """auto_cleanup 显式可选模式 = delete: `rm <ids...>` (进回收站; 不立即释放配额,
    §6.2 裁决) → 失效 parent LRU。"""
    await _run_cli(["rm", *file_ids], json_mode=False, timeout_s=timeout_s)
    # 无条件失效: root 的 parent_id="" 也是合法缓存键, truthiness 判断会
    # 漏掉根目录删除 → 根列表缓存 TTL 内陈旧; 对照 archive_* 两函数本就无条件调用
    invalidate_folder_cache(parent_id)
    logger.info("PikPak 删除完成: removed=%d", len(file_ids))
