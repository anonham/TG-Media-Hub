"""run.py 子进程生命周期 (/§4.2)。

启动 → 就绪探测 (GET /api/settings 200) → 关停。子进程环境变量优先于 .env
(app/config._load_dotenv 对已存在键不回填), 据此注入 DATA_DIR / PORT /
切片族低阈值 (§9.0) / API_KEY 空值 (open-mode, 裁决)。
"""

import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from . import env_gate

_READY_ENDPOINT = "/api/settings"
_STDOUT_CAPTURE = "server_stdout.log"
# 服务器经 scratch 引导器启动 (路径族重定向), 而非直接执行 run.py——
# config 路径族不读 DATA_DIR env, 详见 server_bootstrap.py 模块文档
BOOTSTRAP_PATH = Path(__file__).resolve().parent / "server_bootstrap.py"


def find_free_port(host: str) -> int:
    address = env_gate.loopback_address(host)
    family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as sock:
        sock.bind((str(address), 0))
        return int(sock.getsockname()[1])


def _read_bytes_guarded(path: Path) -> bytes | None:
    """真实锁文件快照读取 (不存在/不可读返回 None); 供隔离断言做前后对比。"""
    try:
        return path.read_bytes()
    except OSError:
        return None


def child_env(data_dir: Path, port: int, host: str) -> dict[str, str]:
    """服务器子进程环境: 注入项覆盖 .env 回退链 (进程环境优先)。"""
    env = os.environ.copy()
    env.update(
        {
            "DATA_DIR": str(data_dir),
            "PORT": str(port),
            "MAX_FILE_SIZE_MB": str(env_gate.max_file_size_mb()),
            "PREMIUM_MAX_FILE_SIZE_MB": str(env_gate.premium_max_file_size_mb()),
            # open-mode: harness 本地实例不做鉴权, 鉴权矩阵由 tests/api/test_auth_api.py 锁定
            "API_KEY": "",
            "TG_API_KEY": "",
            "CONSOLE_REQUIRE_API_KEY": "false",
            "HOST": str(env_gate.loopback_address(host)),
            # harness 自起实例关闭 API 限流 (对齐 open-mode 精神): 终态轮询/注入时序探测
            # 属秒级高频 GET, 120 次/分钟默认档必然误触 429 (包 5 实跑实证); 限流矩阵
            # 本身由 tests/api/test_auth_api.py 429 族锁定, attach 形态不受控 → 测试侧另有退避
            "RATE_LIMIT_PER_MINUTE": "0",
        }
    )
    if env_gate.capture_mode():
        # E2E-LIVE-17 前置: capture 取证三件套的服务器侧开关 (getenv 类配置, env 注入有效)
        env["E2E_CAPTURE_MODE"] = "true"
    if env_gate.force_paged_proxy():
        # Tier 3 分页代理真链路验证的服务器侧开关 (getenv 类配置, env 注入有效)
        env["E2E_FORCE_PAGED_PROXY"] = "true"
    return env


def _probe_ready(base_url: str, timeout: float = 5.0) -> bool:
    """就绪探测: 空代理 handler 显式绕过环境代理 (对齐仓库 trust_env=False 传输规约)。"""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(f"{base_url}{_READY_ENDPOINT}", timeout=timeout) as resp:
            return resp.status == 200
    except (urllib.error.URLError, ConnectionError, OSError):
        return False


class LiveServer:
    """live harness 自起的真实服务器进程。"""

    def __init__(self, data_dir: Path, port: int, host: str) -> None:
        address = env_gate.loopback_address(host)
        self.data_dir = data_dir
        self.port = port
        self.host = str(address)
        url_host = f"[{self.host}]" if address.version == 6 else self.host
        self.base_url = f"http://{url_host}:{port}"
        self.proc: subprocess.Popen[bytes] | None = None
        self.child_env_snapshot: dict[str, str] | None = None
        self.real_lock_before: bytes | None = _read_bytes_guarded(env_gate.REPO_ROOT / "data" / "tg_service.lock")
        self.log_path = data_dir / _STDOUT_CAPTURE

    def start(self) -> None:
        env = child_env(self.data_dir, self.port, self.host)
        self.child_env_snapshot = dict(env)
        log = open(self.log_path, "wb")
        try:
            self.proc = subprocess.Popen(
                [sys.executable, str(BOOTSTRAP_PATH), str(self.data_dir)],
                cwd=str(env_gate.REPO_ROOT),
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        finally:
            log.close()

    def wait_ready(self, timeout: float = 300.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                raise RuntimeError(f"服务器进程提前退出 (code={self.proc.returncode})\n{self.log_tail()}")
            if _probe_ready(self.base_url):
                return
            time.sleep(1.0)
        raise RuntimeError(f"服务器 {timeout}s 内未就绪\n{self.log_tail()}")

    def log_tail(self, limit: int = 4000) -> str:
        try:
            return self.log_path.read_text(encoding="utf-8", errors="ignore")[-limit:]
        except OSError:
            return "<无日志>"

    def stop(self) -> None:
        if self.proc is None:
            return
        if self.proc.poll() is None:
            if sys.platform == "win32":
                # uv venv 的 python.exe 是 trampoline 启动器 (真实解释器为其子进程):
                # 必须整树击杀, 单纯 terminate 可能只杀启动器而遗留孤儿服务器 (R-1 风险面)
                subprocess.run(
                    ["taskkill", "/PID", str(self.proc.pid), "/T", "/F"],
                    capture_output=True,
                    check=False,
                )
            else:
                self.proc.terminate()
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self.proc = None


class ExternalServer:
    """attach 形态 (E2E_LIVE_BASE_URL 指向已起服务器): harness 不管理其生命周期。"""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.data_dir: Path | None = None
        self.proc = None
        self.child_env_snapshot: dict[str, str] | None = None

    def wait_ready(self, timeout: float = 60.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if _probe_ready(self.base_url):
                return
            time.sleep(1.0)
        raise RuntimeError(f"外部服务器 {self.base_url} {timeout}s 内未就绪")

    def stop(self) -> None:
        return
