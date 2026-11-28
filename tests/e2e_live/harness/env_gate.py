"""E2E live harness 门控与配置读取 。

全部变量只在本包内经 os.environ 读取, 不进入 app/config.py, 因此不触发
env-parity 守卫义务 (G1 只查 config.py 的 getenv)。`.env` 中的
TEST_CHANNEL_LINK / TEST_SAMPLE_FOLDER / PORT 由本模块直接解析复用, 不新造键。
"""

import ipaddress
import os
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

_TRUTHY = ("1", "true", "yes")


def is_enabled() -> bool:
    """E2E_LIVE 总开关 (默认 skip; D7: 不进 CI)。"""
    return os.environ.get("E2E_LIVE", "").strip().lower() in _TRUTHY


def keep_scratch() -> bool:
    return os.environ.get("E2E_LIVE_KEEP_SCRATCH", "").strip().lower() in _TRUTHY


def keep_messages() -> bool:
    return os.environ.get("E2E_LIVE_KEEP_MSGS", "").strip().lower() in _TRUTHY


def max_file_size_mb() -> int:
    """切片族低阈值注入·标准门限 (§9.0 裁决, 默认 64)。"""
    return int(os.environ.get("E2E_LIVE_MAX_FILE_SIZE_MB", "64"))


def premium_max_file_size_mb() -> int:
    """切片族低阈值注入·Premium 门限 (§9.0 裁决, 默认 128)。"""
    return int(os.environ.get("E2E_LIVE_PREMIUM_MAX_FILE_SIZE_MB", "128"))


def pikpak_remote_dir() -> str:
    """PikPak 远端测试目录 (E2E-LIVE-06 ~ 17 切片族前置); 未设则逐项 skip。"""
    return os.environ.get("E2E_LIVE_PIKPAK_REMOTE_DIR", "").strip()


def capture_mode() -> bool:
    """E2E_LIVE_CAPTURE: 启动套件时向服务器子进程注入 E2E_CAPTURE_MODE=true
    (E2E-LIVE-17 前置; capture 磁盘成本 ≈3× 源体积/item)。"""
    return os.environ.get("E2E_LIVE_CAPTURE", "").strip().lower() in _TRUTHY


def force_paged_proxy() -> bool:
    """E2E_LIVE_FORCE_PAGED_PROXY: 启动套件时向服务器子进程注入 E2E_FORCE_PAGED_PROXY=true
    (Tier 3 分页代理真链路验证前置; 生产下代理仅由 VSSE 熔断降级透传触发)。"""
    return os.environ.get("E2E_LIVE_FORCE_PAGED_PROXY", "").strip().lower() in _TRUTHY


def source_data_dir() -> Path:
    """克隆源 (真实 data/), 可经 E2E_LIVE_SOURCE_DATA_DIR 覆盖。"""
    return Path(os.environ.get("E2E_LIVE_SOURCE_DATA_DIR", "").strip() or (REPO_ROOT / "data"))


def resolve_scratch_dir() -> Path:
    """为每次运行分配独立的 scratch 目录。"""
    override = os.environ.get("E2E_LIVE_DATA_DIR", "").strip()
    if override:
        return Path(override)
    stamp = uuid.uuid4().hex
    if keep_scratch():
        return REPO_ROOT / "tests" / "tmp" / f"e2e_live_{stamp}"
    return REPO_ROOT / "tests" / ".tmp" / f"e2e_live_{stamp}"


def _read_env_file_key(key: str) -> str | None:
    """极简 .env 键读取 (与 app/config._load_dotenv 同语义: 去引号、去行内注释)。"""
    path = REPO_ROOT / ".env"
    if not path.exists():
        return None
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        if name.strip() != key:
            continue
        value = value.strip()
        if not (value.startswith('"') or value.startswith("'")):
            value = value.split("#")[0].strip()
        return value.strip('"').strip("'") or None
    return None


def server_host() -> str:
    """harness 的回环监听及客户端连接地址，默认 127.0.0.1。

    与项目实例的 HOST 配置独立。
    """
    return os.environ.get("E2E_LIVE_HOST", "").strip() or "127.0.0.1"


def loopback_address(host: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    try:
        address = ipaddress.ip_address("127.0.0.1" if host.lower() == "localhost" else host)
    except ValueError as exc:
        raise ValueError("E2E_LIVE_HOST 必须是回环地址") from exc
    if not address.is_loopback:
        raise ValueError("E2E_LIVE_HOST 必须是回环地址")
    return address


def real_server_port() -> int:
    """真实部署默认端口 (冲突守卫用), 取 .env PORT 回退 8500。"""
    try:
        return int(_read_env_file_key("PORT") or "8500")
    except ValueError:
        return 8500


def base_url() -> str | None:
    """外部已起服务器地址 (attach 形态); 未设置时 harness 自起服务器 (默认形态, D2)。"""
    return os.environ.get("E2E_LIVE_BASE_URL", "").strip() or None


def target_chat() -> str | None:
    """测试频道: E2E_LIVE_TARGET_CHAT 优先, 回退 .env TEST_CHANNEL_LINK (不新造键)。"""
    return os.environ.get("E2E_LIVE_TARGET_CHAT", "").strip() or _read_env_file_key("TEST_CHANNEL_LINK")


def sample_dir() -> Path | None:
    raw = os.environ.get("E2E_LIVE_SAMPLE_DIR", "").strip()
    raw = raw or _read_env_file_key("TEST_SAMPLE_FOLDER") or ""
    return Path(raw) if raw else None


def chat_ref_from_link(link: str) -> str:
    """把 TEST_CHANNEL_LINK 形态的值归一为 API/Telethon 通用的 chat 引用。

    - https://t.me/c/<internal>/... → -100<internal> (私密频道数字 ID)
    - https://t.me/<username>       → <username>
    - 已是 -100... / @username / 其他形态 → 原样返回
    """
    text = (link or "").strip()
    if text.startswith("-") or text.startswith("@"):
        return text
    marker = "t.me/"
    idx = text.find(marker)
    if idx == -1:
        return text
    tail = text[idx + len(marker) :].strip("/")
    if tail.startswith("c/"):
        internal = tail[2:].split("/")[0]
        return f"-100{internal}"
    return tail.split("/")[0]
