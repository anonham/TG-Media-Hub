"""scratch DATA_DIR 构建与真实服务冲突守卫 。

策略: 完整克隆真实 data/ 到 scratch (排除 downloads/ logs/ e2e_capture/
upload_meta 大目录与实例锁文件), 服务器以克隆库 + 克隆会话冷启动; 前置断言
真实服务未运行 (默认端口无存活监听), 否则调用方 skip——避免双进程共用同一
Telegram session (R-1)。
"""

import shutil
import socket
from pathlib import Path

from . import env_gate

_EXCLUDED_TOP_LEVEL = {"downloads", "logs", "e2e_capture", "upload_meta"}
_LOCK_FILE = "tg_service.lock"


def ensure_no_real_server(host: str, port: int) -> None:
    """真实服务存活守卫: 默认端口可连接即视为真实服务在跑, 抛 RuntimeError 由夹具转 skip。"""
    address = env_gate.loopback_address(host)
    family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        if sock.connect_ex((str(address), port)) == 0:
            raise RuntimeError(
                f"默认端口 {host}:{port} 已有服务存活——疑似真实 tg_service 未停。"
                "live harness 与真实服务共用同一 Telegram session, 双进程并存会引发会话冲突"
                " (计划书 R-1); 请先停真实服务再跑 live 套件。"
            )


def clone_source_data(dest: Path) -> Path:
    """完整克隆真实 data/ → dest (排除大目录与锁文件); 返回 dest。

    db 主文件连同 -wal/-shm 一并复制 (源服务已停, 三件一致); 锁文件不复制,
    由 run.py 在 scratch 内重建单实例锁。
    """
    src = env_gate.source_data_dir()
    if not src.is_dir():
        raise RuntimeError(f"克隆源不存在: {src} (E2E_LIVE_SOURCE_DATA_DIR 或仓库 data/)")
    if dest.resolve().is_relative_to(src.resolve()):
        raise ValueError("scratch 目录不能位于克隆源内")
    dest.mkdir(parents=True, exist_ok=False)
    try:
        for item in src.iterdir():
            if item.name in _EXCLUDED_TOP_LEVEL or item.name == _LOCK_FILE:
                continue
            target = dest / item.name
            if item.is_dir():
                shutil.copytree(item, target)
            else:
                shutil.copy2(item, target)
    except BaseException:
        shutil.rmtree(dest)
        raise
    return dest


def find_session_file(data_dir: Path) -> Path | None:
    """定位克隆会话文件: 优先 USERBOT_SESSION_NAME 命名, 回退目录内首个 *.session。"""
    from app.config import settings as app_settings

    preferred = data_dir / f"{app_settings.USERBOT_SESSION_NAME}.session"
    if preferred.exists():
        return preferred
    for candidate in sorted(data_dir.glob("*.session")):
        if candidate.is_file():
            return candidate
    return None
