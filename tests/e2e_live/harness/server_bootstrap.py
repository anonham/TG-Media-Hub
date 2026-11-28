"""scratch 引导器 ( 补强): 产品代码零改动, 将 settings 路径族重定向到
scratch 后拉起 run.py 主流程。

背景 (包 2 执行期代码实证): app/config.py 的路径族 (DATA_DIR/DB_PATH/LOCK_PATH/
LOG_DIR/DOWNLOAD_DIR/PIKPAK_META_DIR) 是 BASE_DIR 派生的类属性, 不读同名环境
变量——仅注入 env 无法改变服务器落点。本引导器在进程内先改 settings 属性、再以
run_name="__main__" 执行 run.py (锁 → 校验 → uvicorn 顺序原样保留), 与根 conftest
的测试沙箱重定向是同一机制。

用法: python server_bootstrap.py <scratch_dir>  (PORT/阈值/API_KEY 走进程环境注入)
"""

import os
import runpy
import sys
from pathlib import Path

# 以脚本方式启动时 sys.path[0] 是本文件目录, 仓库根不在其上——显式补入,
# 否则 from app import config 直接 ModuleNotFoundError
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def redirect_settings_to(scratch: Path) -> None:
    from app import config as app_config

    s = app_config.settings
    root = str(scratch)
    s.DATA_DIR = root
    s.DB_PATH = os.path.join(root, "tg_service.db")
    s.LOCK_PATH = os.path.join(root, "tg_service.lock")
    s.LOG_DIR = os.path.join(root, "logs")
    s.DOWNLOAD_DIR = os.path.join(root, "downloads")
    s.PIKPAK_META_DIR = os.path.join(root, "upload_meta")
    if hasattr(s, "E2E_CAPTURE_DIR"):
        s.E2E_CAPTURE_DIR = os.path.join(root, "e2e_capture")
    for directory in (s.DATA_DIR, s.LOG_DIR, s.DOWNLOAD_DIR, s.PIKPAK_META_DIR):
        os.makedirs(directory, exist_ok=True)


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("用法: server_bootstrap.py <scratch_dir>")
    redirect_settings_to(Path(sys.argv[1]).resolve())
    run_py = Path(__file__).resolve().parents[3] / "run.py"
    sys.argv = [str(run_py), *sys.argv[2:]]
    runpy.run_path(str(run_py), run_name="__main__")


if __name__ == "__main__":
    main()
