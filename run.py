import argparse
import logging
import socket
import sys

import uvicorn

from app import APP_VERSION
from app.config import settings
from app.core.log_setup import configure_logging
from app.core.single_instance import SingleInstanceLock

# 命名归入 app.* 层级 (run.py 以 __main__ 执行, __name__ 路由不到应用 logger 树)
logger = logging.getLogger("app.run")

if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception as enc_err:
        sys.stderr.write(f"Warning: Failed to reconfigure stdout/stderr encoding: {enc_err}\n")


def patch_asyncio_win_10054():
    """在 Windows 平台上静默捕获 asyncio _call_connection_lost 触发的 WinError 10054 连接重置错误"""
    if sys.platform == "win32":
        try:
            from asyncio.proactor_events import _ProactorBasePipeTransport

            orig_call_connection_lost = _ProactorBasePipeTransport._call_connection_lost  # type: ignore[attr-defined]

            def silent_call_connection_lost(self, exc=None):
                try:
                    orig_call_connection_lost(self, exc)
                except (ConnectionResetError, OSError) as e:
                    if getattr(e, "winerror", None) == 10054:
                        return
                    raise

            _ProactorBasePipeTransport._call_connection_lost = silent_call_connection_lost  # type: ignore[attr-defined]
        except Exception as patch_err:
            logging.getLogger("run").debug("Windows asyncio 10054 monkey-patch 跳过: %s", patch_err)


def _resolve_bind_family(host: str) -> socket.AddressFamily:
    """按请求 host 解析探测 socket 的地址族: 硬编码 AF_INET 会让 IPv6
    host (--host ::1 / ::) 的 bind 全部失败并耗尽 20 个端口候选。解析失败回退 AF_INET
    保持旧行为。"""
    try:
        return socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)[0][0]
    except socket.gaierror:
        return socket.AF_INET


def find_available_port(host: str, start_port: int) -> int:
    """从 start_port 起向上探测第一个可绑定端口; 全部不可绑定时显式报错而非静默返回默认值"""
    family = _resolve_bind_family(host)
    for port in range(start_port, start_port + 20):
        with socket.socket(family, socket.SOCK_STREAM) as s:
            if sys.platform == "win32":
                # 对齐 uvicorn/asyncio 的绑定语义 (SO_EXCLUSIVEADDRUSE): 被保留/占用的端口
                # 在探测期就如实失败, 否则 Windows 宽松的默认 bind 会误报"可用"
                s.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            try:
                s.bind((host, port))
                return port
            except OSError:
                continue
    # 20 个候选端口全部绑定失败 (常见于 winnat/Hyper-V 保留段整段覆盖, 如 8430-8529):
    # 静默返回 start_port 只会把失败推迟到 uvicorn 绑定时才以 winerror 10013 暴露
    raise RuntimeError(
        f"端口 {start_port}~{start_port + 19} 均无法绑定, 服务无法启动。"
        "请用 'netsh interface ipv4 show excludedportrange protocol=tcp' 检查 Windows 保留端口段,"
        "或修改 .env 的 PORT 后重试"
    )


def get_graceful_shutdown_timeout(extra_margin: int = 8) -> int:
    """计算 Uvicorn 优雅退出超时预算 (秒)。
    必须大于调度器收割预算 (GRACEFUL_STOP_TIMEOUT + 5s 兜底 cancel) + WAL Checkpoint + 客户端断开时间。
    默认: int(settings.GRACEFUL_STOP_TIMEOUT) + 8 (默认 10 + 8 = 18s，最低保底 15s)。
    """
    base_timeout = int(getattr(settings, "GRACEFUL_STOP_TIMEOUT", 10.0))
    return max(15, base_timeout + extra_margin)


def build_uvicorn_kwargs(trusted_proxies: str) -> dict:
    """构造 uvicorn.run 关键字参数; 非 Windows 启用 uvloop + httptools 提升事件循环调度
    与 HTTP 解析性能。Windows 无 uvloop 轮子 (requirements 环境标记已排除), 保持默认
    asyncio + h11 平稳降级; 抽成函数以便双平台参数断言可测。
    """
    uvicorn_kwargs = {
        "reload": False,
        # 显式单 worker: workers=None 时 uvicorn 会读 WEB_CONCURRENCY 起
        # 多进程 —— 每个 worker 独立跑 lifespan (init_db/recover_orphaned_tasks/
        # scheduler.start) 而 SingleInstanceLock 只在父进程持有, 恢复互踩 + 双调度器;
        # 显式值优先于该环境变量
        "workers": 1,
        "proxy_headers": True,
        "forwarded_allow_ips": trusted_proxies,
        "timeout_graceful_shutdown": get_graceful_shutdown_timeout(),
    }
    if sys.platform != "win32":
        uvicorn_kwargs["loop"] = "uvloop"
        uvicorn_kwargs["http"] = "httptools"
    return uvicorn_kwargs


def build_arg_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器 (--help / --version 在单实例锁与凭证校验前直接响应退出)。"""
    parser = argparse.ArgumentParser(
        prog="run.py",
        description="TG-Media-Hub — Telegram 媒体自动化套件 Web 控制台与后台调度服务",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "环境变量配置 (.env):\n"
            "  必需项: TG_API_ID, TG_API_HASH, TG_BOT_TOKENS\n"
            "  可选项: HOST (代码默认 127.0.0.1), PORT (代码默认 8500), API_KEY, SPLIT_STRATEGY\n"
            "\n"
            "示例:\n"
            "  uv run python run.py\n"
            "  uv run python run.py --host 0.0.0.0 --port 8500\n"
        ),
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"tg-media-hub v{APP_VERSION}",
        help="显示当前程序版本号并退出",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help=f"覆盖服务监听地址 (读取 HOST 环境变量, 代码默认 127.0.0.1; 当前有效值: {settings.HOST})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=(
            f"覆盖起始监听端口, 若被占用自动顺延探测至 +19 端口 (共 20 个候选端口; "
            f"读取 PORT 环境变量, 代码默认 8500; 当前有效值: {settings.PORT})"
        ),
    )
    return parser


def parse_cli_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行参数."""
    return build_arg_parser().parse_args(argv)


if __name__ == "__main__":
    cli_args = parse_cli_args()
    # 应用日志体系 (console + data/logs/app.log 旋转文件 + 环形缓冲):
    # 在 CLI 参数解析后、uvicorn 拉起 app 前配置, 避免 --help / --version 产生日志落盘副作用
    configure_logging()

    host = cli_args.host or settings.HOST
    # 绑定地址分类 (本机免认证资格, app/api/access.py) 必须与实际 uvicorn bind 一致,
    # CLI --host 覆盖值需回写 settings 单例, 否则 --host 0.0.0.0 直跑会被误判为回环部署
    settings.HOST = host
    default_port = cli_args.port if cli_args.port is not None else settings.PORT

    # 单实例锁守卫: 启动最前置加锁，防止多实例并发启动破坏 SQLite 与抢占 Telegram 凭据
    instance_lock = SingleInstanceLock(settings.LOCK_PATH)
    instance_lock.acquire(port=default_port)
    try:
        # 启动期凭证校验: uvicorn 拉起 app 前快速失败, 一次性列出全部缺失键 (Docker CMD 亦走此入口)
        settings.validate_required()
        patch_asyncio_win_10054()
        port = find_available_port(host, default_port)
        if port != default_port:
            logger.warning("⚠️ 提示: 原默认端口 %d 被系统占用或受限，已自动切至可用端口: %d", default_port, port)
            instance_lock.update_port(port)

        logger.info("🌐 正在启动 Telegram Web 控制台: http://%s:%s", host, port)

        # 安全约束: TRUSTED_PROXIES 禁止全信任 ("*"), 否则任意客户端可伪造 X-Forwarded-For 欺骗真实 IP
        trusted_proxies = settings.TRUSTED_PROXIES
        if any(h.strip() == "*" for h in trusted_proxies.split(",")):
            raise ValueError('TRUSTED_PROXIES 禁止设为 "*": 全信任将允许任意客户端伪造真实客户端 IP')

        # proxy_headers + forwarded_allow_ips: 信任白名单内代理追加的 X-Forwarded-For 跳,
        # 使 request.client.host 还原为真实客户端 IP (限流器与 2FA IP 锁零改动自动生效)
        uvicorn.run(
            "app.main:app",
            host=host,
            port=port,
            **build_uvicorn_kwargs(trusted_proxies),
        )
    finally:
        instance_lock.release()
