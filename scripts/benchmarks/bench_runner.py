import argparse
import http.client
import ipaddress
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

# 允许从任意工作目录直接运行：将仓库根加入模块搜索路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.config import settings

SERVER_URL = os.getenv("SERVER_URL", f"http://{settings.HOST}:{settings.PORT}")
TEST_LINK = os.getenv("TEST_CHANNEL_LINK", os.getenv("TEST_LINK", "https://t.me/telegram/1"))


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """禁止基准脚本跟随 HTTP 重定向，防止绕过内网地址校验 (CWE-918)。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "Unverified redirect rejected", headers, fp)


class _PinnedHTTPSHandler(urllib.request.HTTPSHandler):
    """Connect to the validated IP while HTTPSConnection retains the TLS hostname."""

    def https_open(self, req):
        def connection(host, **kwargs):
            conn = http.client.HTTPSConnection(host, **kwargs)
            validated_ip = req.validated_ip
            conn._create_connection = lambda address, *args, **kw: socket.create_connection(
                (validated_ip, address[1]), *args, **kw
            )
            return conn

        return self.do_open(connection, req, context=self._context)


# Proxies would bypass the validated destination (including HTTPS CONNECT tunnels).
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirectHandler(), _PinnedHTTPSHandler())


def _safe_request(
    path: str,
    *,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    server_url: str | None = None,
) -> urllib.request.Request:
    """仅允许向本机/内网部署实例发请求：单次 DNS 解析并固定 IP，阻断 DNS 重绑定与云元数据地址。"""
    parts = urlsplit(server_url or SERVER_URL)
    if parts.scheme not in ("http", "https"):
        raise SystemExit(f"不支持的协议: {parts.scheme} (仅允许 http/https)")
    host = parts.hostname or ""
    try:
        ip = ipaddress.ip_address(socket.gethostbyname(host))
    except (ValueError, socket.gaierror):
        raise SystemExit(f"无法解析目标主机: {host}") from None
    if not (ip.is_loopback or (ip.is_private and not ip.is_link_local)):
        raise SystemExit(f"拒绝向非内网地址发请求: {host} -> {ip}")

    req_headers = dict(headers or {})
    if parts.scheme == "http":
        # HTTP 场景直接连接已校验 IP 并保留 Host 头，彻底消除 DNS 重绑定窗口
        port_suffix = f":{parts.port}" if parts.port else ""
        target_host = f"[{ip}]" if ip.version == 6 else str(ip)
        url = f"http://{target_host}{port_suffix}{path}"
        req_headers.setdefault("Host", parts.netloc)
    else:
        # Preserve hostname for TLS; _PinnedHTTPSHandler connects to the validated IP.
        url = f"https://{parts.netloc}{path}"
    req = urllib.request.Request(url, data=data, headers=req_headers)
    req.validated_ip = str(ip)
    return req


def check_server_health(server_url: str | None = None) -> bool:
    try:
        with _OPENER.open(
            _safe_request("/api/tasks", server_url=server_url), timeout=3
        ) as resp:  # nosemgrep -- 仅允许 http(s) 内网目标，已固定 IP 并禁用重定向
            return resp.status == 200
    except Exception:
        return False


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bench_runner.py",
        description="TG-Media-Hub Web API 下载端到端速度测试工具 (需先启动 Web 服务)",
    )
    parser.add_argument(
        "--server-url",
        default=SERVER_URL,
        help=f"Web 服务基地址, 仅限本机/内网 (代码默认 http://127.0.0.1:8500; 当前有效值: {SERVER_URL})",
    )
    parser.add_argument(
        "--link",
        default=TEST_LINK,
        help=f"待提交下载测试的 Telegram 链接 (默认: {TEST_LINK})",
    )
    return parser


def run_benchmark(server_url: str | None = None, link: str | None = None):
    target_server = server_url or SERVER_URL
    target_link = link or TEST_LINK
    print("🔍 正在检查 Web 服务联通性...")
    if not check_server_health(server_url=target_server):
        msg = f"无法连接到 Web 服务 ({target_server})，请确认服务已通过 `python run.py` 启动。"
        print(f"❌ {msg}")
        return

    print(f"📡 正在向 Web 服务提交下载测试链接: {target_link}")
    req = _safe_request(
        "/api/download/link",
        data=json.dumps({"links": target_link}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        server_url=target_server,
    )

    try:
        with _OPENER.open(req, timeout=10) as resp:  # nosemgrep -- 请求经 _safe_request 白名单校验且禁用重定向
            data = json.loads(resp.read().decode("utf-8"))
            task_id = data["task_id"]
            print(f"✅ 任务成功创建! 任务 ID: {task_id}")
    except Exception as e:
        print(f"❌ 提交任务失败: {e}")
        return

    print("⏱️ 开始监控传输进度与网速...")
    last_log_count = 0
    start_time = time.time()

    while time.time() - start_time < 60:
        time.sleep(1)
        try:
            with _OPENER.open(
                _safe_request("/api/tasks", server_url=target_server), timeout=5
            ) as resp:  # nosemgrep -- 请求经 _safe_request 白名单校验且禁用重定向
                tasks = json.loads(resp.read().decode("utf-8"))
                current_task = next((t for t in tasks if t["id"] == task_id), None)
                if not current_task:
                    continue

                status = current_task.get("status")
                speed = current_task.get("speed_mb_s", 0.0)
                progress = current_task.get("progress_bytes", 0)
                total = current_task.get("total_bytes", 0)
                item = current_task.get("current_item", "")
                logs = current_task.get("logs", [])

                # 打印最新增日志
                if len(logs) > last_log_count:
                    for l in logs[last_log_count:]:
                        print(f"  [日志 {l.get('time')}] {l.get('msg')}")
                    last_log_count = len(logs)

                if status == "running":
                    percent = (progress / total * 100) if total > 0 else 0
                    print(f"⚡ [传输中] 进度: {percent:.1f}% | 速率: {speed:.2f} MB/s | 处理文件: {item}")
                elif status in ("completed", "failed", "cancelled"):
                    print(f"\n🎉 任务已结束! 终态: {status}")
                    break
        except Exception as e:
            print(f"⚠️ 查询任务状态异常: {e}")

    # 校验物理下载文件
    downloads_dir = "./downloads"
    if os.path.exists(downloads_dir):
        files = os.listdir(downloads_dir)
        print(f"\n📂 当前 ./downloads/ 目录中的物理文件列表 ({len(files)} 个):")
        for f in files:
            size_mb = os.path.getsize(os.path.join(downloads_dir, f)) / (1024 * 1024)
            print(f"  - {f} ({size_mb:.2f} MB)")


if __name__ == "__main__":
    cli_args = build_arg_parser().parse_args()
    run_benchmark(server_url=cli_args.server_url, link=cli_args.link)
