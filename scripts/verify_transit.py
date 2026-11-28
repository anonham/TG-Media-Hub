#!/usr/bin/env python3
"""
scripts/verify_transit.py
中转群连通性与权限快速诊断 CLI 运维工具 (支持无头服务器与 Docker 容器一键执行)
"""

import argparse
import asyncio
import os
import sys

# 注入项目根目录到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.client_manager import client_manager
from app.db.database import init_db
from app.services.transit_bridge_service import TransitBridgeService

# 简易 ANSI 颜色支持
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


async def main() -> int:
    print(f"\n{BOLD}{CYAN}==============================================================={RESET}")
    print(f"{BOLD}{CYAN}   [tg-media-hub] Telegram 静默中转群借壳加速健康度诊断看板   {RESET}")
    print(f"{BOLD}{CYAN}==============================================================={RESET}\n")

    # 1. 确保数据库与动态客户端池初始化
    init_db()
    print("⏳ 正在探活 Userbot 与 Bot 令牌池连接...")
    await client_manager.initialize()

    # 2. 调用健康探测核心
    print("🔍 正在探测中转群连通性与权限配置...")
    try:
        report = await TransitBridgeService.verify_transit_group_health()
    except Exception as e:
        print(f"\n{RED}❌ 探测执行异常: {e}{RESET}\n")
        await client_manager.disconnect_all()
        return 1

    # 3. 输出诊断概览
    status = report.get("status", "unknown")
    status_map = {
        "healthy": f"{GREEN}[完全就绪 HEALTHY]{RESET}",
        "partially_healthy": f"{YELLOW}[部分就绪 PARTIALLY HEALTHY]{RESET}",
        "degraded": f"{YELLOW}[功能降级 DEGRADED]{RESET}",
        "unconfigured": f"{CYAN}[未配置 UNCONFIGURED]{RESET}",
        "error": f"{RED}[异常受阻 ERROR]{RESET}",
    }

    print(f"\n{BOLD}【中转群配置与概览】{RESET}")
    print(f" • 综合健康状态: {status_map.get(status, status)}")
    print(f" • 诊断总结结论: {report.get('message', '-')}")
    print(f" • 中转群组 ID  : {report.get('transit_chat_id') or '未配置'}")
    print(f" • 群组解析标题: {report.get('transit_chat_title') or '-'}")
    print(
        f" • 主 Userbot 连通: {'✅ 正常 (具备进群与管理权限)' if report.get('userbot_accessible') else '❌ 无权访问或离线'}"
    )

    # 4. 输出 Bot 令牌池矩阵
    bots = report.get("bots_status", [])
    print(
        f"\n{BOLD}【辅助 Bot 令牌池驻留矩阵】({report.get('ready_bots_count', 0)}/{report.get('total_bots_count', 0)} 就绪){RESET}"
    )
    print("-" * 65)
    print(f"{'Bot 会话名':<15} {'Username':<18} {'中转群驻留':<12} {'就绪状态':<10}")
    print("-" * 65)

    if not bots:
        print(" (系统暂未录入任何 Telegram 辅助 Bot)")
    else:
        for b in bots:
            s_name = b.get("session_name") or f"Bot #{b.get('bot_id')}"
            u_name = f"@{b['bot_username']}" if b.get("bot_username") else "未知"
            if b.get("is_admin"):
                in_group = f"{GREEN}✅ 已在群 (管理员){RESET}"
            elif b.get("in_transit_group"):
                in_group = f"{GREEN}✅ 已在群{RESET}"
            else:
                in_group = f"{RED}❌ 未入群{RESET}"
            ready_str = f"{GREEN}就绪{RESET}" if b.get("is_ready") else f"{YELLOW}未就绪{RESET}"
            print(f"{s_name:<15} {u_name:<18} {in_group:<20} {ready_str:<10}")
            if b.get("error"):
                print(f"   └── ⚠️ 错误原因: {b.get('error')}")

    print("-" * 65)

    # 5. 输出建议与排障指引
    issues = report.get("issues", [])
    if issues:
        print(f"\n{BOLD}{YELLOW}【排障与修复指引】{RESET}")
        for idx, issue in enumerate(issues, 1):
            print(f" {idx}. {issue}")
    else:
        print(
            f"\n{GREEN}🎉 恭喜！中转群配置完全健康，下载私密频道未限制的大文件将自动获得 10~20 条管道借壳满血并发加速！{RESET}"
        )

    print("\n")
    # 优雅关闭连接
    await client_manager.disconnect_all()
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="verify_transit.py",
        description="TG-Media-Hub 中转群连通性与 Bot 池权限快速诊断 CLI 运维工具",
    )


if __name__ == "__main__":
    build_arg_parser().parse_args()
    sys.exit(asyncio.run(main()))
