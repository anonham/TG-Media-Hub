import argparse
import asyncio
import logging
import os
import sys
import time

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO)

# 允许从任意工作目录直接运行：将仓库根加入模块搜索路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.core.client_manager import client_manager
from app.services.downloader_service import DownloaderService

DEFAULT_LINK = "https://t.me/demo_media/673"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bench_multi_client_speed.py",
        description="TG-Media-Hub 多账号/Bot 客户端池并发下载基准测试脚本",
    )
    parser.add_argument(
        "--link",
        default=DEFAULT_LINK,
        help=f"待测试的 Telegram 消息链接 (默认: {DEFAULT_LINK})",
    )
    return parser


async def run_multi_account_download(link: str = DEFAULT_LINK):
    print("[INIT] Initializing Telegram Engine (Userbot + 4 Bots)...", flush=True)
    await client_manager.initialize()

    all_clients = client_manager.get_all_clients()
    print(
        f"[CLIENTS] Active clients in pool: {len(all_clients)} (Userbot + {len(client_manager.bots)} Bots)", flush=True
    )

    print(f"[LINK] Resolving link: {link}", flush=True)

    chat_id, msg = await DownloaderService.resolve_message_from_link(client_manager.userbot, link)
    if not msg or not msg.media:
        print("[ERROR] Message or media not found!", flush=True)
        await client_manager.disconnect()
        return

    file_name, total_bytes = DownloaderService.get_file_name_and_size(msg)
    print(f"[MEDIA] File: {file_name} ({total_bytes / (1024 * 1024):.2f} MB)", flush=True)

    start_time = time.time()
    last_print = start_time

    def progress_cb(current, total, speed, eta, name, concurrency=0, dc_id=0):
        nonlocal last_print
        now = time.time()
        if now - last_print >= 0.5 or current == total:
            percent = (current / total * 100) if total > 0 else 0
            print(
                f"[PROGRESS] {percent:.1f}% | {current / (1024 * 1024):.2f}/{total / (1024 * 1024):.2f} MB | Speed: {speed:.2f} MB/s | Workers: {concurrency}",
                flush=True,
            )
            last_print = now

    out_path = await DownloaderService.download_single_message(
        client=all_clients, chat_id=chat_id, message=msg, progress_callback=progress_cb
    )

    elapsed = time.time() - start_time
    actual_speed = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
    print(f"\n[FINISHED] Download completed in {elapsed:.2f}s! Average Speed: {actual_speed:.2f} MB/s", flush=True)
    print(f"[OUTPUT] File: {out_path}", flush=True)

    await client_manager.disconnect()


if __name__ == "__main__":
    cli_args = build_arg_parser().parse_args()
    asyncio.run(run_multi_account_download(link=cli_args.link))
