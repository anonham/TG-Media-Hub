import argparse
import asyncio
import copy
import logging
import math
import os
import pathlib
import sys
import time

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO)

# 允许从任意工作目录直接运行：将仓库根加入模块搜索路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from telethon import TelegramClient, functions
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.utils import get_input_location

from app.config import settings
from app.core.client_manager import client_manager
from app.services.downloader_service import DownloaderService

export_lock = asyncio.Lock()


def _open_benchmark_output_sync(total_bytes: int):
    download_root = pathlib.Path(settings.DOWNLOAD_DIR).resolve()
    download_root.mkdir(parents=True, exist_ok=True)
    out_path = (download_root / "test_multi_tcp.mp4").resolve()
    if download_root not in out_path.parents:
        raise SystemExit(f"输出路径越界: {out_path}")
    mode = "r+b" if out_path.exists() else "w+b"
    out_file = out_path.open(mode)
    out_file.truncate(total_bytes)
    return out_file


async def create_independent_sender(client: TelegramClient, dc_id: int):
    """为指定 DC 创建真正的独立 TCP 物理连接并完成 Auth 握手"""
    dc = await client._get_dc(dc_id)

    if dc_id == client.session.dc_id:
        sender = MTProtoSender(client.session.auth_key, loggers=client._log)
        await sender.connect(
            client._connection(
                dc.ip_address, dc.port, dc.id, loggers=client._log, proxy=client._proxy, local_addr=client._local_addr
            )
        )
        req = functions.InvokeWithLayerRequest(LAYER, copy.deepcopy(client._init_request))
    else:
        sender = MTProtoSender(None, loggers=client._log)
        await sender.connect(
            client._connection(
                dc.ip_address, dc.port, dc.id, loggers=client._log, proxy=client._proxy, local_addr=client._local_addr
            )
        )
        async with export_lock:
            auth = await client(functions.auth.ExportAuthorizationRequest(dc_id))
            init_req = copy.deepcopy(client._init_request)
            init_req.query = functions.auth.ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
            req = functions.InvokeWithLayerRequest(LAYER, init_req)

    await sender.send(req)
    return sender


DEFAULT_LINK = "https://t.me/demo_media/673"
DEFAULT_SENDERS = 8


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bench_true_multi_tcp.py",
        description="TG-Media-Hub 多独立 TCP MTProtoSender 并发下载基准测试工具",
    )
    parser.add_argument(
        "--link",
        default=DEFAULT_LINK,
        help=f"待测试的 Telegram 消息链接 (默认: {DEFAULT_LINK})",
    )
    parser.add_argument(
        "--senders",
        type=int,
        default=DEFAULT_SENDERS,
        help=f"独立 TCP MTProtoSender 并发连接数 (默认: {DEFAULT_SENDERS})",
    )
    return parser


async def benchmark_true_multi_tcp(link: str = DEFAULT_LINK, num_senders: int = DEFAULT_SENDERS):
    if num_senders < 1:
        raise ValueError("--senders 必须 >= 1")
    print("[INIT] Starting Userbot Client...", flush=True)
    await client_manager.initialize()
    client = client_manager.userbot

    print(f"[LINK] Resolving: {link}", flush=True)

    chat_id, msg = await DownloaderService.resolve_message_from_link(client, link)
    if not msg or not msg.media:
        print("[ERROR] Message/media not found!", flush=True)
        await client_manager.disconnect()
        return

    file_name, total_bytes = DownloaderService.get_file_name_and_size(msg)
    print(f"[MEDIA] File: {file_name} ({total_bytes / (1024 * 1024):.2f} MB)", flush=True)

    dc_id, input_location = get_input_location(msg.media)
    if input_location is None:
        print("[ERROR] Unable to resolve InputFileLocation for message media!", flush=True)
        await client_manager.disconnect()
        return
    if dc_id is None:
        document = getattr(msg.media, "document", None)
        dc_id = getattr(document, "dc_id", None) or client.session.dc_id
    print(f"[DC] Target DC ID: {dc_id}", flush=True)

    print(f"[CONNECT] Creating {num_senders} independent TCP MTProtoSenders concurrently to DC {dc_id}...", flush=True)

    sender_tasks = [create_independent_sender(client, dc_id) for _ in range(num_senders)]
    results = await asyncio.gather(*sender_tasks, return_exceptions=True)
    senders = [s for s in results if isinstance(s, MTProtoSender)]

    print(f"[CONNECT] Successfully created {len(senders)}/{num_senders} independent TCP senders!", flush=True)

    if not senders:
        print("[ERROR] Failed to create any independent senders!", flush=True)
        await client_manager.disconnect()
        return

    print(f"[DOWNLOAD] Starting multi-socket parallel download with {len(senders)} TCP connections...", flush=True)

    chunk_size = 512 * 1024
    num_chunks = math.ceil(total_bytes / chunk_size)
    chunks_queue = asyncio.Queue()
    for i in range(num_chunks):
        chunks_queue.put_nowait(i * chunk_size)

    downloaded = 0
    start_time = time.time()
    last_print = start_time
    # 规范化并校验输出路径：必须落在 DOWNLOAD_DIR 内，防止配置异常导致越界写入
    out_file = _open_benchmark_output_sync(total_bytes)

    lock = asyncio.Lock()
    retry_counts: dict[int, int] = {}
    max_retries = 3
    abort_event = asyncio.Event()
    fatal_error: list[Exception] = []

    async def worker(worker_id: int, sender: MTProtoSender):
        nonlocal downloaded, last_print
        while not abort_event.is_set() and not chunks_queue.empty():
            try:
                offset = chunks_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            req = functions.upload.GetFileRequest(location=input_location, offset=offset, limit=chunk_size)

            try:
                res = await asyncio.wait_for(client._call(sender, req), timeout=12.0)
                data = res.bytes
                expected_len = min(chunk_size, total_bytes - offset)
                valid_data = data[:expected_len]

                async with lock:
                    out_file.seek(offset)
                    out_file.write(valid_data)
                    downloaded += len(valid_data)
                    now = time.time()
                    if now - last_print >= 0.5 or downloaded == total_bytes:
                        elapsed = now - start_time
                        spd = (downloaded / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                        percent = downloaded / total_bytes * 100
                        print(
                            f"[PROGRESS] {percent:.1f}% | {downloaded / (1024 * 1024):.2f}/{total_bytes / (1024 * 1024):.2f} MB | Speed: {spd:.2f} MB/s | Active Senders: {len(senders)}",
                            flush=True,
                        )
                        last_print = now
            except Exception as e:
                async with lock:
                    attempts = retry_counts.get(offset, 0) + 1
                    retry_counts[offset] = attempts
                print(
                    f"⚠️ Worker-{worker_id} error at offset {offset} ({attempts}/{max_retries}): {e}",
                    flush=True,
                )
                if attempts < max_retries:
                    await asyncio.sleep(0.5)
                    await chunks_queue.put(offset)
                else:
                    if not fatal_error:
                        fatal_error.append(e)
                    abort_event.set()
                    break

    workers = [asyncio.create_task(worker(i, senders[i % len(senders)])) for i in range(len(senders))]
    try:
        await asyncio.gather(*workers)
    finally:
        out_file.close()

    if fatal_error:
        print(f"\n❌ [ABORTED] Download aborted after {max_retries} retries: {fatal_error[0]}", flush=True)
    else:
        elapsed = time.time() - start_time
        avg_speed = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
        print(
            f"\n🚀 [FINISHED] Downloaded {total_bytes / (1024 * 1024):.2f} MB in {elapsed:.2f}s! Average Speed: {avg_speed:.2f} MB/s",
            flush=True,
        )

    for s in senders:
        try:
            await s.disconnect()
        except Exception as e:
            logging.debug("Failed to disconnect independent sender cleanly: %s", e)

    await client_manager.disconnect()


if __name__ == "__main__":
    cli_args = build_arg_parser().parse_args()
    asyncio.run(benchmark_true_multi_tcp(link=cli_args.link, num_senders=cli_args.senders))
