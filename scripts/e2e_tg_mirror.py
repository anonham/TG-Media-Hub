#!/usr/bin/env python3
"""
scripts/e2e_tg_mirror.py — E2E 捕获证据 E3 回下载 CLI (计划 D4/E3/E9)

从发布群组按 manifest 逐卷 msg_id 回下载到 tg_mirror/, 流式 sha256 回写 manifest。

用法:
    uv run python scripts/e2e_tg_mirror.py --manifest <manifest.json>
    uv run python scripts/e2e_tg_mirror.py --task <task_id>          # 扫描全部 item
    可选: --session <path> (默认 data/<USERBOT_SESSION_NAME>.session)
          --api-id / --api-hash (默认取 .env 的 TG_API_ID/TG_API_HASH)

语义:
- 仅回下载 status=sent 的卷 (failed/discarded 无消息可回);
- manifest.rolled_back=True → 全部标 skipped (消息已被物理回滚, E3);
- 消息不存在 → 标 failed 并注明"疑似回滚"(与普通失败区分);
- FloodWait 指数退避重试; 其他异常记 failed 不中断后续卷 (人工驱动流程)。
"""

import asyncio
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings

_HASH_CHUNK = 1024 * 1024
_ERROR_MAX_LEN = 500


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(_HASH_CHUNK)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _load_manifest_sync(manifest_path: str) -> tuple[dict, str]:
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)
    return manifest, os.path.dirname(os.path.abspath(manifest_path))


def _save_manifest_atomic_sync(manifest_path: str, manifest: dict) -> None:
    tmp = manifest_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    os.replace(tmp, manifest_path)


def _file_size_sync(path: str) -> int:
    return os.path.getsize(path)


def _file_exists_sync(path: str) -> bool:
    return os.path.exists(path)


async def mirror_manifest(manifest_path: str, client, max_flood_retries: int = 3) -> dict:
    """单 item manifest 回下载编排 (纯 client 注入, 可用 fake client 单测)。"""
    manifest, root = _load_manifest_sync(manifest_path)
    chat_id = manifest.get("target_chat_id")
    rolled_back = bool(manifest.get("rolled_back"))
    manifest.setdefault("tg_mirror", [])
    existing = {m.get("part_name"): m for m in manifest["tg_mirror"]}

    for part in manifest.get("parts", []):
        name = part.get("part_name") or "?"
        msg_id = part.get("msg_id") or 0
        entry = existing.get(name)
        if entry is None:
            entry = {
                "part_name": name,
                "msg_id": msg_id,
                "path": "",
                "size_bytes": 0,
                "document_size": 0,
                "sha256": "",
                "status": "pending",
                "error": "",
            }
            manifest["tg_mirror"].append(entry)
            existing[name] = entry

        if part.get("status") != "sent" or not msg_id:
            continue  # failed/discarded 无对应消息
        if rolled_back:
            entry.update(status="skipped", error="rolled_back: message physically deleted (E3)")
            continue

        for attempt in range(max_flood_retries):
            try:
                msg = await client.get_messages(chat_id, ids=msg_id)
                if msg is None or getattr(msg, "media", None) is None:
                    entry.update(status="failed", error="message missing (suspected rollback, E3)")
                    break
                out_dir = os.path.join(root, "tg_mirror")
                os.makedirs(out_dir, exist_ok=True)
                out_path = os.path.join(out_dir, name)
                downloaded_path = await msg.download_media(file=out_path)
                if downloaded_path is not None:
                    actual_path = os.fspath(downloaded_path)
                elif _file_exists_sync(out_path):
                    actual_path = out_path
                else:
                    entry.update(status="failed", error="download_media returned None")
                    print(f"  ❌ {name}: download_media returned None")
                    break
                actual_name = os.path.basename(actual_path)
                size = _file_size_sync(actual_path)
                if size == 0:
                    entry.update(status="failed", error="downloaded file empty")
                    break
                document = getattr(msg, "document", None)
                doc_size = int(getattr(document, "size", 0) or 0)
                entry.update(
                    msg_id=msg_id,
                    path="tg_mirror/" + actual_name,
                    size_bytes=size,
                    document_size=doc_size,
                    sha256=_sha256_file(actual_path),
                    status="ok",
                    error="",
                )
                print(f"  ✅ {name}: {size} bytes sha256={entry['sha256'][:16]}… doc_size={doc_size}")
                break
            except Exception as e:
                from telethon.errors import FloodWaitError

                if isinstance(e, FloodWaitError) and attempt < max_flood_retries - 1:
                    wait_sec = max(int(getattr(e, "seconds", 5) or 5), 1)
                    print(f"  ⏳ FloodWait {wait_sec}s, 重试 {attempt + 1}/{max_flood_retries}")
                    await asyncio.sleep(wait_sec + 1)
                    continue
                entry.update(status="failed", error=str(e)[:_ERROR_MAX_LEN])
                print(f"  ❌ {name}: {e}")
                break

    _save_manifest_atomic_sync(manifest_path, manifest)
    return manifest


def _collect_manifest_paths(args) -> list:
    if args.manifest:
        return [args.manifest]
    root = os.path.join(settings.E2E_CAPTURE_DIR, str(args.task))
    if not os.path.isdir(root):
        print(f"[e2e-mirror] 任务目录不存在: {root}")
        sys.exit(2)
    found = []
    for entry in sorted(os.listdir(root)):
        m = os.path.join(root, entry, "manifest.json")
        if os.path.isfile(m):
            found.append(m)
    if not found:
        print(f"[e2e-mirror] 任务目录下无 manifest: {root}")
        sys.exit(2)
    return found


async def _amain(args) -> int:
    from telethon import TelegramClient

    api_id = args.api_id or settings.API_ID
    api_hash = args.api_hash or settings.API_HASH
    if not api_id or not api_hash:
        print("[e2e-mirror] 缺少 TG_API_ID/TG_API_HASH (env 或 --api-id/--api-hash)")
        return 2
    session = args.session or os.path.join(settings.DATA_DIR, settings.USERBOT_SESSION_NAME + ".session")

    any_fail = False
    client = TelegramClient(session, api_id, api_hash)
    await client.start()
    try:
        for manifest_path in _collect_manifest_paths(args):
            print(f"\n[e2e-mirror] 回下载: {manifest_path}")
            try:
                manifest = await mirror_manifest(manifest_path, client)
            except (OSError, ValueError, KeyError) as exc:
                print(f"  ❌ manifest 读取/回下载异常 ({manifest_path}): {exc}")
                any_fail = True
                continue
            statuses = [m.get("status") for m in manifest.get("tg_mirror", [])]
            any_fail = any_fail or any(s == "failed" for s in statuses)
    finally:
        await client.disconnect()
    return 1 if any_fail else 0


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="E2E 捕获证据 E3 回下载")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--manifest", help="单 item manifest.json 路径")
    group.add_argument("--task", help="task_id (扫描 E2E_CAPTURE_DIR/<task_id>/*/manifest.json)")
    parser.add_argument("--session", help="Telethon session 文件路径 (默认 data/ 下默认 userbot)")
    parser.add_argument("--api-id", type=int, default=None)
    parser.add_argument("--api-hash", default=None)
    args = parser.parse_args()
    sys.exit(asyncio.run(_amain(args)))


if __name__ == "__main__":
    main()
