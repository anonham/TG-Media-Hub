"""FastTelethon 断点续传: 路径沙箱校验、指纹计算、同步磁盘 I/O 助手与 Single-Flight 元数据保存器.

纯搬迁自原单文件 app/core/fast_telethon.py (拆分重构, 零运行时逻辑改动)。
"""

import asyncio
import hashlib
import json
import logging
import os
import pathlib
import tempfile
import time
from collections.abc import Callable
from contextlib import suppress
from typing import IO, Any

from app.config import settings
from app.core.stream_chunk_provider import BaseChunkProvider

logger = logging.getLogger(__name__)

UPLOAD_PART_TTL_SECONDS = 7200  # Telegram 服务端临时分块生命周期安全阈值 (2小时)


def resolve_breakpoint_path(file_path: str, suffix: str, trust_root: str | None = None) -> str:
    """断点文件路径派生: 路径规范化 + 派生路径符号链接重定向防御。

    下载断点使用成品的目录项名称，拒绝已有的 .part/.meta 符号链接。
    提供 trust_root 时，解析后的路径必须仍位于该目录内。
    """
    raw = pathlib.Path(file_path)
    # 下载收尾替换成品目录项，断点名称也以该目录项为准。
    target = raw.parent.resolve() / raw.name if suffix else raw.resolve()
    root = target.parent
    raw_derived = root / (target.name + suffix)
    derived = raw_derived.resolve()
    if root not in derived.parents:
        raise ValueError(f"断点文件路径越界: {file_path}{suffix}")
    if trust_root is not None and not derived.is_relative_to(pathlib.Path(trust_root).resolve()):
        raise ValueError(f"断点文件路径越界: {file_path}{suffix}")
    if suffix and raw_derived.is_symlink():
        raise ValueError(f"断点文件不能是符号链接: {file_path}{suffix}")
    return str(derived)


def _local_meta_digest(file_path: str) -> str:
    """本地断点 meta 键摘要: sha256(绝对路径) 前 16 位 (task-agnostic; 哈希键天然免疫
    路径穿越与符号链接重定向)。非安全用途, 仅为文件身份去重命名。"""
    return hashlib.sha256(os.path.abspath(file_path).encode("utf-8")).hexdigest()[:16]


def local_upload_meta_path(file_path: str, total_bytes: int) -> str:
    """本地上传断点 meta 收拢: task-agnostic 键落 data/upload_meta 沙箱。

    键 = local_{sha256(绝对路径)[:16]}_{total_bytes}.upload.meta, 按文件身份派生;
    严禁含 task_id (meta.py meta_stem 的 task_id 先例仅适用 PikPak 场景)。
    """
    sandbox = settings.PIKPAK_META_DIR
    os.makedirs(sandbox, exist_ok=True)
    return os.path.join(sandbox, f"local_{_local_meta_digest(file_path)}_{int(total_bytes)}.upload.meta")


def discard_upload_meta(file_path: str) -> None:
    """FILE_PARTS_INVALID 自愈清除断点: 同摘要全部 size 变体的沙箱 meta
    一并清除 —— 清除后调用方 force_fresh 全量重传。删除幂等, 缺失不致命。"""
    sandbox = settings.PIKPAK_META_DIR
    try:
        import glob

        stale_pattern = os.path.join(sandbox, f"local_{_local_meta_digest(file_path)}_*.upload.meta")
        for stale in glob.glob(stale_pattern):
            try:
                os.remove(stale)
            except Exception as rm_err:
                logger.debug("清理失效上传断点文件失败 %s (非致命): %s", stale, rm_err)
    except Exception as glob_err:
        logger.debug("扫描失效上传断点目录异常 (非致命): %s", glob_err)


def _validated_breakpoint(path_str: str, trust_root: str | None = None) -> pathlib.Path:
    """断点文件创建写入前的就地校验: 规范化 + 符号链接重定向防御 (校验与写入须同作用域)。

    trust_root 提供时强校验 resolve 后路径 is_relative_to(trust_root); 未提供时不做目录边界约束。
    """
    raw = pathlib.Path(path_str)
    root = raw.parent.resolve()
    target = raw.resolve()
    if target.parent != root:
        raise ValueError(f"断点文件路径越界: {path_str}")
    if trust_root is not None and not target.is_relative_to(pathlib.Path(trust_root).resolve()):
        raise ValueError(f"断点文件路径越界: {path_str}")
    return target


def _write_meta_disk_sync(
    target_path: str,
    meta_dict: dict[str, Any],
    sync: bool = False,
    *,
    trust_root: str | None = None,
    scene: str = "上传",
) -> None:
    """断点元数据文件安全落盘: 唯一临时文件写入 + 原子替换 (稳态软写 sync=False / 终态强刷 sync=True)。

    trust_root 提供时 (下载链路传 DOWNLOAD_DIR) 强校验写入路径沙箱; scene 仅用于失败日志区分场景。
    """
    tmp_path: str | None = None
    try:
        target = _validated_breakpoint(target_path, trust_root=trust_root)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(target.parent),
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as f:
            tmp_path = f.name
            json.dump(meta_dict, f)
            f.flush()
            if sync:
                os.fsync(f.fileno())
        os.replace(tmp_path, target)
        tmp_path = None
    except Exception as e:
        logger.warning("保存%s元数据文件失败: %s", scene, e)
        if tmp_path:
            with suppress(OSError):
                os.unlink(tmp_path)


def _sync_read_chunk(fd: Any, offset: int, size: int) -> bytes:
    """在独立工作线程中执行同步磁盘 seek 与 read，避免事件循环主线程微冻结"""
    fd.seek(offset)
    # fd 为 Any: bytes() 显式收敛消除 no-any-return (CPython 对精确 bytes 输入返回同对象零拷贝)
    return bytes(fd.read(size))


def _ensure_parent_dir_sync(out_file_path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(out_file_path)), exist_ok=True)


def _verify_small_download_sync(res_path: str | None) -> int:
    if not (res_path and os.path.exists(res_path)):
        return 0
    return os.path.getsize(res_path)


def _prepare_download_breakpoint_sync(
    part_file_path: str | None,
    meta_file_path: str | None,
    total_bytes: int,
    chunk_size: int | None = None,
    *,
    media_identity: str | None = None,
) -> set[int]:
    if not part_file_path:
        return set()
    effective_chunk_size = chunk_size or settings.CHUNK_SIZE
    part_matches = os.path.exists(part_file_path) and os.path.getsize(part_file_path) == total_bytes
    if part_matches and meta_file_path and os.path.exists(meta_file_path):
        try:
            with open(meta_file_path, encoding="utf-8") as f:
                meta_data = json.load(f)
                if (
                    media_identity is not None
                    and meta_data.get("media_identity") == media_identity
                    and meta_data.get("total_bytes") == total_bytes
                    and meta_data.get("chunk_size", effective_chunk_size) == effective_chunk_size
                ):
                    return {
                        o
                        for o in meta_data.get("completed_offsets", [])
                        if type(o) is int and 0 <= o < total_bytes and o % effective_chunk_size == 0
                    }
        except (OSError, ValueError, TypeError, AttributeError) as exc:
            logger.debug("下载断点元数据不可用 %s: %s", meta_file_path, exc)

    # 先删除旧偏移，再写入分块文件。中途退出时旧元数据不能被下次任务误用。
    if meta_file_path:
        _validated_breakpoint(meta_file_path, trust_root=settings.DOWNLOAD_DIR)
        pathlib.Path(meta_file_path).unlink(missing_ok=True)
    if not part_matches:
        with _validated_breakpoint(part_file_path, trust_root=settings.DOWNLOAD_DIR).open("wb") as f:
            f.truncate(total_bytes)
    return set()


def _finalize_download_files_sync(out_file_path: str, part_file_path: str, meta_file_path: str | None) -> None:
    os.replace(part_file_path, out_file_path)
    if meta_file_path and os.path.exists(meta_file_path):
        os.remove(meta_file_path)


def _open_binary_file_sync(path: str, mode: str) -> IO[Any]:
    return open(path, mode)


def _check_upload_file_sync(file_path: str) -> tuple[int, str]:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")
    return os.path.getsize(file_path), os.path.basename(file_path)


def _compute_local_upload_fingerprint_sync(file_path: str, total_bytes: int) -> str:
    """Hash the complete source so checkpoint reuse detects changes at any offset."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while block := f.read(1024 * 1024):
            h.update(block)
    return h.hexdigest()


def _load_upload_breakpoint_sync(
    meta_file_path: str,
    file_name: str,
    total_bytes: int,
    default_file_id: int,
    force_fresh: bool,
    source_fingerprint: str | None = None,
) -> tuple[int, set[int]]:
    file_id = default_file_id
    completed_parts: set[int] = set()
    if force_fresh and os.path.exists(meta_file_path):
        try:
            os.remove(meta_file_path)
            logger.info(f"🔄 强制全量重传模式，已清除旧上传断点: {file_name}")
        except Exception as e:
            logger.debug("清除旧上传元数据失败: %s", e)
    elif os.path.exists(meta_file_path):
        try:
            with open(meta_file_path, encoding="utf-8") as f:
                meta_data = json.load(f)
            updated_at = meta_data.get("updated_at", 0)
            is_expired = (
                (time.time() - updated_at > UPLOAD_PART_TTL_SECONDS)
                if updated_at > 0
                else (time.time() - os.path.getmtime(meta_file_path) > UPLOAD_PART_TTL_SECONDS)
            )
            fingerprint_mismatch = (
                source_fingerprint is not None and meta_data.get("source_fingerprint") != source_fingerprint
            )
            if is_expired or fingerprint_mismatch:
                reason = (
                    f"已过期 (> {UPLOAD_PART_TTL_SECONDS // 3600}小时)"
                    if is_expired
                    else "本地文件指纹(mtime/内容)已变更"
                )
                logger.warning(f"⚠️ 检测到上传断点元数据{reason}，丢弃旧断点重新全量上传: {file_name}")
                try:
                    os.remove(meta_file_path)
                except Exception as rm_err:
                    logger.debug("清理失效断点 meta 异常 (非致命): %s", rm_err)
            elif meta_data.get("total_bytes") == total_bytes:
                file_id = meta_data.get("file_id", file_id)
                completed_parts = set(meta_data.get("completed_parts", []))
        except Exception:
            completed_parts = set()
    return file_id, completed_parts


def _remove_upload_meta_sync(meta_file_path: str) -> None:
    if os.path.exists(meta_file_path):
        try:
            os.remove(meta_file_path)
        except Exception as e:
            logger.debug("删除上传元数据文件异常: %s", e)


def _write_download_meta_disk_sync(target_path: str, meta_dict: dict[str, Any]) -> None:
    """下载断点元数据落盘薄委托: 恒强刷 (fsync) + DOWNLOAD_DIR 沙箱 (符号保留供测试 patch 挂载)。"""
    _write_meta_disk_sync(target_path, meta_dict, True, trust_root=settings.DOWNLOAD_DIR, scene="下载")


class _SingleFlightMetaSaver:
    """异步非阻塞断点元数据保存器基类 (Single-Flight 乱序防护)。

    统一承载在途/待写状态机与 trigger/_worker_loop/flush 盾牌等待逻辑; 子类仅覆写两个钩子:
    `_snapshot_meta()` 生成当前元数据快照 (下载侧在共享 lock 内取样), `_persist_snapshot()`
    执行单次落盘 (稳态软写不 shield / 终态强刷 shield, 与拆分前行为逐路径等价)。
    """

    def __init__(self, meta_file_path: str) -> None:
        self.meta_file_path = meta_file_path
        self.is_saving = False
        self.pending_save = False
        self.save_task: asyncio.Task[None] | None = None

    async def _snapshot_meta(self) -> dict[str, Any]:
        raise NotImplementedError

    async def _persist_snapshot(self, meta_dict: dict[str, Any], *, final_flush: bool) -> None:
        raise NotImplementedError

    async def _worker_loop(self) -> None:
        self.is_saving = True
        try:
            while True:
                self.pending_save = False
                await self._persist_snapshot(await self._snapshot_meta(), final_flush=False)
                if not self.pending_save:
                    break
        finally:
            self.is_saving = False

    def trigger(self) -> None:
        if self.is_saving:
            self.pending_save = True
            return
        self.is_saving = True
        try:
            self.save_task = asyncio.create_task(self._worker_loop())
        except BaseException:
            self.is_saving = False
            raise

    async def flush(self) -> None:
        if self.save_task and not self.save_task.done():
            with suppress(Exception):
                await asyncio.shield(self.save_task)
        await self._persist_snapshot(await self._snapshot_meta(), final_flush=True)


class _DownloadMetaSaver(_SingleFlightMetaSaver):
    """异步非阻塞下载断点元数据保存器 (Single-Flight 乱序防护)"""

    def __init__(
        self,
        meta_file_path: str,
        total_bytes: int,
        completed_offsets: set[int],
        lock: asyncio.Lock,
        chunk_size: int = settings.CHUNK_SIZE,
        sync_part_fn: Callable[[], None] | None = None,
        *,
        media_identity: str | None = None,
    ) -> None:
        super().__init__(meta_file_path)
        self.total_bytes = total_bytes
        self.chunk_size = chunk_size
        self.completed_offsets = completed_offsets
        self.lock = lock
        self._sync_part_fn = sync_part_fn
        self.media_identity = media_identity

    async def _snapshot_meta(self) -> dict[str, Any]:
        async with self.lock:
            if self._sync_part_fn is not None:
                await asyncio.to_thread(self._sync_part_fn)
            return {
                "total_bytes": self.total_bytes,
                "chunk_size": self.chunk_size,
                "media_identity": self.media_identity,
                "completed_offsets": list(self.completed_offsets),
            }

    async def _persist_snapshot(self, meta_dict: dict[str, Any], *, final_flush: bool) -> None:
        write = asyncio.to_thread(_write_download_meta_disk_sync, self.meta_file_path, meta_dict)
        if final_flush:
            await asyncio.shield(write)
        else:
            await write


class _UploadMetaSaver(_SingleFlightMetaSaver):
    """异步非阻塞上传断点元数据保存器 (Single-Flight 乱序防护，无锁)"""

    def __init__(
        self,
        meta_file_path: str,
        total_bytes: int,
        file_id: int,
        completed_parts: set[int],
        num_chunks: int,
        source_fingerprint: str | None = None,
    ) -> None:
        super().__init__(meta_file_path)
        self.total_bytes = total_bytes
        self.file_id = file_id
        self.completed_parts = completed_parts
        self.num_chunks = num_chunks
        self.source_fingerprint = source_fingerprint

    def _build_meta_dict(self) -> dict[str, Any]:
        meta_dict: dict[str, Any] = {
            "total_bytes": self.total_bytes,
            "file_id": self.file_id,
            "completed_parts": list(self.completed_parts),
            "updated_at": time.time(),
        }
        if self.source_fingerprint is not None:
            meta_dict["source_fingerprint"] = self.source_fingerprint
        return meta_dict

    async def _snapshot_meta(self) -> dict[str, Any]:
        return self._build_meta_dict()

    async def _persist_snapshot(self, meta_dict: dict[str, Any], *, final_flush: bool) -> None:
        # 终态 flush: 完成度已达 100% 时不再补写 (盾牌等待在途写盘已由基类 flush 完成)
        if final_flush and len(self.completed_parts) == self.num_chunks:
            return
        write = asyncio.to_thread(
            _write_meta_disk_sync,
            self.meta_file_path,
            meta_dict,
            final_flush,
            trust_root=settings.PIKPAK_META_DIR,
        )
        if final_flush:
            await asyncio.shield(write)
        else:
            await write


def _resolve_upload_meta_file_path(
    file_path: str, total_bytes: int, chunk_provider: BaseChunkProvider | None, meta_path: str | None
) -> str:
    if chunk_provider is not None:
        if not meta_path:
            raise ValueError("Provider 模式必须显式注入 meta_path (沙箱化断点元数据路径)")
        meta_file_path = resolve_breakpoint_path(meta_path, "", trust_root=settings.PIKPAK_META_DIR)
        _ensure_parent_dir_sync(meta_file_path)
        return meta_file_path
    return local_upload_meta_path(file_path, total_bytes)
