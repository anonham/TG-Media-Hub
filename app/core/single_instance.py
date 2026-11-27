"""单实例锁守卫 (Single Instance Lock Guard)

提供跨平台进程级单实例文件锁：
- Windows: 使用 msvcrt.locking 锁定 4096 偏移量单字节，允许读取文件头部的 PID/时间戳/端口元数据；
- POSIX: 使用 fcntl.flock(LOCK_EX | LOCK_NB) 实现非阻塞排他锁；
- 双实例共存防护：若锁已被其他实例占用，快速抛出 RuntimeError 阻止启动并提示当前占用者的 PID 与启动时间；
- 异常退出自愈：内核级进程句柄绑定，进程崩溃或终止后操作系统自动释放锁，避免死锁；
- 退出清理：提供 atexit 与 finally 双保险释放与清理。
"""

import atexit
import json
import logging
import os
import sys
from datetime import datetime

logger = logging.getLogger(__name__)

# Windows 偏移锁定常量 (锁定 4096 处 1 个字节，0~4095 留作可读元数据)
_WIN_LOCK_OFFSET = 4096

# 当前进程已持有的单实例锁规范路径集合 (用于 run.py 与 ASGI lifespan 同进程复用，避免重复加锁)
_held_lock_paths: set[str] = set()


def _norm_lock_path(lock_path: str) -> str:
    return os.path.normcase(os.path.abspath(lock_path))


def acquire_process_singleton_lock(lock_path: str, port: int | None = None) -> "SingleInstanceLock | None":
    """若当前进程尚未持有该路径的单实例锁，则获取并返回新锁；若已持有则静默复用并返回 None。"""
    norm_path = _norm_lock_path(lock_path)
    if norm_path in _held_lock_paths:
        return None
    lock = SingleInstanceLock(lock_path)
    lock.acquire(port=port)
    return lock


class SingleInstanceLock:
    """跨平台单实例文件锁管理器"""

    def __init__(self, lock_path: str):
        self.lock_path = lock_path
        self._fd: int | None = None
        self._acquired: bool = False
        self._start_time_iso: str | None = None
        self._port: int | None = None

    @property
    def is_acquired(self) -> bool:
        return self._acquired

    def acquire(self, port: int | None = None) -> None:
        """尝试获取独占文件锁。若已被占用则抛出 RuntimeError 阻断服务启动。"""
        if self._acquired:
            raise RuntimeError("本实例已持有该单实例锁，请勿重复加锁")

        os.makedirs(os.path.dirname(os.path.abspath(self.lock_path)), exist_ok=True)
        now_iso = datetime.now().isoformat()
        self._start_time_iso = now_iso
        self._port = port

        if sys.platform == "win32":
            import msvcrt

            fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o666)
            os.lseek(fd, _WIN_LOCK_OFFSET, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            except OSError:
                # 加锁失败，读取头部占用者信息
                os.lseek(fd, 0, os.SEEK_SET)
                raw = os.read(fd, _WIN_LOCK_OFFSET)
                os.close(fd)
                pid, start_time, occupant_port = self._parse_metadata(raw)
                port_str = f", 端口: {occupant_port}" if occupant_port else ""
                raise RuntimeError(
                    f"已有实例正在运行 (PID: {pid}, 启动时间: {start_time}{port_str})，请勿多开。"
                ) from None

            # 加锁成功，将元数据写入文件头部 (以 null 字节填满元数据区避免残留)
            try:
                meta = {"pid": os.getpid(), "start_time": now_iso, "port": port}
                payload = json.dumps(meta, ensure_ascii=False).encode("utf-8")
                padded = payload.ljust(_WIN_LOCK_OFFSET, b"\x00")
                os.lseek(fd, 0, os.SEEK_SET)
                os.write(fd, padded)
                os.ftruncate(fd, _WIN_LOCK_OFFSET + 1)
                self._fd = fd
                self._acquired = True
            except BaseException:
                try:
                    os.lseek(fd, _WIN_LOCK_OFFSET, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                except Exception as e_unlock:
                    logger.debug("写入元数据失败后解除 Windows 锁异常: %s", e_unlock)
                os.close(fd)
                raise
        else:
            import fcntl

            fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o666)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (BlockingIOError, OSError):
                os.lseek(fd, 0, os.SEEK_SET)
                raw = os.read(fd, 4096)
                os.close(fd)
                pid, start_time, occupant_port = self._parse_metadata(raw)
                port_str = f", 端口: {occupant_port}" if occupant_port else ""
                raise RuntimeError(
                    f"已有实例正在运行 (PID: {pid}, 启动时间: {start_time}{port_str})，请勿多开。"
                ) from None

            try:
                meta = {"pid": os.getpid(), "start_time": now_iso, "port": port}
                payload = json.dumps(meta, ensure_ascii=False).encode("utf-8")
                os.lseek(fd, 0, os.SEEK_SET)
                os.ftruncate(fd, 0)
                os.write(fd, payload)
                self._fd = fd
                self._acquired = True
            except BaseException:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except Exception as e_unlock:
                    logger.debug("写入元数据失败后解除 POSIX 锁异常: %s", e_unlock)
                os.close(fd)
                raise

        _held_lock_paths.add(_norm_lock_path(self.lock_path))
        atexit.register(self.release)
        logger.debug("成功获取单实例锁: %s (PID: %d)", self.lock_path, os.getpid())

    def update_port(self, port: int) -> None:
        """更新锁文件中的监听端口信息"""
        if not self._acquired or self._fd is None:
            return
        self._port = port
        meta = {"pid": os.getpid(), "start_time": self._start_time_iso, "port": port}
        payload = json.dumps(meta, ensure_ascii=False).encode("utf-8")
        try:
            os.lseek(self._fd, 0, os.SEEK_SET)
            if sys.platform == "win32":
                padded = payload.ljust(_WIN_LOCK_OFFSET, b"\x00")
                os.write(self._fd, padded)
                os.ftruncate(self._fd, _WIN_LOCK_OFFSET + 1)
            else:
                os.ftruncate(self._fd, 0)
                os.write(self._fd, payload)
        except OSError as e:
            logger.warning("更新单实例锁文件端口信息失败: %s", e)

    def release(self) -> None:
        """释放锁句柄并清理锁文件"""
        if not self._acquired or self._fd is None:
            return

        try:
            try:
                atexit.unregister(self.release)
            except Exception as e_atexit:
                logger.debug("注销 atexit 回调异常: %s", e_atexit)
            if sys.platform == "win32":
                import msvcrt

                try:
                    os.lseek(self._fd, _WIN_LOCK_OFFSET, os.SEEK_SET)
                    msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
                except OSError as e_unlock:
                    logger.debug("Windows 解除文件锁异常 (非致命): %s", e_unlock)
            else:
                import fcntl

                try:
                    fcntl.flock(self._fd, fcntl.LOCK_UN)
                except OSError as e_flock:
                    logger.debug("POSIX 解除文件锁异常 (非致命): %s", e_flock)

            try:
                os.close(self._fd)
            except OSError as e_close:
                logger.debug("关闭锁文件句柄异常 (非致命): %s", e_close)
        finally:
            self._fd = None
            self._acquired = False
            _held_lock_paths.discard(_norm_lock_path(self.lock_path))
            logger.debug("单实例锁已释放: %s", self.lock_path)

    def _parse_metadata(self, raw: bytes) -> tuple:
        """安全解析已有锁文件的元数据 (容忍 null 字节填充与截断)"""
        try:
            clean = raw.split(b"\x00")[0].decode("utf-8", errors="replace").strip()
            if not clean:
                return "未知", "未知", None
            data = json.loads(clean)
            return (data.get("pid", "未知"), data.get("start_time", "未知"), data.get("port"))
        except Exception:
            return "未知", "未知", None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
