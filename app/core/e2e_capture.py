"""E2E 捕获模式: 超限流式切片发布的取证证据留存。

生产模式 (E2E_CAPTURE_MODE=false) 语义与零盘承诺完全不变; 捕获模式为一条发布
item 留存三份证据到 E2E_CAPTURE_DIR/<task_id>/<item_id>/ (计划 D1-D9 + 勘误
E1-E9, 详见 docs/testing/E2E_GUIDE.md §5.1):

- E1 source/<原文件名>: 独立 provider 全量顺序读 + 流式 sha256 (不做流内 tee——
  VSSE 探测/上传读流是稀疏的, fMP4 流不经过 Python 字节层);
- E2 parts/<part_name>: 上传流内 tee (VSSE: TeeChunkProvider 装饰 envelope
  provider; fMP4: 伴随 _enqueue_chunk 的 capture_sink), 证明"上传的就是这些字节";
- E3 tg_mirror/<part_name>: 离线脚本 scripts/e2e_tg_mirror.py 回下载。

设计要点:
- 一切以 manifest.json 为锚 (运行时写入 + 脚本回写, 原子写; 三方解耦);
- 定位写 (offset 寻址) 而非 append: 重试重复读/预取超前读/乱序到达天然幂等;
- 捕获失败降级不阻断发布: 异常 logger.warning + manifest 标记, 不上抛;
- fMP4 sink 工厂以 truncate 模式开 .part (engines 每 attempt 重建, E2);
  成功才 rename 去掉 .part, 失败残留不会被误当有效产物;
- 会话 inactive 透传: 捕获关闭/目录不可创建时所有方法安全 no-op,
  make_vsse_tee 原样返回内层 provider, make_fmp4_sink 返回 None。

模块红线: 纯 core 层, 禁止 import app.services / app.api; manifest 走文件系统,
绕开 SQLite 单写者约束。
"""

import asyncio
import dataclasses
import datetime
import hashlib
import json
import logging
import os
import time
from collections.abc import Callable
from typing import BinaryIO

from app.config import settings
from app.core.stream_chunk_provider import BaseChunkProvider

logger = logging.getLogger(__name__)

MANIFEST_SCHEMA_VERSION = 1
MANIFEST_NAME = "manifest.json"
PART_SUFFIX = ".part"
SNAPSHOT_CHUNK_SIZE = 4 * 1024 * 1024  # E1 源快照顺序读块
_ERROR_MAX_LEN = 500


def _atomic_write_json(path: str, payload: dict) -> None:
    """manifest 原子写: 先写 .tmp 再 os.replace (读方永远看到完整 JSON)。

    Windows 下目标文件刚被读取方关闭时 os.replace 可能瞬时 WinError 5
    (索引器/杀软短暂持柄), 以短退避重试兜底。
    """
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    last_err: Exception | None = None
    for _ in range(5):
        try:
            os.replace(tmp, path)
            return
        except PermissionError as e:
            last_err = e
            time.sleep(0.02)
    if last_err is not None:
        raise last_err


def serialize_plan(plan) -> dict:
    """VsseVolumePlan / Fmp4PartPlan → 可 JSON 化 dict。

    E6: VsseVolumePlan 的 envelope_start/end/length 是 property 别名 (非字段),
    dataclasses.asdict 取不到, 须显式补充。
    """
    data = dataclasses.asdict(plan)
    for alias in ("envelope_start", "envelope_end", "envelope_length"):
        if hasattr(plan, alias):
            data[alias] = getattr(plan, alias)
    return data


class _CaptureFileWriter:
    """产物取证定位写器 (VSSE tee 与 fMP4 sink 共用)。

    - truncate 模式开: 每 attempt 重新调用工厂即天然重建 (E2);
    - write_at 同步短写: 512 KiB 级页面缓存写, 事件循环可容忍;
    - 写失败置 broken 并停写, 只告警一次不上抛 (D7: 捕证失败不得杀死上传;
      fMP4 侧 engines 的整卷重试 except 面极宽, 冒泡会触发无意义 FFmpeg 重跑, E2);
    - bytes_written 记录最大写入末端, 供完备性信号 (A2 断言 len(parts)==H+L)。
    """

    __slots__ = ("_fh", "_max_end", "broken", "path")

    def __init__(self, path: str):
        self.path = path
        self.broken = False
        self._max_end = 0
        self._fh: BinaryIO | None = None

    def _ensure_open(self):
        if self._fh is None:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self._fh = open(self.path, "wb")
        return self._fh

    def write_at(self, offset: int, data: bytes) -> None:
        if self.broken or not data:
            return
        try:
            fh = self._ensure_open()
            fh.seek(offset)
            fh.write(data)
            end = offset + len(data)
            self._max_end = max(self._max_end, end)
        except OSError as e:
            self.broken = True
            logger.warning("E2E 捕获产物写盘失败, 停止该卷后续写入 (%s): %s", self.path, e)

    def close(self) -> None:
        if self._fh is not None:
            try:
                self._fh.close()
            except OSError as e:
                self.broken = True
                logger.debug("E2E 捕获产物句柄关闭异常 (%s): %s", self.path, e)
            self._fh = None

    @property
    def bytes_written(self) -> int:
        return self._max_end


class TeeChunkProvider(BaseChunkProvider):
    """VSSE 产物取证 tee: 委托内层 provider, 将读到的字节定位写入 .part 临时文件。

    - 属性照 _SpeedMeasuringProvider 先例做构造时值拷贝 (E4): total_bytes/
      file_id/file_name —— upload_file_parallel 只依赖这三者 + read_chunk;
    - 定位写 (seek(offset)+write): 断点续传被 D8 强制 force_fresh 后仍可能有
      预取/重试重复读, 幂等覆盖;
    - 不改变 read_chunk 返回长度/offset 语义 (L1 环形缓存按 offset+len 键控);
    - aclose 只关 tee 句柄, 不代关内层 (内层生命周期由 engines 外层 finally
      持有, 同 SlicedChunkProvider 哲学)。
    """

    def __init__(self, inner: BaseChunkProvider, writer: _CaptureFileWriter):
        self._inner = inner
        self._writer = writer
        self.total_bytes = inner.total_bytes
        self.file_id = inner.file_id
        self.file_name = inner.file_name

    async def read_chunk(self, offset: int, size: int) -> bytes:
        data = await self._inner.read_chunk(offset, size)
        self._writer.write_at(offset, data)
        return data

    @property
    def bytes_written(self) -> int:
        return self._writer.bytes_written

    async def aclose(self) -> None:
        self._writer.close()


class E2ECaptureSession:
    """单 item 取证会话: 目录/manifest 管理 + 源快照 + tee/sink 工厂。

    生命周期 (engines 侧): 路由判定后 open(); await snapshot_source() (D2 先基准
    后实验); 各通道经 make_vsse_tee / make_fmp4_sink 取产物写入口; 逐卷成功
    record_part_sent (close→rename→manifest), 失败 record_part_failed;
    fMP4 attempt 失败 discard_part_sink (E2); 整体失败回滚 mark_rolled_back (E3)。

    inactive 会话 (捕获关闭或目录不可创建): 所有方法安全 no-op / 透传, engines
    代码无需条件分支。
    """

    def __init__(
        self,
        task_id,
        item_id,
        *,
        target_chat_id=None,
        split_strategy: str | None = None,
        threshold_bytes: int | None = None,
    ):
        self.active = False
        self._task_id = task_id
        self._item_id = item_id
        self._root: str | None = None
        self._manifest: dict = {}
        self._writers: dict = {}  # part_name -> _CaptureFileWriter
        try:
            root = os.path.join(settings.E2E_CAPTURE_DIR, str(task_id), str(item_id))
            os.makedirs(os.path.join(root, "parts"), exist_ok=True)
            os.makedirs(os.path.join(root, "tg_mirror"), exist_ok=True)
            self._root = root
            self._manifest = {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "task_id": task_id,
                "item_id": item_id,
                "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
                "target_chat_id": target_chat_id,
                "split_strategy": split_strategy,
                "threshold_bytes": threshold_bytes,
                # D8: capture 下 VSSE 恒传 force_fresh=True (禁断点续传, 保 tee 完备)
                "force_fresh": True,
                "rolled_back": False,
                "channel_switch": None,
                "source": {
                    "name": "",
                    "path": "",
                    "size_bytes": 0,
                    "sha256": "",
                    "status": "pending",
                    "error": "",
                },
                "parts": [],
                "tg_mirror": [],
                "compare": {"status": "pending", "report": ""},
            }
            self._flush()
            self.active = True
        except OSError as e:
            logger.warning(
                "E2E 捕获目录创建失败, 本次发布取证整体 skipped (task=%s, item=%s): %s",
                task_id,
                item_id,
                e,
            )

    @classmethod
    def inactive(cls) -> "E2ECaptureSession":
        """no-op 透传哨兵: 捕获关闭 / 通道函数直调 (无会话注入) 时使用。"""
        session = cls.__new__(cls)
        session.active = False
        session._task_id = None
        session._item_id = None
        session._root = None
        session._manifest = {}
        session._writers = {}
        return session

    @classmethod
    def open(cls, task_id, item_id, **kwargs) -> "E2ECaptureSession":
        """工厂: 捕获关闭时直接返回 inactive 会话 (目录零创建的回归保障)。"""
        if not settings.E2E_CAPTURE_MODE:
            return cls.inactive()
        return cls(task_id, item_id, **kwargs)

    # ---- 内部工具 -------------------------------------------------------

    def _flush(self) -> None:
        if not self._root:
            return
        try:
            _atomic_write_json(os.path.join(self._root, MANIFEST_NAME), self._manifest)
        except OSError as e:
            logger.warning("E2E 捕获 manifest 落盘失败 (item=%s): %s", self._item_id, e)

    def _part_tmp_path(self, part_name: str) -> str:
        return os.path.join(self._root or "", "parts", part_name + PART_SUFFIX)

    def _part_final_path(self, part_name: str) -> str:
        return os.path.join(self._root or "", "parts", part_name)

    def _find_part(self, part_name: str) -> dict | None:
        for entry in self._manifest["parts"]:
            if entry.get("part_name") == part_name:
                return entry
        return None

    def _upsert_part(self, part_name: str, channel: str | None, plan, header_len: int | None) -> dict:
        entry = self._find_part(part_name)
        if entry is None:
            entry = {
                "part_name": part_name,
                "channel": channel,
                "plan": serialize_plan(plan) if plan is not None else None,
                "header_len": header_len if header_len is not None else 0,
                "envelope_start": getattr(plan, "envelope_start", 0) if plan is not None else 0,
                "envelope_length": getattr(plan, "envelope_length", 0) if plan is not None else 0,
                "path": "",
                "bytes_written": 0,
                "sha256": "",
                "msg_id": 0,
                "status": "pending",
                "error": "",
            }
            self._manifest["parts"].append(entry)
        return entry

    # ---- E1 源快照 -------------------------------------------------

    async def snapshot_source(
        self, make_provider: Callable[[], BaseChunkProvider], name: str, total_bytes: int
    ) -> None:
        """独立 provider 全量顺序读 + 定位写 + 流式 sha256; 失败降级标记不上抛。"""
        if not self.active or not self._root or self._manifest["source"]["status"] == "ok":
            return
        src_path = os.path.join(self._root, "source", os.path.basename(name))
        provider = None
        try:
            os.makedirs(os.path.dirname(src_path), exist_ok=True)
            provider = make_provider()
            digest = hashlib.sha256()
            written = 0
            f = await asyncio.to_thread(open, src_path, "wb")
            try:
                while written < total_bytes:
                    data = await provider.read_chunk(written, min(SNAPSHOT_CHUNK_SIZE, total_bytes - written))
                    if not data:
                        raise OSError(f"源快照短读: offset={written}, 期望 >0 字节")
                    await asyncio.to_thread(f.write, data)
                    digest.update(data)
                    written += len(data)
            finally:
                await asyncio.to_thread(f.close)
            if written != total_bytes:
                raise OSError(f"源快照字节数不符: 预期 {total_bytes}, 实际 {written}")
            self._manifest["source"] = {
                "name": os.path.basename(name),
                "path": "source/" + os.path.basename(name),
                "size_bytes": total_bytes,
                "sha256": digest.hexdigest(),
                "status": "ok",
                "error": "",
            }
        except Exception as e:
            logger.warning("E2E 捕获源快照失败, 后续源侧断言将降级跳过 (item=%s): %s", self._item_id, e)
            self._manifest["source"] = {
                "name": os.path.basename(name),
                "path": "",
                "size_bytes": total_bytes,
                "sha256": "",
                "status": "failed",
                "error": str(e)[:_ERROR_MAX_LEN],
            }
        finally:
            if provider is not None:
                try:
                    await provider.aclose()
                except Exception as e:
                    logger.warning("E2E 捕获源快照 provider 关闭异常 (item=%s): %s", self._item_id, e)
        self._flush()

    # ---- E2 产物写入口 ----------------------------------------------

    def make_vsse_tee(self, plan, inner: BaseChunkProvider, header_len: int) -> BaseChunkProvider:
        """VSSE 产物 tee 工厂: 返回包装后的 provider (inactive 时原样透传)。"""
        if not self.active:
            return inner
        writer = _CaptureFileWriter(self._part_tmp_path(plan.part_name))
        self._writers[plan.part_name] = writer
        self._upsert_part(plan.part_name, "vsse", plan, header_len)
        return TeeChunkProvider(inner, writer)

    def make_fmp4_sink(self, part_name: str, part_size: int) -> Callable[[int, int, bytes], None] | None:
        """fMP4 产物 sink 工厂: 返回 (idx, flag, data) -> None 同步可调用。

        定位写 seek(idx*part_size)——字节到达顺序非 0,1,2,… (part 0 与尾块最后
        入队), 定位写是必需; 尾块/单卷块可短于 part_size, 不得假设定长 (E1/E2)。
        inactive 时返回 None (uploader 侧 capture_sink=None 热路径零开销)。
        """
        if not self.active:
            return None
        writer = _CaptureFileWriter(self._part_tmp_path(part_name))
        self._writers[part_name] = writer

        def sink(idx: int, flag: int, data: bytes) -> None:
            writer.write_at(idx * part_size, data)

        return sink

    # ---- 产物记录 ------------------------------------------

    def record_part_sent(
        self,
        part_name: str,
        *,
        msg_id: int,
        channel: str | None = None,
        plan=None,
        header_len: int | None = None,
        sent_attributes: dict | None = None,
    ) -> None:
        """卷发送成功: close 句柄 → rename 去掉 .part → manifest 记录 (失败降级)。

        sent_attributes: 消息实发首个 attributes 元素快照
        {duration, w, h, supports_streaming} (A5 消息标签层取证, 供 compare 比对);
        None 不写键 —— 旧调用面与特性前 manifest 兼容。"""
        if not self.active:
            return
        entry = self._upsert_part(part_name, channel, plan, header_len)
        entry["msg_id"] = int(msg_id)
        entry["status"] = "sent"
        if sent_attributes is not None:
            entry["sent_attributes"] = dict(sent_attributes)
        writer = self._writers.pop(part_name, None)
        if writer is not None:
            writer.close()
            entry["bytes_written"] = writer.bytes_written
            if writer.broken:
                entry["error"] = "capture_write_failed"
            else:
                try:
                    os.replace(writer.path, self._part_final_path(part_name))
                    entry["path"] = "parts/" + part_name
                except OSError as e:
                    logger.warning("E2E 捕获产物 rename 失败 (part=%s): %s", part_name, e)
                    entry["error"] = "capture_rename_failed"
        self._flush()

    def record_part_failed(
        self, part_name: str, error: str, channel: str | None = None, plan=None, header_len: int | None = None
    ) -> None:
        """卷发送失败: 关句柄保留 .part 残留取证 (不 rename), manifest 标记 failed。"""
        if not self.active:
            return
        entry = self._upsert_part(part_name, channel, plan, header_len)
        entry["status"] = "failed"
        entry["error"] = str(error)[:_ERROR_MAX_LEN]
        writer = self._writers.pop(part_name, None)
        if writer is not None:
            writer.close()
            entry["bytes_written"] = writer.bytes_written
        self._flush()

    def discard_part_sink(self, part_name: str) -> None:
        """fMP4 attempt 失败: 关句柄 + 删除 .part (下 attempt 工厂 truncate 重建, E2)。"""
        if not self.active:
            return
        writer = self._writers.pop(part_name, None)
        if writer is not None:
            writer.close()
            try:
                os.remove(writer.path)
            except OSError as e:
                logger.debug("E2E 捕获产物残留删除异常 (part=%s): %s", part_name, e)

    def fail_pending_parts(self, error: str) -> None:
        """通道整体中止: 仍处 pending 的卷 (未走到成功/失败记录) 统一标 failed, 关句柄保留 .part 残留。"""
        if not self.active:
            return
        err_msg = str(error)[:_ERROR_MAX_LEN]
        for entry in self._manifest["parts"]:
            if entry.get("status") == "pending":
                entry["status"] = "failed"
                entry["error"] = err_msg
                writer = self._writers.pop(entry["part_name"], None)
                if writer is not None:
                    writer.close()
                    entry["bytes_written"] = writer.bytes_written
        # fMP4 sink 仅在 _writers 中登记, 若未走到 record_part_sent/failed 即中止需兜底关闭并入账
        for part_name, writer in list(self._writers.items()):
            self._writers.pop(part_name, None)
            writer.close()
            entry = self._upsert_part(part_name, "fmp4", None, None)
            entry["status"] = "failed"
            entry["error"] = err_msg
            entry["bytes_written"] = writer.bytes_written
        self._flush()

    def mark_parts_discarded(self, reason: str = "replan") -> None:
        """fMP4 重规划回滚: 已成功旧卷消息被物理删除, 记录标 discarded (E2)。"""
        if not self.active:
            return
        for entry in self._manifest["parts"]:
            if entry.get("status") == "sent":
                entry["status"] = "discarded"
                entry["error"] = str(reason)[:_ERROR_MAX_LEN]
        self._flush()

    # ---- 任务级标记 (E3/熔断降级) ----------------------------------------

    def mark_rolled_back(self) -> None:
        """整体失败回滚: 已发消息被物理删除, manifest 置 rolled_back (E3)。"""
        if not self.active:
            return
        self._manifest["rolled_back"] = True
        self._flush()

    def record_channel_switch(self, reason: str) -> None:
        """VSSE 熔断降级 fMP4: 记录通道切换事实 (两段产物分别归属各自通道)。"""
        if not self.active:
            return
        self._manifest["channel_switch"] = str(reason)[:_ERROR_MAX_LEN]
        self._flush()
