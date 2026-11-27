"""发布任务编排 (Publish Runner) —— D4 重组自 publish_executor_service。

承载 run_publish_folder_task / run_publish_pikpak_task 两个执行入口 (策略 execute
唯一调用) 与其 ≤120 行协作函数组: 共享诊断落库 (J7 封顶信封)、终态统一收尾、
发布结果终态化。进度回调经共享工厂 make_executor_progress_cb (条目协议)。
"""

import asyncio
import logging
from dataclasses import asdict
from typing import Any

from app.core.connection_manager import connection_manager
from app.core.executor_progress import make_executor_progress_cb
from app.core.pikpak import PikPakError
from app.core.pikpak.expander import notify_expansion_truncation
from app.core.redact import redact_payload
from app.core.runtime import (
    active_task_controls,
    is_task_cancelled,
    is_task_paused,
    mark_task_pause_exit,
    release_runner_controls,
    try_claim_executor_entry,
)
from app.core.scheduler import UploadCancelledError, UploadPausedError
from app.core.session_errors import is_session_invalid_error, report_session_invalid
from app.core.stream_chunk_provider import ChunkProviderError
from app.db.task_repository import TaskQueueRepository
from app.db.write_pool import submit_db_write
from app.services.pikpak_publisher_service import PikPakPublisherService
from app.services.publisher_service import PublisherService

logger = logging.getLogger(__name__)


def _cap_param(entries, cap: int = 50) -> dict:
    """包4 (J7): 诊断明细封顶信封 —— params_json 防膨胀: 前 cap 条 + 总数。
    适用于新增的 failed_files / dedup_skipped 两键 (skipped_files 既有裸列表形态不动)。"""
    return {"items": list(entries[:cap]), "total": len(entries)}


async def _finalize_placeholder_slots(task_id: str) -> None:
    """任务终态 failed → 占位槽终态化 (❌ caption 编辑, 严禁删除消息)。
    能力实现在 app/core (J9: 调度器同款调用); 异常全包围 (J12), 不影响任务终态。"""
    try:
        from app.core.pipeline.placeholder import PlaceholderSlotCoordinator

        await PlaceholderSlotCoordinator().finalize_terminal(task_id)
    except Exception as finalize_err:
        logger.warning("占位槽终态化异常 (不影响任务终态): %s", finalize_err)


def _enrich_placeholder_states(target_chat_id, failed_details) -> None:
    """失败明细按身份富化占位槽当前状态 (诊断表"占位状态"列数据源;
    ≤50 封顶不变)。异常静默 —— 富化失败不影响诊断明细本身。"""
    try:
        from app.db.publish_repository import PlaceholderSlotRepository

        identities = [(d.get("file_path"), int(d.get("file_size") or 0)) for d in failed_details if d.get("file_path")]
        if not identities:
            return
        states = PlaceholderSlotRepository.states_for_identities(target_chat_id, identities)
        for d in failed_details:
            key = f"{d.get('file_path')}|{int(d.get('file_size') or 0)}"
            if key in states:
                d["placeholder_state"] = states[key]
    except Exception as enrich_err:
        logger.debug("占位槽状态富化跳过: %s", enrich_err)


async def _publish_terminal(task_id: str, status: str, log_text: str) -> None:
    """终态统一收尾: 状态落库 + 任务日志 + task_update 广播 + 控制项清理"""
    if status == "paused":
        mark_task_pause_exit(task_id)
    if await TaskQueueRepository.update_status_async(task_id, status, from_statuses=("running", status)):
        await TaskQueueRepository.append_log_async(task_id, log_text)
        await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": status})
    release_runner_controls(task_id)


async def _persist_publish_diagnostics(task_id: str, res, target_channel: str) -> None:
    """发布结果诊断明细落 params (封顶信封, J7): 跳过表 / 失败表 (含占位状态富化) /
    防重命中表 —— 两个发布任务共享同一落库口径。三键每次终态化全量覆写为最近一次执行
    结果 (空结果写空信封/空列表), 重试成功后不再残留早前尝试的旧诊断;
    skipped_files 中用户手动跳过行 (oversize_kind=manual_skip) 属任务级操作记录, 经
    overwrite_params_list 在同一独占事务内读取并保留 —— 无 read-before-write TOCTOU,
    并发 skip_current_file 插入的行不会丢失, 事务失败时存量行整体保留 (批 A 复审)。"""
    skipped_files = getattr(res, "skipped_files", []) or []
    result_skips = [s.model_dump() if hasattr(s, "model_dump") else s for s in skipped_files]
    await asyncio.to_thread(
        TaskQueueRepository.overwrite_params_list,
        task_id,
        "skipped_files",
        result_skips,
        keep_pred=_is_manual_skip_row,
    )
    failed_details = getattr(res, "failed_details", []) or []
    if failed_details:
        _enrich_placeholder_states(target_channel, failed_details)  # 占位状态列
    await asyncio.to_thread(
        TaskQueueRepository.merge_params,
        task_id,
        {"failed_files": _cap_param(failed_details)},
    )
    dedup_skipped = getattr(res, "dedup_skipped", []) or []
    await asyncio.to_thread(
        TaskQueueRepository.merge_params,
        task_id,
        {
            "dedup_skipped": _cap_param(dedup_skipped),
            "newly_published_count": getattr(res, "newly_published_count", None),
        },
    )


def _is_manual_skip_row(row: Any) -> bool:
    return isinstance(row, dict) and row.get("oversize_kind") == "manual_skip"


async def _finalize_publish_result(task_id: str, res, target_channel: str, progress_cb, *, pikpak: bool) -> None:
    """发布结果终态化 (两发布任务共享): 诊断落库 → 失败判定 (skipped 豁免, D3) →
    占位槽 ❌ 终态化 → 汇总日志 + 终态进度回写 + 双广播。

    pikpak=True: 汇总文案带 PikPak 前缀, 终态进度取字节粘滞口径, WS 载荷经
    redact_payload 且含 pikpak_download_speed_mb_s 键 (J8 载荷键集合零改动)。
    """
    published_count = int(res)
    total_count = getattr(res, "total_files_count", published_count)
    failed_files = getattr(res, "failed_files", [])
    skipped_files = getattr(res, "skipped_files", [])

    await _persist_publish_diagnostics(task_id, res, target_channel)

    ctrl = active_task_controls.get(task_id, {})
    if not ctrl.get("cancelled") and not ctrl.get("paused"):
        # D3: 终态公式加 skipped 豁免 (对齐 pikpak 版) —— 纯超限跳过不再判 failed
        has_failures = bool(failed_files) or (total_count > 0 and (published_count + len(skipped_files)) < total_count)
        final_status = "failed" if has_failures else "completed"
        if not await TaskQueueRepository.update_status_async(task_id, final_status, from_statuses=("running",)):
            return
        failed_details = getattr(res, "failed_details", []) or []
        if final_status == "failed":
            await _finalize_placeholder_slots(task_id)  # 占位槽 ❌ 终态化
            if failed_details:
                # 终态化后重富化 (占位状态列反映 ❌ 实况, 抽屉不再显示过期 ⏳)
                _enrich_placeholder_states(target_channel, failed_details)
                await asyncio.to_thread(
                    TaskQueueRepository.merge_params,
                    task_id,
                    {"failed_files": _cap_param(failed_details)},
                )

        # 终态进度口径: 无失败即报 100% (纯 skip 任务 published=0 时旧式计算广播 0.0,
        # 与仓储完成态表征矛盾); 有失败保留真实部分百分比
        calc_percent = (
            100.0
            if not has_failures
            else (round((published_count / total_count * 100), 1) if total_count > 0 else 100.0)
        )
        if has_failures:
            item_desc = f"[{published_count}/{total_count}] 发布结束 (部分失败: {len(failed_files)} 个文件)"
            if pikpak:
                summary_log = (
                    f"⚠️ PikPak 发布未全量完成: 成功 {published_count}/{total_count} 个，"
                    f"失败 {len(failed_files)} 个文件 (已发布文件已入库防重，可重试断点续传)"
                )
            else:
                summary_log = (
                    f"⚠️ 发布未全量完成: 成功 {published_count}/{total_count} 个，"
                    f"失败 {len(failed_files)} 个文件 (已发布文件已入库，可点击【重试】断点续传)"
                )
        else:
            item_desc = f"[{published_count}/{total_count}] 全部发布完成"
            if pikpak:
                summary_log = f"🎉 成功发布 {published_count} 个 PikPak 文件/媒体到目标频道！"
            else:
                summary_log = f"🎉 成功发布 {published_count} 个文件/媒体到目标频道！"
            if skipped_files:
                summary_log += f" (跳过超限 {len(skipped_files)} 个文件)"

        final_last_done, final_all_bytes = progress_cb.last_progress_bytes()
        if pikpak:
            final_done_bytes = final_all_bytes if not has_failures else final_last_done
        else:
            final_done_bytes = final_last_done or (final_all_bytes if not has_failures else 0)
        await TaskQueueRepository.update_progress_async(
            task_id, final_done_bytes, final_all_bytes, published_count, total_count, 0.0, 0.0, item_desc
        )
        await TaskQueueRepository.append_log_async(task_id, summary_log)
        progress_data: dict[str, Any] = {
            "processed_bytes": final_done_bytes,
            "total_bytes": final_all_bytes,
            "processed_items": published_count,
            "total_items": total_count,
            "percent": calc_percent,
            "speed_mb_s": 0.0,
            **({"pikpak_download_speed_mb_s": 0.0} if pikpak else {}),
            "eta_s": 0.0,
            "current_item": item_desc,
            "concurrency": 0,
            "concurrency_total": 0,
            "dc_id": 0,
        }
        progress_payload: dict[str, Any] = {
            "type": "progress",
            "task_id": task_id,
            "progress": progress_data,
        }
        if pikpak:
            await connection_manager.broadcast(redact_payload(progress_payload))
        else:
            await connection_manager.broadcast(progress_payload)
        await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": final_status})


async def _claim_and_mark_running(task_id: str) -> bool:
    """调度窗口自检并原子置位 running；已被取消/暂停时跳过执行且不覆盖终态。"""
    if not await asyncio.to_thread(try_claim_executor_entry, task_id):
        await TaskQueueRepository.append_log_async(
            task_id, "[执行器] 任务在调度窗口内已被用户取消/暂停，跳过执行且不覆盖终态"
        )
        return False
    if not await TaskQueueRepository.update_status_async(
        task_id, "running", from_statuses=("queued", "pending", "running")
    ):
        is_managed = bool(active_task_controls.get(task_id, {}).get("scheduler_managed"))
        if is_managed or await asyncio.to_thread(TaskQueueRepository.get_status, task_id) is not None:
            await TaskQueueRepository.append_log_async(
                task_id, "[执行器] 任务在调度窗口内已被用户取消/暂停或删除，跳过执行且不覆盖终态"
            )
            release_runner_controls(task_id)
            return False
    return True


async def run_publish_folder_task(
    task_id: str,
    folder_path: str,
    target_channel: str,
    album_mode: bool,
    delay_seconds: float,
    account_id: int | None = None,
    upload_concurrency: int | None = None,
    upload_in_flight_per_connection: int | None = None,
    force_republish: bool = False,
    oversize_policy: str = "skip_and_report",
    send_placeholder_card: bool | None = None,
):
    """publish_folder 任务执行入口 (策略 execute 唯一调用): 调度窗口自检 → running →
    配置回读 → PublisherService.publish_folder → 终态四分支 (completed/failed、
    paused、cancelled、session_invalid) + task_update 广播 + controls 清理。"""
    if not await _claim_and_mark_running(task_id):
        return
    await TaskQueueRepository.append_log_async(task_id, f"开始发布文件夹 [{folder_path}] 到频道 [{target_channel}]...")

    # 条目协议进度回调 (共享工厂): SQLite 节流 + 速率粘滞; 字节走新鲜 kwarg 回退链
    progress_cb = make_executor_progress_cb(
        task_id,
        submit_fn=submit_db_write,
    )

    current_account_id = account_id
    current_upload_concurrency = upload_concurrency
    current_upload_in_flight = upload_in_flight_per_connection
    current_force_republish = force_republish
    current_oversize_policy = oversize_policy
    # 占位卡开关显式闭环 (None=未指定 → params_json 键优先, 仍缺省时由门面按
    # oversize_policy 派生 —— 先例 pikpak_publisher_service.py:131-132)
    current_send_placeholder_card = send_placeholder_card
    if (
        current_account_id in (None, 0)
        or current_upload_concurrency is None
        or current_upload_in_flight is None
        or current_send_placeholder_card is None
    ):
        try:
            t_rec = await asyncio.to_thread(TaskQueueRepository.get_by_id, task_id)
            if t_rec:
                if current_account_id in (None, 0):
                    db_acc_id = t_rec.get("account_id")
                    current_account_id = db_acc_id if db_acc_id is not None else None
                p_dict = t_rec.get("params") or {}
                if current_upload_concurrency is None:
                    current_upload_concurrency = p_dict.get("upload_concurrency")
                if current_upload_in_flight is None:
                    current_upload_in_flight = p_dict.get("upload_in_flight_per_connection") or p_dict.get(
                        "upload_in_flight"
                    )
                if "force_republish" in p_dict:
                    current_force_republish = bool(p_dict.get("force_republish"))
                if "oversize_policy" in p_dict:
                    current_oversize_policy = p_dict.get("oversize_policy") or current_oversize_policy
                # null 保持 None → 门面按 oversize_policy 派生 (bool(None) 会把
                # "未指定"变成恒 False, skip_and_report 下默认 True 被静默禁用)
                if p_dict.get("send_placeholder_card") is not None:
                    current_send_placeholder_card = bool(p_dict.get("send_placeholder_card"))
        except Exception as exc:
            logger.debug("获取本地发布任务配置参数异常 (非致命): %s", exc)

    try:
        res = await PublisherService.publish_folder(
            folder_path=folder_path,
            target_chat_id=target_channel,
            album_mode=album_mode,
            delay_seconds=delay_seconds,
            progress_callback=progress_cb,
            is_cancelled=lambda: is_task_cancelled(task_id),
            is_paused=lambda: is_task_paused(task_id),
            task_id=task_id,
            account_id=current_account_id,
            upload_concurrency=current_upload_concurrency,
            upload_in_flight_per_connection=current_upload_in_flight,
            force_republish=current_force_republish,
            oversize_policy=current_oversize_policy,
            send_placeholder_card=current_send_placeholder_card,
        )

        await _finalize_publish_result(task_id, res, target_channel, progress_cb, pikpak=False)
        release_runner_controls(task_id)

    except UploadPausedError:
        await _publish_terminal(
            task_id, "paused", "[调度器] 发布任务已安全暂停：已发布进度已入库防重，已完全释放网络并发槽位让位"
        )
        raise
    except UploadCancelledError:
        await _publish_terminal(task_id, "cancelled", "发布任务已被用户手动取消")
        raise
    except Exception as e:
        if is_session_invalid_error(e):
            await report_session_invalid(task_id, e, account_id=current_account_id)
            release_runner_controls(task_id)
            return
        await TaskQueueRepository.append_log_async(task_id, f"❌ 发布任务异常中断: {e}")
        release_runner_controls(task_id)
        raise


async def _expand_pikpak_remote_files(
    task_id: str,
    remote_files: list,
    expand_dir_tree: dict,
    *,
    max_files: int = 5000,
    truncation_out: dict[str, Any] | None = None,
) -> tuple[list, dict]:
    """目录树后台递归展开 (展开产物携带 dir_tree 结构化归档素材, 重试回读保真)
    + 展开产物按 file_id 幂等去重
    + 纯叶子超上限截断 (保持输入顺序)."""
    raw_parsed_items = PikPakPublisherService.items_from_snapshot(remote_files or [])
    if any(item.is_dir for item in raw_parsed_items):
        await TaskQueueRepository.append_log_async(task_id, "🔍 正在后台异步递归展开目录树...")
        raw_parsed_items, new_tree = await PikPakPublisherService.expand_remote_files(
            raw_parsed_items,
            task_id=task_id,
            is_cancelled=lambda: is_task_cancelled(task_id),
            is_paused=lambda: is_task_paused(task_id),
            max_files=max_files,
            truncation_out=truncation_out,
        )
        expand_dir_tree.update(new_tree)
        exp_truncated = bool(truncation_out and truncation_out.get("truncated"))
        exp_limit = int((truncation_out or {}).get("max_files") or (max_files if exp_truncated else 0))
        await asyncio.to_thread(
            TaskQueueRepository.merge_params,
            task_id,
            {
                # 与截断分支 (asdict) 同口径全字段序列化: 手写字段清单曾丢 audio_codec/video_codec,
                # 重试经 items_from_snapshot 还原为 None, 媒体属性描述符与首次执行漂移
                "remote_files": [asdict(i) for i in raw_parsed_items],
                "dir_tree": expand_dir_tree,
                "expansion_truncated": exp_truncated,
                "expansion_limit": exp_limit,
            },
        )

    if raw_parsed_items:
        seen_fids = set()
        deduped_items = []
        for it in raw_parsed_items:
            if it.file_id in seen_fids:
                continue
            seen_fids.add(it.file_id)
            deduped_items.append(it)
        if len(deduped_items) != len(raw_parsed_items):
            await TaskQueueRepository.append_log_async(
                task_id,
                f"⚠️ 展开产物含重复文件 {len(raw_parsed_items) - len(deduped_items)} 个, 已按 file_id 去重",
            )
        raw_parsed_items = deduped_items
        if len(raw_parsed_items) > max_files:
            raw_parsed_items = raw_parsed_items[:max_files]
            await asyncio.to_thread(
                TaskQueueRepository.merge_params,
                task_id,
                {
                    "remote_files": [asdict(item) for item in raw_parsed_items],
                    "expansion_truncated": True,
                    "expansion_limit": max_files,
                },
            )
            if not (truncation_out and truncation_out.get("truncated")):
                await notify_expansion_truncation(
                    task_id=task_id,
                    max_files=max_files,
                    kept_files=len(raw_parsed_items),
                    truncation_out=truncation_out,
                )
    return raw_parsed_items, expand_dir_tree


async def _resolve_pikpak_run_config(
    task_id: str,
    *,
    account_id,
    upload_concurrency,
    upload_in_flight_per_connection,
    prefetch_concurrency,
    auto_cleanup_mode,
    oversize_policy,
    split_strategy,
    send_placeholder_card,
    force_republish,
    is_folder_publish,
    folder_name,
    album_mode,
    dir_tree,
) -> dict:
    """PikPak 运行配置回读: 入参缺省键从任务行 params_json 回填。

    返回统一运行配置 dict; expand_dir_tree 含回读覆盖 (重试回读保真)。"""
    cfg = {
        "account_id": account_id,
        "upload_concurrency": upload_concurrency,
        "upload_in_flight_per_connection": upload_in_flight_per_connection,
        "prefetch_concurrency": prefetch_concurrency,
        "auto_cleanup_mode": auto_cleanup_mode,
        "oversize_policy": oversize_policy,
        "split_strategy": split_strategy,
        "send_placeholder_card": send_placeholder_card,
        "force_republish": force_republish,
        "is_folder_publish": is_folder_publish,
        "folder_name": folder_name,
        "album_mode": album_mode,
        "dir_tree": dict(dir_tree or {}),
        "expansion_truncated": False,
        "expansion_limit": 0,
    }
    try:
        t_rec = await asyncio.to_thread(TaskQueueRepository.get_by_id, task_id)
        if t_rec:
            if cfg["account_id"] in (None, 0):
                db_acc_id = t_rec.get("account_id")
                cfg["account_id"] = db_acc_id if db_acc_id is not None else None
            p_dict = t_rec.get("params") or {}
            if cfg["upload_concurrency"] is None:
                cfg["upload_concurrency"] = p_dict.get("upload_concurrency")
            if cfg["upload_in_flight_per_connection"] is None:
                cfg["upload_in_flight_per_connection"] = p_dict.get("upload_in_flight_per_connection") or p_dict.get(
                    "upload_in_flight"
                )
            if cfg["prefetch_concurrency"] is None:
                cfg["prefetch_concurrency"] = p_dict.get("pikpak_prefetch_concurrency") or p_dict.get(
                    "prefetch_concurrency"
                )
            if "auto_cleanup_mode" in p_dict:
                cfg["auto_cleanup_mode"] = p_dict.get("auto_cleanup_mode") or cfg["auto_cleanup_mode"]
            if "oversize_policy" in p_dict:
                cfg["oversize_policy"] = p_dict.get("oversize_policy") or cfg["oversize_policy"]
            if "split_strategy" in p_dict:
                cfg["split_strategy"] = p_dict.get("split_strategy") or cfg["split_strategy"]
            # null 保持 None → 门面按 oversize_policy 派生 (同本地路径口径)
            if p_dict.get("send_placeholder_card") is not None:
                cfg["send_placeholder_card"] = bool(p_dict.get("send_placeholder_card"))
            if "force_republish" in p_dict:
                cfg["force_republish"] = bool(p_dict.get("force_republish"))
            if "is_folder_publish" in p_dict:
                cfg["is_folder_publish"] = bool(p_dict.get("is_folder_publish"))
            if "folder_name" in p_dict:
                cfg["folder_name"] = p_dict.get("folder_name")
            if "dir_tree" in p_dict:
                cfg["dir_tree"] = dict(p_dict.get("dir_tree") or {})
            if "album_mode" in p_dict:
                cfg["album_mode"] = bool(p_dict.get("album_mode"))
            if "expansion_truncated" in p_dict:
                cfg["expansion_truncated"] = bool(p_dict.get("expansion_truncated"))
            if "expansion_limit" in p_dict:
                cfg["expansion_limit"] = int(p_dict.get("expansion_limit") or 0)
    except Exception as exc:
        logger.debug("获取PikPak流式发布任务配置参数异常 (非致命): %s", exc)
    return cfg


async def _fail_empty_pikpak_publish(task_id: str) -> None:
    """空任务护栏 (tristate 配套 UX 改造): 展开后 0 个可发布项 → fail-fast failed 终态,
    杜绝静默「成功 0/0」 (旧路径: publisher 返回 PublishFolderResult(0,0,[]) 被判 completed)。"""
    if await TaskQueueRepository.update_status_async(task_id, "failed", from_statuses=("running",)):
        await TaskQueueRepository.append_log_async(
            task_id, "❌ 未选择任何可发布的远端文件 (空目录或全部被过滤), 任务终止"
        )
        await connection_manager.broadcast({"type": "task_update", "task_id": task_id, "status": "failed"})
    release_runner_controls(task_id)


async def run_publish_pikpak_task(
    task_id: str,
    target_channel: str,
    remote_files: list,
    auto_cleanup_mode: str = "archive",
    send_delay_seconds: float = 3.0,
    account_id: int | None = None,
    upload_concurrency: int | None = None,
    oversize_policy: str = "split",
    # 签名默认 False→None (消除"签名默认 False + 回读 skip_and_report 置 True"怪形态);
    # params_json 键优先, 仍缺省时由门面按 oversize_policy 派生 (pikpak_publisher_service.py:131-132)
    send_placeholder_card: bool | None = None,
    force_republish: bool = False,
    is_folder_publish: bool = False,
    folder_name: str | None = None,
    dir_tree: dict[str, str] | None = None,
    album_mode: bool = False,
    prefetch_concurrency: int | None = None,
    pikpak_prefetch_concurrency: int | None = None,
    split_strategy: str | None = None,
    upload_in_flight_per_connection: int | None = None,
):
    """pikpak_publish 任务执行入口 (策略 execute 唯一调用): 调度窗口自检 → running →
    配置回读 → 目录树展开/去重/空护栏 → PikPakPublisherService.publish_pikpak_files →
    终态四分支 + task_update 广播 + controls 清理。"""
    if not await _claim_and_mark_running(task_id):
        return

    cfg = await _resolve_pikpak_run_config(
        task_id,
        account_id=account_id,
        upload_concurrency=upload_concurrency,
        upload_in_flight_per_connection=upload_in_flight_per_connection,
        prefetch_concurrency=prefetch_concurrency or pikpak_prefetch_concurrency,
        auto_cleanup_mode=auto_cleanup_mode,
        oversize_policy=oversize_policy,
        split_strategy=split_strategy,
        send_placeholder_card=send_placeholder_card,
        force_republish=force_republish,
        is_folder_publish=is_folder_publish,
        folder_name=folder_name,
        album_mode=album_mode,
        dir_tree=dir_tree,
    )
    expand_dir_tree = cfg["dir_tree"]

    # 开始日志置于参数回读之后 (修复: 原先打印函数默认参数, 与实际执行参数不一致误导排查)
    await TaskQueueRepository.append_log_async(
        task_id,
        f"开始 PikPak 流式发布: {len(remote_files or [])} 个远端项 → [{target_channel}] "
        f"(auto_cleanup={cfg['auto_cleanup_mode']}, oversize_policy={cfg['oversize_policy']})...",
    )

    # 展开及之后的全部准备/发布均在 try 内: 任务已标 running, expansion
    # 抛出的暂停/取消/CLI 异常若在本层逃逸, 会跳过 error_detail 落库、会话失效上报与
    # 中文终态日志, active_task_controls 条目也无人清理 (scheduler 仅兜底终态, 不管控制项)
    try:
        # 1. 后台异步递归展开目录树 + 幂等去重
        truncation_info: dict[str, Any] = (
            {"truncated": True, "max_files": int(cfg.get("expansion_limit") or 5000)}
            if cfg.get("expansion_truncated")
            else {}
        )
        raw_parsed_items, expand_dir_tree = await _expand_pikpak_remote_files(
            task_id,
            remote_files or [],
            expand_dir_tree,
            truncation_out=truncation_info,
        )

        # 空任务护栏: 展开后 0 个可发布项 → fail-fast
        if not raw_parsed_items:
            await _fail_empty_pikpak_publish(task_id)
            return

        initial_total_bytes = sum(int(f.size_bytes or 0) for f in raw_parsed_items)
        if initial_total_bytes > 0:
            initial_total_items = len(raw_parsed_items)
            first_item_name = raw_parsed_items[0].name or raw_parsed_items[0].file_id
            await TaskQueueRepository.update_progress_async(
                task_id,
                0,
                initial_total_bytes,
                0,
                initial_total_items,
                0.0,
                0.0,
                f"[0/{initial_total_items}] 正在准备: {first_item_name}",
                0,
                0,
                concurrency_total=cfg.get("upload_concurrency") or None,
            )

        # 包3 (J8): 双速率粘滞 + 字节粘滞 —— speed (TG 上传) 与 pikpak_download_speed_mb_s
        # (PikPak 拉流) 各自沿用最后已知非零值; 字节粘滞初值 = 预解析体积和。
        # WS 载荷键集合与 redact_payload 调用零改动 (共享工厂 download_speed_key/redact 开关)。
        progress_cb = make_executor_progress_cb(
            task_id,
            submit_fn=submit_db_write,
            sticky_download_speed=True,
            sticky_bytes=True,
            initial_total_bytes=initial_total_bytes,
            download_speed_key=True,
            redact=True,
        )

        res = await PikPakPublisherService.publish_pikpak_files(
            remote_files=raw_parsed_items,
            target_chat_id=target_channel,
            task_id=task_id,
            progress_callback=progress_cb,
            is_cancelled=lambda: is_task_cancelled(task_id),
            is_paused=lambda: is_task_paused(task_id),
            send_delay_seconds=send_delay_seconds,
            expansion_truncated=bool(truncation_info.get("truncated")),
            expansion_limit=int(truncation_info.get("max_files") or 0),
            **{
                k: cfg[k]
                for k in (
                    "account_id",
                    "auto_cleanup_mode",
                    "upload_concurrency",
                    "upload_in_flight_per_connection",
                    "prefetch_concurrency",
                    "oversize_policy",
                    "send_placeholder_card",
                    "force_republish",
                    "dir_tree",
                    "is_folder_publish",
                    "folder_name",
                    "album_mode",
                    "split_strategy",
                )
            },
        )

        await _finalize_publish_result(task_id, res, target_channel, progress_cb, pikpak=True)
        release_runner_controls(task_id)

    except UploadPausedError:
        await _publish_terminal(
            task_id, "paused", "[调度器] PikPak 发布任务已安全暂停：已发布进度已入库防重，已完全释放网络并发槽位让位"
        )
        raise
    except UploadCancelledError:
        await _publish_terminal(task_id, "cancelled", "PikPak 发布任务已被用户手动取消")
        raise
    except Exception as e:
        # PikPak 域异常 (CLI 域 PikPakAuthError 携带 UNAUTHORIZED 码文本 / provider 域 401 文本)
        # 会命中 is_session_invalid_error 的 "unauthorized" 文本兜底特征, 若不放行守卫会把
        # PikPak 凭据失败误报为 Telegram 会话失效 → 误伤无关 userbot 账号 (,
        # 文本误伤同类教训); 两域异常一律走既有 PikPak 失败处理
        if not isinstance(e, (PikPakError, ChunkProviderError)) and is_session_invalid_error(e):
            await report_session_invalid(task_id, e, account_id=cfg.get("account_id"))
            release_runner_controls(task_id)
            return
        # 失败分类 → UI 可读文案 即中文文案; Fatal 路径调度器不写
        # error_detail, 由本层兜底落库; Retryable 路径调度器 mark_retry 会以同文案覆写)
        category = getattr(e, "category", "")
        detail = getattr(e, "user_detail", None) or str(e)
        if category == "throttled":
            await TaskQueueRepository.append_log_async(task_id, f"⏳ 触发 CDN 频控/过载保护: {detail}")
        elif category == "risk_control":
            await TaskQueueRepository.append_log_async(task_id, f"🟠 触发风控安全保护: {detail}")
        else:
            await TaskQueueRepository.append_log_async(task_id, f"❌ PikPak 发布失败: {detail}")
        try:
            await asyncio.to_thread(TaskQueueRepository.set_error_detail, task_id, detail)
        except Exception as set_err:
            logger.debug("记录任务错误详情失败 (非致命): %s", set_err)
        release_runner_controls(task_id)
        raise
