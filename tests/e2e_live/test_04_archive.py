"""PikPak 结构化归档真网盘断言 (E2E-LIVE-19; 承 E2E-PUB-04)。

前置 (F10 缺失即 skip): PikPak logged_in + `E2E_LIVE_PIKPAK_REMOTE_DIR` 测试目录
内预置 `剧集A/第一季/` 且含任一低于注入阈值 (E2E_LIVE_MAX_FILE_SIZE_MB, 默认 64 MiB;
提速推荐设为 8 MiB) 的媒体文件 (制备命令见 tests/e2e_live/README.md)。

被测语义 (代码实证 ): is_folder_publish 提交目录快照 → 服务端 expander
递归展开并自建 dir_tree ({目录id: 相对路径}) → 发布成功后 auto_cleanup_mode=archive
将文件移入网盘根级 [已发布归档] 并按 dir_tree 重建层级
([已发布归档]/剧集A/第一季/<文件>) → 空源目录自底向上删除。

副作用说明: [已发布归档] 位于网盘根 (产品标准行为), 测试样本将留在其中 (小体积);
发布防重 (chat+file_id+size) 意味着同名同体积文件重跑会被跳过 —— 重跑须更换文件名
或清空归档。harness 仅做只读断言, 不移动/删除网盘文件。
"""

import os

import pytest

from app.config import settings as app_settings
from e2e_live.harness import db_readonly, env_gate, pikpak_probe
from e2e_live.harness import tg_reader as tg_reader_mod
from e2e_live.harness.tg_reader import is_video_message

pytestmark = [pytest.mark.e2e_live]

_ARCHIVE_ROOT_NAME = "已发布归档"  # app/services/pikpak_service.py ARCHIVE_FOLDER_NAME


@pytest.fixture(scope="module")
def archive_stage(live_gate, live_api) -> dict:
    """归档用例前置: logged_in → 测试目录 → 剧集A/第一季/<小样本> 逐级解析。"""
    state = pikpak_probe.auth_state(live_api)
    if state != "logged_in":
        pytest.skip(f"PikPak 未就绪 (state={state}); 结构化归档真链路需要真实网盘")
    remote_dir = os.environ.get("E2E_LIVE_PIKPAK_REMOTE_DIR", "").strip()
    if not remote_dir:
        pytest.skip("未配置 E2E_LIVE_PIKPAK_REMOTE_DIR; 归档前置见 tests/e2e_live/README.md")
    test_dir_id = pikpak_probe.resolve_remote_folder(live_api, remote_dir)
    if test_dir_id is None:
        pytest.skip(f"PikPak 远端测试目录不存在: {remote_dir}")

    def _find_dir(folder_id: str, name: str) -> dict | None:
        return next(
            (
                i
                for i in pikpak_probe.list_remote_page(live_api, folder_id)
                if i.get("is_dir") and i.get("name") == name
            ),
            None,
        )

    drama = _find_dir(test_dir_id, "剧集A")
    if drama is None:
        pytest.skip("测试目录缺 剧集A/ (归档层级锚点); 预置方式见 README")
    season = _find_dir(drama["file_id"], "第一季")
    if season is None:
        pytest.skip("剧集A 缺 第一季/ 子目录 (归档层级锚点); 预置方式见 README")
    season_items = pikpak_probe.list_remote_page(live_api, season["file_id"])
    all_season_file_ids = {str(i["file_id"]) for i in season_items if not i.get("is_dir") and i.get("file_id")}
    video_exts = {ext.lower() for ext in app_settings.VIDEO_EXTENSIONS}
    leaves = [
        i
        for i in season_items
        if not i.get("is_dir")
        and os.path.splitext(str(i.get("name") or "").lower())[1] in video_exts
        and int(i.get("size_bytes") or 0) < env_gate.max_file_size_mb() * 1024 * 1024
    ]
    if not leaves:
        pytest.skip(
            f"第一季/ 无 <{env_gate.max_file_size_mb()} MiB 视频文件 (低阈值注入下超限或非视频会被跳过); 预置方式见 README"
        )
    leaf = sorted(leaves, key=lambda i: i.get("name") or "")[0]
    unsubmitted_file_ids = all_season_file_ids - {str(leaf["file_id"])}
    return {
        "test_dir_id": test_dir_id,
        "drama": drama,
        "season": season,
        "leaf": leaf,
        "unsubmitted_file_ids": unsubmitted_file_ids,
        "remote_dir": remote_dir,
    }


async def test_structured_archive_dir_tree_and_source_prune(
    archive_stage, live_api, tg_reader, target_chat, live_cleanup, live_db
) -> None:
    """E2E-LIVE-19 (承 E2E-PUB-04): 结构化归档保留层级 + 源目录清空。

    断言: 任务 completed 且 published=1 → publish_history 记 pikpak 逻辑路径 →
    网盘 [已发布归档]/剧集A/第一季/<文件> 层级重建 → 源 剧集A 子树无残留文件 →
    频道产物到达。
    """
    drama, season, leaf = archive_stage["drama"], archive_stage["season"], archive_stage["leaf"]
    leaf_name = leaf.get("name") or ""
    season_id = season["file_id"]

    try:
        chat = await tg_reader.resolve_chat(target_chat)
    except tg_reader_mod.ChatUnresolvableError as exc:
        pytest.skip(f"无 userbot 会话可解析测试频道 {target_chat}: {exc}")
    mark = await tg_reader.max_message_id(chat)
    live_cleanup.register_watermark(target_chat, mark)

    resp = live_api.post(
        "/api/pikpak/publish",
        json={
            # 叶子文件快照契约 (models/pikpak.py _reject_directory_items): 目录条目已废除,
            # 422 拒绝; 目录层级改由 dir_tree 显式提供 ({folder_id: 相对路径})
            "remote_files": [
                {
                    "file_id": leaf["file_id"],
                    "name": leaf_name,
                    "is_dir": False,
                    "size_bytes": leaf.get("size_bytes") or 0,
                    "parent_id": season_id,
                }
            ],
            "target_channel": target_chat,
            "auto_cleanup_mode": "archive",
            "oversize_policy": "skip_and_report",
            "is_folder_publish": True,
            "folder_name": drama.get("name") or "剧集A",
            "dir_tree": {
                drama["file_id"]: drama.get("name") or "剧集A",
                season_id: f"{drama.get('name') or '剧集A'}/第一季",
            },
            "send_delay_seconds": 0.5,
        },
    )
    assert resp.status_code == 200, resp.text
    task_id = resp.json()["task_id"]
    live_cleanup.register_task(task_id)

    final = live_api.wait_terminal(task_id, timeout=1200)
    pikpak_probe.completed_or_env_skip(live_api, final)
    assert final["progress"]["processed_items"] == 1, final["progress"]

    hist = db_readonly.fetch_one(
        live_db,
        "SELECT tg_message_id FROM publish_history WHERE file_path = ? AND source_type = 'pikpak'",
        (f"pikpak://{season_id}/{leaf_name}",),
    )
    assert hist is not None and int(hist["tg_message_id"] or 0) > 0, (
        f"publish_history 应记 pikpak 逻辑路径 pikpak://{season_id}/{leaf_name}"
    )

    msgs = await tg_reader.collect_messages_after(chat, mark)
    videos = [m for m in msgs if is_video_message(m)]
    assert len(videos) == 1 and leaf_name in (videos[0].text or ""), (
        f"频道应新增 1 条 {leaf_name} 视频消息, 实得 {[(m.text or '')[:40] for m in msgs]}"
    )
    for msg in msgs:
        live_cleanup.register_message(target_chat, int(msg.id))

    # ---- 网盘只读断言: 归档层级重建 + 源目录清空 ----
    root_items = pikpak_probe.list_remote_page(live_api, "")
    archive_root = next((i for i in root_items if i.get("is_dir") and i.get("name") == _ARCHIVE_ROOT_NAME), None)
    assert archive_root is not None, f"网盘根应存在 [{_ARCHIVE_ROOT_NAME}] 目录 (产品标准归档落点)"
    archived_drama = next(
        (
            i
            for i in pikpak_probe.list_remote_page(live_api, archive_root["file_id"])
            if i.get("is_dir") and i.get("name") == (drama.get("name") or "剧集A")
        ),
        None,
    )
    assert archived_drama is not None, "归档内应重建 剧集A/ 层级"
    archived_season = next(
        (
            i
            for i in pikpak_probe.list_remote_page(live_api, archived_drama["file_id"])
            if i.get("is_dir") and i.get("name") == "第一季"
        ),
        None,
    )
    assert archived_season is not None, "归档内应重建 剧集A/第一季/ 层级 (dir_tree 结构化归档)"
    archived_names = [i.get("name") for i in pikpak_probe.list_remote_page(live_api, archived_season["file_id"])]
    assert leaf_name in archived_names, f"归档第一季/ 应含 {leaf_name}: {archived_names}"

    unsubmitted_ids: set[str] = archive_stage["unsubmitted_file_ids"]
    leaf_id = str(leaf["file_id"])
    source_drama_items = pikpak_probe.list_remote_page(live_api, drama["file_id"])
    source_season = next((i for i in source_drama_items if i.get("is_dir") and i.get("name") == "第一季"), None)
    if source_season is None:
        assert not unsubmitted_ids, f"源 第一季/ 尚有未提交文件 {unsubmitted_ids}, 不应被整目录删除"
        return  # D2 空目录自底向上删除已生效 (仅单文件时 第一季 整目录移除)
    residual_files = [
        i for i in pikpak_probe.list_remote_page(live_api, source_season["file_id"]) if not i.get("is_dir")
    ]
    residual_ids = {str(i.get("file_id")) for i in residual_files}
    residual_names = {str(i.get("name")) for i in residual_files}
    assert leaf_id not in residual_ids and leaf_name not in residual_names, (
        f"已提交叶子 {leaf_name} ({leaf_id}) 应已从源 第一季/ 移出: {[i.get('name') for i in residual_files]}"
    )
    assert unsubmitted_ids.issubset(residual_ids), (
        f"未提交文件应保留在源 第一季/ 中: missing={unsubmitted_ids - residual_ids}"
    )
