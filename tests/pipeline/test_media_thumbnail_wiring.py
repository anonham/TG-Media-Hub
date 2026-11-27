"""Phase 2 封面管线集成测试 (纯离线, mock 服务与核心边界).

覆盖:
1. pikpak_service.get_thumbnail_link 单次有界 CLI 调用、无鉴权恢复/重试、合法 URL 校验;
2. PikPakPublisherService 装配注入 thumbnail_link_provider;
3. PikPak edit_item 上传优先复用 CDN 直链、提供独立命名 BytesIO、以及回填失败→raw 单发单次选图复用;
4. LocalDiskTransportEngine 本地视频 Userbot/Bot 单发与 edit_item 注入 thumb=、回填降级复用、非视频跳过。
"""

import io
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.pikpak.taxonomy import PikPakAuthError
from app.core.pipeline.engines import LocalDiskTransportEngine, PikPakStreamTransportEngine
from app.core.pipeline.engines.exceptions import TransportError
from app.core.pipeline.models import PublishContext, PublishItem
from app.services import pikpak_service

# ---------------------------------------------------------------------------
# 1. pikpak_service.get_thumbnail_link 服务边界
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_thumbnail_link_single_cli_call_and_validation() -> None:
    """单次 _run_cli(['get', file_id, '--raw'], timeout_s=...), 仅返回顶层合法 http(s) 字符串."""
    with patch.object(
        pikpak_service,
        "_run_cli",
        new_callable=AsyncMock,
        return_value={"id": "F1", "thumbnail_link": "  https://thumb.test/f1.jpg  "},
    ) as mock_cli:
        url = await pikpak_service.get_thumbnail_link("F1")

    assert url == "https://thumb.test/f1.jpg"
    mock_cli.assert_awaited_once_with(["get", "F1", "--raw"], timeout_s=pikpak_service.THUMBNAIL_LOOKUP_TIMEOUT_S)

    for bad_payload in (
        {"thumbnail_link": "ftp://bad.test/a.jpg"},
        {"thumbnail_link": ""},
        {"thumbnail_link": 123},
        {"nested": {"thumbnail_link": "https://thumb.test/nested.jpg"}},
        ["not-a-dict"],
    ):
        with patch.object(pikpak_service, "_run_cli", new_callable=AsyncMock, return_value=bad_payload):
            assert await pikpak_service.get_thumbnail_link("F1") is None

    with patch.object(pikpak_service, "_run_cli", new_callable=AsyncMock) as mock_cli_empty:
        assert await pikpak_service.get_thumbnail_link("   ") is None
        mock_cli_empty.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_thumbnail_link_no_auth_recovery_or_retry() -> None:
    """get_thumbnail_link 遇 PikPakAuthError 不走 _with_auth_recovery 也不重试."""
    with (
        patch.object(
            pikpak_service,
            "_run_cli",
            new_callable=AsyncMock,
            side_effect=PikPakAuthError("unauth"),
        ) as mock_cli,
        patch.object(pikpak_service, "_with_auth_recovery", new_callable=AsyncMock) as mock_recovery,
        pytest.raises(PikPakAuthError),
    ):
        await pikpak_service.get_thumbnail_link("F_AUTH")

    mock_cli.assert_awaited_once()
    mock_recovery.assert_not_awaited()


# ---------------------------------------------------------------------------
# 2. PikPak edit_item 与 backfill -> raw 单次选图复用
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pikpak_edit_item_passes_fresh_thumb_and_reuses_uploaded_url_and_memoizes_on_fallback(cover_jpeg) -> None:
    """PikPak edit_item 上传后复用当前 CDN URL 生成封面并传 thumb=, 失败降级 raw 单发时复用同一封面且各用独立 BytesIO."""
    link_calls: list[list[str]] = []

    async def fake_link_provider(ids: list[str]) -> list[str]:
        link_calls.append(list(ids))
        return [f"https://cdn.test/{ids[0]}?n={len(link_calls)}"]

    async def fake_thumb_link_provider(fid: str) -> str | None:
        return f"https://thumb.test/{fid}.jpg"

    captured_kwargs: list[dict] = []
    captured_thumb_urls: list[str] = []
    select_count = 0

    def fake_make_thumb_provider(parsed_item, url_provider=None, **kwargs):
        captured_kwargs.append(kwargs)

        async def _provide() -> bytes | None:
            nonlocal select_count
            select_count += 1
            if url_provider is not None:
                res = url_provider()
                captured_thumb_urls.append(await res)
            return cover_jpeg

        return _provide

    client = AsyncMock()
    client.edit_message = AsyncMock(side_effect=RuntimeError("edit window expired"))
    client.send_file = AsyncMock(return_value=MagicMock(id=888))

    engine = PikPakStreamTransportEngine(
        link_provider=fake_link_provider,
        thumbnail_link_provider=fake_thumb_link_provider,
    )
    ctx = PublishContext(
        task_id="t_pp_edit",
        target_chat_id=-100123,
        userbot_client=client,
        source_type="pikpak",
        video_split_threshold_bytes=10_000,
    )
    item = PublishItem(
        item_id="fid_v1",
        name="clip.mp4",
        size_bytes=500,
        duration_s=15.0,
        width=1280,
        height=720,
        source_type="pikpak",
        thumbnail_data=b"raw-bytes-input",
    )

    async def fake_upload_parallel(_client, _path, **kwargs):
        chunk_provider = kwargs["chunk_provider"]
        # 模拟上传期间 chunk_provider 已拿到当前 CDN URL
        inner = getattr(chunk_provider, "_inner", chunk_provider)
        inner._current_url = "https://cdn.test/from-upload"
        return MagicMock(name="uploaded_handle")

    with (
        patch("app.core.pipeline.engines.pikpak.engine.make_thumb_provider", side_effect=fake_make_thumb_provider),
        patch("app.core.fast_telethon.upload_file_parallel", side_effect=fake_upload_parallel),
    ):
        with pytest.raises(TransportError):
            await engine.edit_item(item, ctx, msg_id=42)

        # 回填失败后常规 raw 单发紧随其后
        outcome = await engine.send_item(item, ctx)

    assert outcome.message_ids == [888]
    # 仅执行过一次封面选择 (回填结果已记忆, raw 单发复用)
    assert select_count == 1
    # 上传已设 _current_url → 缩略图直接复用, 零额外 get_download_links 调用
    assert captured_thumb_urls == ["https://cdn.test/from-upload"]
    assert link_calls == []
    # provided_bytes 与 thumbnail_link_provider 均透传给 make_thumb_provider (无旧 bytes 旁路)
    assert captured_kwargs[0]["provided_bytes"] == b"raw-bytes-input"
    assert captured_kwargs[0]["thumbnail_link_provider"] is fake_thumb_link_provider

    edit_thumb = client.edit_message.await_args.kwargs["thumb"]
    send_thumb = client.send_file.await_args.kwargs["thumb"]
    assert isinstance(edit_thumb, io.BytesIO)
    assert isinstance(send_thumb, io.BytesIO)
    assert edit_thumb is not send_thumb
    assert edit_thumb.name == "thumbnail.jpg"
    assert send_thumb.name == "thumbnail.jpg"
    assert edit_thumb.getvalue() == cover_jpeg
    assert send_thumb.getvalue() == cover_jpeg


@pytest.mark.asyncio
async def test_pikpak_non_video_audio_skips_thumb_provider() -> None:
    """带 duration_s 的音频条目不进入视频封面管线."""
    engine = PikPakStreamTransportEngine(link_provider=AsyncMock(return_value=["https://cdn.test/a.mp3"]))
    item = PublishItem(
        item_id="fid_a1",
        name="song.mp3",
        size_bytes=500,
        duration_s=180.0,
        source_type="pikpak",
    )
    media_url = AsyncMock()
    provider = engine._make_thumb_provider(item, url_provider=media_url)
    assert await provider() is None
    media_url.assert_not_called()


# ---------------------------------------------------------------------------
# 3. LocalDiskTransportEngine 本地视频 Userbot / Bot 单发与 edit_item
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_video_edit_fallback_to_userbot_send_reuses_thumb_with_fresh_streams(tmp_path, cover_jpeg) -> None:
    """本地视频 edit_item 与降级 Userbot send_item 共用单次抽帧结果, 各自提供新鲜 BytesIO 并保留 MIME."""
    vid = tmp_path / "sample.m4v"
    vid.write_bytes(b"fake-video-data")

    userbot = AsyncMock()
    userbot.edit_message = AsyncMock(side_effect=RuntimeError("edit failed"))
    userbot.send_file = AsyncMock(return_value=MagicMock(id=901))

    registry = MagicMock()
    registry.userbot = userbot
    registry.bots = []

    engine = LocalDiskTransportEngine(registry=registry)
    ctx = PublishContext(
        task_id="t_loc_edit",
        target_chat_id=-100456,
        userbot_client=userbot,
        source_type="local",
    )
    item = PublishItem(
        item_id=str(vid),
        name="sample.m4v",
        size_bytes=vid.stat().st_size,
        duration_s=12.0,
        width=640,
        height=360,
        source_type="local",
    )

    with (
        patch(
            "app.core.pipeline.engines.local_disk.get_local_thumbnail", new_callable=AsyncMock, return_value=cover_jpeg
        ) as mock_thumb,
        patch("app.core.fast_telethon.upload_file_parallel", new_callable=AsyncMock, return_value=MagicMock()),
        patch.object(engine, "_ensure_local_video_metadata", new_callable=AsyncMock),
    ):
        with pytest.raises(TransportError):
            await engine.edit_item(item, ctx, msg_id=77)
        outcome = await engine.send_item(item, ctx)

    assert outcome.message_ids == [901]
    mock_thumb.assert_awaited_once_with(str(vid), 12.0)

    edit_thumb = userbot.edit_message.await_args.kwargs["thumb"]
    send_kwargs = userbot.send_file.await_args.kwargs
    send_thumb = send_kwargs["thumb"]
    assert isinstance(edit_thumb, io.BytesIO)
    assert isinstance(send_thumb, io.BytesIO)
    assert edit_thumb is not send_thumb
    assert edit_thumb.getvalue() == cover_jpeg
    assert send_thumb.getvalue() == cover_jpeg
    assert send_kwargs["mime_type"] == "video/mp4"
    assert send_kwargs["supports_streaming"] is True


@pytest.mark.asyncio
async def test_local_video_bot_send_passes_thumb_and_document_skips_thumb(tmp_path, cover_jpeg) -> None:
    """本地视频走 Bot Pool 单发携带 thumb=, 文档文件跳过封面生成."""
    vid = tmp_path / "clip.ts"
    vid.write_bytes(b"ts-bytes")
    doc = tmp_path / "notes.pdf"
    doc.write_bytes(b"pdf-bytes")

    async def send_with_progress(_chat, _file, **kwargs):
        kwargs["progress_callback"](4, 8)
        return MagicMock(id=501)

    bot = AsyncMock()
    bot.send_file = AsyncMock(side_effect=send_with_progress)

    registry = MagicMock()
    registry.userbot = MagicMock(name="other_userbot")
    registry.bots = [bot]
    registry.bot_index = 1
    registry.get_next_available_bot.return_value = bot

    engine = LocalDiskTransportEngine(registry=registry)
    progress_callback = MagicMock()
    ctx = PublishContext(
        task_id="t_loc_bot",
        target_chat_id=-100789,
        bot_routing_threshold_bytes=10 * 1024 * 1024,
        source_type="local",
        progress_callback=progress_callback,
    )

    vid_item = PublishItem(item_id=str(vid), name="clip.ts", size_bytes=8, duration_s=9.0, source_type="local")
    doc_item = PublishItem(item_id=str(doc), name="notes.pdf", size_bytes=9, source_type="local")

    with (
        patch(
            "app.core.pipeline.engines.local_disk.get_local_thumbnail", new_callable=AsyncMock, return_value=cover_jpeg
        ) as mock_thumb,
        patch.object(engine, "_ensure_local_video_metadata", new_callable=AsyncMock),
    ):
        await engine.send_item(vid_item, ctx)
        vid_kwargs = bot.send_file.await_args.kwargs

        await engine.send_item(doc_item, ctx)
        doc_kwargs = bot.send_file.await_args.kwargs

    mock_thumb.assert_awaited_once_with(str(vid), 9.0)
    assert isinstance(vid_kwargs["thumb"], io.BytesIO)
    assert vid_kwargs["thumb"].name == "thumbnail.jpg"
    assert vid_kwargs["mime_type"] == "video/mp2t"
    assert vid_kwargs["supports_streaming"] is True
    assert "thumb" not in doc_kwargs
    assert doc_kwargs["supports_streaming"] is False
    assert progress_callback.call_count == 2
