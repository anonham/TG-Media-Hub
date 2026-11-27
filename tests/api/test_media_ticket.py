"""API Layer: 短时效媒体 ticket 契约测试

覆盖范围:
- POST /api/auth/media-ticket 签发: 双凭证鉴权 (缺 Key 401 / 缺 2FA 403) + 成功签发
- TTL 语义: 有效期内可重复消费 (兼容 <video> 同 URL 206 Range 分段请求), 过期即失效 (惰性剔除)
- 签发并发安全: 惰性清扫不误伤活票
"""

import time

from fastapi.testclient import TestClient

from app.api.auth import (
    MEDIA_TICKET_TTL_SECONDS,
    _media_tickets,
    consume_media_ticket,
    issue_2fa_token,
    issue_media_ticket,
)
from app.config import settings
from app.main import app

client = TestClient(app, client=("127.0.0.1", 50000))


def _dual_auth_headers(api_key: str) -> dict[str, str]:
    return {"X-API-Key": api_key, "X-2FA-Token": issue_2fa_token()}


def test_issue_media_ticket_requires_api_key(monkeypatch):
    """缺 API Key -> 401 (媒体 ticket 签发挂双层鉴权)"""
    monkeypatch.setattr(settings, "API_KEY", "ticket_issue_secret_0123456789abcdef")
    resp = client.post("/api/auth/media-ticket")
    assert resp.status_code == 401


def test_issue_media_ticket_requires_2fa_session(monkeypatch):
    """有 Key 但缺/错 2FA 会话 -> 403 (detail 以 2FA 开头, 前端据此引导重验)"""
    monkeypatch.setattr(settings, "API_KEY", "ticket_issue_secret_0123456789abcdef")
    resp = client.post("/api/auth/media-ticket", headers={"X-API-Key": "ticket_issue_secret_0123456789abcdef"})
    assert resp.status_code == 403
    assert resp.json()["detail"].startswith("2FA")


def test_issue_media_ticket_success(monkeypatch):
    """双凭证齐备 -> 200: 返回非空 ticket 与规范 TTL"""
    monkeypatch.setattr(settings, "API_KEY", "ticket_issue_secret_0123456789abcdef")
    resp = client.post(
        "/api/auth/media-ticket",
        headers=_dual_auth_headers("ticket_issue_secret_0123456789abcdef"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ticket"]
    assert body["expires_in"] == MEDIA_TICKET_TTL_SECONDS
    assert consume_media_ticket(body["ticket"]) is True


def test_ticket_repeatable_within_ttl():
    """核心设计约束: ticket 为 TTL 内可重复消费 —— <video> 以同一 URL 发起多次
    206 Range 分段请求, 一次性消费会直接破坏播放与进度条拖动, 严禁做成单次有效"""
    ticket = issue_media_ticket()
    assert consume_media_ticket(ticket) is True
    assert consume_media_ticket(ticket) is True
    assert consume_media_ticket(ticket) is True
    assert ticket in _media_tickets


def test_ticket_expired_is_rejected_and_pruned():
    """过期 ticket 立即失效并遭惰性剔除"""
    ticket = issue_media_ticket()
    _media_tickets[ticket] = time.time() - 1
    assert consume_media_ticket(ticket) is False
    assert ticket not in _media_tickets


def test_issue_prunes_expired_tickets_without_touching_live_ones():
    """签发时的惰性清扫只移除过期票据, 不误伤活票"""
    live = issue_media_ticket()
    expired = issue_media_ticket()
    _media_tickets[expired] = time.time() - 1
    stale = "stale_ticket_no_longer_registered"
    _media_tickets[stale] = time.time() - 5

    fresh = issue_media_ticket()

    assert expired not in _media_tickets
    assert stale not in _media_tickets
    assert consume_media_ticket(live) is True
    assert consume_media_ticket(fresh) is True
