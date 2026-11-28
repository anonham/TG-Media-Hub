"""API Layer: 系统设置中心、独立 Tab 保存隔离、版本号下发与审计日志 API 测试 (Settings & Audit API)

覆盖范围:
- 原 test_settings_api: 设置项读取与全局更新、PikPak 配置校验
- 原 test_settings_partial_update: 设置项独立 Tab 保存与隔离、版本号回显与自愈默认值
- 原 test_audit_api: 审计日志查询、分页、严重级别过滤与只读防御"""

import json
import os

from _safeio import safe_write_bytes
from fastapi.testclient import TestClient

from app.api.auth import issue_2fa_token
from app.config import settings
from app.core.failure_logger import FailureLogger
from app.core.scheduler import task_scheduler
from app.db.account_repository import SystemSettingsRepository
from app.db.database import _bootstrap_from_env, get_db
from app.db.forward_repository import ForwardLedgerRepository
from app.db.task_repository import TaskQueueRepository
from app.main import app

# ==============================================================================
# Part: test_settings_api.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))


def auth_headers():
    headers = {}
    if settings.API_KEY:
        headers["X-API-Key"] = settings.API_KEY
        token = issue_2fa_token()
        if token:
            headers["X-2FA-Token"] = token
    return headers


def test_get_and_update_settings():
    # 1. 获取系统配置
    resp = client.get("/api/settings", headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert "categories" in data
    assert "all_settings" in data
    assert "concurrency" in data["categories"]

    # 2. 更新配置
    payload = {
        "settings": {
            "max_global_concurrency": "5",
            "userbot_upload_threshold_mb": "50",
            "custom_note": "test_note_value",
        }
    }
    resp = client.put("/api/settings", json=payload, headers=auth_headers())
    assert resp.status_code == 200
    up_data = resp.json()
    assert up_data["all_settings"]["max_global_concurrency"] == "5"
    assert up_data["all_settings"]["userbot_upload_threshold_mb"] == "50"
    assert up_data["all_settings"]["custom_note"] == "test_note_value"

    # 3. 验证调度器读取热配置
    assert task_scheduler.get_max_global_concurrency() == 5

    # 4. 校验非法值
    resp = client.put("/api/settings", json={"settings": {"max_global_concurrency": "-1"}}, headers=auth_headers())
    assert resp.status_code == 400

    resp = client.put("/api/settings", json={"settings": {"transit_min_file_size_mb": "-5"}}, headers=auth_headers())
    assert resp.status_code == 400

    resp = client.put("/api/settings", json={"settings": {}}, headers=auth_headers())
    assert resp.status_code == 400

    # 5. 更新并验证中转加速配置
    transit_payload = {
        "settings": {
            "enable_transit_acceleration": "true",
            "transit_chat_id": "-1009000000010",
            "transit_min_file_size_mb": "25",
        }
    }
    resp = client.put("/api/settings", json=transit_payload, headers=auth_headers())
    assert resp.status_code == 200
    up_transit = resp.json()["all_settings"]
    assert up_transit["enable_transit_acceleration"] == "true"
    assert up_transit["transit_chat_id"] == "-1009000000010"
    assert up_transit["transit_min_file_size_mb"] == "25"

    # 6. 调用 /verify-transit 探针接口
    resp = client.post("/api/settings/verify-transit", headers=auth_headers())
    assert resp.status_code == 200
    v_data = resp.json()
    assert "status" in v_data
    assert "transit_chat_id" in v_data
    assert "bots_status" in v_data


def test_pikpak_settings_read_update_and_validation():
    """验证 PikPak 系统设置默认值注入、读取、更新与范围/枚举校验。"""
    # 1. 验证获取包含 pikpak 分类与默认值
    resp = client.get("/api/settings", headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert "pikpak" in data["categories"]
    all_s = data["all_settings"]
    assert "pikpak_upload_concurrency" in all_s
    assert "pikpak_upload_in_flight" in all_s
    assert "pikpak_ring_buffer_mb" in all_s
    assert "pikpak_default_cleanup_mode" in all_s
    assert "pikpak_default_oversize_policy" in all_s
    assert "pikpak_default_split_strategy" in all_s

    # 2. 正向更新 PikPak 配置
    update_payload = {
        "settings": {
            "pikpak_upload_concurrency": "4",
            "pikpak_upload_in_flight": "2",
            "pikpak_ring_buffer_mb": "100",
            "pikpak_default_cleanup_mode": "archive",
            "pikpak_default_oversize_policy": "skip_and_report",
            "pikpak_default_split_strategy": "fmp4_universal",
        }
    }
    resp = client.put("/api/settings", json=update_payload, headers=auth_headers())
    assert resp.status_code == 200
    updated = resp.json()["all_settings"]
    assert updated["pikpak_upload_concurrency"] == "4"
    assert updated["pikpak_upload_in_flight"] == "2"
    assert updated["pikpak_ring_buffer_mb"] == "100"
    assert updated["pikpak_default_cleanup_mode"] == "archive"
    assert updated["pikpak_default_oversize_policy"] == "skip_and_report"
    assert updated["pikpak_default_split_strategy"] == "fmp4_universal"

    # 3. 校验非法范围/枚举
    # concurrency < 1 或 > 16
    assert (
        client.put(
            "/api/settings", json={"settings": {"pikpak_upload_concurrency": "0"}}, headers=auth_headers()
        ).status_code
        == 400
    )
    assert (
        client.put(
            "/api/settings", json={"settings": {"pikpak_upload_concurrency": "17"}}, headers=auth_headers()
        ).status_code
        == 400
    )

    # in_flight < 1 或 > 4
    assert (
        client.put(
            "/api/settings", json={"settings": {"pikpak_upload_in_flight": "0"}}, headers=auth_headers()
        ).status_code
        == 400
    )
    assert (
        client.put(
            "/api/settings", json={"settings": {"pikpak_upload_in_flight": "5"}}, headers=auth_headers()
        ).status_code
        == 400
    )

    # ring_buffer_mb < 10 或 > 1024
    assert (
        client.put(
            "/api/settings", json={"settings": {"pikpak_ring_buffer_mb": "0"}}, headers=auth_headers()
        ).status_code
        == 400
    )
    assert (
        client.put(
            "/api/settings", json={"settings": {"pikpak_ring_buffer_mb": "9"}}, headers=auth_headers()
        ).status_code
        == 400
    )
    assert (
        client.put(
            "/api/settings", json={"settings": {"pikpak_ring_buffer_mb": "1025"}}, headers=auth_headers()
        ).status_code
        == 400
    )

    # cleanup_mode 非法枚举
    assert (
        client.put(
            "/api/settings", json={"settings": {"pikpak_default_cleanup_mode": "trash"}}, headers=auth_headers()
        ).status_code
        == 400
    )

    # oversize_policy 非法枚举 (包含已被收敛移除的 strict_fail)
    assert (
        client.put(
            "/api/settings", json={"settings": {"pikpak_default_oversize_policy": "ignore"}}, headers=auth_headers()
        ).status_code
        == 400
    )
    assert (
        client.put(
            "/api/settings",
            json={"settings": {"pikpak_default_oversize_policy": "strict_fail"}},
            headers=auth_headers(),
        ).status_code
        == 400
    )

    # split_strategy 非法枚举
    assert (
        client.put(
            "/api/settings",
            json={"settings": {"pikpak_default_split_strategy": "invalid_split"}},
            headers=auth_headers(),
        ).status_code
        == 400
    )


# ==============================================================================
# Part: test_settings_partial_update.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))


def test_settings_api_returns_app_version():
    """验证 GET /api/settings 返回权威版本号 app_version == '1.0.0'"""
    resp = client.get("/api/settings", headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert "app_version" in data
    assert data["app_version"] == "1.0.0"


def test_partial_update_single_tab_keys_isolation():
    """验证单 Tab 提交仅局部增量更新本 Tab 字段，不破坏其他 Tab 已有字段。"""
    # 1. 确保初始字段存在
    initial_resp = client.get("/api/settings", headers=auth_headers())
    assert initial_resp.status_code == 200
    all_s_before = initial_resp.json()["all_settings"]
    original_oversize = all_s_before.get("pikpak_default_oversize_policy", "skip_and_report")
    original_transit = all_s_before.get("transit_min_file_size_mb", "20")

    # 2. 仅提交本地发布 Tab 字段
    local_publish_payload = {"settings": {"upload_parallel_connections": "6", "upload_in_flight_per_connection": "3"}}
    resp = client.put("/api/settings", json=local_publish_payload, headers=auth_headers())
    assert resp.status_code == 200
    all_s_after = resp.json()["all_settings"]

    # 验证本地发布字段成功更新
    assert all_s_after["upload_parallel_connections"] == "6"
    assert all_s_after["upload_in_flight_per_connection"] == "3"

    # 验证未提交的其他 Tab 字段完好保留
    assert all_s_after.get("pikpak_default_oversize_policy") == original_oversize
    assert all_s_after.get("transit_min_file_size_mb") == original_transit


def test_pikpak_default_oversize_policy_database_self_healing():
    """验证存量库在丢失该项时通过 _bootstrap_from_env 自动幂等自愈。"""
    conn = get_db()
    cursor = conn.cursor()
    # 模拟极端存量库缺失该行
    cursor.execute("DELETE FROM system_settings WHERE key = 'pikpak_default_oversize_policy'")
    conn.commit()

    # 触发自愈
    _bootstrap_from_env(cursor)
    conn.commit()

    val = SystemSettingsRepository.get("pikpak_default_oversize_policy")
    assert val == "skip_and_report", f"预期自愈默认值为 'skip_and_report'，实际为 {val}"


def test_pikpak_default_split_strategy_database_self_healing():
    """验证存量库在丢失 pikpak_default_split_strategy 时通过 _bootstrap_from_env 自动幂等自愈。"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM system_settings WHERE key = 'pikpak_default_split_strategy'")
    conn.commit()

    # 触发自愈
    _bootstrap_from_env(cursor)
    conn.commit()

    val = SystemSettingsRepository.get("pikpak_default_split_strategy")
    assert val == "vsse_with_fallback", f"预期自愈默认值为 'vsse_with_fallback'，实际为 {val}"


# ==============================================================================
# Part: test_audit_api.py
# ==============================================================================
client = TestClient(app, client=("127.0.0.1", 50000))


def test_task_and_system_logs_api():
    task_id = "test_log_api_1"
    TaskQueueRepository.create(task_id, "download_link", {})
    TaskQueueRepository.append_log(task_id, "日志消息 1: 正在连接")
    TaskQueueRepository.append_log(task_id, "日志消息 2: 正在下载文件 sample.mp4")
    TaskQueueRepository.append_log(task_id, "日志消息 3: 下载完成")

    # 1. 查询全部日志
    resp = client.get(f"/api/logs/tasks/{task_id}", headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert data["task_id"] == task_id
    assert len(data["logs"]) == 3
    assert data["last_seq"] == 3
    assert data["has_more"] is False

    # 2. 增量 seq 拉取 (after_seq=2)
    resp = client.get(f"/api/logs/tasks/{task_id}?after_seq=2", headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["logs"]) == 1
    assert "下载完成" in data["logs"][0]["message"]

    # 3. 关键词过滤
    resp = client.get(f"/api/logs/tasks/{task_id}?query=sample", headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["logs"]) == 1
    assert "sample.mp4" in data["logs"][0]["message"]

    # 4. 系统日志流
    resp = client.get("/api/logs/system?limit=10", headers=auth_headers())
    assert resp.status_code == 200
    assert "logs" in resp.json()
    assert isinstance(resp.json()["logs"], list)

    TaskQueueRepository.delete(task_id)


def test_forward_skips_audit_api():
    # 插入转发跳过记录
    task_id = "fwd_audit_task"
    source = "-100900001"
    target = "-100900024"
    ForwardLedgerRepository.commit_batch_records(
        task_id=task_id,
        source_chat=source,
        target_chat=target,
        mappings=[(101, 201)],
        skips=[(102, "bad_id", "坏消息已被删除", target), (103, "filter", "已过滤服务消息", None)],
        seg_start=101,
        seg_end=103,
    )

    # 1. 查询全部 skips
    resp = client.get(f"/api/audit/forward-skips?task_id={task_id}", headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    assert len(data["items"]) == 2

    # 2. 按 skip_type 过滤
    resp = client.get(f"/api/audit/forward-skips?task_id={task_id}&skip_type=bad_id", headers=auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["items"][0]["msg_id"] == 102
    assert data["items"][0]["reason"] == "坏消息已被删除"


def test_publish_failures_audit_api(tmp_dir):
    old_log_dir = settings.LOG_DIR
    settings.LOG_DIR = tmp_dir
    try:
        # 记录几条发布失败样本
        test_file = safe_write_bytes(os.path.join(tmp_dir, "test_file.mp4"), b"0" * 1024)

        # 记录超时失败
        FailureLogger.log_failure(
            file_path=test_file,
            target_chat_id="-100900037",
            error_obj=TimeoutError("Connection timed out"),
            client_type="Bot",
            task_id="pub_fail_1",
        )
        # 记录超大文件失败
        FailureLogger.log_failure(
            file_path=test_file,
            target_chat_id="-100900037",
            error_obj=Exception("entity_too_large: 超过 2000 MiB (1.95 GiB) 上传限制"),
            client_type="Userbot",
            task_id="pub_fail_2",
        )

        # 查询审计列表
        resp = client.get("/api/audit/publish-failures", headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["items"]) == 2

        # 验证类型分类与重试标识
        timeout_item = next(i for i in data["items"] if i["task_id"] == "pub_fail_1")
        assert timeout_item["type"] == "NETWORK_TIMEOUT"
        assert timeout_item["retryable"] is True

        oversize_item = next(i for i in data["items"] if i["task_id"] == "pub_fail_2")
        assert oversize_item["type"] == "FILE_TOO_LARGE"
        assert oversize_item["retryable"] is False

        # 按 type 过滤
        resp = client.get("/api/audit/publish-failures?type=NETWORK_TIMEOUT", headers=auth_headers())
        assert resp.status_code == 200
        assert resp.json()["total"] == 1

    finally:
        settings.LOG_DIR = old_log_dir


def test_publish_failures_filters_and_pagination(tmp_dir):
    """验证流式过滤下沉后: 多维过滤、分页、跨滚动归档倒序与畸形行容错行为与既有语义严格一致"""
    old_log_dir = settings.LOG_DIR
    settings.LOG_DIR = tmp_dir
    FailureLogger._loggers.clear()
    try:
        test_file = safe_write_bytes(os.path.join(tmp_dir, "filter_case.mp4"), b"x" * 1024)

        # 当前活跃卷写入 3 条不同 task/chat/type 的记录
        FailureLogger.log_failure(test_file, "-100111", TimeoutError("Timeout A"), "Bot", "task_1")
        FailureLogger.log_failure(test_file, "-100222", Exception("floodwait 60"), "Bot", "task_2")
        FailureLogger.log_failure(test_file, "-100111", Exception("entity_too_large"), "Bot", "task_3")

        # 手造 .1 归档卷: 一条旧记录 (chat 与 task_1/task_3 相同) + 一条畸形行
        archive_content = (
            json.dumps(
                {
                    "time": "2026-09-18 08:00:00",
                    "task_id": "arch_old",
                    "chat_id": "-100111",
                    "file": "old.mp4",
                    "size_mb": 3.0,
                    "client": "Bot",
                    "type": "OTHER_ERROR",
                    "reason": "old err",
                }
            )
            + "\n"
            + "{broken json line}\n"
        )
        safe_write_bytes(os.path.join(tmp_dir, "publish_failures.jsonl.1"), archive_content.encode("utf-8"))

        # 1. 无过滤: total=4 (畸形行不计入), 跨归档严格倒序
        resp = client.get("/api/audit/publish-failures", headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 4
        assert [i["task_id"] for i in data["items"]] == ["task_3", "task_2", "task_1", "arch_old"]

        # 2. task_id 过滤
        resp = client.get("/api/audit/publish-failures?task_id=task_1", headers=auth_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["type"] == "NETWORK_TIMEOUT"
        assert data["items"][0]["retryable"] is True

        # 3. chat_id 过滤 (task_1/task_3/arch_old)
        resp = client.get("/api/audit/publish-failures?chat_id=-100111", headers=auth_headers())
        assert resp.status_code == 200
        assert resp.json()["total"] == 3

        # 4. type 过滤 (小写入参亦应命中大写归一化)
        resp = client.get("/api/audit/publish-failures?type=flood_wait", headers=auth_headers())
        assert resp.status_code == 200
        assert resp.json()["total"] == 1
        assert resp.json()["items"][0]["task_id"] == "task_2"

        # 5. 分页: total 不随页码变化, 切片严格对齐
        resp_p1 = client.get("/api/audit/publish-failures?page=1&limit=2", headers=auth_headers())
        assert resp_p1.status_code == 200
        assert resp_p1.json()["total"] == 4
        assert [i["task_id"] for i in resp_p1.json()["items"]] == ["task_3", "task_2"]

        resp_p2 = client.get("/api/audit/publish-failures?page=2&limit=2", headers=auth_headers())
        assert resp_p2.status_code == 200
        assert resp_p2.json()["total"] == 4
        assert [i["task_id"] for i in resp_p2.json()["items"]] == ["task_1", "arch_old"]

    finally:
        FailureLogger._loggers.clear()
        settings.LOG_DIR = old_log_dir


def test_settings_rejects_fractional_bool_nan_and_normalizes_null_proxy():
    # 1. 浮点数/布尔值/小数字符串不可通过整型校验
    for bad in (1.5, 1.0, True, "1.5"):
        resp = client.put(
            "/api/settings",
            json={"settings": {"max_global_concurrency": bad}},
            headers=auth_headers(),
        )
        assert resp.status_code == 400

    # 2. NaN / Infinity 不可通过浮点配额校验
    for bad_q in ("NaN", "inf", "-inf", True):
        resp = client.put(
            "/api/settings",
            json={"settings": {"max_storage_quota_gb": bad_q}},
            headers=auth_headers(),
        )
        assert resp.status_code == 400

    # 3. null 代理值必须规范化为空串而非 "None"
    resp_proxy = client.put(
        "/api/settings",
        json={"settings": {"pikpak_proxy_url": None}},
        headers=auth_headers(),
    )
    assert resp_proxy.status_code == 200
    assert SystemSettingsRepository.get("pikpak_proxy_url") == ""


def test_auto_remux_mpegts_setting_seed_and_validation(monkeypatch):
    resp = client.get("/api/settings", headers=auth_headers())
    assert resp.status_code == 200
    all_s = resp.json()["all_settings"]
    expected_initial = "true" if settings.AUTO_REMUX_MPEGTS_TO_MP4 else "false"
    assert all_s.get("auto_remux_mpegts_to_mp4") == expected_initial

    conn = get_db()
    cursor = conn.cursor()
    for flag_val, expected_seed in ((False, "false"), (True, "true")):
        cursor.execute("DELETE FROM system_settings WHERE key = 'auto_remux_mpegts_to_mp4'")
        conn.commit()
        monkeypatch.setattr(settings, "AUTO_REMUX_MPEGTS_TO_MP4", flag_val)
        _bootstrap_from_env(cursor)
        conn.commit()
        assert SystemSettingsRepository.get("auto_remux_mpegts_to_mp4") == expected_seed

    for raw_val, expected in ((False, "false"), ("TRUE", "true"), ("false", "false"), (True, "true")):
        up = client.put(
            "/api/settings",
            json={"settings": {"auto_remux_mpegts_to_mp4": raw_val}},
            headers=auth_headers(),
        )
        assert up.status_code == 200
        assert up.json()["all_settings"]["auto_remux_mpegts_to_mp4"] == expected

    for bad_val in ("yes", 1, "invalid", None):
        bad = client.put(
            "/api/settings",
            json={"settings": {"auto_remux_mpegts_to_mp4": bad_val}},
            headers=auth_headers(),
        )
        assert bad.status_code == 400
