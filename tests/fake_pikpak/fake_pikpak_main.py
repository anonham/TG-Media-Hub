"""Fake pikpak CLI (测试专用, /T3): 按 argv/场景分支输出罐头数据。

场景经环境变量 PIKPAK_FAKE_SCENARIO 驱动 (由测试 fixture 设置):
normal | quota | not_found | auth_error | auth_status_ok | auth_flaky_first |
network_error | bad_json | slow | hang | denied | expired |
auth_then_network | token_refresh_failed
(扩展: ls 分页/download/mkdir/mv/rm 罐头, 由包F 增补分支;
 tristate 扩展: auth_then_network/token_refresh_failed 供探针交叉归因用例。)

调用流水: 每次 invocation 在本脚本目录下 _runs/inv-<uuid>.json 写一个事件文件
(enter 时创建, exit 时补全 exit_ts/code; 被强杀的进程天然只留 enter 文件),
供测试断言 spawn 计数/并发峰值/单飞/透传。路径全部由本脚本 __file__ 常量派生,
并统一经 _safe_target 做 resolve + 目录边界校验 (照 tests/_safeio.safe_write_bytes
同范式); 不接受环境变量传入的路径 (防路径穿越); 测试 fixture 负责清理 _runs。

罐头数据依据 pikpak CLI 官方文档字段构造; 全部为构造假值, 不得含真实凭据。
设备码轮询非 success 状态字符串与 mkdir -F json 形状属「待校准」输入缺口
(OAuth 设备码惯例预设), 真实样本到位后校准。
"""

import hashlib
import json
import os
import pathlib
import sys
import time
import uuid

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

_RUN_DIR = (
    pathlib.Path(os.environ["PIKPAK_FAKE_RUN_DIR"]).resolve()
    if os.environ.get("PIKPAK_FAKE_RUN_DIR")
    else pathlib.Path(__file__).resolve().parent / "_runs"
)


def _safe_target(name: str) -> pathlib.Path:
    """状态文件路径: basename 固化 + resolve + 目录边界校验 (_runs 内), 照 _safeio 同范式"""
    base = _RUN_DIR
    target = (base / os.path.basename(name)).resolve()
    if base != target and base not in target.parents:
        raise ValueError(f"fake CLI 状态路径越界: {name}")
    return target


_ENTRY = {}

_ENV_KEYS = ("PIKPAK_DIR", "PIKPAK_NO_LOG", "PIKPAK_NO_UPDATE_NOTIFIER")


def _journal_enter():
    _RUN_DIR.mkdir(parents=True, exist_ok=True)
    _ENTRY["file"] = _safe_target("inv-" + uuid.uuid4().hex + ".json")
    _ENTRY["data"] = {
        "pid": os.getpid(),
        "enter_ts": time.time(),
        "args": sys.argv[1:],
        "env": {k: os.environ.get(k) for k in _ENV_KEYS if k in os.environ},
    }
    if "PIKPAK_TOKEN" in os.environ:
        _ENTRY["data"]["env"]["PIKPAK_TOKEN"] = hashlib.sha256(os.environ["PIKPAK_TOKEN"].encode()).hexdigest()
    _ENTRY["file"].write_text(json.dumps(_ENTRY["data"], ensure_ascii=False), encoding="utf-8")


def _journal_exit(code):
    if not _ENTRY.get("file"):
        return
    _ENTRY["data"]["exit_ts"] = time.time()
    _ENTRY["data"]["code"] = code
    _ENTRY["file"].write_text(json.dumps(_ENTRY["data"], ensure_ascii=False), encoding="utf-8")


def _marker_exists(name):
    return _safe_target(name).exists()


def _mark_once(name):
    """O_EXCL 原子一次性标记: 首个创建者返回 True (并发进程安全)"""
    p = _safe_target(name)
    try:
        fd = os.open(str(p), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except FileExistsError:
        return False


def _read_counter(name):
    p = _safe_target(name)
    if p.exists():
        return int(p.read_text(encoding="utf-8") or "0")
    return 0


def _write_counter(name, value):
    _safe_target(name).write_text(str(value), encoding="utf-8")


def _fail(code, reason="", hint=""):
    """统一错误 schema (CLI 文档: -F json 时 stderr 根 error{code,message,reason,hint}, 退出码 1)"""
    _journal_exit(1)
    sys.stderr.write(
        json.dumps({"error": {"code": code, "message": reason or code, "reason": reason, "hint": hint}}) + "\n"
    )
    sys.exit(1)


def _out(obj):
    _journal_exit(0)
    print(json.dumps(obj, ensure_ascii=False))


# ---- 罐头数据 (构造值; 数值口径参考 , 非真实账号) ----

AUTH_STATUS = {
    "logged_in": True,
    "user_id": "fake-user-001",
    "name": "Fake User",
    "email": "fake@example.test",
    "status": "ACTIVE",
    "created": "2026-01-01 00:00:00",
    "vip": {"type": "NONE", "status": "INVALID", "expire_time": "2026-01-01 00:00:00", "items": []},
    "storage": {
        "total": 2199023255552,
        "used": 108857181792,
        "in_trash": 0,
        "is_unlimited": False,
        "complimentary": 0,
        "expires": "",
    },
    "additional_quotas": {
        "upload_flow": "0",
        "decompress_limit": "0",
        "space_size": "0",
        "trash_duration": "15",
    },
}

DEVICE_START = {
    "device_code": "FAKEDEVICECODE123",
    "verification_uri": "https://fake.example.test/activate",
    "user_code": "ABCD-1234",
    "expires_in": 1800,
    "interval": 5,
}

DEVICE_SUCCESS = {
    "status": "success",
    "user_id": "fake-user-001",
    "name": "Fake User",
    "email": "fake@example.test",
}


def _handle_auth_status(scenario):
    if scenario == "auth_flaky_first":
        if _mark_once("auth_flaky_once.flag"):
            _fail("UNAUTHENTICATED", "Not logged in (first call)", "Re-run 'pikpak auth login'")
    _out(AUTH_STATUS)


def _handle_device_poll(scenario):
    if scenario == "denied":
        _out({"status": "access_denied"})
        return
    if scenario == "expired":
        _out({"status": "expired_token"})
        return
    count = _read_counter("poll_count.txt") + 1
    _write_counter("poll_count.txt", count)
    if count >= 3:
        _out(DEVICE_SUCCESS)
    else:
        # 非 success 状态值按 OAuth 设备码惯例预设, 待真实样本校准
        _out({"status": "authorization_pending"})


# ---- 文件域罐头 (ls --raw 两个分页; 字段语义照 CLI 文档: kind/字符串字节 size/
# 字符串 params.duration/audit{status,message,title}/thumbnail_link/parent_id) ----

LS_PAGE1 = {
    "next_page_token": "T2",
    "files": [
        {
            "id": "file-001",
            "name": "movie.mp4",
            "kind": "drive#file",
            "size": "392927574",
            "parent_id": "",
            "thumbnail_link": "https://thumb.fake.test/t1.jpg",
            "audit": {"status": "PASS", "message": "", "title": ""},
            "params": {"duration": "1420", "width": "1920", "height": "1080"},
        },
        {
            "id": "folder-100",
            "name": "子目录",
            "kind": "drive#folder",
            "size": "0",
            "parent_id": "",
            "thumbnail_link": "",
            "audit": {"status": "N/A", "message": "", "title": ""},
            "params": {},
        },
    ],
}

LS_PAGE2 = {
    "next_page_token": None,
    "files": [
        {
            "id": "file-002",
            "name": "second.mkv",
            "kind": "drive#file",
            "size": "805306368",
            "parent_id": "",
            "thumbnail_link": "https://thumb.fake.test/t2.jpg",
            "audit": {"status": "PASS", "message": "", "title": ""},
            "params": {"duration": "5400", "width": "3840", "height": "2160"},
        },
    ],
}


def _arg_value(argv, flag):
    for i, a in enumerate(argv):
        if a == flag and i + 1 < len(argv):
            return argv[i + 1]
    return None


def _auth_gate(scenario):
    """auth_status_ok (单飞业务命令) 与 auth_flaky_first (首次) 的认证拦截"""
    if scenario == "auth_status_ok":
        _fail("UNAUTHENTICATED", "Not logged in", "Re-run 'pikpak auth login'")
        return True
    if scenario == "auth_flaky_first" and _mark_once("auth_flaky_once.flag"):
        _fail("UNAUTHENTICATED", "Not logged in (first call)", "Re-run 'pikpak auth login'")
        return True
    return False


def _handle_ls(argv, scenario):
    if _auth_gate(scenario):
        return
    token = _arg_value(argv, "-t")
    page = LS_PAGE2 if token == "T2" else LS_PAGE1
    parent = _arg_value(argv, "-p") or ""
    files = list(page["files"])
    # mkdir 幂等测试: 已创建目录以标记文件持久化 (内容 {id,name,parent_id}), ls 并入匹配 parent 的条目
    if _RUN_DIR.is_dir():
        for marker in sorted(_RUN_DIR.glob("folder-*.json")):
            try:
                created = json.loads(marker.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            if created.get("parent_id") == parent:
                files.append(
                    {
                        "id": created["id"],
                        "name": created["name"],
                        "kind": "drive#folder",
                        "size": "0",
                        "parent_id": parent,
                        "thumbnail_link": "",
                        "audit": {"status": "N/A", "message": "", "title": ""},
                        "params": {},
                    }
                )
    _out({"files": files, "next_page_token": page["next_page_token"]})


def _handle_download(argv, scenario):
    if _auth_gate(scenario):
        return
    idx = argv.index("download") if "download" in argv else 0
    ids = [a for a in argv[idx + 1 :] if not a.startswith("-")]
    lines = [f"https://cdn.fake.test/dl/{fid}?Expires=9999999999&X-Sign=FAKESIGN" for fid in ids]
    _journal_exit(0)
    sys.stdout.write("\n".join(lines) + ("\n" if lines else ""))


def _handle_mkdir(argv, scenario):
    if _auth_gate(scenario):
        return
    idx = argv.index("mkdir")
    name = next((a for a in argv[idx + 1 :] if not a.startswith("-") and a != _arg_value(argv, "-p")), "")
    parent = _arg_value(argv, "-p") or ""
    seq = _read_counter("mkdir_seq.txt") + 1
    _write_counter("mkdir_seq.txt", seq)
    folder_id = f"folder-created-{seq}"
    marker = _safe_target(f"folder-{seq}.json")
    marker.write_text(
        json.dumps({"id": folder_id, "name": name, "parent_id": parent}, ensure_ascii=False), encoding="utf-8"
    )
    # mkdir -F json 输出形状属「待校准」缺口: 罐头按 files API 对象构造
    _out({"id": folder_id, "name": name, "kind": "drive#folder", "parent_id": parent})


def main():
    _journal_enter()
    scenario = os.environ.get("PIKPAK_FAKE_SCENARIO", "normal")
    argv = sys.argv[1:]

    if scenario == "hang":
        time.sleep(120)
        _journal_exit(0)
        return
    if scenario == "slow":
        time.sleep(0.3)
        scenario = "normal"
    if scenario == "bad_json":
        _journal_exit(0)
        print("NOT-JSON-OUTPUT")
        return

    # 全局错误场景: 对任意命令生效 (映射四象限 + 文件域错误映射复用)
    error_scenarios = {
        "quota": "CONNECTED_APPS_QUOTA_EXCEEDED",
        "not_found": "FILE_NOT_FOUND",
        "auth_error": "UNAUTHENTICATED",
        "network_error": "NETWORK_ERROR",
        "token_refresh_failed": "TOKEN_REFRESH_FAILED",
    }
    if scenario in error_scenarios:
        _fail(error_scenarios[scenario], "canned failure", "canned hint")
        return

    if scenario == "auth_then_network":
        # 首个 invocation (业务命令) 失败认证码, 其后 (含恢复探针) 全部网络错 ——
        # 交叉归因用例: 探针网络错 = 「够不到 PikPak」实证 (tristate plan §5.4)
        if _mark_once("auth_then_network_once.flag"):
            _fail("UNAUTHENTICATED", "Not logged in (first call)", "Re-run 'pikpak auth login'")
        _fail("NETWORK_ERROR", "network unreachable (subsequent)", "check proxy/network")
        return

    if "auth" in argv and "status" in argv:
        _handle_auth_status(scenario)
        return
    if "auth" in argv and "login" in argv and "--continue" in argv:
        _handle_device_poll(scenario)
        return
    if "auth" in argv and "login" in argv and "-m" in argv and "device" in argv:
        _out(DEVICE_START)
        return
    if argv and argv[0] == "version":
        _out({"version": "0.5.2-fake"})
        return

    if argv and argv[0] == "ls":
        _handle_ls(argv, scenario)
        return
    if argv and argv[0] == "download":
        _handle_download(argv, scenario)
        return
    if argv and argv[0] == "mkdir":
        _handle_mkdir(argv, scenario)
        return
    if argv and argv[0] in ("mv", "rm"):
        # mv/rm: 退出码 0 即成功; stdout 形状未文档化, 服务层以 json_mode=False 忽略输出
        _journal_exit(0)
        return

    _journal_exit(2)
    sys.stderr.write("USAGE ERROR: unknown command\n")
    sys.exit(2)


if __name__ == "__main__":
    main()
