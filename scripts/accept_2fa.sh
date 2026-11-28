#!/usr/bin/env bash
# =============================================================================
# Web 2FA 服务端强制 + JWT 会话化 + 真实客户端 IP —— curl 验收脚本
#
# 用法:
#   BASE_URL=http://127.0.0.1:8500 API_KEY=YourKey ./scripts/accept_2fa.sh
#
# 可选开关:
#   TOKEN=<已签发的2FA token>   追加"重启后端后旧 token 仍有效"检查 (步骤 5)
#   RATE_LIMIT_TEST=1           追加"限流按真实 IP 区分"检查 (步骤 6,
#                               需以 RATE_LIMIT_PER_MINUTE=3 启动后端)
#
# 步骤 (对应实施计划测试章节):
#   1. 正确 API Key + 无 2FA token 调业务路由      -> 期望 403
#   2. 豁免路由 /api/telegram/status 无 2FA token  -> 期望 200
#   3. 走完整 2FA (人工从 Telegram 收藏夹读码)     -> 签发 token, 业务路由全通
#   4. 篡改 token (payload 改一字符)               -> 期望 403 (依赖步骤 3 的 token)
#   5. [可选] 重启后端后复用已签发 token            -> 仍有效 (HS256 无状态)
#   6. [可选] 两个不同 X-Forwarded-For 限流独立计数 -> 第 4 次 429 / 另一 IP 200
# =============================================================================
set -u

BASE_URL="${BASE_URL:-http://127.0.0.1:8500}"
API_KEY="${API_KEY:-}"
PASS=0
FAIL=0
CURL_TIMEOUT_OPTS=(--connect-timeout "${CURL_CONNECT_TIMEOUT:-5}" --max-time "${CURL_MAX_TIME:-15}")

note()  { printf '%s\n' "$*"; }
pass()  { PASS=$((PASS+1)); note "[PASS] $*"; }
fail()  { FAIL=$((FAIL+1)); note "[FAIL] $*"; }

# Parse before any request carrying credentials; fail closed if Python is unavailable.
if ! python3 - "$BASE_URL" "${ALLOW_INSECURE_HTTP:-0}" <<'PYURL'
import sys
from urllib.parse import urlsplit

try:
    raw = sys.argv[1]
    if any(ord(ch) <= 32 or ord(ch) == 127 for ch in raw) or "\\" in raw:
        raise ValueError("BASE_URL contains whitespace, control characters or backslashes")
    parts = urlsplit(raw)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("BASE_URL requires an http(s) hostname")
    if parts.username is not None or parts.password is not None:
        raise ValueError("BASE_URL must not contain userinfo")
    parts.port  # Reject invalid ports before curl sees the URL.
    if (parts.scheme == "http"
            and parts.hostname not in ("127.0.0.1", "localhost", "::1")
            and sys.argv[2] != "1"):
        raise ValueError("Non-loopback HTTP requires ALLOW_INSECURE_HTTP=1")
except ValueError as exc:
    print(f"Invalid BASE_URL: {exc}", file=sys.stderr)
    sys.exit(1)
PYURL
then
  exit 1
fi

code_of() { curl -s "${CURL_TIMEOUT_OPTS[@]}" -o /dev/null -w '%{http_code}' "$@"; }

check() { # check <期望码> <描述> <curl参数...>
  local expect="$1" desc="$2"; shift 2
  local got
  got=$(code_of "$@")
  if [ "$got" = "$expect" ]; then pass "$desc (HTTP $got)"; else fail "$desc: 期望 $expect, 实际 $got"; fi
}

extract_json_str() { # extract_json_str <json> <字段>
  printf '%s' "$1" | sed -n "s/.*\"$2\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p"
}

if [ -z "$API_KEY" ]; then
  note "错误: 请通过环境变量 API_KEY 提供控制台访问口令 (后端未配置 API_KEY 时无需本脚本)"
  exit 1
fi
AUTH=(-H "X-API-Key: $API_KEY")

note "== 步骤 1: 正确 API Key + 无 2FA token -> 业务路由 403 (而非 401) =="
check 403 "POST /api/download/range (Key 无 2FA)" -X POST "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d '{}' "$BASE_URL/api/download/range"
resp=$(curl -s "${CURL_TIMEOUT_OPTS[@]}" -X POST "${AUTH[@]}" -H 'Content-Type: application/json' -d '{}' "$BASE_URL/api/download/range")
if printf '%s' "$resp" | grep -q '"detail"[[:space:]]*:[[:space:]]*"2FA'; then
  pass "403 detail 以 2FA 开头 (前端识别契约)"
else
  fail "403 detail 前缀契约: $resp"
fi

note "== 步骤 2: 豁免路由无需 2FA token =="
check 200 "GET /api/auth/status (无凭证)" "$BASE_URL/api/auth/status"
check 200 "GET /api/telegram/status (仅 Key)" "${AUTH[@]}" "$BASE_URL/api/telegram/status"

note "== 步骤 3: 完整 2FA 流程 (需人工读码) =="
check 200 "POST /api/telegram/web-2fa/send" -X POST "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d '{}' "$BASE_URL/api/telegram/web-2fa/send"
read -r -p "请输入 Telegram 收藏夹 (Saved Messages) 收到的 6 位验证码: " otp
verify_resp=$(curl -s "${CURL_TIMEOUT_OPTS[@]}" -X POST "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d "{\"code\":\"$otp\"}" "$BASE_URL/api/telegram/web-2fa/verify")
TOKEN_GOT=$(extract_json_str "$verify_resp" token)
if [ -n "$TOKEN_GOT" ]; then
  pass "web-2fa/verify 签发 token 成功"
else
  fail "web-2fa/verify 未返回 token: $verify_resp"
fi
if [ -n "$TOKEN_GOT" ]; then
  check 200 "GET /api/tasks (Key + 2FA token)" "${AUTH[@]}" \
    -H "X-2FA-Token: $TOKEN_GOT" "$BASE_URL/api/tasks"
  check 200 "GET /api/presets (Key + 2FA token)" "${AUTH[@]}" \
    -H "X-2FA-Token: $TOKEN_GOT" "$BASE_URL/api/presets"

  note "== 步骤 4: 篡改 token (保持合法 JSON 结构修改 payload 声明) -> 403 =="
  head_seg=${TOKEN_GOT%%.*}
  rest=${TOKEN_GOT#*.}
  payload_seg=${rest%%.*}
  sig_seg=${rest#*.}
  tampered_payload=""
  if command -v python3 >/dev/null 2>&1; then
    tampered_payload=$(python3 -c 'import base64,json,sys; p=sys.argv[1]; d=json.loads(base64.urlsafe_b64decode(p+"="*(-len(p)%4))); d["tampered"]=True; print(base64.urlsafe_b64encode(json.dumps(d,separators=(",",":")).encode()).rstrip(b"=").decode())' "$payload_seg" 2>/dev/null || true)
  fi
  if [ -z "$tampered_payload" ]; then
    first_char=${payload_seg:0:1}
    if [ "$first_char" = "A" ]; then flipped="B"; else flipped="A"; fi
    tampered_payload="${flipped}${payload_seg:1}"
  fi
  tampered="${head_seg}.${tampered_payload}.${sig_seg}"
  check 403 "GET /api/tasks (篡改 token)" "${AUTH[@]}" \
    -H "X-2FA-Token: $tampered" "$BASE_URL/api/tasks"
fi

if [ -n "${TOKEN:-}" ]; then
  note "== 步骤 5: 复用重启前签发的 token (请确认已重启后端) =="
  check 200 "GET /api/tasks (重启前 token)" "${AUTH[@]}" \
    -H "X-2FA-Token: $TOKEN" "$BASE_URL/api/tasks"
else
  note "-- 跳过步骤 5 (重启持久性): 重启后端后执行 TOKEN=<token> $0 复验 --"
fi

if [ "${RATE_LIMIT_TEST:-0}" = "1" ]; then
  note "== 步骤 6: 限流按 X-Forwarded-For 真实 IP 独立计数 (需 RATE_LIMIT_PER_MINUTE=3) =="
  ip_a=203.0.113.10
  ip_b=203.0.113.20
  ok=1
  for i in 1 2 3; do
    got=$(code_of -H "X-Forwarded-For: $ip_a" "$BASE_URL/api/auth/status")
    [ "$got" = "200" ] || { ok=0; break; }
  done
  [ "$ok" = "1" ] && pass "IP $ip_a 前 3 次请求均 200" || fail "IP $ip_a 前 3 次出现非 200"
  check 429 "IP $ip_a 第 4 次触发限流" -H "X-Forwarded-For: $ip_a" "$BASE_URL/api/auth/status"
  check 200 "IP $ip_b 不受 $ip_a 计数影响" -H "X-Forwarded-For: $ip_b" "$BASE_URL/api/auth/status"
else
  note "-- 跳过步骤 6 (真实 IP 限流): 以 RATE_LIMIT_PER_MINUTE=3 启动后端后加 RATE_LIMIT_TEST=1 --"
fi

note ""
note "==== 结果: PASS=$PASS FAIL=$FAIL ===="
[ "$FAIL" = "0" ]
