"""PikPak 链路脱敏基元: URL 签名参数值与 PIKPAK_TOKEN 字面值擦除。

红线: 含签名参数的 PikPak CDN 直链 (X-Sign/Sign/auth_key/Expires
等查询参数) 与 PIKPAK_TOKEN 值永不进入日志/DB/WS/API 响应; WS 广播侧的 redact_payload
由 接入。范围仅 PikPak 新链路 (裁决) —— 存量 accounts 明文回显治理另立安全
工单, 本模块不接入; PIKPAK_TOKEN 纯只读 (env 来源), 本模块只擦除、不存取不回显。
"""

import logging
import re
from typing import Any

from app.config import settings

_REDACTED = "***"

# 查询参数名清单 (大小写不敏感): ;
# 值中含 %XX URL 编码同样被覆盖 ([^&\s] 匹配至 & 或空白前), 参数名保留仅值替换
_SIGNED_QUERY_RE = re.compile(
    r"([?&](?:x-sign|sign|auth_key|expires|token|access_token|api_key|api_hash)=)[^&\s\"']+",
    re.IGNORECASE,
)

# 代理或 HTTP URL 凭证敏感信息擦除 (http://user:pass@host:port -> http://***:***@host:port)
_URL_USERINFO_RE = re.compile(
    r"([a-zA-Z][a-zA-Z0-9+.-]*://)([^:/?#@\s\"']+):([^/?#@\s\"']+)@",
)

# Python printf 格式化占位符识别 (%s, %.1f, %(key)s, %% 等)
_PRINTF_SPEC_RE = re.compile(r"%(?:\([^)]+\))?[#0\- +]?(?:\*|\d+)?(?:\.(?:\*|\d+))?[hlL]?[diouxXeEfFgGcrsa%]")


def redact_text(s: Any) -> Any:
    """擦除文本中的 URL 签名参数值、代理认证凭证与 PIKPAK_TOKEN 字面值 (参数名/其余文本原样保留)。

    PIKPAK_TOKEN 每次调用时从 settings 懒读取 (不缓存到模块常量, 兼容运行期 monkeypatch/
    环境注入); 非字符串输入原样返回。"""
    if not isinstance(s, str) or not s:
        return s
    out = _SIGNED_QUERY_RE.sub(lambda m: m.group(1) + _REDACTED, s)
    out = _URL_USERINFO_RE.sub(r"\1***:***@", out)
    token = settings.PIKPAK_TOKEN
    if token:
        out = out.replace(token, _REDACTED)
    return out


def _redact_container(obj: Any, seen: set[int]) -> Any:
    if isinstance(obj, dict):
        return {k: redact_payload(v, seen) for k, v in obj.items()}
    rebuilt = [redact_payload(v, seen) for v in obj]
    if isinstance(obj, tuple):
        return type(obj)(*rebuilt) if hasattr(obj, "_fields") else type(obj)(rebuilt)
    return rebuilt


def redact_payload(obj: Any, _seen: set[int] | None = None) -> Any:
    """递归脱敏 dict/list/tuple/namedtuple/str (供 WS 广播消息与任意嵌套负载使用), 其余类型原样."""
    if isinstance(obj, str):
        return redact_text(obj)
    if not isinstance(obj, (dict, list, tuple)):
        return obj
    seen = _seen if _seen is not None else set()
    obj_id = id(obj)
    if obj_id in seen:
        return "[...]"
    seen.add(obj_id)
    try:
        return _redact_container(obj, seen)
    finally:
        seen.discard(obj_id)


def _has_unredacted_secret(text: str) -> bool:
    for m in _SIGNED_QUERY_RE.finditer(text):
        val = m.group(0)[len(m.group(1)) :]
        if not val.startswith(_REDACTED):
            return True
    for m in _URL_USERINFO_RE.finditer(text):
        if m.group(2) != _REDACTED or m.group(3) != _REDACTED:
            return True
    token = settings.PIKPAK_TOKEN
    return bool(token and token in text)


class RedactLogFilter(logging.Filter):
    """全局日志脱敏过滤器 (挂 handler 层, 对经该 handler 的所有 record 生效,
    覆盖第三方 logger 冒泡路径): 保留 record.args 容器结构并经 redact_payload 递归脱敏,
    仅当模板占位符处于敏感参数位置或渲染后仍暴露敏感文本时才先渲染再脱敏并清空 args; 永不吞记录。"""

    def filter(self, record: logging.LogRecord) -> bool:
        if not record.args:
            if isinstance(record.msg, str):
                record.msg = redact_text(record.msg)
            return True

        sanitized_msg = redact_text(record.msg) if isinstance(record.msg, str) else record.msg
        sanitized_args: Any = record.args
        try:
            sanitized_args = redact_payload(record.args)
            placeholders_intact = not isinstance(record.msg, str) or _PRINTF_SPEC_RE.findall(
                record.msg
            ) == _PRINTF_SPEC_RE.findall(sanitized_msg)
            if placeholders_intact:
                candidate = sanitized_msg % sanitized_args if isinstance(sanitized_msg, str) else str(sanitized_msg)
                if not _has_unredacted_secret(candidate):
                    record.msg = sanitized_msg
                    record.args = sanitized_args
                    return True
        except Exception:
            sanitized_args = record.args

        try:
            rendered = record.getMessage()
        except Exception:
            record.msg = redact_text(str(record.msg))
            record.args = None
        else:
            record.msg = redact_text(rendered)
            record.args = None
        return True
