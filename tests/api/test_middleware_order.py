"""中间件顺序测试: CORS 必须位于 rate-limit 外层。

Starlette 后注册者位于外层 —— CORS 在外层才能把限流 429 等中间件生成的响应也包上
CORS 头, 否则跨域前端 (Vite dev :5173 → :8500) 收到不透明 CORS 错误而非 429。"""

from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from app.main import app


def test_cors_wraps_rate_limit_middleware():
    classes = [m.cls for m in app.user_middleware]
    assert CORSMiddleware in classes, "CORS 中间件必须注册"
    assert BaseHTTPMiddleware in classes, "@app.middleware(http) 限流中间件必须注册"
    assert classes.index(CORSMiddleware) < classes.index(BaseHTTPMiddleware), (
        "CORS 必须比 rate-limit 更晚注册 (user_middleware 中 index 越小越外层), 否则 429 绕过 CORS 头"
    )
