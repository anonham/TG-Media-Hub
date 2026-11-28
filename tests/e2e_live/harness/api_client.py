"""REST 客户端 (open-mode) 与终态轮询辅助 。

仅打 harness 自起 / 外部指定的本地实例; 写操作全部走应用 API (F8: harness 对
DB 只读直查, 不绕过 submit_db_write)。终态判定以调度器状态机为准: pending /
queued / running / paused 为非终态, completed / failed / cancelled 为终态。
"""

import logging
import time

import httpx

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


class LiveApiClient:
    def __init__(self, base_url: str, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        # trust_env=False: harness 打的是本机 loopback, 不吃环境代理 (对齐仓库传输规约)
        self._client = httpx.Client(base_url=self.base_url, timeout=timeout, trust_env=False)

    def request(self, method: str, path: str, **kwargs: object) -> httpx.Response:
        return self._client.request(method, path, **kwargs)

    def get(self, path: str, **kwargs: object) -> httpx.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: object) -> httpx.Response:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: object) -> httpx.Response:
        return self.request("PUT", path, **kwargs)

    def patch(self, path: str, **kwargs: object) -> httpx.Response:
        return self.request("PATCH", path, **kwargs)

    def delete(self, path: str, **kwargs: object) -> httpx.Response:
        return self.request("DELETE", path, **kwargs)

    def wait_terminal(self, task_id: str, timeout: float = 900.0, poll_interval: float = 2.0) -> dict:
        """轮询任务至终态; paused 视为停滞提前返回 (由用例自行处置)。

        429 (attach 形态下外部服务器限流不可控) 按退避容忍继续轮询; 其余 HTTP
        错误照常上抛。超时抛 TimeoutError (调用方决定 fail 或 skip, D3: FloodWait
        停滞非故障); 超时前对任务 best-effort 取消 —— 低速网络下大样本超时遗留的
        running 任务会占住账号并发槽, 级联污染同会话后续用例 (
        fmp4 超时后 4 个用例连锁 pending)。
        """
        deadline = time.monotonic() + timeout
        last: dict = {}
        while time.monotonic() < deadline:
            try:
                resp = self.get(f"/api/tasks/{task_id}")
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    time.sleep(max(poll_interval, 2.0))
                    continue
                raise
            last = resp.json()
            status = str(last.get("status", ""))
            if status in TERMINAL_STATUSES or status == "paused":
                return last
            time.sleep(poll_interval)
        try:
            cancel_resp = self.post(f"/api/tasks/{task_id}/cancel")
            cancel_resp.raise_for_status()
            logger.warning("任务 %s 等待终态超时 (%ss), 已 best-effort 取消以释放并发槽", task_id, timeout)
        except Exception as exc:
            logger.warning("任务 %s 超时后取消失败 (并发槽可能被遗留占用): %s", task_id, exc)
        raise TimeoutError(f"任务 {task_id} 在 {timeout}s 内未达终态, 最后状态 {last.get('status')!r}")

    def close(self) -> None:
        self._client.close()
