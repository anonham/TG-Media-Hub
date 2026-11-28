import argparse
import asyncio
import logging
import os
import sys
import time
from typing import Any

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# 将工程根目录加入模块路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.core.client_manager import canonical_phone_number, client_manager
from app.core.fast_telethon import upload_file_parallel
from app.core.scheduler import UploadCancelledError

logger = logging.getLogger("bench_upload")


def resolve_account_client(account_selector: str | None) -> Any:
    """按本地账号 ID、名称或完整国际格式手机号选择客户端。"""
    if not account_selector:
        return client_manager.userbot

    selector = account_selector.strip()
    if selector.isascii() and selector.isdecimal() and int(selector) > 0:
        account = client_manager.get_account(int(selector))
        if account is not None:
            return account.client

    phone = canonical_phone_number(selector) if selector.startswith("+") else None
    matches = [
        account.client
        for account in client_manager._accounts.values()
        if account.account_name.casefold() == selector.casefold()
        or (phone is not None and account.phone and canonical_phone_number(account.phone) == phone)
    ]
    return matches[0] if len(matches) == 1 else None


def _get_file_size_sync(file_path: str) -> int:
    if not os.path.exists(file_path):
        return -1
    return os.path.getsize(file_path)


async def run_single_benchmark(
    client: Any, account_label: str, file_path: str, concurrency: int, in_flight: int, max_mb: float = 0.0
) -> dict[str, Any]:
    file_size = _get_file_size_sync(file_path)
    file_size_mb = file_size / (1024 * 1024)
    target_bytes = int(max_mb * 1024 * 1024) if max_mb > 0 else 0

    setting_tag = f"{concurrency}C * {in_flight}InFlight"
    logger.info("=========================================================================")
    logger.info("🧪 开始测试 Setting: [%s] | 账号: %s", setting_tag, account_label)
    logger.info(
        "⚙️ 参数: 并发连接=%d, 单连接在途=%d, 目标窗口=%.2f MB",
        concurrency,
        in_flight,
        max_mb if max_mb > 0 else file_size_mb,
    )
    logger.info("=========================================================================")

    start_time = time.time()
    last_print = start_time
    peak_speed = 0.0
    speed_samples = []
    last_current_bytes = 0
    stop_triggered = False
    reported_dc = 0

    def progress_callback(current, total, speed, eta, workers, dc_id, workers_total=0):
        nonlocal last_print, peak_speed, last_current_bytes, stop_triggered, reported_dc
        now = time.time()
        last_current_bytes = current
        reported_dc = dc_id
        peak_speed = max(peak_speed, speed)
        if speed > 0:
            speed_samples.append(speed)

        if target_bytes > 0 and current >= target_bytes:
            stop_triggered = True

        if now - last_print >= 0.5 or current == total or stop_triggered:
            percent = (current / total * 100) if total > 0 else 0
            cur_mb = current / (1024 * 1024)
            tot_mb = total / (1024 * 1024)
            active_w = workers if workers is not None else concurrency
            tot_w = workers_total or concurrency
            print(
                f"\r[{setting_tag}] {percent:5.1f}% | {cur_mb:6.2f}/{tot_mb:6.2f} MB | "
                f"实时速度: {speed:5.2f} MB/s (峰值: {peak_speed:5.2f}) | "
                f"通道: {active_w}/{tot_w} | DC: {dc_id}",
                end="",
                flush=True,
            )
            last_print = now

    def check_cancelled():
        return stop_triggered

    metrics_out: dict[str, Any] = {}
    try:
        try:
            await upload_file_parallel(
                client=client,
                file_path=file_path,
                progress_callback=progress_callback,
                concurrency=concurrency,
                get_concurrency=lambda: concurrency,
                in_flight_per_connection=in_flight,
                get_in_flight_per_connection=lambda: in_flight,
                is_cancelled=check_cancelled,
                force_fresh=True,  # 每次测试均全新传输，杜绝历史缓存干扰
                metrics_out=metrics_out,
            )
        except (UploadCancelledError, asyncio.CancelledError):
            if not stop_triggered:
                raise
    except Exception as e:
        print()
        error_type = type(e).__name__
        logger.error("❌ [%s] 测试过程中异常: %s", setting_tag, error_type)
        return {
            "setting": setting_tag,
            "concurrency": concurrency,
            "in_flight": in_flight,
            "error": error_type,
            "avg_speed": 0.0,
            "valley_speed": 0.0,
            "peak_speed": 0.0,
            "p50": 0.0,
            "p90": 0.0,
            "rtt_p50": 0.0,
            "rtt_p90": 0.0,
            "rtt_tp95": 0.0,
            "rtt_tp99": 0.0,
            "uploaded_mb": last_current_bytes / (1024 * 1024),
            "elapsed_s": time.time() - start_time,
            "dc_id": reported_dc,
        }

    print()  # 换行
    elapsed = max(time.time() - start_time, 0.001)
    actual_uploaded_mb = last_current_bytes / (1024 * 1024)
    avg_speed = actual_uploaded_mb / elapsed

    valid_spds = [s for s in speed_samples if s > 0.0]
    valley_speed = min(valid_spds) if valid_spds else 0.0

    p50 = 0.0
    p90 = 0.0
    if speed_samples:
        sorted_spds = sorted(speed_samples)
        p50 = sorted_spds[len(sorted_spds) // 2]
        p90 = sorted_spds[int(len(sorted_spds) * 0.9)]

    rtt_p50 = metrics_out.get("rtt_p50", 0.0)
    rtt_p90 = metrics_out.get("rtt_p90", 0.0)
    rtt_tp95 = metrics_out.get("rtt_tp95", 0.0)
    rtt_tp99 = metrics_out.get("rtt_tp99", 0.0)

    logger.info(
        "🏁 [%s] 阶段测试完成: 上传 %.2f MB | 耗时: %.2fs | "
        "速度(均/谷/峰/P90): %.2f / %.2f / %.2f / %.2f MB/s | "
        "RTT长尾(P50/P90/TP95/TP99): %.0f / %.0f / %.0f / %.0f ms",
        setting_tag,
        actual_uploaded_mb,
        elapsed,
        avg_speed,
        valley_speed,
        peak_speed,
        p90,
        rtt_p50,
        rtt_p90,
        rtt_tp95,
        rtt_tp99,
    )

    return {
        "setting": setting_tag,
        "concurrency": concurrency,
        "in_flight": in_flight,
        "error": None,
        "avg_speed": avg_speed,
        "valley_speed": valley_speed,
        "peak_speed": peak_speed,
        "p50": p50,
        "p90": p90,
        "rtt_p50": rtt_p50,
        "rtt_p90": rtt_p90,
        "rtt_tp95": rtt_tp95,
        "rtt_tp99": rtt_tp99,
        "uploaded_mb": actual_uploaded_mb,
        "elapsed_s": elapsed,
        "dc_id": reported_dc,
    }


def _parse_setting_tuples(args) -> list[tuple[int, int]]:
    """解析并发设置矩阵：显式 --settings 优先，其次 -c/-i 单点参数，未传时使用默认对比矩阵。"""
    raw_settings = args.settings
    if raw_settings is None and args.concurrency is None and args.in_flight is None:
        raw_settings = "4*1,6*1,8*1,8*2"

    if not raw_settings:
        return [
            (
                args.concurrency if args.concurrency is not None else 6,
                args.in_flight if args.in_flight is not None else 1,
            )
        ]

    setting_tuples: list[tuple[int, int]] = []
    for item in raw_settings.split(","):
        token = item.strip()
        if not token:
            continue
        if "*" in token:
            parts = token.split("*")
            setting_tuples.append((int(parts[0]), int(parts[1])))
        else:
            setting_tuples.append((int(token), 1))
    return setting_tuples


async def main_async(args) -> int:
    raw_size = _get_file_size_sync(args.file)
    if raw_size < 0:
        logger.error("❌ 测试文件不存在")
        return 1

    file_size_mb = raw_size / (1024 * 1024)
    setting_tuples = _parse_setting_tuples(args)
    results: list[dict[str, Any]] = []
    try:
        logger.info("🚀 正在初始化 Telegram 连接池...")
        await client_manager.initialize()
        client = resolve_account_client(args.account)
        if not client or not client.is_connected():
            logger.error("❌ 未找到匹配且已连接的 Telegram 账号")
            return 1

        account_label = "所选账号" if args.account else "默认账号"
        dc_id = getattr(getattr(client, "session", None), "dc_id", 0)
        logger.info("👤 测试账号: %s | 数据中心: DC %d", account_label, dc_id)
        logger.info("📁 测试文件大小: %.2f MB", file_size_mb)
        for idx, (c, inf) in enumerate(setting_tuples, 1):
            logger.info("▶️ 正在执行 [%d/%d] 轮测试配置...", idx, len(setting_tuples))
            res = await run_single_benchmark(
                client=client,
                account_label=account_label,
                file_path=args.file,
                concurrency=c,
                in_flight=inf,
                max_mb=args.max_mb,
            )
            results.append(res)
            # 轮次间休眠 2 秒，确保连接安全收割与 Telegram 突发令牌回填
            if idx < len(setting_tuples):
                logger.info("⏳ 轮次冷却 2 秒...")
                await asyncio.sleep(2.0)

        # 输出最终长尾对比分析报告
        print("\n" + "=" * 126)
        print("                            📊 上传性能与长尾延迟对比报告")
        print("=" * 126)
        print(f"👤 测试账号: {account_label} | 数据中心: DC {dc_id}")
        print(f"📦 样本窗口: 约 {args.max_mb if args.max_mb > 0 else file_size_mb:.1f} MB / 测试轮次: {len(results)}")
        print("-" * 126)
        print(
            f"{'配置方案 (Setting)':<18} | {'均速':<9} | {'谷值':<9} | {'峰值':<9} | {'速度P90':<9} | {'RTT P50':<9} | {'RTT P90':<9} | {'RTT TP95':<9} | {'RTT TP99':<9} | {'状态'}"
        )
        print("-" * 126)
        for r in results:
            status_text = "✅ 成功" if not r["error"] else f"❌ {r['error']}"
            print(
                f"{r['setting']:<18} | {r['avg_speed']:>6.2f}MB | {r['valley_speed']:>6.2f}MB | {r['peak_speed']:>6.2f}MB | {r['p90']:>6.2f}MB | "
                f"{r['rtt_p50']:>6.0f}ms | {r['rtt_p90']:>6.0f}ms | {r['rtt_tp95']:>6.0f}ms | {r['rtt_tp99']:>6.0f}ms | {status_text}"
            )
        print("=" * 126 + "\n")
        return int(any(r["error"] for r in results))

    finally:
        await client_manager.disconnect_all()
        logger.info("🔌 Telegram 连接已安全断开并析构")


def main() -> int:
    parser = argparse.ArgumentParser(description="TG-Media-Hub Telegram 上传性能测试")
    parser.add_argument(
        "--file",
        "-f",
        required=True,
        help="待测试的视频文件路径",
    )
    parser.add_argument(
        "--account",
        "-a",
        help="本地账号 ID、账号名称或带 + 的完整国际格式手机号，省略时使用默认账号",
    )
    parser.add_argument(
        "--settings",
        "-s",
        default=None,
        help="并发设置对比矩阵，逗号分隔，如 '4*1,6*1,8*1,8*2' (默认不传且未指定 -c/-i 时启用该矩阵)",
    )
    parser.add_argument(
        "--concurrency", "-c", type=int, default=None, help="单次测试并发通道数 (当未指定 --settings 时生效，默认: 6)"
    )
    parser.add_argument(
        "--in-flight",
        "-i",
        type=int,
        default=None,
        help="单次测试单连接在途分块数 (当未指定 --settings 时生效，默认: 1)",
    )
    parser.add_argument("--max-mb", "-m", type=float, default=50.0, help="每轮测试上传的样本窗口兆字节数 (默认: 50MB)")
    args = parser.parse_args()

    handler = logging.StreamHandler()
    # Telegram 客户端的诊断可能包含账号标识，测速只输出自身的摘要。
    handler.addFilter(logging.Filter(logger.name))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[handler],
        force=True,
    )
    try:
        return asyncio.run(main_async(args))
    except Exception as exc:
        logger.error("❌ 测速失败: %s", type(exc).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
