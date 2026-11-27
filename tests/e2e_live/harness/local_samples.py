"""本地 V3 用例样本制备 (E2E-LIVE-14~16; ffmpeg 比特率定容, 幂等)。

样本不随任务构建重复生成: 首次生成后常驻缓存目录 (默认 tests/tmp/e2e_live_samples,
E2E_LIVE_LOCAL_SAMPLES 可覆盖), 体积经 4 Mbit/s 视频码率锚定 (提速裁决:
15s → ~7.5 MiB / 8s → ~4 MiB; 上传是 live 链路绝对瓶颈, 样本全面瘦身)。
相册大图以 2000×2000 噪点单帧 JPEG 制备 (每张 ~2-3 MiB, 噪点保证体积; 逐张随机
种子互不相同)。

体积约束 (harness 低阈值注入 E2E_LIVE_MAX_FILE_SIZE_MB=8 / E2E_LIVE_PREMIUM_MAX_FILE_SIZE_MB=12;
本机测试账号当前为非 Premium, 超限谓词命中 MAX 门限):
- "正常发布"文件须 < 8 MiB (v3_a/v3_m ~7.5 MiB / v3_s ~4 MiB);
- "超限"文件须 > 12 MiB (v3_big ~145 MiB, MKV/AC3 兼作 F3s 特征, 零上传成本)。
"""

import os
import shutil
import subprocess
from pathlib import Path

from . import env_gate


def samples_root() -> Path:
    """样本缓存根 (E2E_LIVE_LOCAL_SAMPLES 可覆盖; 默认 tests/tmp/e2e_live_samples)。"""
    override = os.environ.get("E2E_LIVE_LOCAL_SAMPLES", "").strip()
    if override:
        return Path(override)
    return env_gate.REPO_ROOT / "tests" / "tmp" / "e2e_live_samples"


_AUDIO = ["-c:a", "aac", "-b:a", "128k", "-shortest"]
_VIDEO = ["-c:v", "libx264", "-preset", "ultrafast", "-b:v", "4M", "-pix_fmt", "yuv420p"]


def _ffmpeg(target: Path, args: list[str]) -> None:
    proc = subprocess.run(
        ["ffmpeg", "-y", "-v", "error", *args, str(target)],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    if proc.returncode != 0 or not target.exists() or target.stat().st_size == 0:
        raise RuntimeError(f"ffmpeg 样本制备失败 ({target.name}): {proc.stderr[-500:]}")


def _video_args(duration: int) -> list[str]:
    return [
        "-f",
        "lavfi",
        "-i",
        f"testsrc2=size=1280x720:rate=30:duration={duration}",
        "-f",
        "lavfi",
        "-i",
        f"sine=frequency=440:duration={duration}",
        *_AUDIO,
        *_VIDEO,
        "-movflags",
        "+faststart",
    ]


def ensure_v3_base() -> dict[str, Path]:
    """四份基础样本 (幂等): v3_a (正常发布档 ~7.5MB) / v3_m (小文件档 ~7.5MB) /
    v3_s (UI 观测种子档 ~4MB) / v3_big (超限 MKV+AC3)。

    体积约束 (harness 低阈值注入 E2E_LIVE_MAX_FILE_SIZE_MB=8 / E2E_LIVE_PREMIUM_MAX_FILE_SIZE_MB=12,
    提速裁决: 低速网络下上传是绝对瓶颈, 样本全面瘦身):
    - "正常发布"文件须 < 8 MiB (v3_a/v3_m ~7.5 MiB / v3_s ~4 MiB);
    - "超限"文件须 > 12 MiB (v3_big ~145 MiB, MKV/AC3 兼作 F3s 特征, 零上传成本)。
    """
    root = samples_root()
    root.mkdir(parents=True, exist_ok=True)
    out: dict[str, Path] = {}

    a = root / "v3_a.mp4"
    if not (a.exists() and 5 * 1024 * 1024 < a.stat().st_size < 8 * 1024 * 1024):
        _ffmpeg(a, _video_args(15))
    out["a"] = a

    m = root / "v3_m.mp4"
    if not (m.exists() and 5 * 1024 * 1024 < m.stat().st_size < 8 * 1024 * 1024):
        _ffmpeg(m, _video_args(15))
    out["m"] = m

    s = root / "v3_s.mp4"
    if not (s.exists() and s.stat().st_size > 2 * 1024 * 1024):
        _ffmpeg(s, _video_args(8))
    out["s"] = s

    big = root / "v3_big.mkv"
    if not (big.exists() and big.stat().st_size > 130 * 1024 * 1024):
        src = root / "v3_big_src.mp4"
        if not (src.exists() and src.stat().st_size > 130 * 1024 * 1024):
            _ffmpeg(src, _video_args(280))
        _ffmpeg(big, ["-i", str(src), "-c:v", "copy", "-c:a", "ac3", "-b:a", "192k"])
    out["big"] = big
    return out


def ensure_thumb_clips() -> dict[str, Path]:
    """封面用例样本 (幂等, E2E-LIVE-24~25): bright (正常内容 640x360) /
    black (全黑片源 320x240 —— 近黑帧判定 → 占位海报回退的判别锚点)。"""
    root = samples_root()
    root.mkdir(parents=True, exist_ok=True)
    out: dict[str, Path] = {}

    bright = root / "thumb_bright.mp4"
    if not (bright.exists() and bright.stat().st_size > 200_000):
        _ffmpeg(
            bright,
            [
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=640x360:rate=30:duration=6",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=6",
                *_AUDIO,
                *_VIDEO,
                "-movflags",
                "+faststart",
            ],
        )
    out["bright"] = bright

    black = root / "thumb_black.mp4"
    if not (black.exists() and black.stat().st_size > 50_000):
        _ffmpeg(
            black,
            [
                "-f",
                "lavfi",
                "-i",
                "color=c=black:size=320x240:rate=10:duration=6",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=6",
                *_AUDIO,
                *_VIDEO,
                "-movflags",
                "+faststart",
            ],
        )
    out["black"] = black
    return out


def ensure_admission_family() -> dict[str, Path]:
    """准入用例样本 (幂等, E2E-LIVE-26): fake_ts (TypeScript 文本伪装) /
    genuine_ts (真 MPEG-TS, libx264+aac) / m4v (MP4 内容挂 .m4v 后缀)。"""
    root = samples_root()
    root.mkdir(parents=True, exist_ok=True)
    out: dict[str, Path] = {}

    fake = root / "admission_fake.ts"
    payload = b"export interface Foo { bar: string }\n" * 60
    if not (fake.exists() and fake.stat().st_size == len(payload)):
        fake.write_bytes(payload)
    out["fake_ts"] = fake

    genuine = root / "admission_real.ts"
    if not (genuine.exists() and genuine.stat().st_size > 20_000):
        _ffmpeg(
            genuine,
            [
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=320x240:rate=15:duration=4",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=4",
                *_AUDIO,
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-pix_fmt",
                "yuv420p",
                "-f",
                "mpegts",
            ],
        )
    out["genuine_ts"] = genuine

    m4v = root / "admission_clip.m4v"
    if not (m4v.exists() and m4v.stat().st_size > 20_000):
        _ffmpeg(
            m4v,
            [
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=320x240:rate=15:duration=4",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=4",
                *_AUDIO,
                *_VIDEO,
                "-movflags",
                "+faststart",
            ],
        )
    out["m4v"] = m4v
    return out


def ensure_album_jpegs(count: int = 8) -> list[Path]:
    """相册大图 ×N (幂等): 2000×2000 噪点单帧 JPEG, 单张 ~2-3 MiB (须 < 8 MiB
    注入阈值, 否则相册成员会被超限跳过 —— 4000×4000 旧规格已废)。"""
    root = samples_root()
    root.mkdir(parents=True, exist_ok=True)
    outs: list[Path] = []
    for i in range(1, count + 1):
        target = root / f"v3_album_{i:02d}.jpg"
        if not (target.exists() and 1_000_000 < target.stat().st_size < 6_000_000):
            _ffmpeg(
                target,
                [
                    "-f",
                    "lavfi",
                    "-i",
                    "testsrc2=size=2000x2000:rate=1,noise=alls=45:allf=t",
                    "-frames:v",
                    "1",
                    "-q:v",
                    "1",
                ],
            )
        outs.append(target)
    return outs


def copy_into(folder: Path, source: Path, name: str) -> Path:
    """样本复制进任务文件夹 (字节保真: 回填槽身份含 size, 须同字节重放)。"""
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / name
    shutil.copy2(source, target)
    return target
