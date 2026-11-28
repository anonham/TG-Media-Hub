"""生成发布占位卡静态海报资产。

产物: app/assets/placeholder_poster.jpg —— 1280×720 (16:9) JPEG ≤40KB,
暗底 + 居中几何沙漏图标 + 大号清晰文字。随应用分发, 运行时零自愈写回。

使用方式 (pillow>=13.0.0 已由 pyproject.toml 管理，直接通过项目虚拟环境运行):
    uv run python scripts/generate_placeholder_poster.py
Pillow 对彩色 emoji (⏳) 渲染不可靠 → 图标用几何绘制, 文字用系统字体。
"""

import argparse
import os
import sys

from PIL import Image, ImageDraw, ImageFont

W, H = 1280, 720
BG_TOP = (32, 36, 43)
BG_BOTTOM = (24, 27, 33)
ACCENT = (86, 98, 118)
FG = (205, 211, 222)
OUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app", "assets", "placeholder_poster.jpg"
)


def _font(size: int):
    cjk_candidates = (
        r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\msyhbd.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
        "/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
    )
    for candidate in cjk_candidates:
        if os.path.exists(candidate):
            try:
                return ImageFont.truetype(candidate, size)
            except Exception as exc:
                print(f"WARNING: 无法加载字体 {candidate}: {exc}", file=sys.stderr)
                continue
    for fallback in ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",):
        if os.path.exists(fallback):
            try:
                print(
                    f"WARNING: 未找到 CJK 字体，回退至 {fallback}（中文字符可能无法正确渲染）",
                    file=sys.stderr,
                )
                return ImageFont.truetype(fallback, size)
            except Exception as exc:
                print(f"WARNING: 无法加载回退字体 {fallback}: {exc}", file=sys.stderr)
                continue
    print("WARNING: 未找到可用 TrueType 字体，回退至 Pillow 默认字体（中文字符将无法渲染）", file=sys.stderr)
    return ImageFont.load_default()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="generate_placeholder_poster.py",
        description="生成发布占位卡静态海报资产 (1280x720 JPEG <= 40 KB)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=OUT,
        help=f"输出 JPEG 路径 (默认: {OUT})",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    out_path = args.output
    img = Image.new("RGB", (W, H), BG_TOP)
    draw = ImageDraw.Draw(img)
    # 纵向微渐变背景
    for y in range(H):
        t = y / H
        color = tuple(int(BG_TOP[i] + (BG_BOTTOM[i] - BG_TOP[i]) * t) for i in range(3))
        draw.line([(0, y), (W, y)], fill=color)
    # 居中几何沙漏图标 (两条对称梯形 + 中柱, 避开彩色 emoji 渲染不可靠问题)
    cx, cy = W / 2, H * 0.40
    bar_w, half_h, bar_h = 150, 95, 16
    draw.polygon(
        [(cx - bar_w, cy - half_h), (cx + bar_w, cy - half_h), (cx, cy), (cx, cy)],
        outline=ACCENT,
        width=6,
    )
    draw.polygon(
        [(cx - bar_w, cy + half_h), (cx + bar_w, cy + half_h), (cx, cy), (cx, cy)],
        outline=ACCENT,
        width=6,
    )
    draw.rectangle([cx - bar_w - 24, cy - half_h - bar_h, cx + bar_w + 24, cy - half_h], fill=ACCENT)
    draw.rectangle([cx - bar_w - 24, cy + half_h, cx + bar_w + 24, cy + half_h + bar_h], fill=ACCENT)
    # 主文案 + 副文案
    draw.text((cx, H * 0.68), "媒体补发中", font=_font(72), fill=FG, anchor="mm")
    draw.text((cx, H * 0.80), "任务重试后将在原位刷新为正式媒体", font=_font(36), fill=(150, 157, 170), anchor="mm")
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    quality = 88
    while quality >= 60:  # ≤40KB 硬指标, 逐级降质直至达标
        img.save(out_path, "JPEG", quality=quality, optimize=True)
        if os.path.getsize(out_path) <= 40 * 1024:
            break
        quality -= 6
    size_kb = os.path.getsize(out_path) / 1024
    print(f"placeholder poster written: {out_path} ({size_kb:.1f} KB, quality={quality})")
    return 0 if size_kb <= 40 else 1


if __name__ == "__main__":
    sys.exit(main())
