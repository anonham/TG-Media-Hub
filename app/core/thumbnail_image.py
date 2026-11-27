"""Bounded Pillow thumbnail normalization, near-black detection, and poster helpers."""

import io
import logging
from pathlib import Path

from PIL import Image, ImageOps, ImageStat, UnidentifiedImageError

from app.config import settings

logger = logging.getLogger(__name__)

THUMBNAIL_MAX_DIM = 320
THUMBNAIL_PASSTHROUGH_MAX_BYTES = 20_000
THUMBNAIL_HARD_MAX_BYTES = 200_000
THUMBNAIL_MAX_INPUT_BYTES = 8 * 1024 * 1024
# Allows 3840×2160 and limits a full-resolution RGBA buffer to 32 MiB.
THUMBNAIL_MAX_DECODE_PIXELS = 8_388_608

_NEAR_BLACK_MEAN_LUMA = 16.0
_NEAR_BLACK_STD_LUMA = 12.0
_EXIF_ORIENTATION_TAG = 274
_JPEG_QUALITIES = (82, 72, 60, 48, 35)
_IMAGE_FORMATS = ("JPEG", "PNG", "WEBP", "GIF", "BMP")

_POSTER_CACHE: dict[Path, bytes] = {}


def _encode_jpeg_under_limit(img: Image.Image) -> bytes | None:
    """Aim for 20 kB without reducing resolution, with Telegram's hard limit as fallback."""
    smallest = None
    for quality in _JPEG_QUALITIES:
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True)
        encoded = buf.getvalue()
        if len(encoded) <= THUMBNAIL_PASSTHROUGH_MAX_BYTES:
            return encoded
        smallest = encoded
    return smallest if smallest and len(smallest) < THUMBNAIL_HARD_MAX_BYTES else None


def _promote_transparency(img: Image.Image) -> Image.Image:
    """Turn a color key or palette transparency into alpha before resampling."""
    if "transparency" not in img.info:
        return img
    if img.mode != "I;16":
        return img.convert("RGBA")

    # Pillow's RGBA conversion can miss a 16-bit key above 255. Match it at native bit depth.
    alpha_values = [255] * 65536
    alpha_values[img.info["transparency"]] = 0
    with img.convert("I") as samples:
        alpha = samples.point(alpha_values, "L")
    with alpha:
        gray = img.convert("L")
        gray.putalpha(alpha)
    gray.info.pop("transparency", None)
    return gray


def _to_rgb(img: Image.Image) -> Image.Image:
    """Convert any Pillow image mode safely to RGB."""
    if img.mode == "RGB":
        return img
    if img.mode in ("RGBA", "LA"):
        bg = Image.new("RGB", img.size, (0, 0, 0))
        with img.getchannel("A") as alpha:
            bg.paste(img, mask=alpha)
        return bg
    return img.convert("RGB")


def normalize_thumbnail_bytes(
    data: bytes | bytearray | memoryview | None,
    *,
    max_input_bytes: int = THUMBNAIL_MAX_INPUT_BYTES,
) -> bytes | None:
    """Validate, bounded-decode, resize (<=320px), and JPEG-encode thumbnail bytes.

    Returns the original bytes unchanged when already a valid <=320px upright JPEG
    <= 20 kB; otherwise re-encodes to JPEG strictly under 200,000 bytes.
    Returns None on invalid, empty, oversized, or corrupted image data.
    """
    if not isinstance(data, (bytes, bytearray, memoryview)):
        return None
    size = data.nbytes if isinstance(data, memoryview) else len(data)
    if size == 0 or size > max_input_bytes:
        return None
    raw = bytes(data)

    try:
        with Image.open(io.BytesIO(raw), formats=_IMAGE_FORMATS) as img:
            w, h = img.size
            if w <= 0 or h <= 0 or (w * h) > THUMBNAIL_MAX_DECODE_PIXELS:
                return None

            img_format = (img.format or "").upper()
            orientation = img.getexif().get(_EXIF_ORIENTATION_TAG, 1)

            # Force full decode to reject truncated/malformed payloads
            img.load()

            if (
                img_format == "JPEG"
                and img.mode in ("RGB", "L")
                and orientation in (None, 1)
                and w <= THUMBNAIL_MAX_DIM
                and h <= THUMBNAIL_MAX_DIM
                and len(raw) <= THUMBNAIL_PASSTHROUGH_MAX_BYTES
            ):
                return raw

            cover = _promote_transparency(img)
            if cover is not img:
                img.close()
            # EXIF orientation preserves the long edge. Resize before rotating or compositing.
            cover.thumbnail((THUMBNAIL_MAX_DIM, THUMBNAIL_MAX_DIM), Image.Resampling.LANCZOS)
            return _encode_jpeg_under_limit(_to_rgb(ImageOps.exif_transpose(cover)))
    except (OSError, ValueError, SyntaxError, Image.DecompressionBombError):
        return None


def is_near_black_image(data: bytes) -> bool:
    """Detect near-black/flat-dark frames using downsampled grayscale luminance stats."""
    if not data:
        return True
    try:
        with Image.open(io.BytesIO(data)) as img:
            w, h = img.size
            if w <= 0 or h <= 0 or (w * h) > THUMBNAIL_MAX_DECODE_PIXELS:
                return True
            gray = img.convert("L")
            gray.thumbnail((64, 64), Image.Resampling.BILINEAR)
            stat = ImageStat.Stat(gray)
            mean_luma = float(stat.mean[0])
            std_luma = float(stat.stddev[0])
            return mean_luma < _NEAR_BLACK_MEAN_LUMA and std_luma < _NEAR_BLACK_STD_LUMA
    except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombError):
        return True


def load_normalized_placeholder_poster() -> bytes | None:
    """Load and normalize the packaged app/assets/placeholder_poster.jpg (cached)."""
    poster_path = Path(settings.PLACEHOLDER_ASSET_PATH)
    cached = _POSTER_CACHE.get(poster_path)
    if cached is not None:
        return cached

    try:
        raw = poster_path.read_bytes()
    except OSError as exc:
        logger.warning("Failed to read packaged placeholder poster: err_type=%s", type(exc).__name__)
        return None

    normalized = normalize_thumbnail_bytes(raw)
    if normalized is None:
        logger.warning("Packaged placeholder poster failed thumbnail normalization")
        return None

    _POSTER_CACHE.clear()
    _POSTER_CACHE[poster_path] = normalized
    return normalized


def make_named_thumbnail_stream(data: bytes) -> io.BytesIO:
    """Return a fresh seek-0 BytesIO with .name set for Telethon thumbnail uploads."""
    stream = io.BytesIO(data)
    stream.name = "thumbnail.jpg"
    return stream
