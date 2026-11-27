"""
ISO-BMFF Exception Hierarchy.

These exceptions govern syntax/parsing errors and non-standard MP4 layout
detections that trigger automatic fallback to the fMP4 universal pipeline.
"""


class IsoBmffError(Exception):
    """Base exception for all ISO-BMFF parsing and synthesis errors."""


class IsoBmffParseError(IsoBmffError):
    """Raised when an ISO-BMFF box structure is malformed, corrupted, or truncated."""


class UnsupportedMp4LayoutError(IsoBmffError):
    """
    Base exception for non-standard MP4 layouts that cannot be safely processed
    by the VSSE single-envelope reconstruction engine.

    Catching this exception triggers seamless fallback to the fMP4 universal channel.
    """


class FragmentedMp4Error(UnsupportedMp4LayoutError):
    """Raised when fragmented MP4 is detected (e.g., mvex or moof box present)."""


class MultipleMdatError(UnsupportedMp4LayoutError):
    """Raised when multiple mdat boxes or discontinuous media data are detected."""


class CompactSampleSizeError(UnsupportedMp4LayoutError):
    """Raised when stz2 (compact sample size) box is detected."""


class UnsupportedCodecError(UnsupportedMp4LayoutError):
    """Raised when an unsupported codec is detected (e.g. non-AAC audio or non-AVC/HEVC video)."""


class NonInterleavedMp4Error(UnsupportedMp4LayoutError):
    """Raised when audio and video tracks are physically separated (non-interleaved) causing envelope explosion."""


__all__ = [
    "CompactSampleSizeError",
    "FragmentedMp4Error",
    "IsoBmffError",
    "IsoBmffParseError",
    "MultipleMdatError",
    "NonInterleavedMp4Error",
    "UnsupportedCodecError",
    "UnsupportedMp4LayoutError",
]
