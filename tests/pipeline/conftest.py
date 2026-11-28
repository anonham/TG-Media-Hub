"""Pipeline test fixtures."""

import builtins
import io
import os
from pathlib import Path

import pytest

from app.config import settings


@pytest.fixture
def no_download_file_writes(monkeypatch):
    """Observe write opens during a transfer, including files removed before it finishes."""
    root = Path(settings.DOWNLOAD_DIR).resolve()
    writes = []

    def record(path):
        if not isinstance(path, int) and Path(os.fsdecode(path)).resolve().is_relative_to(root):
            writes.append(os.fsdecode(path))

    def observe_open(original):
        def wrapped(file, mode="r", *args, **kwargs):
            if any(flag in mode for flag in "wax+"):
                record(file)
            return original(file, mode, *args, **kwargs)

        return wrapped

    original_os_open = os.open

    def observe_os_open(path, flags, *args, **kwargs):
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC):
            record(path)
        return original_os_open(path, flags, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(builtins, "open", observe_open(builtins.open))
        patch.setattr(io, "open", observe_open(io.open))
        patch.setattr(os, "open", observe_os_open)
        yield
    assert not writes, f"streaming transfer opened files for writing in DOWNLOAD_DIR: {writes}"
