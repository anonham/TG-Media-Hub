"""Deterministic async waiting utilities for tests (replacing fixed blind sleeps)."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any


class _ModuleProxy:
    """Delegate unpatched attributes to an underlying stdlib module."""

    def __init__(self, target: Any, **overrides: Any) -> None:
        self._target = target
        self._overrides = overrides

    def __getattr__(self, name: str) -> Any:
        if name in self._overrides:
            return self._overrides[name]
        return getattr(self._target, name)


class FakeClock:
    """Deterministic virtual clock + non-blocking sleep proxy scoped to a module."""

    def __init__(
        self,
        initial: float = 0.0,
        *,
        on_async_sleep: Callable[[float, int], None] | None = None,
    ) -> None:
        self.now = initial
        self.sleep_calls: list[float] = []
        self._on_async_sleep = on_async_sleep

    def monotonic(self) -> float:
        return self.now

    def perf_counter(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)
        self.now += max(0.0, seconds)

    async def async_sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)
        self.now += max(0.0, seconds)
        if self._on_async_sleep is not None:
            self._on_async_sleep(seconds, len(self.sleep_calls))
        await asyncio.sleep(0)

    def advance(self, seconds: float) -> None:
        self.now += max(0.0, seconds)

    def time_namespace(self) -> _ModuleProxy:
        return _ModuleProxy(time, monotonic=self.monotonic, perf_counter=self.perf_counter, sleep=self.sleep)

    def asyncio_namespace(self) -> _ModuleProxy:
        return _ModuleProxy(asyncio, sleep=self.async_sleep)


async def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout_s: float = 2.0,
    interval: float = 0.005,
    message: str = "Condition was not met before timeout",
) -> None:
    """Poll predicate until truthy or raise AssertionError after timeout_s."""
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError(f"{message} (timeout={timeout_s:.2f}s)")
        await asyncio.sleep(interval)
