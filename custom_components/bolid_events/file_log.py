"""JSONL logs for Bolid Events."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

EVENT_LOG_MAX_SIZE = 20 * 1024 * 1024
DIAGNOSTIC_LOG_MAX_SIZE = 5 * 1024 * 1024
ROTATED_LOGS = 3


class JsonLinesFile:
    """Small rotating JSON Lines file."""

    def __init__(self, path: Path, max_size: int) -> None:
        self.path = path
        self.max_size = max_size
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Create the file when this log is enabled."""
        async with self._lock:
            await asyncio.to_thread(self._initialize_sync)

    async def write(self, record: dict[str, Any]) -> None:
        """Append one JSON object without blocking Home Assistant's event loop."""
        async with self._lock:
            await asyncio.to_thread(self._write_sync, record)

    def _initialize_sync(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)

    def _write_sync(self, record: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._rotate_if_needed()
        with self.path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str))
            handle.write("\n")

    def _rotate_if_needed(self) -> None:
        if not self.path.exists() or self.path.stat().st_size < self.max_size:
            return

        oldest = self.path.with_name(f"{self.path.name}.{ROTATED_LOGS}")
        if oldest.exists():
            oldest.unlink()

        for index in range(ROTATED_LOGS - 1, 0, -1):
            source = self.path.with_name(f"{self.path.name}.{index}")
            target = self.path.with_name(f"{self.path.name}.{index + 1}")
            if source.exists():
                source.replace(target)

        self.path.replace(self.path.with_name(f"{self.path.name}.1"))


class DisabledJsonLinesFile:
    """No-op sink used when deep diagnostics are disabled."""

    async def initialize(self) -> None:
        return

    async def write(self, record: dict[str, Any]) -> None:
        return


class BolidFileLogs:
    """DEBUG-only event and diagnostic JSONL logs."""

    def __init__(self, event_path: Path | None, diagnostic_path: Path | None) -> None:
        self.events = (
            JsonLinesFile(event_path, EVENT_LOG_MAX_SIZE)
            if event_path is not None
            else DisabledJsonLinesFile()
        )
        self.diagnostics = (
            JsonLinesFile(diagnostic_path, DIAGNOSTIC_LOG_MAX_SIZE)
            if diagnostic_path is not None
            else DisabledJsonLinesFile()
        )

    async def initialize(self) -> None:
        await self.events.initialize()
        await self.diagnostics.initialize()
