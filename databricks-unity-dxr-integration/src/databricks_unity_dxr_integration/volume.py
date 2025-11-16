from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

from .config import VolumeConfig


@dataclass(frozen=True)
class VolumeFile:
    """Represents a file stored in a Unity Catalog volume."""

    absolute_path: str
    relative_path: str
    size_bytes: int
    modification_time: int

    @property
    def upload_name(self) -> str:
        return self.relative_path.replace(os.sep, "/")


class UnityVolumeScanner:
    """Utility that walks a Unity Catalog volume mounted in the Databricks workspace."""

    def __init__(self, config: VolumeConfig):
        self._config = config

    def list_files(self) -> List[VolumeFile]:
        """Return all files inside the configured volume (optionally scoped by prefix)."""
        root = Path(self._config.root_path)
        files: List[VolumeFile] = []
        for path in self._walk(root):
            if not path.is_file():
                continue
            relative = path.relative_to(root).as_posix()
            stat = path.stat()
            files.append(
                VolumeFile(
                    absolute_path=str(path),
                    relative_path=relative,
                    size_bytes=stat.st_size,
                    modification_time=int(stat.st_mtime),
                )
            )
        return files

    def _walk(self, start: Path) -> Iterator[Path]:
        stack = [start]
        while stack:
            current = stack.pop()
            if not current.exists():
                continue
            if current.is_file():
                yield current
                continue
            try:
                for child in current.iterdir():
                    stack.append(child)
            except PermissionError:
                continue
