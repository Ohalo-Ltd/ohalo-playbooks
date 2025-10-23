from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Optional

from databricks.sdk import WorkspaceClient

from .config import DatabricksConfig


@dataclass
class VolumeFile:
    path: str
    size: int
    modification_time: int


class DatabricksVolumeClient:
    """Thin wrapper around the Databricks SDK for working with Unity Catalog volumes."""

    def __init__(self, config: DatabricksConfig, workspace_client: Optional[WorkspaceClient] = None):
        self._config = config
        self._workspace = workspace_client or WorkspaceClient(
            host=config.host,
            token=config.token,
        )

    @property
    def source_volume_uri(self) -> str:
        return f"/Volumes/{self._config.catalog}/{self._config.schema}/{self._config.source_volume}"

    @property
    def workspace(self) -> WorkspaceClient:
        return self._workspace

    def list_files(self, prefix: Optional[str] = None) -> Iterable[VolumeFile]:
        """List files in the configured volume."""
        base_path = self.source_volume_uri
        if prefix:
            base_path = f"{base_path.rstrip('/')}/{prefix.lstrip('/')}"

        yield from self._walk_files(base_path)

    def read_file(self, path: str) -> bytes:
        """Download file content as bytes."""
        download = self._workspace.files.download(path)
        contents = getattr(download, "contents", None)

        if contents is None:
            raise RuntimeError("Databricks SDK download response did not include contents.")

        if isinstance(contents, (bytes, bytearray)):
            return bytes(contents)

        if isinstance(contents, Iterable):
            try:
                iterator = iter(contents)
            except TypeError:
                iterator = None
            if iterator is not None:
                data = bytearray()
                for chunk in iterator:
                    if not chunk:
                        continue
                    data.extend(chunk)
                return bytes(data)

        if hasattr(contents, "read"):
            return contents.read()

        if callable(contents):
            # Some SDK versions expose contents as a generator function.
            data = bytearray()
            for chunk in contents():
                data.extend(chunk)
            return bytes(data)

        if hasattr(download, "read") and callable(download.read):
            return download.read()

        raise TypeError(f"Unsupported download contents type: {type(contents)}")

    def ensure_label_volume(self, label: str) -> str:
        """Ensure a Unity Catalog volume exists for the given label. Returns the volume URI."""
        # TODO: Call Unity Catalog Volumes API to create or fetch the destination volume.
        return ""

    def copy_file_to_label_volume(self, source_path: str, label: str) -> None:
        """Copy the given file into the volume that corresponds to the supplied label."""
        raise NotImplementedError("Copy logic not implemented yet.")

    def _walk_files(self, root_path: str) -> Iterator[VolumeFile]:
        """Depth-first traversal of files starting at the provided root path."""
        stack = [root_path]
        while stack:
            current_path = stack.pop()
            entries = self._workspace.files.list_directory_contents(current_path)
            for file_info in entries:
                path = getattr(file_info, "path", None)
                is_dir = getattr(file_info, "is_dir", False)
                if is_dir and path:
                    stack.append(path)
                    continue

                if not path:
                    continue

                yield VolumeFile(
                    path=path,
                    size=getattr(file_info, "file_size", 0) or 0,
                    modification_time=getattr(file_info, "modification_time", 0) or 0,
                )
