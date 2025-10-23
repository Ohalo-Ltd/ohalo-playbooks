from __future__ import annotations

from databricks_unity_dxr_integration.config import DatabricksConfig
from databricks_unity_dxr_integration.databricks_client import DatabricksVolumeClient, VolumeFile


class StubFileInfo:
    def __init__(self, path: str, is_dir: bool, file_size: int | None = None, modification_time: int | None = None):
        self.path = path
        self.is_dir = is_dir
        self.file_size = file_size
        self.modification_time = modification_time


class StubFilesAPI:
    def __init__(self, listings: dict[str, list[StubFileInfo]], downloads: dict[str, object]):
        self._listings = listings
        self._downloads = downloads

    def list(self, path: str):
        return self._listings.get(path, [])

    def download(self, path: str):
        return self._downloads[path]


class StubWorkspaceClient:
    def __init__(self, files_api: StubFilesAPI):
        self.files = files_api


def build_client(listings: dict[str, list[StubFileInfo]], downloads: dict[str, object]) -> DatabricksVolumeClient:
    config = DatabricksConfig(
        host="https://workspace",
        token="token",
        catalog="cat",
        schema="sch",
        source_volume="vol",
        label_volume_prefix="label_",
    )
    workspace = StubWorkspaceClient(StubFilesAPI(listings, downloads))
    return DatabricksVolumeClient(config, workspace_client=workspace)


def test_list_files_recurses_directories():
    root = "/Volumes/cat/sch/vol"
    listings = {
        root: [
            StubFileInfo(path=f"{root}/folder", is_dir=True),
            StubFileInfo(path=f"{root}/file1.txt", is_dir=False, file_size=10, modification_time=1),
        ],
        f"{root}/folder": [
            StubFileInfo(path=f"{root}/folder/file2.txt", is_dir=False, file_size=20, modification_time=2),
        ],
    }
    downloads = {}
    client = build_client(listings, downloads)

    files = list(client.list_files())
    assert files == [
        VolumeFile(path=f"{root}/file1.txt", size=10, modification_time=1),
        VolumeFile(path=f"{root}/folder/file2.txt", size=20, modification_time=2),
    ]


def test_read_file_handles_bytes():
    root = "/Volumes/cat/sch/vol"
    listings = {root: []}
    downloads = {
        f"{root}/file.bin": StubDownloadResponse(b"payload"),
    }
    client = build_client(listings, downloads)

    assert client.read_file(f"{root}/file.bin") == b"payload"


def test_read_file_handles_stream():
    root = "/Volumes/cat/sch/vol"
    listings = {root: []}

    def generator():
        yield b"part1"
        yield b"part2"

    downloads = {
        f"{root}/file.bin": StubDownloadResponse(generator()),
    }
    client = build_client(listings, downloads)

    assert client.read_file(f"{root}/file.bin") == b"part1part2"


class StubDownloadResponse:
    def __init__(self, contents):
        self.contents = contents
