from databricks_unity_dxr_integration.config import VolumeConfig
from databricks_unity_dxr_integration.volume import UnityVolumeScanner


def test_volume_scanner_lists_files(tmp_path, monkeypatch):
    base = tmp_path / "Volumes" / "cat" / "sch" / "vol"
    base.mkdir(parents=True)
    (base / "a").write_text("data-a")
    (base / "nested").mkdir()
    (base / "nested" / "b").write_text("data-b")

    config = VolumeConfig(catalog="cat", schema="sch", volume="vol", base_path=str(tmp_path / "Volumes"))
    scanner = UnityVolumeScanner(config)

    files = scanner.list_files()
    assert sorted(file.relative_path for file in files) == ["a", "nested/b"]
