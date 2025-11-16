from databricks_unity_dxr_integration.job import plan_batches
from databricks_unity_dxr_integration.volume import VolumeFile


def test_plan_batches_respects_byte_budget():
    files = [
        VolumeFile("/Volumes/cat/sch/vol/file1", "file1", 5, 1),
        VolumeFile("/Volumes/cat/sch/vol/file2", "file2", 4, 1),
        VolumeFile("/Volumes/cat/sch/vol/file3", "file3", 6, 1),
    ]

    batches = plan_batches(files, max_bytes=10)

    assert len(batches) == 2
    assert [f.relative_path for f in batches[0]] == ["file1", "file2"]
    assert [f.relative_path for f in batches[1]] == ["file3"]


def test_plan_batches_raises_when_large_file():
    files = [VolumeFile("/Volumes/cat/sch/vol/large", "large", 50, 1)]

    try:
        plan_batches(files, max_bytes=10)
    except ValueError as exc:
        assert "exceeds" in str(exc)
    else:
        raise AssertionError("Expected large file to raise ValueError")
