import pytest


def test_import_dxrclient():
    try:
        from dxrpy import DXRClient
    except ImportError as e:
        pytest.fail(f"ImportError: {e}")


if __name__ == "__main__":
    pytest.main()
    print("Done")
