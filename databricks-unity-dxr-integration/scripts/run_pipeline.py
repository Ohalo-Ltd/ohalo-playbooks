import sys
from pathlib import Path
from typing import Iterable

PACKAGE_NAME = "databricks_unity_dxr_integration"


def _possible_package_roots(script_path: Path) -> Iterable[Path]:
    """Yield candidate sys.path entries that may contain the package."""
    for parent in script_path.parents:
        yield parent / "src"
        yield parent


def _ensure_package_importable() -> None:
    script_path = Path(__file__).resolve()
    for candidate in _possible_package_roots(script_path):
        package_dir = candidate / PACKAGE_NAME
        if package_dir.exists():
            sys.path.insert(0, str(candidate))
            return
    raise ModuleNotFoundError(
        f"Unable to locate package '{PACKAGE_NAME}'. Expected to find it relative to {script_path}. "
        "Ensure the repo is synced to Databricks with the 'src' directory present."
    )


_ensure_package_importable()

from databricks_unity_dxr_integration.job import run_job  # noqa: E402


if __name__ == "__main__":
    run_job()
