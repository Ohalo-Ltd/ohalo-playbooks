import inspect
import sys
from pathlib import Path
from typing import Iterable

PACKAGE_NAME = "databricks_unity_dxr_integration"


def _possible_package_roots(script_path: Path) -> Iterable[Path]:
    """Yield candidate sys.path entries that may contain the package."""
    seen: set[Path] = set()
    for parent in [script_path] + list(script_path.parents):
        for candidate in (parent / "src", parent):
            candidate = candidate.resolve()
            if candidate in seen:
                continue
            seen.add(candidate)
            yield candidate


def _discover_script_path() -> Path:
    """Best effort path detection for Databricks notebook exec contexts."""
    if "__file__" in globals():
        return Path(__file__).resolve()  # type: ignore[name-defined]

    frame = inspect.currentframe()
    while frame:
        filename = frame.f_code.co_filename
        if filename and filename not in ("<stdin>", "<string>"):
            return Path(filename).resolve()
        frame = frame.f_back

    if sys.argv and sys.argv[0]:
        return Path(sys.argv[0]).resolve()

    return Path.cwd()


def _ensure_package_importable() -> None:
    script_path = _discover_script_path()
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
