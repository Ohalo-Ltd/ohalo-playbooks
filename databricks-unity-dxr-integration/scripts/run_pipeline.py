"""Entry point script for running the Databricks Unity DXR integration pipeline.

This script handles path setup and parameter parsing before executing the main job.
It's designed to work in both local and Databricks notebook execution contexts.
"""
import inspect
import os
import sys
from pathlib import Path
from typing import Dict, Iterable

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
    """Add the package root to sys.path if not already importable."""
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


def _parse_job_parameters(argv: list[str]) -> Dict[str, str]:
    """Read Databricks job parameter conventions into env overrides."""
    overrides: Dict[str, str] = {}
    pending_key: str | None = None

    for token in argv:
        if pending_key is None:
            if token.startswith("--"):
                pending_key = token[2:]
            elif "=" in token:
                key, value = token.split("=", 1)
                overrides[key.strip()] = value
            else:
                pending_key = token
        else:
            overrides[pending_key.strip()] = token
            pending_key = None

    if pending_key is not None:
        raise ValueError(f"Missing value for parameter '{pending_key}'.")

    normalized = {key.strip("- ").upper(): value for key, value in overrides.items() if key}
    return normalized


def _apply_job_parameters() -> None:
    """Translate job parameters (CLI args) into environment variables."""
    argv = sys.argv[1:]
    if not argv:
        return

    overrides = _parse_job_parameters(argv)
    if not overrides:
        return

    for key, value in overrides.items():
        if key:
            os.environ[key] = value

    # Remove consumed parameters so downstream code doesn't see them.
    sys.argv = sys.argv[:1]


def main() -> None:
    """Main entry point: setup environment and run the job."""
    _ensure_package_importable()
    _apply_job_parameters()
    
    # Import after path setup is complete
    from databricks_unity_dxr_integration.job import run_job
    
    run_job()


if __name__ == "__main__":
    main()
