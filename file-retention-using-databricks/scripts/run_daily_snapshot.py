from __future__ import annotations

import sys
from pathlib import Path

from pathlib import Path
import sys


def _resolve_repo_root() -> Path:
    """Best-effort detection of repo root for Databricks Jobs."""
    if "__file__" in globals():  # standard python execution path
        return Path(__file__).resolve().parents[1]
    cwd = Path.cwd().resolve()
    for candidate in [cwd, *cwd.parents]:
        if (candidate / "src" / "query_data_xray").exists():
            return candidate
    raise RuntimeError("Unable to locate repo root; expected 'src/query_data_xray' next to this script")


SRC_DIR = _resolve_repo_root() / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from query_data_xray.job import main  # noqa: E402


if __name__ == "__main__":
    main()
