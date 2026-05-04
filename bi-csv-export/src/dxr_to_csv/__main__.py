"""Allow ``python -m dxr_to_csv`` as an alternative entry point."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "scripts"))

from export_to_csv import main  # noqa: E402

main()
