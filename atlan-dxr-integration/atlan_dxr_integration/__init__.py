"""Atlan ↔ Data X-Ray integration package."""

from __future__ import annotations

def run() -> None:
    """Run the DXR → Atlan sync once."""

    from .pipeline import run as _run

    _run()


__all__ = ["run"]
