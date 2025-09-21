"""Command-line entrypoint for running the DXR → Atlan sync."""

from .pipeline import run

if __name__ == "__main__":
    run()
