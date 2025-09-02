"""
pvlib: Lightweight modules for the Purview <-> Data X-Ray integration.

This package exposes focused helpers grouped by concern:
 - config: environment, logging, HTTP session
 - dxr: DXR API calls (tags, datasources, label stats)
 - atlas: Purview data-plane helpers (auth, REST wrappers, entity lookups)
 - typedefs: creation/merge of custom entity/relationship types

These modules are intended to make the integration easier to understand and
extend for third-party developers. The existing purview_dxr_integration.py
entrypoint can be gradually refactored to consume pvlib.
"""

from . import config, dxr, atlas, typedefs  # noqa: F401

__all__ = [
    "config",
    "dxr",
    "atlas",
    "typedefs",
]

