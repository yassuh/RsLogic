"""Backwards-compatible top-level config module.

Historically callers imported ``CONFIG`` from a top-level ``config`` module.
The package now owns canonical configuration at :mod:`rslogic.config`, but this
file preserves compatibility for existing non-package imports.
"""

from rslogic.config import CONFIG

__all__ = ["CONFIG"]
