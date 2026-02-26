"""RsLogic package root."""

from importlib.metadata import version as _pkg_version

__all__ = ["__version__"]

try:
    __version__ = _pkg_version("rslogic")
except Exception:
    __version__ = "0.1.0"
