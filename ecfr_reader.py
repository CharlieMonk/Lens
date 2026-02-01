"""Backwards compatibility shim - use ecfr.ECFRDatabase instead."""

from ecfr import ECFRDatabase

# Backwards compatibility alias
ECFRReader = ECFRDatabase

__all__ = ["ECFRReader", "ECFRDatabase"]
