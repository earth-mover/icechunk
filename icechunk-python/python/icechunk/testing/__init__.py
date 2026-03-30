"""Testing utilities for icechunk.

This module provides tools for testing icechunk behavior under conditions
that are hard to reproduce with real storage backends, such as high latency.

.. note::

    These utilities are intended for testing only. They should not be used
    in production code.
"""

from icechunk._icechunk_python import LatencyStorage

__all__ = [
    "LatencyStorage",
]
