"""Zarr tree descriptor and materialization.

The tree descriptor (GroupNode / ArrayNode) is a pure data structure.
Materialization writes it into any zarr store, so the same tree can be
written to MemoryStore, IcechunkStore, etc. for comparison testing.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

import zarr

# ---------------------------------------------------------------------------
# Tree descriptor
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArrayNode:
    shape: tuple[int, ...]
    dtype: np.dtype


@dataclass(frozen=True)
class GroupNode:
    children: dict[str, ArrayNode | GroupNode] = field(default_factory=dict)

    def _walk(self, prefix: str = ""):
        """Yield ``(path, child)`` for every node, depth-first."""
        for name, child in self.children.items():
            p = f"{prefix}/{name}" if prefix else name
            yield p, child
            if isinstance(child, GroupNode):
                yield from child._walk(p)

    def nodes(self, prefix: str = "") -> list[str]:
        """Return paths of all descendant nodes (excludes root)."""
        return [p for p, _ in self._walk(prefix)]

    def groups(self, prefix: str = "") -> list[str]:
        """Return paths of all descendant group nodes (excludes root)."""
        return [p for p, c in self._walk(prefix) if isinstance(c, GroupNode)]

    def arrays(self, prefix: str = "") -> list[str]:
        """Return paths of all array nodes."""
        return [p for p, c in self._walk(prefix) if isinstance(c, ArrayNode)]


Node = ArrayNode | GroupNode


# ---------------------------------------------------------------------------
# Materialization — write a tree descriptor into any zarr store
# ---------------------------------------------------------------------------


def materialize(tree: GroupNode, store) -> zarr.Group:
    """Write *tree* into *store* and return the root group."""
    root = zarr.open_group(store, mode="w")

    def _materialize_children(group: zarr.Group, node: GroupNode) -> None:
        for name, child in node.children.items():
            if isinstance(child, ArrayNode):
                group.create_array(name, shape=child.shape, dtype=child.dtype)
            else:
                sub = group.create_group(name)
                _materialize_children(sub, child)

    _materialize_children(root, tree)
    return root
