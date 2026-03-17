"""Zarr tree descriptors, materialization, and Hypothesis strategies.

The tree descriptor (GroupNode / ArrayNode) is a pure data structure.
Materialization writes it into any zarr store, so the same tree can be
written to MemoryStore, IcechunkStore, etc. for comparison testing.

The ``zarr_trees_strategy`` strategy uses st.recursive for tree structure (good
structural shrinking) and @composite for pool-based name assignment
that produces realistic prefix collisions (e.g. ``EC-Earth3`` / ``EC-Earth3-Veg``).
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field

import hypothesis.strategies as st
import numpy as np

import zarr
import zarr.abc.store
import zarr.testing.strategies as zrst
from zarr.testing.strategies import node_names

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

    def _walk(self, prefix: str = "") -> Iterator[tuple[str, Node]]:
        """Yield ``(path, child)`` for every node, depth-first."""
        for name, child in self.children.items():
            p = f"{prefix}/{name}" if prefix else name
            yield p, child
            if isinstance(child, GroupNode):
                yield from child._walk(p)

    def nodes(self, prefix: str = "", *, include_root: bool = False) -> list[str]:
        """Return paths of all nodes, optionally including root."""
        root = [prefix] if include_root else []
        return root + [p for p, _ in self._walk(prefix)]

    def groups(self, prefix: str = "", *, include_root: bool = False) -> list[str]:
        """Return paths of all group nodes, optionally including root."""
        root = [prefix] if include_root else []
        return root + [p for p, c in self._walk(prefix) if isinstance(c, GroupNode)]

    def arrays(self, prefix: str = "") -> list[str]:
        """Return paths of all array nodes."""
        return [p for p, c in self._walk(prefix) if isinstance(c, ArrayNode)]

    def materialize(self, store: zarr.abc.store.Store) -> zarr.Group:
        """Write this tree into *store* and return the root group."""
        root = zarr.open_group(store, mode="w")

        def _write(group: zarr.Group, node: GroupNode) -> None:
            for name, child in node.children.items():
                if isinstance(child, ArrayNode):
                    group.create_array(name, shape=child.shape, dtype=child.dtype)
                else:
                    _write(group.create_group(name), child)

        _write(root, self)
        return root


Node = ArrayNode | GroupNode


# ---------------------------------------------------------------------------
# Name strategies — pool-based derivation for prefix collisions
# ---------------------------------------------------------------------------

# Fixed dtype/shape — we're testing path handling, not dtype serialization.
DTYPE = np.dtype("i4")
SHAPE = (1,)

# Short affix drawn from the zarr key alphabet for prefix/suffix collisions.
affix = st.text(zrst.zarr_key_chars, min_size=1, max_size=3)
separators = st.sampled_from(["_", "-", "."])


@st.composite
def derived_name(draw: st.DrawFn, name_pool: list[str]) -> str:
    """Mutate an existing name from the pool by adding an affix or compounding."""
    base = draw(st.sampled_from(name_pool))
    branch = draw(st.integers(min_value=0, max_value=2))
    if branch == 0:
        result = base + draw(affix)
    elif branch == 1:
        result = draw(affix) + base
    else:
        other = draw(st.sampled_from(name_pool))
        result = base + draw(separators) + other
    if result.startswith("__") or result.lower() == "zarr.json":
        result = "x" + result
    return result


@st.composite
def unique_sibling_names(draw: st.DrawFn, name_pool: list[str], n: int) -> list[str]:
    """Draw *n* names unique among themselves, feeding each into the shared pool."""
    names: list[str] = []
    seen: set[str] = set()
    for _ in range(n):
        strategy = (
            st.one_of(node_names, derived_name(name_pool)) if name_pool else node_names
        )
        name = draw(strategy)
        # On collision, fall back to a guaranteed-fresh name instead of rejection sampling.
        if name in seen:

            def _not_in_seen(nm: str) -> bool:
                return nm not in seen

            name = draw(node_names.filter(_not_in_seen))
        names.append(name)
        seen.add(name)
        name_pool.append(name)
    return names


# ---------------------------------------------------------------------------
# Tree skeleton + naming
# ---------------------------------------------------------------------------


def skeletons(*, max_leaves: int = 50, max_children: int = 4) -> st.SearchStrategy[Node]:
    """Unnamed tree skeletons via st.recursive.

    Produces trees with placeholder index keys ("0", "1", ...) as child names.
    Real names are assigned later by zarr_trees_strategy.
    """
    leaves = st.just(ArrayNode(shape=SHAPE, dtype=DTYPE))

    def extend(children: st.SearchStrategy[Node]) -> st.SearchStrategy[GroupNode]:
        return st.lists(children, min_size=1, max_size=max_children).map(
            lambda child_list: GroupNode(
                children={str(i): child for i, child in enumerate(child_list)}
            )
        )

    return st.recursive(leaves, extend, max_leaves=max_leaves)


@st.composite
def zarr_trees_strategy(
    draw: st.DrawFn,
    *,
    max_leaves: st.SearchStrategy[int] = st.integers(min_value=5, max_value=50),  # noqa: B008
    max_children: st.SearchStrategy[int] = st.integers(min_value=1, max_value=4),  # noqa: B008
) -> GroupNode:
    """Strategy producing a GroupNode tree descriptor.

    Uses st.recursive for the tree structure (good structural shrinking)
    and @composite for name assignment (pool-based prefix collisions).

    Examples
    --------
    >>> @given(tree=zarr_trees_strategy())
    ... def test_something(tree):
    ...     reference = tree.materialize(zarr.storage.MemoryStore())
    ...     under_test = tree.materialize(my_icechunk_store())
    ...     # compare...
    """
    name_pool: list[str] = []

    def assign_names(node: Node) -> Node:
        if isinstance(node, ArrayNode):
            return node
        child_nodes = list(node.children.values())
        names = draw(unique_sibling_names(name_pool, len(child_nodes)))
        return GroupNode(
            children={
                name: assign_names(child)
                for name, child in zip(names, child_nodes, strict=True)
            }
        )

    skeleton = draw(
        skeletons(max_leaves=draw(max_leaves), max_children=draw(max_children))
    )
    node = assign_names(skeleton)

    if isinstance(node, ArrayNode):
        node = GroupNode(children={"data": node})

    return node
