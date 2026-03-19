"""Zarr tree descriptors, materialization, and Hypothesis strategies.

The tree descriptor (GroupNode / ArrayNode) is a pure data structure.
Materialization writes it into any zarr store, so the same tree can be
written to MemoryStore, IcechunkStore, etc. for comparison testing.

The ``trees`` strategy uses st.recursive for tree structure (good
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


def similar_name(
    non_sibling_names: set[str], sibling_names: set[str]
) -> st.SearchStrategy[str]:
    """Strategy that picks a name similar or the same as existing one.

    Either an affixed variant of any known name (e.g. "foo" → "foo-bar")
    or an exact copy of a non-sibling name.
    """
    all_names = sorted(non_sibling_names | sibling_names)
    non_siblings = sorted(non_sibling_names - sibling_names)

    affixed = st.sampled_from(all_names).flatmap(
        lambda base: st.one_of(
            separators.flatmap(lambda sep: affix.map(lambda afx: base + sep + afx)),
            separators.flatmap(lambda sep: affix.map(lambda afx: afx + sep + base)),
        )
    )
    if non_siblings:
        return st.one_of(affixed, st.sampled_from(non_siblings))
    return affixed


@st.composite
def unique_sibling_names(draw: st.DrawFn, name_pool: set[str], n: int) -> list[str]:
    """Draw *n* names unique among themselves, feeding each into the shared pool."""
    names: set[str] = set()

    for _ in range(n):
        strategy = (
            st.one_of(node_names, similar_name(name_pool, names))
            if name_pool
            else node_names
        ).filter(lambda name_: name_ not in names)
        name = draw(strategy)
        names.add(name)
    return list(names)


# ---------------------------------------------------------------------------
# Tree skeleton + naming
# ---------------------------------------------------------------------------


def skeletons(
    *, max_leaves: int = 50, max_children: int = 4
) -> st.SearchStrategy[GroupNode]:
    """Unnamed tree skeletons via st.recursive.

    Always returns a GroupNode (the root group). Child names are placeholder
    indices ("0", "1", ...); real names are assigned later by ``trees``.
    """
    leaves = st.just(ArrayNode(shape=SHAPE, dtype=DTYPE))

    def extend(children: st.SearchStrategy[Node]) -> st.SearchStrategy[GroupNode]:
        return st.lists(children, min_size=1, max_size=max_children).map(
            lambda child_list: GroupNode(
                children={str(i): child for i, child in enumerate(child_list)}
            )
        )

    # Wrap in extend so the top level is always a GroupNode (the root group).
    return extend(st.recursive(leaves, extend, max_leaves=max_leaves))


@st.composite
def trees(
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
    >>> @given(tree=trees())
    ... def test_something(tree):
    ...     reference = tree.materialize(zarr.storage.MemoryStore())
    ...     under_test = tree.materialize(my_icechunk_store())
    ...     # compare...
    """

    def rebuild_with_names(
        group: GroupNode, name_pool: set[str]
    ) -> tuple[GroupNode, set[str]]:
        new_names = draw(unique_sibling_names(name_pool, len(group.children)))
        name_pool = name_pool | set(new_names)
        children: dict[str, Node] = {}
        for name, child in zip(new_names, group.children.values(), strict=True):
            if isinstance(child, GroupNode):
                child, name_pool = rebuild_with_names(child, name_pool)
            children[name] = child
        return GroupNode(children=children), name_pool

    # Two-step generation: first draw the tree structure (skeletons uses
    # st.recursive which gives good structural shrinking), then assign real
    # names via @composite (which allows pool-based derivation for realistic
    # prefix collisions). Doing both in one step would sacrifice either
    # structural shrinking or name similarity.
    skeleton = draw(
        skeletons(max_leaves=draw(max_leaves), max_children=draw(max_children))
    )
    result, _ = rebuild_with_names(skeleton, set())
    return result
