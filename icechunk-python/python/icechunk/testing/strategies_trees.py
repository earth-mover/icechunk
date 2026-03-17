"""Hypothesis strategy for random zarr tree descriptors.

st.recursive generates an unnamed skeleton using GroupNode/ArrayNode directly
(with placeholder index keys), then a @composite pass assigns real names
with pool-based prefix collisions (e.g. ``EC-Earth3`` / ``EC-Earth3-Veg``).

This gives st.recursive's optimized structural shrinking for tree shape,
and @composite's draw-by-draw control for realistic name generation.
"""

from __future__ import annotations

import hypothesis.strategies as st
import numpy as np

import zarr.testing.strategies as zrst
from icechunk.testing.zarr_tree import ArrayNode, GroupNode, Node
from zarr.testing.strategies import node_names

# Fixed dtype/shape — we're testing path handling, not dtype serialization.
DTYPE = np.dtype("i4")
SHAPE = (1,)


# ---------------------------------------------------------------------------
# Name strategies — pool-based derivation for prefix collisions
# ---------------------------------------------------------------------------

# Short affix drawn from the zarr key alphabet for prefix/suffix collisions.
affix = st.text(zrst.zarr_key_chars, min_size=1, max_size=3)
separators = st.sampled_from(["_", "-", "."])


@st.composite
def derived_name(draw: st.DrawFn, pool: list[str]) -> str:
    """Mutate an existing name from the pool."""
    base = draw(st.sampled_from(pool))
    mutation = draw(st.sampled_from(["suffix", "prefix", "compound"]))
    if mutation == "suffix":
        result = base + draw(affix)
    elif mutation == "prefix":
        result = draw(affix) + base
    else:
        other = draw(st.sampled_from(pool))
        result = base + draw(separators) + other
    if result.startswith("__") or result.lower() == "zarr.json":
        result = "x" + result
    return result


@st.composite
def pool_name(draw: st.DrawFn, pool: list[str]) -> str:
    """Fresh name, or derivation of an existing one."""
    use_pool = pool and draw(st.integers(min_value=0, max_value=max(len(pool), 2))) > 0
    if use_pool:
        # Sometimes reuse a name exactly (valid in different parts of the tree),
        # sometimes derive a near-collision.
        if draw(st.booleans()):
            return draw(st.sampled_from(pool))
        return draw(derived_name(pool))
    else:
        return draw(node_names)


@st.composite
def unique_sibling_names(draw: st.DrawFn, pool: list[str], n: int) -> list[str]:
    """Draw *n* names unique among themselves, feeding each into the shared pool."""
    names: list[str] = []
    seen: set[str] = set()
    for _ in range(n):
        name = draw(pool_name(pool).filter(lambda nm, _seen=seen: nm not in _seen))
        names.append(name)
        seen.add(name)
        pool.append(name)
    return names


# ---------------------------------------------------------------------------
# Tree skeleton + naming
# ---------------------------------------------------------------------------


def skeletons(*, max_leaves: int = 50, max_children: int = 4) -> st.SearchStrategy[Node]:
    """Unnamed tree skeletons via st.recursive."""
    leaves = st.just(ArrayNode(shape=SHAPE, dtype=DTYPE))
    return st.recursive(
        leaves,
        lambda children: st.lists(children, min_size=1, max_size=max_children).map(
            lambda cs: GroupNode(children={str(i): c for i, c in enumerate(cs)})
        ),
        max_leaves=max_leaves,
    )


def assign_names(draw: st.DrawFn, node: Node, pool: list[str]) -> Node:
    """Walk a skeleton and replace placeholder keys with pool-derived names."""
    if isinstance(node, ArrayNode):
        return node

    child_nodes = list(node.children.values())
    names = draw(unique_sibling_names(pool, len(child_nodes)))
    return GroupNode(
        children={
            name: assign_names(draw, child, pool)
            for name, child in zip(names, child_nodes, strict=False)
        }
    )


@st.composite
def zarr_trees(
    draw: st.DrawFn,
    *,
    max_leaves: st.SearchStrategy[int] = st.integers(min_value=5, max_value=50),  # noqa: B008
    max_children: st.SearchStrategy[int] = st.integers(min_value=1, max_value=4),  # noqa: B008
) -> GroupNode:
    """Strategy producing a :class:`GroupNode` tree descriptor.

    Uses st.recursive for the tree structure (good structural shrinking)
    and @composite for name assignment (pool-based prefix collisions).

    Usage::

        @given(tree=zarr_trees())
        def test_something(tree):
            reference = materialize(tree, zarr.storage.MemoryStore())
            under_test = materialize(tree, my_icechunk_store())
            # compare...
    """
    skeleton = draw(
        skeletons(max_leaves=draw(max_leaves), max_children=draw(max_children))
    )
    pool: list[str] = []
    node = assign_names(draw, skeleton, pool)

    if isinstance(node, ArrayNode):
        node = GroupNode(children={"data": node})

    return node
