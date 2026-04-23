"""Zarr tree descriptors, materialization, and Hypothesis strategies.


The ``trees`` strategy uses st.recursive for tree structure (good
structural shrinking) and @composite for pool-based name assignment
that produces realistic prefix collisions (e.g. ``EC-Earth3`` / ``EC-Earth3-Veg``).
"""

from __future__ import annotations

import posixpath
from itertools import combinations
from typing import Literal

import hypothesis.strategies as st
import numpy as np

import zarr.testing.strategies as zrst
from icechunk.testing.models import ArrayNode, GroupNode, Node
from zarr.testing.strategies import node_names

InvalidMoveReason = Literal[
    "self",
    "descendant",
    "missing_parent",
    "parent_not_group",
    "overwrite",
    "source_missing",
]

# ---------------------------------------------------------------------------
# Name strategies — pool-based derivation for prefix collisions
# ---------------------------------------------------------------------------

# Short affix drawn from the zarr key alphabet for prefix/suffix collisions.
affix = st.text(zrst.zarr_key_chars, min_size=1, max_size=3)
separators = st.sampled_from(["_", "-", "."])


def similar_name(
    non_sibling_names: set[str], sibling_names: set[str]
) -> st.SearchStrategy[str]:
    """Strategy that picks a name similar to existing ones, for prefix collisions.

    Either an affixed variant of a sibling name (e.g. ``"foo"`` → ``"foo-bar"``)
    or an exact copy of a non-sibling (e.g. cousin) name.

    Parameters
    ----------
    non_sibling_names : set[str]
        Names from elsewhere in the tree (cousins, ancestors, etc.).
        These may be reused exactly as the generated name.
    sibling_names : set[str]
        Names of nodes at the same level as the one being generated.
        These are used as bases for affixed variants (prefix/suffix collisions).

    Examples
    --------
    Given a tree like::

        /
        ├── alpha/
        │   ├── x
        │   └── y
        └── beta/
            ├── z
            └── ?   ← generating a new name here

    ``sibling_names = {"z"}``, ``non_sibling_names = {"alpha", "x", "y", "beta"}``.
    The strategy might produce ``"z_0"`` (affixed sibling) or ``"x"`` (reused cousin).
    or ``beta`` or a new random name entirely.
    """
    siblings = sorted(sibling_names)
    non_siblings = sorted(non_sibling_names - sibling_names)

    strategies = []
    if bool(siblings):
        # if there are any named siblings we can affix a sibling name
        # choosing to not affix all names in the tree (e.g. cousin names) because
        # that doesn't seem likely to bring a bug, and would expand the search space.
        strategies.append(
            st.sampled_from(siblings).flatmap(
                lambda base: st.one_of(
                    separators.flatmap(
                        lambda sep: affix.map(lambda afx: base + sep + afx)
                    ),
                    separators.flatmap(
                        lambda sep: affix.map(lambda afx: afx + sep + base)
                    ),
                )
            )
        )
    if bool(non_siblings):
        strategies.append(st.sampled_from(non_siblings))
    return st.one_of(*strategies).filter(lambda name: name not in sibling_names)


def fresh_name(pool: set[str], exclude: set[str]) -> st.SearchStrategy[str]:
    """A single-segment name biased toward similarity with ``pool``, not in ``exclude``.

    Falls back to random ``node_names`` when both ``pool`` and ``exclude`` are empty.
    Uniqueness across multiple draws is *not* guaranteed — callers that need
    distinct names must filter or use ``unique_sibling_names``.
    """
    if not pool and not exclude:
        return node_names
    return st.one_of(node_names, similar_name(pool, exclude)).filter(
        lambda n: n not in exclude
    )


@st.composite
def unique_sibling_names(
    draw: st.DrawFn,
    existing_names: set[str],
    num_names: int,
    existing_siblings: set[str] | None = None,
) -> list[str]:
    """Draw *num_names* names from ``fresh_name`` that are unique among themselves
    and not in ``existing_siblings``.

    Parameters
    ----------
    existing_names : set[str]
        Pool of names used to bias the draws toward realistic prefix collisions
        (passed through to ``fresh_name``).
    num_names : int
        Number of unique names to generate.
    existing_siblings : set[str] | None
        Names that must not be reused (e.g. the existing children at a destination).
    """
    generated: set[str] = set()
    initial_exclude = existing_siblings or set()
    for _ in range(num_names):
        generated.add(draw(fresh_name(existing_names, initial_exclude | generated)))
    return list(generated)


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
    leaves = st.just(ArrayNode(shape=(1,), dtype=np.dtype("i4")))

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
        group: GroupNode, existing_names: set[str]
    ) -> tuple[GroupNode, set[str]]:
        new_names = draw(
            unique_sibling_names(existing_names, num_names=len(group.children))
        )
        existing_names = existing_names | set(new_names)
        children: dict[str, Node] = {}
        for name, child in zip(new_names, group.children.values(), strict=True):
            if isinstance(child, GroupNode):
                child, existing_names = rebuild_with_names(child, existing_names)
            children[name] = child
        return GroupNode(children=children), existing_names

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


# ---------------------------------------------------------------------------
# Move strategies
# ---------------------------------------------------------------------------


def child_names_at(parent: str, all_paths: set[str]) -> set[str]:
    """Names of direct children of *parent* in a flat path set.

    >>> sorted(child_names_at("foo", {"foo/a", "foo/b/c", "bar"}))
    ['a', 'b']
    >>> sorted(child_names_at("", {"foo", "bar/baz"}))
    ['bar', 'foo']
    """
    results: set[str] = set()
    prefix = parent + "/" if parent else ""
    for path in all_paths:
        if path.startswith(prefix):
            results.add(path[len(prefix) :].split("/")[0])
    return results


def is_descendant(path: str, ancestor: str) -> bool:
    """True if *path* is a strict descendant of *ancestor*."""
    return path.startswith(ancestor + "/")


# A move paired with the regex pattern its rejection error must match.
# ``None`` indicates a valid move (no expected error).
MixedMove = tuple[str, str, str | None]


def absolute(path: str) -> str:
    """Convert a tree-internal path (no leading slash, root="") to icechunk's `/`-prefixed form."""
    return "/" if path == "" else f"/{path}"


@st.composite
def invalid_move(
    draw: st.DrawFn,
    all_nodes: set[str],
    all_arrays: set[str],
    name_pool: set[str],
) -> tuple[str, str, str]:
    """One invalid move with the regex its rejection error must match."""
    eligible: list[InvalidMoveReason] = ["self", "descendant", "source_missing"]
    # Overwrite needs two distinct nodes neither an ancestor of the other; a
    # pure chain (e.g. /a, /a/b, /a/b/c) fails every pairing.
    if any(not is_descendant(b, a) for a, b in combinations(sorted(all_nodes), 2)):
        eligible.append("overwrite")
    if all_nodes:
        eligible.append("missing_parent")
    # parent_not_group needs an array AND a src that isn't an ancestor of the
    # array (otherwise the descendant check fires first and shadows this one).
    if any(
        n != arr and not is_descendant(arr, n) for arr in all_arrays for n in all_nodes
    ):
        eligible.append("parent_not_group")

    reason = draw(st.sampled_from(eligible))
    match reason:
        case "self":
            s = draw(st.sampled_from(sorted(all_nodes | {""})))
            return s, s, "into itself or its own descendant"
        case "descendant":
            s = draw(st.sampled_from(sorted(all_nodes | {""})))
            leaf = draw(fresh_name(name_pool, child_names_at(s, all_nodes)))
            return s, posixpath.join(s, leaf), "into itself or its own descendant"
        case "overwrite":
            s, d = draw(
                st.tuples(
                    st.sampled_from(sorted(all_nodes)),
                    st.sampled_from(sorted(all_nodes)),
                ).filter(
                    lambda sd: (
                        sd[0] != sd[1]
                        and not is_descendant(sd[1], sd[0])
                        and not is_descendant(sd[0], sd[1])
                    )
                )
            )
            return s, d, "overwrite existing node"
        case "missing_parent":
            s = draw(st.sampled_from(sorted(all_nodes)))
            parent = draw(fresh_name(name_pool, exclude=name_pool))
            leaf = draw(fresh_name(name_pool, exclude=name_pool))
            return s, posixpath.join(parent, leaf), "destination's parent group"
        case "source_missing":
            # Two unique top-level anchors guarantee src != dst. ``src`` may
            # be deeply nested so we exercise the case of a non-existent
            # multi-segment source; ``dst`` doesn't matter since icechunk's
            # source check fires before any destination validation.
            src_anchor, dst = draw(
                unique_sibling_names(name_pool, 2, existing_siblings=name_pool)
            )
            src_extra = draw(
                st.lists(fresh_name(name_pool, exclude=name_pool), max_size=3)
            )
            src = posixpath.join(src_anchor, *src_extra)
            return src, dst, "node not found|could not create path"
        case "parent_not_group":
            array_parent = draw(st.sampled_from(sorted(all_arrays)))
            leaf = draw(fresh_name(name_pool, exclude=name_pool))
            # src must not be the array or an ancestor of it; otherwise the
            # descendant check fires first and shadows parent_not_group.
            src_candidates = sorted(
                n
                for n in all_nodes
                if n != array_parent and not is_descendant(array_parent, n)
            )
            src = draw(st.sampled_from(src_candidates))
            return (
                src,
                posixpath.join(array_parent, leaf),
                "parent .* is an array",
            )


@st.composite
def valid_move(
    draw: st.DrawFn,
    all_nodes: set[str],
    all_groups: set[str],
    name_pool: set[str],
) -> tuple[str, str]:
    """One valid move against the given tree state."""
    source = draw(st.sampled_from(sorted(all_nodes)))
    candidates = sorted(
        g for g in all_groups if g != source and not is_descendant(g, source)
    )
    dest_parent = draw(st.sampled_from(candidates))
    dest_name = draw(fresh_name(name_pool, child_names_at(dest_parent, all_nodes)))
    return source, posixpath.join(dest_parent, dest_name)


@st.composite
def mixed_moves(
    draw: st.DrawFn,
    tree: GroupNode,
    n_moves: st.SearchStrategy[int] = st.integers(min_value=1, max_value=10),  # noqa: B008
    p_invalid: st.SearchStrategy[float] = st.floats(min_value=0.0, max_value=1.0),  # noqa: B008
) -> list[MixedMove]:
    """A sequence of valid and invalid moves combined.

    Each iteration picks invalid with probability ``p_invalid`` (drawn once per
    sequence so hypothesis can shrink it). Valid moves update the tracked
    state; invalid moves don't.
    """
    from icechunk.testing.utils import update_paths_after_move

    all_nodes = set(tree.nodes())
    all_groups = set(tree.groups(include_root=True))
    all_arrays = all_nodes - all_groups
    name_pool = {path.split("/")[-1] for path in all_nodes}

    p = draw(p_invalid)
    roll = st.floats(min_value=0.0, max_value=1.0, exclude_max=True)
    moves: list[MixedMove] = []
    for _ in range(draw(n_moves)):
        if draw(roll) < p:
            src, dst, pattern = draw(invalid_move(all_nodes, all_arrays, name_pool))
            moves.append((absolute(src), absolute(dst), pattern))
        else:
            src, dst = draw(valid_move(all_nodes, all_groups, name_pool))
            moves.append((absolute(src), absolute(dst), None))
            all_arrays, all_groups = update_paths_after_move(
                src, dst, all_arrays, all_groups
            )
            all_nodes = all_arrays | (all_groups - {""})
    return moves


@st.composite
def valid_moves(
    draw: st.DrawFn,
    tree: GroupNode,
    n_moves: st.SearchStrategy[int] = st.integers(min_value=1, max_value=10),  # noqa: B008
) -> list[tuple[str, str]]:
    """A sequence of valid moves; ``mixed_moves`` with all-valid probability."""
    mixed = draw(mixed_moves(tree, n_moves=n_moves, p_invalid=st.just(0.0)))
    return [(src, dst) for src, dst, _ in mixed]


@st.composite
def invalid_moves(
    draw: st.DrawFn,
    tree: GroupNode,
    n_moves: st.SearchStrategy[int] = st.integers(min_value=1, max_value=10),  # noqa: B008
) -> list[tuple[str, str, str]]:
    """A sequence of invalid moves; ``mixed_moves`` with all-invalid probability."""
    mixed = draw(mixed_moves(tree, n_moves=n_moves, p_invalid=st.just(1.0)))
    return [(src, dst, pattern) for src, dst, pattern in mixed if pattern is not None]


@st.composite
def tree_and_valid_moves(
    draw: st.DrawFn,
    max_leaves: st.SearchStrategy[int] | None = None,
    max_children: st.SearchStrategy[int] | None = None,
    n_moves: st.SearchStrategy[int] = st.integers(min_value=1, max_value=10),  # noqa: B008
) -> tuple[GroupNode, list[tuple[str, str]]]:
    """Generate a tree paired with a sequence of valid moves."""
    kwargs = {}
    if max_leaves is not None:
        kwargs["max_leaves"] = max_leaves
    if max_children is not None:
        kwargs["max_children"] = max_children
    tree = draw(trees(**kwargs))
    moves = draw(valid_moves(tree, n_moves=n_moves))
    return tree, moves


@st.composite
def tree_and_invalid_moves(
    draw: st.DrawFn,
    max_leaves: st.SearchStrategy[int] | None = None,
    max_children: st.SearchStrategy[int] | None = None,
    n_moves: st.SearchStrategy[int] = st.integers(min_value=1, max_value=10),  # noqa: B008
) -> tuple[GroupNode, list[tuple[str, str, str]]]:
    """Generate a tree paired with a sequence of invalid moves."""
    kwargs = {}
    if max_leaves is not None:
        kwargs["max_leaves"] = max_leaves
    if max_children is not None:
        kwargs["max_children"] = max_children
    tree = draw(trees(**kwargs))
    moves = draw(invalid_moves(tree, n_moves=n_moves))
    return tree, moves


@st.composite
def tree_and_mixed_moves(
    draw: st.DrawFn,
    max_leaves: st.SearchStrategy[int] | None = None,
    max_children: st.SearchStrategy[int] | None = None,
    n_moves: st.SearchStrategy[int] = st.integers(min_value=1, max_value=10),  # noqa: B008
    p_invalid: st.SearchStrategy[float] = st.floats(min_value=0.0, max_value=1.0),  # noqa: B008
) -> tuple[GroupNode, list[MixedMove]]:
    """Generate a tree paired with a mixed sequence of valid and invalid moves."""
    kwargs = {}
    if max_leaves is not None:
        kwargs["max_leaves"] = max_leaves
    if max_children is not None:
        kwargs["max_children"] = max_children
    tree = draw(trees(**kwargs))
    moves = draw(mixed_moves(tree, n_moves=n_moves, p_invalid=p_invalid))
    return tree, moves
