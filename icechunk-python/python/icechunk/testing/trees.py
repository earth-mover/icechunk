"""Zarr tree descriptors, materialization, and Hypothesis strategies.


The ``trees`` strategy uses st.recursive for tree structure (good
structural shrinking) and @composite for pool-based name assignment
that produces realistic prefix collisions (e.g. ``EC-Earth3`` / ``EC-Earth3-Veg``).
"""

from __future__ import annotations

import posixpath
import re
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

# Error-message regex fragments that uniquely identify each rejection reason.
# Shared between ``invalid_move`` (which returns one per generated invalid move)
# and ``@example`` seeds in tests, so the strategy and seed values can't drift.
ERR_SELF_OR_DESCENDANT = "into itself or its own descendant"
ERR_OVERWRITE = "overwrite existing node"
ERR_MISSING_PARENT = "destination's parent group"
ERR_SOURCE_MISSING = "node not found|could not create path"
ERR_PARENT_NOT_GROUP = "is an array, not a group"

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

    Parameters
    ----------
    pool : set[str]
        Existing names used to seed ``similar_name`` for realistic prefix
        collisions (affixed variants, reused cousins).
    exclude : set[str]
        Names that must not be drawn — applied as a strategy filter.

    Returns
    -------
    SearchStrategy[str]
        Strategy producing one name. Falls back to plain ``node_names`` when
        both ``pool`` and ``exclude`` are empty.

    Notes
    -----
    Uniqueness across multiple draws is *not* guaranteed. Callers that need
    distinct names should use *fresh_names*.
    """
    if not pool and not exclude:
        return node_names
    return st.one_of(node_names, similar_name(pool, exclude)).filter(
        lambda n: n not in exclude
    )


@st.composite
def fresh_names(draw: st.DrawFn, pool: set[str], exclude: set[str], n: int) -> list[str]:
    """``n`` distinct *fresh_name* draws.

    Parameters
    ----------
    pool : set[str]
        Passed through to *fresh_name* to bias each draw toward
        pool-similar candidates.
    exclude : set[str]
        Names that must not appear in the result.
    n : int
        Length of the returned list.

    Returns
    -------
    list[str]
        ``n`` names, all distinct from each other and from ``exclude``.

    Notes
    -----
    Threads the accumulating set of already-drawn names back into
    *fresh_name* as additional ``exclude`` on each iteration. This is
    load-bearing: ``similar_name`` uses its ``sibling_names`` argument (i.e.
    ``exclude`` here) as the **base** for affixed variants like ``"foo_0"``,
    so feeding prior draws back in is what produces the prefix-collision
    candidates the strategy exists to generate. ``st.lists(..., unique=True)``
    cannot do this — its element strategy is fixed at construction time.
    """
    drawn: set[str] = set()
    for _ in range(n):
        drawn.add(draw(fresh_name(pool, exclude | drawn)))
    return list(drawn)


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
            fresh_names(existing_names, exclude=set(), n=len(group.children))
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


def wrap_tolerant(pattern: str) -> str:
    """Make *pattern* match across miette's ``\\n  | `` continuation wraps.

    miette's ``GraphicalReportHandler`` wraps long lines and inserts a ``  | ``
    continuation prefix, replacing a single space in the source text with
    ``\\n  | ``. Replace each space-run in *pattern* with a regex fragment that
    accepts either form. Other regex syntax in *pattern* (alternation, etc.)
    is left intact.
    """
    return re.sub(r" +", lambda _: r"\s+\|?\s*", pattern)


def _invalid_move_eligibility(
    all_nodes: set[str], all_arrays: set[str]
) -> list[InvalidMoveReason]:
    """Reasons that can be generated against the given tree state.

    Pure function — call once and reuse until the tree state changes.
    """
    # these are always possible - root moves give us the first two
    # and source missing can be anything
    eligible: list[InvalidMoveReason] = ["self", "descendant", "source_missing"]
    # Overwrite needs two distinct nodes neither an ancestor of the other; a
    # pure chain (e.g. /a, /a/b, /a/b/c) fails every pairing.
    if any(not is_descendant(b, a) for a, b in combinations(all_nodes, 2)):
        eligible.append("overwrite")
    if all_nodes:
        eligible.append("missing_parent")
    # parent_not_group needs an array AND a src that isn't an ancestor of the
    # array (otherwise the descendant check fires first and shadows this one).
    if any(
        n != arr and not is_descendant(arr, n) for arr in all_arrays for n in all_nodes
    ):
        eligible.append("parent_not_group")
    return eligible


@st.composite
def invalid_move(
    draw: st.DrawFn,
    all_nodes: set[str],
    all_arrays: set[str],
    name_pool: set[str],
    *,
    eligible: list[InvalidMoveReason] | None = None,
) -> tuple[str, str, str]:
    """One invalid move with the regex its rejection error must match.

    ``eligible`` may be passed pre-computed by callers that draw many
    invalid moves against the same tree state.
    """
    if eligible is None:
        eligible = _invalid_move_eligibility(all_nodes, all_arrays)
    reason = draw(st.sampled_from(eligible))
    match reason:
        case "self":
            s = draw(st.sampled_from(sorted(all_nodes | {""})))
            return s, s, ERR_SELF_OR_DESCENDANT
        case "descendant":
            s = draw(st.sampled_from(sorted(all_nodes | {""})))
            leaf = draw(fresh_name(name_pool, child_names_at(s, all_nodes)))
            return s, posixpath.join(s, leaf), ERR_SELF_OR_DESCENDANT
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
            return s, d, ERR_OVERWRITE
        case "missing_parent":
            s = draw(st.sampled_from(sorted(all_nodes)))
            parent = draw(fresh_name(name_pool, exclude=name_pool))
            leaf = draw(fresh_name(name_pool, exclude=name_pool))
            return s, posixpath.join(parent, leaf), ERR_MISSING_PARENT
        case "source_missing":
            # Two unique top-level anchors guarantee src != dst. ``src`` may
            # be deeply nested so we exercise the case of a non-existent
            # multi-segment source; ``dst`` doesn't matter since icechunk's
            # source check fires before any destination validation.
            src_anchor, dst = draw(fresh_names(name_pool, exclude=name_pool, n=2))
            src_extra = draw(
                st.lists(fresh_name(name_pool, exclude=name_pool), max_size=3)
            )
            src = posixpath.join(src_anchor, *src_extra)
            return src, dst, ERR_SOURCE_MISSING
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
            return src, posixpath.join(array_parent, leaf), ERR_PARENT_NOT_GROUP


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
def valid_and_invalid_moves(
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
    eligible_invalid_moves = _invalid_move_eligibility(all_nodes, all_arrays)
    for _ in range(draw(n_moves)):
        if draw(roll) < p:
            src, dst, pattern = draw(
                invalid_move(
                    all_nodes, all_arrays, name_pool, eligible=eligible_invalid_moves
                )
            )
            moves.append((absolute(src), absolute(dst), pattern))
        else:
            src, dst = draw(valid_move(all_nodes, all_groups, name_pool))
            moves.append((absolute(src), absolute(dst), None))
            all_arrays, all_groups = update_paths_after_move(
                src, dst, all_arrays, all_groups
            )
            all_nodes = all_arrays | (all_groups - {""})
            # valid move mutated state; recompute eligibility
            eligible_invalid_moves = _invalid_move_eligibility(all_nodes, all_arrays)
    return moves


@st.composite
def valid_moves(
    draw: st.DrawFn,
    tree: GroupNode,
    n_moves: st.SearchStrategy[int] = st.integers(min_value=1, max_value=10),  # noqa: B008
) -> list[tuple[str, str]]:
    """A sequence of valid moves; ``mixed_moves`` with all-valid probability."""
    mixed = draw(valid_and_invalid_moves(tree, n_moves=n_moves, p_invalid=st.just(0.0)))
    return [(src, dst) for src, dst, _ in mixed]


@st.composite
def invalid_moves(
    draw: st.DrawFn,
    tree: GroupNode,
    n_moves: st.SearchStrategy[int] = st.integers(min_value=1, max_value=10),  # noqa: B008
) -> list[tuple[str, str, str]]:
    """A sequence of invalid moves; ``mixed_moves`` with all-invalid probability."""
    mixed = draw(valid_and_invalid_moves(tree, n_moves=n_moves, p_invalid=st.just(1.0)))
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
    moves = draw(valid_and_invalid_moves(tree, n_moves=n_moves, p_invalid=p_invalid))
    return tree, moves
