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
from typing import Any

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

# Fixed dtype/shape — we're testing path handling, not dtype serialization.
DTYPE = np.dtype("i4")
SHAPE = (1,)


def tree_from_str(spec: str) -> GroupNode:
    """Build a GroupNode tree from a compact string specification.

    Each line is ``"a: /path"`` for an array or ``"g: /path"`` for a group.
    Intermediate groups are created implicitly.

    Example::

        tree_from_str('''
            a: /foo/bar/array
            g: /foo/baz
            g: /qux
        ''')
    """
    # Nested dict built incrementally; values are ArrayNode or sub-dicts.
    tree: dict[str, Any] = {}

    for line in spec.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        kind, _, path = line.partition(":")
        kind = kind.strip().lower()
        path = path.strip().strip("/")
        parts = path.split("/")

        current = tree
        for part in parts[:-1]:
            current = current.setdefault(part, {})

        leaf_name = parts[-1]
        if kind == "a":
            current[leaf_name] = ArrayNode(shape=SHAPE, dtype=DTYPE)
        elif kind == "g":
            current.setdefault(leaf_name, {})
        else:
            raise ValueError(f"unknown node kind '{kind}', expected 'a' or 'g'")

    def _to_group(d: dict[str, Any]) -> GroupNode:
        children: dict[str, Node] = {}
        for name, value in d.items():
            if isinstance(value, ArrayNode):
                children[name] = value
            else:
                children[name] = _to_group(value)
        return GroupNode(children=children)

    return _to_group(tree)


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


@st.composite
def unique_sibling_names(
    draw: st.DrawFn,
    existing_names: set[str],
    num_names: int,
    existing_siblings: set[str] | None = None,
) -> list[str]:
    """Draw *num_names* unique names, biased toward collisions with existing ones.

    Parameters
    ----------
    existing_names : set[str]
        All names already present in the tree. Used to generate
        similar-looking candidates (affixed siblings, reused cousins).
    num_names : int
        Number of unique names to generate.
    existing_siblings : set[str] | None
        Names already present at the destination that must not be reused.
        Used by valid_moves to avoid collisions with existing children.

    Returns
    -------
    list[str]
        The generated names, unique among themselves and not in existing_siblings.
    """
    generated_names: set[str] = set()
    already_taken = existing_siblings or set()

    for _ in range(num_names):
        excluded = generated_names | already_taken
        # Filter the whole strategy — similar_name can produce collisions.
        generated_names.add(
            draw(
                (
                    st.one_of(node_names, similar_name(existing_names, excluded))
                    if bool(existing_names) | bool(generated_names)
                    else node_names
                ).filter(lambda name_, ex=excluded: name_ not in ex)  # type: ignore[misc]
            )
        )
    return list(generated_names)


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


@st.composite
def valid_moves(
    draw: st.DrawFn,
    tree: GroupNode,
    n_moves: st.SearchStrategy[int] = st.just(1),  # noqa: B008
) -> list[tuple[str, str]]:
    """Generate a sequence of valid moves for a tree.

    Each move updates the tracked paths so subsequent moves see the new state
    (e.g. moving into an already-moved group, or moving the same node again).
    """
    from icechunk.testing.utils import update_paths_after_move

    all_nodes = set(tree.nodes())
    all_groups = set(tree.groups(include_root=True))
    all_arrays = all_nodes - all_groups
    name_pool = {path.split("/")[-1] for path in all_nodes}

    num_moves = draw(n_moves)
    moves: list[tuple[str, str]] = []
    for _ in range(num_moves):
        if not all_nodes:
            break

        source = draw(st.sampled_from(sorted(all_nodes)))

        source_and_descendants = {source} | {
            g for g in all_groups if g.startswith(source + "/")
        }
        dest_parent = draw(st.sampled_from(sorted(all_groups - source_and_descendants)))

        existing_siblings = child_names_at(dest_parent, all_nodes)
        (dest_name,) = draw(
            unique_sibling_names(name_pool, 1, existing_siblings=existing_siblings)
        )

        dest = f"{dest_parent}/{dest_name}" if dest_parent else dest_name
        moves.append((source, dest))
        all_arrays, all_groups = update_paths_after_move(
            source, dest, all_arrays, all_groups
        )
        all_nodes = all_arrays | (all_groups - {""})

    return moves


@st.composite
def tree_and_moves(
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
