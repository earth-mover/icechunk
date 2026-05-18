"""Models for comparison testing.

ModelStore is a superclass of the zarr memory store with added icechunk
methods shift_array, move, and utility copy.

The tree descriptors (GroupNode / ArrayNode) are pure data structures.
Materialization writes it into any zarr store, so the same tree can be
written to MemoryStore, IcechunkStore, etc. for comparison testing.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any

import numpy as np

import zarr
import zarr.abc.store
import zarr.api.asynchronous
from zarr.core.buffer import default_buffer_prototype
from zarr.storage import MemoryStore

_PROTOTYPE = default_buffer_prototype()


class ModelStore(MemoryStore):
    """MemoryStore with move, copy, and shift_array methods for testing."""

    async def shift_array(
        self,
        array_path: str,
        offset: tuple[int, ...],
        num_chunks: tuple[int, ...],
    ) -> None:
        """Shift chunk indices, modeling icechunk's reindex_array.

        Out-of-bounds sources are dropped, vacated positions are cleared.
        """
        prefix = f"{array_path}/c/"

        # Collect mutations before applying any, to avoid read-after-write.
        writes: dict[str, Any] = {}
        deletes: set[str] = set()
        for dst_idx in itertools.product(*(range(n) for n in num_chunks)):
            src_idx = tuple(dst - off for dst, off in zip(dst_idx, offset, strict=True))
            src_key = f"{prefix}{'/'.join(str(i) for i in src_idx)}"
            dst_key = f"{prefix}{'/'.join(str(i) for i in dst_idx)}"
            if any(
                src < 0 or src >= size
                for src, size in zip(src_idx, num_chunks, strict=True)
            ):
                deletes.add(dst_key)
                continue
            data = await self.get(src_key, prototype=_PROTOTYPE)
            if data is not None:
                writes[dst_key] = data
            else:
                deletes.add(dst_key)

        for key in deletes:
            await self.delete(key)
        for key, data in writes.items():
            await self.set(key, data)

    spec_version: int

    async def move(self, source: str, dest: str) -> None:
        """Move all keys from source to dest.

        Paths must be absolute (leading ``/``); root is ``"/"``.
        Store keys always have form "node/zarr.json" or "node/c/...", never bare "node".
        """
        if not source.startswith("/") or not dest.startswith("/"):
            raise ValueError(
                f"paths must start with '/'; got source={source!r}, dest={dest!r}"
            )
        source = source[1:]
        dest = dest[1:]
        all_keys = [k async for k in self.list_prefix("")]
        keys_to_move = [k for k in all_keys if k.startswith(source + "/")]
        for old_key in keys_to_move:
            new_key = dest + old_key[len(source) :]
            data = await self.get(old_key, prototype=_PROTOTYPE)
            if data is not None:
                await self.set(new_key, data)
                await self.delete(old_key)

    async def copy(self) -> ModelStore:
        """Create a copy of this store."""
        new_store = ModelStore()
        new_store.spec_version = self.spec_version
        async for key in self.list_prefix(""):
            data = await self.get(key, prototype=_PROTOTYPE)
            if data is not None:
                await new_store.set(key, data)
        return new_store


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

    def walk(
        self, prefix: str | PurePosixPath = ""
    ) -> Iterator[tuple[PurePosixPath, Node]]:
        """Yield ``(path, child)`` for every node, depth-first."""
        for name, child in self.children.items():
            p = PurePosixPath(prefix) / name
            yield p, child
            if isinstance(child, GroupNode):
                yield from child.walk(p)

    def nodes(self, prefix: str = "", *, include_root: bool = False) -> list[str]:
        """Return paths of all nodes, optionally including root."""
        root = [prefix] if include_root else []
        return root + [str(p) for p, _ in self.walk(prefix)]

    def groups(self, prefix: str = "", *, include_root: bool = False) -> list[str]:
        """Return paths of all group nodes, optionally including root."""
        root = [prefix] if include_root else []
        return root + [str(p) for p, c in self.walk(prefix) if isinstance(c, GroupNode)]

    def arrays(self, prefix: str = "") -> list[str]:
        """Return paths of all array nodes."""
        return [str(p) for p, c in self.walk(prefix) if isinstance(c, ArrayNode)]

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

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> GroupNode:
        """Convert a nested dict (with ArrayNode leaves) to a GroupNode tree."""
        children: dict[str, ArrayNode | GroupNode] = {}
        for name, value in d.items():
            if isinstance(value, ArrayNode):
                children[name] = value
            else:
                children[name] = cls.from_dict(value)
        return cls(children=children)

    @classmethod
    def from_paths(cls, arrays: set[str], groups: set[str]) -> GroupNode:
        """Build a GroupNode from flat sets of array and group paths.

        Paths must be absolute (leading ``/``); root is ``"/"``.

        Example::

            GroupNode.from_paths(
                arrays={"/a/x", "/b"},
                groups={"/a"},
            )
        """
        for path in arrays | groups:
            if not path.startswith("/"):
                raise ValueError(f"path must start with '/'; got {path!r}")
        tree: dict[str, Any] = {}
        normalized_groups = {g[1:] for g in groups}
        normalized_arrays = {a[1:] for a in arrays}
        for path in sorted(normalized_groups - {""}):
            current = tree
            for part in path.split("/"):
                current = current.setdefault(part, {})
        for path in sorted(normalized_arrays):
            parts = path.split("/")
            current = tree
            for part in parts[:-1]:
                current = current.setdefault(part, {})
            current[parts[-1]] = ArrayNode(shape=(1,), dtype=np.dtype("i4"))
        return cls.from_dict(tree)

    @classmethod
    async def from_store_async(cls, store: zarr.abc.store.Store) -> GroupNode:
        """Build a GroupNode by reading a zarr store's structure.

        Example::

            await GroupNode.from_store_async(some_memory_store)
        """
        root = await zarr.api.asynchronous.open_group(store, mode="r")
        tree: dict[str, Any] = {}
        async for path, obj in root.members(max_depth=None):
            parts = path.split("/")
            current = tree
            if isinstance(obj, zarr.AsyncArray):
                for part in parts[:-1]:
                    current = current.setdefault(part, {})
                current[parts[-1]] = ArrayNode(shape=obj.shape, dtype=obj.dtype)
            else:
                for part in parts:
                    current = current.setdefault(part, {})
        return cls.from_dict(tree)

    @classmethod
    def from_store(cls, store: zarr.abc.store.Store) -> GroupNode:
        """Build a GroupNode by reading a zarr store's structure.

        Example::

            GroupNode.from_store(some_memory_store)
        """
        root = zarr.open_group(store, mode="r")
        tree: dict[str, Any] = {}
        for path, obj in root.members(max_depth=None):
            parts = path.split("/")
            current = tree
            if isinstance(obj, zarr.Array):
                for part in parts[:-1]:
                    current = current.setdefault(part, {})
                current[parts[-1]] = ArrayNode(shape=obj.shape, dtype=obj.dtype)
            else:
                for part in parts:
                    current = current.setdefault(part, {})
        return cls.from_dict(tree)


Node = ArrayNode | GroupNode
