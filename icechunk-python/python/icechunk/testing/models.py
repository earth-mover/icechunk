"""Model stores for comparison testing."""

from __future__ import annotations

import itertools
from typing import Any

from zarr.core.buffer import Buffer, default_buffer_prototype
from zarr.storage import MemoryStore

_PROTOTYPE = default_buffer_prototype()


class ModelStore(MemoryStore):
    """MemoryStore with move, copy, and shift_array methods for testing."""

    _deleted: set[str]

    def __init__(self) -> None:
        super().__init__()
        self._deleted = set()

    async def delete(self, key: str) -> None:
        self._deleted.add(key)
        await super().delete(key)

    async def set(
        self, key: str, value: Buffer, byte_range: tuple[int, int] | None = None
    ) -> None:
        self._deleted.discard(key)
        await super().set(key, value, byte_range)

    def new_session(self) -> None:
        """Clear deletion tracking when a new icechunk write session begins."""
        self._deleted.clear()

    async def shift_array(
        self,
        array_path: str,
        offset: tuple[int, ...],
        num_chunks: tuple[int, ...],
    ) -> None:
        """Shift chunk indices, modeling icechunk's reindex_array.

        Out-of-bounds chunks are dropped, vacated positions retain stale data.
        With a few exceptions for known bugs.
        """
        prefix = f"{array_path}/c/"

        # Collect mutations before applying any, to avoid read-after-write.
        _DELETED = object()  # sentinel: source was explicitly deleted this session
        mutations: dict[str, Any] = {}
        for dst_idx in itertools.product(*(range(n) for n in num_chunks)):
            src_idx = tuple(dst - off for dst, off in zip(dst_idx, offset, strict=True))
            src_key = f"{prefix}{'/'.join(str(i) for i in src_idx)}"
            dst_key = f"{prefix}{'/'.join(str(i) for i in dst_idx)}"
            if any(
                src < 0 or src >= size
                for src, size in zip(src_idx, num_chunks, strict=True)
            ):
                # out of bounds source — but if it was explicitly deleted this session
                # (e.g. by a resize), icechunk's change_set still has a None
                # entry and reindex_array propagates it to the destination.
                if src_key in self._deleted:
                    mutations[dst_key] = _DELETED
                continue
            data = await self.get(src_key, prototype=_PROTOTYPE)
            if data is not None:
                mutations[dst_key] = data
            elif src_key in self._deleted:
                # In-bounds but deleted this session — propagate deletion.
                mutations[dst_key] = _DELETED
            else:  # never written — dst keeps stale data
                pass
                # TODO: once https://github.com/earth-mover/icechunk/issues/1862 is
                # resolved, this branch will be equivalent to the elif above and the
                # _deleted / new_session() tracking can be removed.

        for dst_key, data in mutations.items():
            if data is _DELETED:
                await self.delete(dst_key)
            else:
                await self.set(dst_key, data)

    spec_version: int

    async def move(self, source: str, dest: str) -> None:
        """Move all keys from source to dest.

        Store keys always have form "node/zarr.json" or "node/c/...", never bare "node".
        """
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
        new_store._deleted = set(self._deleted)
        async for key in self.list_prefix(""):
            data = await self.get(key, prototype=_PROTOTYPE)
            if data is not None:
                await new_store.set(key, data)
        return new_store
