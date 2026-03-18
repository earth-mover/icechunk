"""Model stores for comparison testing."""

from __future__ import annotations

import itertools
from typing import Any

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
        """Shift chunk indices for an array.

        Out-of-bounds chunks are dropped, vacated positions retain stale data.
        """
        prefix = f"{array_path}/c/"

        # Collect mutations before applying any, to avoid read-after-write.
        mutations: dict[str, Any] = {}
        for dst_idx in itertools.product(*(range(n) for n in num_chunks)):
            src_idx = tuple(dst - off for dst, off in zip(dst_idx, offset, strict=True))
            if any(
                src < 0 or src >= size
                for src, size in zip(src_idx, num_chunks, strict=True)
            ):
                continue
            src_key = f"{prefix}{'/'.join(str(i) for i in src_idx)}"
            dst_key = f"{prefix}{'/'.join(str(i) for i in dst_idx)}"
            mutations[dst_key] = await self.get(src_key, prototype=_PROTOTYPE)

        for dst_key, data in mutations.items():
            if data is not None:
                await self.set(dst_key, data)
            else:
                # we end up here if the source chunk was in t he bounds of the array
                # but was deleted by an operation in this session prior to the shift.
                await self.delete(dst_key)

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
        async for key in self.list_prefix(""):
            data = await self.get(key, prototype=_PROTOTYPE)
            if data is not None:
                await new_store.set(key, data)
        return new_store
