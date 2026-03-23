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
