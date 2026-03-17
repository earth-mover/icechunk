"""Model stores for comparison testing."""

from __future__ import annotations

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

        This simulates what shift_array does to the chunk store keys.
        Out-of-bounds chunks are dropped, vacated positions retain stale data.
        """
        prefix = f"{array_path}/c/"

        # Read all chunks keyed by their new indices, discarding out-of-bounds
        chunk_data: dict[tuple[int, ...], Any] = {}
        async for key in self.list_prefix(prefix):
            parts = key.split("/")
            idx_start = parts.index("c") + 1
            old_idx = tuple(int(p) for p in parts[idx_start:])
            new_idx = tuple(idx + off for idx, off in zip(old_idx, offset, strict=True))
            if any(
                idx < 0 or idx >= nchunks
                for idx, nchunks in zip(new_idx, num_chunks, strict=True)
            ):
                continue
            data = await self.get(key, prototype=_PROTOTYPE)
            if data is not None:
                chunk_data[new_idx] = data

        # Write chunks at new positions
        for new_idx, data in chunk_data.items():
            new_key = f"{prefix}{'/'.join(str(idx) for idx in new_idx)}"
            await self.set(new_key, data)

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
