from __future__ import annotations

from zarr.core.buffer import default_buffer_prototype
from zarr.storage import MemoryStore

PROTOTYPE = default_buffer_prototype()


class IcechunkModel(MemoryStore):
    """MemoryStore with move, copy, and lifecycle methods for testing.

    Tracks all_arrays/all_groups alongside store data so that copy()
    preserves them.
    """

    spec_version: int

    def __init__(self) -> None:
        super().__init__()
        self.all_arrays: set[str] = set()
        self.all_groups: set[str] = set()
        self.baseline: frozenset[str] = frozenset()
        self.claims: set[str] = set()

    @classmethod
    def from_store(cls, store: MemoryStore) -> IcechunkModel:
        """Promote a plain MemoryStore to an IcechunkModel."""
        model = cls()
        model._store_dict = store._store_dict
        return model

    def _claim_node(self, key: str) -> None:
        """Record which array or group a key belongs to."""
        for array in self.all_arrays:
            if key.startswith(array + "/"):
                self.claims.add(array)
                return
        for group in self.all_groups:
            if key.startswith(group + "/"):
                self.claims.add(group)
                return

    async def set(
        self, key: str, value: object, byte_range: tuple[int, int] | None = None
    ) -> None:
        self._claim_node(key)
        await super().set(key, value, byte_range)

    async def delete(self, key: str) -> None:
        self._claim_node(key)
        await super().delete(key)

    async def move(self, source: str, dest: str) -> None:
        """Move all keys from source to dest.

        Store keys always have form "node/zarr.json" or "node/c/...", never bare "node".
        """
        all_keys = [k async for k in self.list_prefix("")]
        keys_to_move = [k for k in all_keys if k.startswith(source + "/")]
        for old_key in keys_to_move:
            new_key = dest + old_key[len(source) :]
            data = await self.get(old_key, prototype=PROTOTYPE)
            if data is not None:
                await self.set(new_key, data)
                await self.delete(old_key)

    async def copy(self) -> IcechunkModel:
        """Create a copy of this store (data + arrays + groups)."""
        new_store = IcechunkModel()
        new_store.spec_version = self.spec_version
        new_store.all_arrays = self.all_arrays.copy()
        new_store.all_groups = self.all_groups.copy()
        async for key in self.list_prefix(""):
            data = await self.get(key, prototype=PROTOTYPE)
            if data is not None:
                await new_store.set(key, data)
        return new_store

    # things for tracking changes relative to committed baseline
    @property
    def nodes(self) -> frozenset[str]:
        """Current set of all node paths in this model."""
        return frozenset(self.all_arrays) | frozenset(self.all_groups)

    def changes(self) -> set[str]:
        """Paths modified since last sync: structural diff + data claims."""
        return set(self.claims) | (self.nodes ^ self.baseline)

    def sync_baseline(self, committed) -> None:
        """Record committed state as this actor's baseline."""
        self.baseline = frozenset(committed.all_arrays) | frozenset(committed.all_groups)

    # ── Lifecycle methods ─────────────────────────────────────────────

    async def commit(self) -> IcechunkModel:
        """Snapshot this model as the new committed baseline.

        Returns the committed copy; caller stores it as shared state.
        """
        committed = await self.copy()
        self.claims.clear()
        self.sync_baseline(committed)
        return committed

    async def rebase(self, committed: IcechunkModel) -> IcechunkModel:
        """Merge committed baseline with local changes.

        Returns a new model with the merge result; caller swaps it in.
        """
        merged = await committed.copy()
        my_changes = self.changes()

        merged.all_arrays = (committed.all_arrays - my_changes) | (
            my_changes & self.all_arrays
        )
        merged.all_groups = (committed.all_groups - my_changes) | (
            my_changes & self.all_groups
        )

        for path in my_changes:
            keys_to_delete = [k async for k in merged.list_prefix(path + "/")]
            for key in keys_to_delete:
                await merged.delete(key)
            async for key in self.list_prefix(path + "/"):
                data = await self.get(key, prototype=PROTOTYPE)
                if data is not None:
                    await merged.set(key, data)

        merged.sync_baseline(committed)
        return merged

    async def new_session(self, committed: IcechunkModel) -> IcechunkModel:
        """Reset to the committed baseline, discarding local changes."""
        fresh = await committed.copy()
        fresh.claims.clear()
        fresh.sync_baseline(committed)
        return fresh

    async def rewrite_manifests(self, committed: IcechunkModel) -> IcechunkModel:
        """Reset to committed baseline (rewrite_manifests doesn't change data)."""
        fresh = await committed.copy()
        fresh.claims.clear()
        fresh.sync_baseline(committed)
        return fresh
