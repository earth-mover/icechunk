from typing import AsyncGenerator

class PyIcechunkStore:
    async def commit(self, update_branch_name: str, message: str) -> str: ...
    async def empty(self) -> bool: ...
    async def clear(self) -> None: ...
    async def get(
        self, key: str, byte_range: tuple[int | None, int | None] | None = None
    ) -> bytes: ...
    async def get_partial_values(
        self, key_ranges: list[tuple[str, tuple[int | None, int | None]]]
    ) -> list[bytes]: ...
    async def exists(self, key: str) -> bool: ...
    @property
    def supports_writes(self) -> bool: ...
    async def set(self, key: str, value: bytes) -> None: ...
    async def delete(self, key: str) -> None: ...
    @property
    def supports_partial_writes(self) -> bool: ...
    async def set_partial_values(
        self, key_start_values: list[tuple[str, int, bytes]]
    ) -> None: ...
    @property
    def supports_listings(self) -> bool: ...
    async def list(self) -> PyAsyncStringGenerator: ...
    async def list_prefix(self, prefix: str) -> PyAsyncStringGenerator: ...
    async def list_dir(self, prefix: str) -> PyAsyncStringGenerator: ...

class PyAsyncStringGenerator(AsyncGenerator[str, None]):
    def __aiter__(self) -> PyAsyncStringGenerator: ...
    async def __anext__(self) -> str | None: ...

async def pyicechunk_store_from_json_config(config: str) -> PyIcechunkStore: ...
