from typing import AsyncGenerator

class IcechunkStore:
    def __init__(self, config: str) -> None: ...
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
    async def list(self) -> AsyncStringGenerator: ...
    async def list_prefix(self, prefix: str) -> AsyncStringGenerator: ...
    async def list_dir(self, prefix: str) -> AsyncStringGenerator: ...

class AsyncStringGenerator(AsyncGenerator[str, None]):
    def __aiter__(self) -> AsyncStringGenerator: ...
    async def __anext__(self) -> str | None: ...
