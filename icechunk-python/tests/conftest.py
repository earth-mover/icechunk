from typing import Literal

from icechunk import IcechunkStore, Storage
import pytest


async def parse_store(store: Literal["local", "memory"], path: str) -> IcechunkStore:
    if store == "local":
        return await IcechunkStore.create(
            storage=Storage.filesystem(path),
        )
    if store == "memory":
        return await IcechunkStore.create(
            storage=Storage.memory(path),
        )


@pytest.fixture(scope="function")
async def store(request: pytest.FixtureRequest, tmpdir: str) -> IcechunkStore:
    param = request.param
    return await parse_store(param, str(tmpdir))
