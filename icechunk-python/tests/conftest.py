from typing import Literal

from icechunk import IcechunkStore
import pytest


async def parse_store(store: Literal["local", "memory"], path: str) -> IcechunkStore:
    if store == "local":
        return await IcechunkStore.from_config(
            {
                "storage": {"type": "local_filesystem", "root": path},
                "dataset": {},
            }, 
            mode="w"
        )
    if store == "memory":
        return await IcechunkStore.from_config(
            {
                "storage": {"type": "in_memory"},
                "dataset": {},
            }, 
            mode="w"
        )


@pytest.fixture(scope="function")
async def store(request: pytest.FixtureRequest, tmpdir: str) -> IcechunkStore:
    param = request.param
    return await parse_store(param, str(tmpdir))