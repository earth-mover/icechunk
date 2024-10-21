from typing import Literal

import pytest
from icechunk import IcechunkStore, StorageConfig


def parse_store(store: Literal["local", "memory"], path: str) -> IcechunkStore:
    if store == "local":
        return IcechunkStore.create(
            storage=StorageConfig.filesystem(path),
        )
    if store == "memory":
        return IcechunkStore.create(
            storage=StorageConfig.memory(path),
        )


@pytest.fixture(scope="function")
def store(request: pytest.FixtureRequest, tmpdir: str) -> IcechunkStore:
    param = request.param
    return parse_store(param, str(tmpdir))
