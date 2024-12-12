from typing import Literal

import pytest

from icechunk import Repository, StorageConfig


def parse_repo(store: Literal["local", "memory"], path: str) -> Repository:
    if store == "local":
        return Repository.create(
            storage=StorageConfig.filesystem(path),
        )
    if store == "memory":
        return Repository.create(
            storage=StorageConfig.memory(path),
        )


@pytest.fixture(scope="function")
def repo(request: pytest.FixtureRequest, tmpdir: str) -> tuple[Repository, str]:
    param = request.param
    repo = parse_repo(param, tmpdir)
    return repo, tmpdir
