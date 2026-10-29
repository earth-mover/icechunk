import math
import os
import shutil
import sys
from enum import Enum
from pathlib import Path
from typing import Literal, cast

import boto3
import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.database import DirectoryBasedExampleDatabase
from mypy_boto3_s3.client import S3Client
from packaging.version import Version

import zarr
import zarr.testing.strategies as zarr_st
from icechunk import (
    Repository,
    SpecVersion,
    Storage,
    filesystem_storage,
    in_memory_storage,
    local_filesystem_storage,
)

# Zarr may gate rectilinear chunk grids behind a config flag.
# Icechunk supports them, so enable unconditionally for tests.
zarr_has_rectilinear_chunks = "array.rectilinear_chunks" in zarr.config
if zarr_has_rectilinear_chunks:
    zarr.config.set({"array.rectilinear_chunks": True})

# TODO: remove this block once icechunk accepts bare-int axes in rectilinear
# `chunk_shapes` (https://github.com/earth-mover/icechunk/pull/2458).
# zarr 3.4.1's strategies draw them and icechunk rejects them, so on zarr >= 3.4.1
# hypothesis draws only regular grids and has NO rectilinear coverage.
if Version(zarr.__version__) >= Version("3.4.1"):
    zarr_st.chunk_grids = lambda *, shape: st.builds(  # type: ignore[attr-defined, unused-ignore]
        zarr_st.RegularChunkGridMetadata,  # type: ignore[attr-defined, unused-ignore]
        chunk_shape=zarr_st.chunk_shapes(shape=shape),
    )

# ---------------------------------------------------------------------------
# Hypothesis profiles
#
# Inherits from hypothesis's built-in "default" and "ci" profiles.
# hypothesis auto-selects "ci" when the CI env var is set.
# Select with: pytest --hypothesis-profile=nightly
# ---------------------------------------------------------------------------
# CI runs every hypothesis test in each of N shards, so each shard gets 1/N of the examples
HYPOTHESIS_SHARDS = max(1, int(os.environ.get("ICECHUNK_HYPOTHESIS_SHARDS", "1")))
CI_MAX_EXAMPLES = 200

settings.register_profile(
    "default",
    parent=settings.get_profile("default"),
    deadline=None,
    suppress_health_check=[HealthCheck.filter_too_much, HealthCheck.too_slow],
)
settings.register_profile(
    "ci",
    parent=settings.get_profile("ci"),
    # +1 per shard: every run tries the same simplest example first
    max_examples=math.ceil(CI_MAX_EXAMPLES / HYPOTHESIS_SHARDS)
    + (1 if HYPOTHESIS_SHARDS > 1 else 0),
    stateful_step_count=75,
    suppress_health_check=[HealthCheck.filter_too_much, HealthCheck.too_slow],
    # The built-in "ci" profile sets derandomize=True and database=None.
    # Undo both so failures persist in .hypothesis/examples and replay across runs.
    derandomize=False,
    database=DirectoryBasedExampleDatabase(".hypothesis/examples"),
)
settings.register_profile(
    "nightly",
    parent=settings.get_profile("ci"),
    max_examples=1000,
    stateful_step_count=500,
    derandomize=False,
    suppress_health_check=[HealthCheck.filter_too_much, HealthCheck.too_slow],
)


class Permission(Enum):
    READONLY = 1
    MODIFY = 2
    SUPERUSER = 3  # temporary, while figuring out right permissions

    def keys(self) -> tuple[str, str]:
        match self:
            case Permission.READONLY:
                return ("readonly", "basicuser")
            case Permission.MODIFY:
                return ("modify", "modifydata")
            case Permission.SUPERUSER:
                return ("test123", "test123")


FS_BACKENDS = [
    "local_filesystem",
    pytest.param(
        "filesystem",
        marks=pytest.mark.skipif(
            sys.platform == "win32", reason="FilesystemStorage is Unix-only"
        ),
    ),
]


@pytest.fixture(params=FS_BACKENDS)
def fs_backend(request: pytest.FixtureRequest) -> str:
    return cast(str, request.param)


def fs_storage(
    backend: str, path: str, spec_version: SpecVersion | int | None = None
) -> Storage:
    if backend == "filesystem":
        if spec_version == 1:
            pytest.skip("FilesystemStorage refuses IC1 repos")
        return filesystem_storage(path)
    return local_filesystem_storage(path)


def readable_path(path: str, backend: str, tmp_path: Path) -> str:
    """The filesystem backend refuses repos in the object store layout: open a
    converted copy."""
    if backend != "filesystem":
        return path
    copy = tmp_path / "repo"
    shutil.copytree(path, copy)
    storage = filesystem_storage(str(copy))
    assert storage.convert_to_filesystem_layout()
    return str(copy)


def parse_repo(
    store: Literal["local", "filesystem", "memory"],
    path: str,
    spec_version: SpecVersion | int | None,
) -> Repository:
    if store == "memory":
        storage = in_memory_storage()
    elif store in ("local", "filesystem"):
        backend = "local_filesystem" if store == "local" else store
        storage = fs_storage(backend, path, spec_version)
    else:
        raise ValueError(f"Unknown store type: {store}")
    return Repository.create(storage=storage, spec_version=spec_version)


@pytest.fixture(scope="function")
def repo(
    request: pytest.FixtureRequest,
    tmpdir: str,
    any_spec_version: SpecVersion | int | None,
) -> tuple[Repository, str]:
    param = request.param
    repo = parse_repo(param, tmpdir, spec_version=any_spec_version)
    return repo, tmpdir


minio_client = None


def get_minio_client() -> S3Client:
    global minio_client
    if minio_client is None:
        (aws_access_key_id, aws_secret_access_key) = Permission.MODIFY.keys()
        minio_client = boto3.client(
            "s3",
            endpoint_url="http://localhost:4200",
            use_ssl=False,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
    return minio_client


def write_chunks_to_minio(
    chunks: list[tuple[str, bytes]], bucket: str = "testbucket"
) -> list[str]:
    """Write chunks to local minio returning their etags"""
    s3 = get_minio_client()
    etags = []
    for key, data in chunks:
        etag = s3.put_object(Bucket=bucket, Key=key, Body=data)["ETag"]
        etags.append(etag)

    return etags


@pytest.fixture(
    scope="function",
    params=[1, SpecVersion.v2, None],
    ids=["spec-v1", "spec-v2", "no-spec-version"],
)
def any_spec_version(request: pytest.FixtureRequest) -> SpecVersion | int | None:
    return cast(SpecVersion | int | None, request.param)


SHARD_ENV = "ICECHUNK_PYTEST_SHARD"


# Runs after -m/-k deselection. Round-robin over the collected order, so the cases
# of one parametrized test spread over the shards and every process agrees.
@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    spec = os.environ.get(SHARD_ENV)
    if not spec:
        return
    index, count = (int(part) for part in spec.split("/"))
    dropped = [item for pos, item in enumerate(items) if pos % count != index]
    items[:] = items[index::count]
    config.hook.pytest_deselected(items=dropped)
