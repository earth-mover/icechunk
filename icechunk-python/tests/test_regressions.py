from typing import cast

import pytest

import zarr
import zarr.core
import zarr.core.array
from icechunk import (
    Credentials,
    ObjectStoreConfig,
    RepositoryConfig,
    S3Credentials,
    S3Options,
    S3StaticCredentials,
    Storage,
    VirtualChunkContainer,
)
from icechunk.repository import Repository
from tests.conftest import write_chunks_to_minio


async def write_minio_virtual_refs():
    write_chunks_to_minio(
        [
            ("path/to/python/new/chunk-1", b"first"),
            ("path/to/python/new/chunk-2", b"second"),
            ("path/to/python/new/chunk-3", b"third"),
            ("path/to/python/new/chunk-4", b"fourth"),
            ("path/to/python/new/chunk-5", b"fifth"),
        ]
    )


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_issue_418():
    # See https://github.com/earth-mover/icechunk/issues/418
    await write_minio_virtual_refs()
    config = RepositoryConfig.default()
    store_config = ObjectStoreConfig.S3Compatible(
        S3Options(
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
        )
    )
    container = VirtualChunkContainer("s3", "s3://", store_config)
    config.set_virtual_chunk_container(container)
    credentials = {
        "s3": Credentials.S3(
            S3Credentials.Static(S3StaticCredentials("minio123", "minio123"))
        )
    }
    repo = Repository.create(
        storage=Storage.in_memory(),
        config=config,
        virtual_chunk_credentials=credentials,
    )
    session = repo.writable_session("main")
    store = session.store

    root = zarr.Group.from_store(store=store, zarr_format=3)
    time = root.require_array(name="time", shape=((2,)), chunk_shape=((1,)), dtype="i4")
    root.require_array(name="lon", shape=((1,)), chunk_shape=((1,)), dtype="i4")

    # Set longitude
    store.set_virtual_ref(
        "lon/c/0", "s3://testbucket/path/to/python/new/chunk-5", offset=0, length=4
    )

    store.set_virtual_ref(
        "time/c/0", "s3://testbucket/path/to/python/new/chunk-1", offset=0, length=4
    )
    store.set_virtual_ref(
        "time/c/1", "s3://testbucket/path/to/python/new/chunk-2", offset=1, length=4
    )

    assert (await store._store.get("lon/c/0")) == b"fift"
    assert (await store._store.get("time/c/0")) == b"firs"
    assert (await store._store.get("time/c/1")) == b"econ"

    session.commit("Initial commit")

    session = repo.writable_session("main")
    store = session.store

    root = zarr.Group.open(store=store)
    time = cast(zarr.core.array.Array, root["time"])
    root.require_array(name="lon", shape=((1,)), chunk_shape=((1,)), dtype="i4")

    # resize the array and append a new chunk
    time.resize((3,))

    store.set_virtual_ref(
        "time/c/2", "s3://testbucket/path/to/python/new/chunk-3", offset=0, length=4
    )

    assert (await store._store.get("lon/c/0")) == b"fift"
    assert (await store._store.get("time/c/0")) == b"firs"
    assert (await store._store.get("time/c/1")) == b"econ"
    assert (await store._store.get("time/c/2")) == b"thir"

    # commit
    session.commit("Append virtual ref")

    assert (await store._store.get("lon/c/0")) == b"fift"
    assert (await store._store.get("time/c/0")) == b"firs"
    assert (await store._store.get("time/c/1")) == b"econ"
    assert (await store._store.get("time/c/2")) == b"thir"
