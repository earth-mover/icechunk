from typing import cast

import pytest
from object_store import ClientOptions, ObjectStore

import zarr
import zarr.core
import zarr.core.array
from icechunk import (
    S3Credentials,
    StorageConfig,
    VirtualRefConfig,
)
from icechunk.repository import Repository


def write_chunks_to_minio(chunks: list[tuple[str, bytes]]):
    client_options = ClientOptions(
        allow_http=True,  # type: ignore
    )
    store = ObjectStore(
        "s3://testbucket",
        {
            "access_key_id": "minio123",
            "secret_access_key": "minio123",
            "aws_region": "us-east-1",
            "aws_endpoint": "http://localhost:9000",
        },
        client_options=client_options,
    )

    for key, data in chunks:
        store.put(key, data)


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


async def test_issue_418():
    # FIXME
    pytest.xfail(
        "Temporary flagged as failing while we implement new virtual chunk mechanism"
    )
    # See https://github.com/earth-mover/icechunk/issues/418
    await write_minio_virtual_refs()

    repo = Repository.create(
        storage=StorageConfig.memory("test"),
        virtual_ref_config=VirtualRefConfig.s3_from_config(
            credentials=S3Credentials(
                access_key_id="minio123",
                secret_access_key="minio123",
            ),
            endpoint_url="http://localhost:9000",
            allow_http=True,
            region="us-east-1",
        ),
    )
    session = repo.writable_session("main")
    store = session.store()

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
    store = session.store()

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
