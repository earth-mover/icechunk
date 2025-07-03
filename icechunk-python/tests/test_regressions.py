from typing import cast

import pytest

import zarr
import zarr.core
import zarr.core.array
from icechunk import (
    RepositoryConfig,
    VirtualChunkContainer,
    containers_credentials,
    in_memory_storage,
    s3_credentials,
    s3_store,
)
from icechunk.repository import Repository
from tests.conftest import write_chunks_to_minio


async def write_minio_virtual_refs() -> None:
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
async def test_issue_418() -> None:
    # See https://github.com/earth-mover/icechunk/issues/418
    await write_minio_virtual_refs()
    config = RepositoryConfig.default()
    store_config = s3_store(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        s3_compatible=True,
        force_path_style=True,
    )
    container = VirtualChunkContainer("s3://testbucket", store_config)
    config.set_virtual_chunk_container(container)
    credentials = containers_credentials(
        {
            "s3://testbucket": s3_credentials(
                access_key_id="minio123", secret_access_key="minio123"
            )
        }
    )

    repo = Repository.create(
        storage=in_memory_storage(),
        config=config,
        authorize_virtual_chunk_access=credentials,
    )
    session = repo.writable_session("main")
    store = session.store

    root = zarr.Group.from_store(store=store, zarr_format=3)
    time = root.require_array(name="time", shape=((2,)), chunks=((1,)), dtype="i4")
    root.require_array(name="lon", shape=((1,)), chunks=((1,)), dtype="i4")

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
    assert (await store._store.get("time/c/0")) == b"firs"  # codespell:ignore firs
    assert (await store._store.get("time/c/1")) == b"econ"

    session.commit("Initial commit")

    session = repo.writable_session("main")
    store = session.store

    root = zarr.Group.open(store=store)
    time = cast(zarr.core.array.Array, root["time"])
    root.require_array(name="lon", shape=((1,)), chunks=((1,)), dtype="i4")

    # resize the array and append a new chunk
    time.resize((3,))

    store.set_virtual_ref(
        "time/c/2", "s3://testbucket/path/to/python/new/chunk-3", offset=0, length=4
    )

    assert (await store._store.get("lon/c/0")) == b"fift"
    assert (await store._store.get("time/c/0")) == b"firs"  # codespell:ignore firs
    assert (await store._store.get("time/c/1")) == b"econ"
    assert (await store._store.get("time/c/2")) == b"thir"

    # commit
    session.commit("Append virtual ref")

    assert (await store._store.get("lon/c/0")) == b"fift"
    assert (await store._store.get("time/c/0")) == b"firs"  # codespell:ignore firs
    assert (await store._store.get("time/c/1")) == b"econ"
    assert (await store._store.get("time/c/2")) == b"thir"


async def test_read_chunks_from_old_array() -> None:
    # This regression appeared during the change to manifest per array
    repo = Repository.create(
        storage=in_memory_storage(),
    )
    session = repo.writable_session("main")
    store = session.store

    root = zarr.Group.from_store(store=store, zarr_format=3)
    array1 = root.require_array(name="array1", shape=((2,)), chunks=((1,)), dtype="i4")

    array1[:] = 42
    assert array1[0] == 42
    print("about to commit 1")
    session.commit("create array 1")

    session = repo.writable_session("main")
    store = session.store
    root = zarr.group(store=store, zarr_format=3)
    # array1 = root.require_array(name="array1", shape=((2,)), chunks=((1,)), dtype="i4")
    array2 = root.require_array(name="array2", shape=((2,)), chunks=((1,)), dtype="i4")
    # assert array1[0] == 42
    array2[:] = 84
    print("about to commit 2")
    session.commit("create array 2")

    session = repo.readonly_session(branch="main")
    store = session.store
    root = zarr.Group.open(store=store, zarr_format=3)
    array1 = root.require_array(name="array1", shape=((2,)), chunks=((1,)), dtype="i4")
    assert array1[0] == 42
