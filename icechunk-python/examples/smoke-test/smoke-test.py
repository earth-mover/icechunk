import asyncio
import math
import random
import string
import time
from typing import Literal

import numpy as np
import zarr
from icechunk import IcechunkStore, S3Credentials, StorageConfig, StoreConfig
from zarr.abc.store import Store
from zarr.storage import LocalStore, MemoryStore, RemoteStore


def rdms(n):
    return "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(n)
    )


def generate_array_chunks(size: int, dtype=np.int32):
    # dim sizes
    nz = 64
    nt = 128
    nx = ny = int(math.sqrt(size / nz / nt))

    # chunk sizes
    ct = 2
    cz = 8
    cx = max(nx // 3, 1)
    cy = max(ny // 2, 1)
    chunk_shape = (cx, cy, cz, ct)
    shape = (nx, ny, nz, nt)

    array = np.arange(nx * ny * nz * nt, dtype=dtype).reshape(shape)

    return array, chunk_shape


def create_array(*, group, name, size, dtype, fill_value) -> np.ndarray:
    dims = ("x", "y", "z", "t")
    attrs = {"description": "icechunk test data"}

    array, chunk_shape = generate_array_chunks(size=size, dtype=dtype)

    group.require_array(
        name=name,
        shape=array.shape,
        dtype=array.dtype,
        fill_value=fill_value,
        chunk_shape=chunk_shape,
        dimension_names=dims,
        attributes=attrs,
        data=array,
        exists_ok=True,
    )

    return array


async def run(store: Store) -> None:
    write_start = time.time()
    group = zarr.group(store=store, overwrite=True)
    group.attrs["foo"] = "foo"
    print(group)

    first_commit = None
    if isinstance(store, IcechunkStore):
        first_commit = store.commit("initial commit")

    expected = {}
    expected["root-foo"] = create_array(
        group=group, name="root-foo", size=1 * 1024 * 256, dtype=np.int32, fill_value=-1
    )

    print(f"Root group members are: {group.members()}")
    print(f"Root group attrs: {dict(group['root-foo'].attrs)}")

    group["root-foo"].attrs["update"] = "new attr"

    if isinstance(store, IcechunkStore):
        _second_commit = store.commit("added array, updated attr")

    assert len(group["root-foo"].attrs) == 2
    assert len(group.members()) == 1

    if isinstance(store, IcechunkStore) and first_commit is not None:
        store.checkout(first_commit)
    group.attrs["update"] = "new attr 2"

    if isinstance(store, IcechunkStore):
        try:
            store.commit("new attr 2")
        except ValueError:
            pass
        else:
            raise ValueError("should have conflicted")

        store.reset()
        store.checkout(branch="main")

    group["root-foo"].attrs["update"] = "new attr 2"
    if isinstance(store, IcechunkStore):
        _third_commit = store.commit("new attr 2")

        try:
            store.commit("rewrote array")
        except ValueError:
            pass
        else:
            raise ValueError("should have failed, committing without changes.")

    newgroup = zarr.group(store=store, path="group1/")
    expected["group1/foo1"] = create_array(
        group=newgroup,
        name="foo1",
        dtype=np.float32,
        size=1 * 1024 * 128,
        fill_value=-1234,
    )
    expected["group1/foo2"] = create_array(
        group=newgroup,
        name="foo2",
        dtype=np.float16,
        size=1 * 1024 * 64,
        fill_value=-1234,
    )
    newgroup = zarr.group(store=store, path="group2/")
    expected["group2/foo3"] = create_array(
        group=newgroup,
        name="foo3",
        dtype=np.int64,
        size=1 * 1024 * 32,
        fill_value=-1234,
    )
    if isinstance(store, IcechunkStore):
        _fourth_commit = store.commit("added groups and arrays")

    print(f"Write done in {time.time() - write_start} secs")

    read_start = time.time()

    root_group = zarr.group(store=store)
    for key, value in expected.items():
        print(key)
        array = root_group[key]
        assert isinstance(array, zarr.Array)

        print(
            f"numchunks: {math.prod(s // c for s, c in zip(array.shape, array.chunks, strict=False))}"
        )
        np.testing.assert_array_equal(array[:], value)

    print(f"Read done in {time.time() - read_start} secs")


def create_icechunk_store(*, storage: StorageConfig) -> IcechunkStore:
    return IcechunkStore.open_or_create(
        storage=storage, mode="w", config=StoreConfig(inline_chunk_threshold_bytes=1)
    )


async def create_zarr_store(*, store: Literal["memory", "local", "s3"]) -> Store:
    if store == "local":
        return await LocalStore.open(f"/tmp/{rdms(6)}", mode="w")
    if store == "memory":
        return await MemoryStore.open(mode="w")
    if store == "s3":
        return RemoteStore.from_url(
            "s3://testbucket/root-zarr",
            mode="w",
            storage_options={
                "anon": False,
                "key": "minio123",
                "secret": "minio123",
                "endpoint_url": "http://localhost:9000",
            },
        )


if __name__ == "__main__":
    MEMORY = StorageConfig.memory("new")
    MINIO = StorageConfig.s3_from_config(
        bucket="testbucket",
        prefix="root-icechunk",
        credentials=S3Credentials(
            access_key_id="minio123",
            secret_access_key="minio123",
            session_token=None,
        ),
        region="us-east-1",
        allow_http=True,
        endpoint_url="http://localhost:9000",
    )

    print("Icechunk store")
    store = create_icechunk_store(storage=MINIO)
    asyncio.run(run(store))

    print("Zarr store")
    zarr_store = asyncio.run(create_zarr_store(store="s3"))
    asyncio.run(run(zarr_store))
