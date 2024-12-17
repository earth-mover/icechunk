import numpy as np
import pytest
from object_store import ClientOptions, ObjectStore

import zarr
import zarr.core
import zarr.core.buffer
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


async def test_write_minio_virtual_refs():
    # FIXME
    pytest.xfail(
        "Temporary flagged as failing while we implement new virtual chunk mechanism"
    )
    write_chunks_to_minio(
        [
            ("path/to/python/chunk-1", b"first"),
            ("path/to/python/chunk-2", b"second"),
        ]
    )

    # Open the store
    repo = Repository.open_or_create(
        storage=StorageConfig.memory("virtual"),
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

    array = zarr.Array.create(store, shape=(1, 1, 3), chunk_shape=(1, 1, 1), dtype="i4")

    store.set_virtual_ref(
        "c/0/0/0", "s3://testbucket/path/to/python/chunk-1", offset=0, length=4
    )
    store.set_virtual_ref(
        "c/0/0/1", "s3://testbucket/path/to/python/chunk-2", offset=1, length=4
    )
    # we write a ref that simulates a lost chunk
    store.set_virtual_ref(
        "c/0/0/2", "s3://testbucket/path/to/python/non-existing", offset=1, length=4
    )

    buffer_prototype = zarr.core.buffer.default_buffer_prototype()

    first = await store.get("c/0/0/0", prototype=buffer_prototype)
    assert first is not None
    assert first.to_bytes() == b"firs"

    second = await store.get("c/0/0/1", prototype=buffer_prototype)
    assert second is not None
    assert second.to_bytes() == b"econ"

    assert array[0, 0, 0] == 1936877926
    assert array[0, 0, 1] == 1852793701

    # fetch uninitialized chunk should be None
    assert await store.get("c/0/0/3", prototype=buffer_prototype) is None

    # fetching a virtual ref that disappeared should be an exception
    with pytest.raises(ValueError):
        # TODO: we should include the key and other info in the exception
        await store.get("c/0/0/2", prototype=buffer_prototype)

    _snapshot_id = session.commit("Add virtual refs")


async def test_from_s3_public_virtual_refs(tmpdir):
    # FIXME
    pytest.xfail(
        "Temporary flagged as failing while we implement new virtual chunk mechanism"
    )
    # Open the store,
    repo = Repository.open_or_create(
        storage=StorageConfig.filesystem(f"{tmpdir}/virtual"),
        virtual_ref_config=VirtualRefConfig.s3_anonymous(
            region="us-east-1", allow_http=False
        ),
    )
    session = repo.writable_session("main")
    store = session.store()

    root = zarr.Group.from_store(store=store, zarr_format=3)
    year = root.require_array(
        name="year", shape=((72,)), chunk_shape=((72,)), dtype="float32"
    )

    store.set_virtual_ref(
        "year/c/0",
        "s3://earthmover-sample-data/netcdf/oscar_vel2018.nc",
        offset=22306,
        length=288,
    )

    nodes = [n async for n in store.list()]
    assert "year/c/0" in nodes

    year_values = year[:]
    assert len(year_values) == 72
    actual_values = np.array(
        [
            2018.0,
            2018.0139,
            2018.0278,
            2018.0416,
            2018.0555,
            2018.0695,
            2018.0834,
            2018.0972,
            2018.1111,
            2018.125,
            2018.1389,
            2018.1528,
            2018.1666,
            2018.1805,
            2018.1945,
            2018.2084,
            2018.2222,
            2018.2361,
            2018.25,
            2018.2639,
            2018.2778,
            2018.2916,
            2018.3055,
            2018.3195,
            2018.3334,
            2018.3472,
            2018.3611,
            2018.375,
            2018.3889,
            2018.4028,
            2018.4166,
            2018.4305,
            2018.4445,
            2018.4584,
            2018.4722,
            2018.4861,
            2018.5,
            2018.5139,
            2018.5278,
            2018.5416,
            2018.5555,
            2018.5695,
            2018.5834,
            2018.5972,
            2018.6111,
            2018.625,
            2018.6389,
            2018.6528,
            2018.6666,
            2018.6805,
            2018.6945,
            2018.7084,
            2018.7222,
            2018.7361,
            2018.75,
            2018.7639,
            2018.7778,
            2018.7916,
            2018.8055,
            2018.8195,
            2018.8334,
            2018.8472,
            2018.8611,
            2018.875,
            2018.8889,
            2018.9028,
            2018.9166,
            2018.9305,
            2018.9445,
            2018.9584,
            2018.9722,
            2018.9861,
        ],
        dtype="float32",
    )
    assert np.allclose(year_values, actual_values)
