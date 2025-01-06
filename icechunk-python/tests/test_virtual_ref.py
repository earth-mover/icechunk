import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

import zarr
import zarr.core
import zarr.core.buffer
from icechunk import (
    IcechunkError,
    ObjectStoreConfig,
    RepositoryConfig,
    S3Options,
    VirtualChunkContainer,
    containers_credentials,
    in_memory_storage,
    local_filesystem_storage,
    s3_credentials,
    s3_store,
)
from icechunk.repository import Repository
from tests.conftest import write_chunks_to_minio


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_write_minio_virtual_refs() -> None:
    prefix = str(uuid.uuid4())
    etags = write_chunks_to_minio(
        [
            (f"{prefix}/chunk-1", b"first"),
            (f"{prefix}/chunk-2", b"second"),
        ],
    )

    config = RepositoryConfig.default()
    store_config = s3_store(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        s3_compatible=True,
    )
    container = VirtualChunkContainer("s3", "s3://", store_config)
    config.set_virtual_chunk_container(container)
    credentials = containers_credentials(
        s3=s3_credentials(access_key_id="minio123", secret_access_key="minio123")
    )

    # Open the store
    repo = Repository.open_or_create(
        storage=in_memory_storage(),
        config=config,
        virtual_chunk_credentials=credentials,
    )
    session = repo.writable_session("main")
    store = session.store

    array = zarr.create_array(
        store, shape=(5, 1, 3), chunks=(1, 1, 1), dtype="i4", compressors=None
    )

    # We add the virtual chunk refs without checksum, with the right etag, and with the wrong wrong etag and datetime.
    # This way we can check retrieval operations that should fail
    old = datetime.now(UTC) - timedelta(weeks=1)
    new = datetime.now(UTC) + timedelta(minutes=1)

    store.set_virtual_ref(
        "c/0/0/0", f"s3://testbucket/{prefix}/chunk-1", offset=0, length=4
    )
    store.set_virtual_ref(
        "c/1/0/0",
        f"s3://testbucket/{prefix}/chunk-1",
        offset=0,
        length=4,
        checksum=etags[0],
    )
    store.set_virtual_ref(
        "c/2/0/0",
        f"s3://testbucket/{prefix}/chunk-1",
        offset=0,
        length=4,
        checksum="bad etag",
    )
    store.set_virtual_ref(
        "c/3/0/0",
        f"s3://testbucket/{prefix}/chunk-1",
        offset=0,
        length=4,
        checksum=old,
    )
    store.set_virtual_ref(
        "c/4/0/0",
        f"s3://testbucket/{prefix}/chunk-1",
        offset=0,
        length=4,
        checksum=new,
    )

    store.set_virtual_ref(
        "c/0/0/1", f"s3://testbucket/{prefix}/chunk-2", offset=1, length=4
    )
    store.set_virtual_ref(
        "c/1/0/1",
        f"s3://testbucket/{prefix}/chunk-2",
        offset=1,
        length=4,
        checksum=etags[1],
    )
    store.set_virtual_ref(
        "c/2/0/1",
        f"s3://testbucket/{prefix}/chunk-2",
        offset=1,
        length=4,
        checksum="bad etag",
    )
    store.set_virtual_ref(
        "c/3/0/1",
        f"s3://testbucket/{prefix}/chunk-2",
        offset=1,
        length=4,
        checksum=old,
    )
    store.set_virtual_ref(
        "c/4/0/1",
        f"s3://testbucket/{prefix}/chunk-2",
        offset=1,
        length=4,
        checksum=new,
    )

    # we write a ref that simulates a lost chunk
    store.set_virtual_ref(
        "c/0/0/2", f"s3://testbucket/{prefix}/non-existing", offset=1, length=4
    )

    buffer_prototype = zarr.core.buffer.default_buffer_prototype()

    first = await store.get("c/0/0/0", prototype=buffer_prototype)
    assert first is not None
    assert first.to_bytes() == b"firs"
    assert await store.get("c/1/0/0", prototype=buffer_prototype) == first
    assert await store.get("c/4/0/0", prototype=buffer_prototype) == first

    second = await store.get("c/0/0/1", prototype=buffer_prototype)
    assert second is not None
    assert second.to_bytes() == b"econ"
    assert await store.get("c/1/0/1", prototype=buffer_prototype) == second
    assert await store.get("c/4/0/1", prototype=buffer_prototype) == second

    assert array[0, 0, 0] == 1936877926
    assert array[0, 0, 1] == 1852793701

    # fetch uninitialized chunk should be None
    assert await store.get("c/0/0/3", prototype=buffer_prototype) is None

    # fetching a virtual ref that disappeared should be an exception
    with pytest.raises(IcechunkError):
        # TODO: we should include the key and other info in the exception
        await store.get("c/0/0/2", prototype=buffer_prototype)

    all_locations = set(session.all_virtual_chunk_locations())
    assert f"s3://testbucket/{prefix}/non-existing" in all_locations
    assert f"s3://testbucket/{prefix}/chunk-1" in all_locations
    assert f"s3://testbucket/{prefix}/chunk-2" in all_locations

    # check we cannot get refs with bad etag o that were modified

    with pytest.raises(IcechunkError, match="chunk has changed"):
        await store.get("c/2/0/0", prototype=buffer_prototype)
    with pytest.raises(IcechunkError, match="chunk has changed"):
        await store.get("c/3/0/0", prototype=buffer_prototype)
    with pytest.raises(IcechunkError, match="chunk has changed"):
        await store.get("c/2/0/1", prototype=buffer_prototype)
    with pytest.raises(IcechunkError, match="chunk has changed"):
        await store.get("c/3/0/1", prototype=buffer_prototype)

    _snapshot_id = session.commit("Add virtual refs")


async def test_from_s3_public_virtual_refs(tmpdir: Path) -> None:
    config = RepositoryConfig.default()
    store_config = ObjectStoreConfig.S3(
        S3Options(
            region="us-east-1",
            anonymous=True,
        )
    )
    container = VirtualChunkContainer(
        "sample-data", "s3://earthmover-sample-data", store_config
    )
    config.set_virtual_chunk_container(container)

    repo = Repository.open_or_create(
        storage=local_filesystem_storage(f"{tmpdir}/virtual"),
        config=config,
    )
    session = repo.writable_session("main")
    store = session.store

    root = zarr.Group.from_store(store=store, zarr_format=3)
    year = root.require_array(
        name="year", shape=((72,)), chunks=((72,)), dtype="float32", compressors=None
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
