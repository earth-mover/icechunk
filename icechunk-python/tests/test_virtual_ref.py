import numpy as np
import pytest
import zarr
import zarr.core
import zarr.core.buffer
from icechunk import (
    IcechunkStore,
    S3Credentials,
    StorageConfig,
    StoreConfig,
    VirtualRefConfig,
)
from object_store import ClientOptions, ObjectStore


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
    write_chunks_to_minio(
        [
            ("path/to/python/chunk-1", b"first"),
            ("path/to/python/chunk-2", b"second"),
        ]
    )

    # Open the store
    store = IcechunkStore.open_or_create(
        storage=StorageConfig.memory("virtual"),
        mode="w",
        config=StoreConfig(
            virtual_ref_config=VirtualRefConfig.s3_from_config(
                credentials=S3Credentials(
                    access_key_id="minio123",
                    secret_access_key="minio123",
                ),
                endpoint_url="http://localhost:9000",
                allow_http=True,
                region="us-east-1",
            )
        ),
    )

    array = zarr.Array.create(store, shape=(1, 1, 3), chunk_shape=(1, 1, 1), dtype="i4")

    store.set_virtual_ref(
        "c/0/0/0", "s3://testbucket/path/to/python/chunk-1", offset=0, length=4
    )
    store.set_virtual_ref(
        "c/0/0/1", "s3://testbucket/path/to/python/chunk-2", offset=1, length=4
    )
    # we write a ref that simulates a lost chunk
    await store.async_set_virtual_ref(
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

    _snapshot_id = store.commit("Add virtual refs")


async def test_from_s3_public_virtual_refs(tmpdir):
    # Open the store,
    store = IcechunkStore.open_or_create(
        storage=StorageConfig.filesystem(f'{tmpdir}/virtual'),
        read_only=False,
        config=StoreConfig(
            virtual_ref_config=VirtualRefConfig.s3_anonymous(region="us-east-1", allow_http=False)
        ),
    )
    root = zarr.Group.from_store(store=store, zarr_format=3)
    depth = root.require_array(
        name="depth", shape=((10, )), chunk_shape=((10,)), dtype="float64"
    )

    store.set_virtual_ref(
        "depth/c/0", 
        "s3://noaa-nos-ofs-pds/dbofs/netcdf/202410/dbofs.t00z.20241009.fields.f030.nc",
        offset=119339, 
        length=80
    )

    nodes = [n async for n in store.list()]
    assert "depth/c/0" in nodes

    depth_values = depth[:]
    assert len(depth_values) == 10
    actual_values = np.array([-0.95,-0.85,-0.75,-0.65,-0.55,-0.45,-0.35,-0.25,-0.15,-0.05])
    assert np.allclose(depth_values, actual_values)
