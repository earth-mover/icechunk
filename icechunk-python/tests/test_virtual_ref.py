import numpy as np
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


async def test_write_minino_virtual_refs():
    write_chunks_to_minio(
        [
            ("path/to/python/chunk-1", b"first"),
            ("path/to/python/chunk-2", b"second"),
        ]
    )

    # Open the store
    store = await IcechunkStore.open(
        storage=StorageConfig.memory("virtual"),
        mode="r+",
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

    array = zarr.Array.create(store, shape=(1, 1, 2), chunk_shape=(1, 1, 1), dtype="i4")

    await store.set_virtual_ref(
        "c/0/0/0", "s3://testbucket/path/to/python/chunk-1", offset=0, length=4
    )
    await store.set_virtual_ref(
        "c/0/0/1", "s3://testbucket/path/to/python/chunk-2", offset=1, length=4
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

    _snapshot_id = await store.commit("Add virtual refs")


async def test_from_s3_public_virtual_refs(tmpdir):
    # Open the store,
    store = await IcechunkStore.open(
        storage=StorageConfig.filesystem(f'{tmpdir}/virtual'),
        mode="w",
        config=StoreConfig(
            virtual_ref_config=VirtualRefConfig.s3_anonymous()
        ),
    )
    root = zarr.Group.from_store(store=store, zarr_format=3)
    depth = root.require_array(
        name="depth", shape=((22, )), chunk_shape=((22,)), dtype="float64"
    )

    await store.set_virtual_ref(
        "depth/c/0", 
        "s3://noaa-nos-ofs-pds/dbofs/netcdf/202410/dbofs.t00z.20241009.regulargrid.f030.nc", 
        offset=42499, 
        length=176
    )

    nodes = [n async for n in store.list()]
    assert "depth/c/0" in nodes

    depth_values = depth[:]
    assert len(depth_values) == 22
    actual_values = np.array([
          0.,   1.,   2.,   4.,   6.,   8.,  10.,  12.,  15.,  20.,  25.,
        30.,  35.,  40.,  45.,  50.,  60.,  70.,  80.,  90., 100., 125.
    ])
    assert np.allclose(depth_values, actual_values)



