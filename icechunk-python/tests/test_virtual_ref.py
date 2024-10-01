from object_store import ClientOptions, ObjectStore
from icechunk import (
    IcechunkStore,
    StorageConfig,
    StoreConfig,
    S3Credentials,
    VirtualRefConfig,
)
import zarr
import zarr.core
import zarr.core.buffer


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

    # Open the store, the S3 credentials must be set in environment vars for this to work for now
    store = await IcechunkStore.open(
        storage=StorageConfig.s3_from_config(
            bucket="testbucket",
            prefix="python-virtual-ref",
            credentials=S3Credentials(
                access_key_id="minio123",
                secret_access_key="minio123",
            ),
            endpoint_url="http://localhost:9000",
            allow_http=True,
        ),
        mode="r+",
        config=StoreConfig(
            virtual_ref_config=VirtualRefConfig.s3_from_config(
                credentials=S3Credentials(
                    access_key_id="minio123",
                    secret_access_key="minio123",
                ),
                endpoint_url="http://localhost:9000",
                allow_http=True,
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
