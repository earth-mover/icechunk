import icechunk
from datetime import datetime, UTC
from time import time
import zarr
import asyncio
from random import randrange
from concurrent.futures import ThreadPoolExecutor


def mk_repo(local: bool):
    if local:
        # storage = icechunk.local_filesystem_storage("/tmp/testslow")
        # storage = icechunk.in_memory_storage()
        bucket = "testbucket"
        prefix = "seba-tests/flatbuffers-manifest-u16"

        storage = icechunk.s3_storage(
            bucket=bucket,
            prefix=prefix,
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
            access_key_id="minio123",
            secret_access_key="minio123",
        )

    else:
        bucket = "icechunk-test"
        prefix = "seba-tests/manifest-download-1"

        storage = icechunk.s3_storage(
            bucket=bucket,
            prefix=prefix,
            region="us-east-1",
        )

    config = icechunk.RepositoryConfig.default()
    settings = storage.default_settings()
    conc_settings = settings.concurrency
    # conc_settings.max_concurrent_requests_for_object = 4
    # conc_settings.ideal_concurrent_request_size = 1_000_000
    settings.concurrency = conc_settings
    config.storage = settings
    config.compression = icechunk.CompressionConfig(level=3)

    return icechunk.Repository.open_or_create(
        storage=storage,
        config=config,
    )


async def prepare(repo, num_chunks: int):
    print("Preparing repo")
    session = repo.writable_session(branch="main")
    store = session.store

    group = zarr.open_group(store, zarr_format=3)
    shape = (100 * num_chunks,)
    chunk_shape = (100,)

    group.create_array(
        "array",
        shape=shape,
        chunks=chunk_shape,
        dtype="int8",
        fill_value=0,
        compressors=None,
    )
    session.commit("array created")

    session = repo.writable_session(branch="main")
    store = session.store

    def doit(idx):
        if (idx + 1) % 1000 == 0:
            print(f"{idx+1}/{num_chunks}")
        data_start = randrange(1_000_000_000)
        store.set_virtual_ref(
            f"array/c/{idx}",
            "s3://earthmover-sample-data/netcdf/oscar_vel2018.nc",
            offset=data_start,
            length=100,
            checksum=datetime.now(UTC),
        )

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(doit, range(num_chunks))

    session.commit("data written")


async def read(local: bool):
    print("Opening repo")
    repo = mk_repo(local)

    print("Creating store")
    session = repo.readonly_session(branch="main")
    store = session.store
    print("Opening group")
    group = zarr.open_group(store=store, zarr_format=3, mode="r")

    print("Reading data")
    start = time()
    #for idx in range(10_000):
    #    if (idx + 1) % 1000 == 0:
    #        print(f"{idx+1}/10_000")
    #    group["array"][0]
    print(group["array"][0])
    end = time()
    print(end - start)


asyncio.run(prepare(mk_repo(True), 1_000_000))
#asyncio.run(read(True))


#
#
# with flatbuffers:
#    manifest size: 14.4MB (compression 1)
#    writing 1M chunks took 1:15 min,
#    the read of first item took 1.6 sec
#
# with msgpack:
#    manifest size:  6.5MB (copmression 1), 7MB (compression 3)
#    writing 1M chunks took 1:19 min 
#    the read of first item took  1.7 sec
