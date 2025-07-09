import asyncio
import concurrent.futures
import random
import time
import uuid
from random import randrange
from threading import Event

import pytest
from termcolor import colored

import icechunk
import zarr
from tests.conftest import write_chunks_to_minio

N = 15


async def write_to_store(
    array: zarr.Array, x: int, y: int, barrier: asyncio.Barrier
) -> None:
    await barrier.wait()
    await asyncio.sleep(random.uniform(0, 0.5))
    array[x, y] = x * y
    # await asyncio.sleep(0)


async def read_store(array: zarr.Array, x: int, y: int, barrier: asyncio.Barrier) -> None:
    await barrier.wait()
    while True:
        value = array[x, y]
        if value == x * y:
            break
        await asyncio.sleep(random.uniform(0, 0.1))


async def list_store(store: icechunk.IcechunkStore, barrier: asyncio.Barrier) -> None:
    expected = set(
        ["zarr.json", "array/zarr.json"]
        + [f"array/c/{x}/{y}" for x in range(N) for y in range(N - 1)]
    )
    await barrier.wait()
    while True:
        current: set[str] | None = set([k async for k in store.list_prefix("")])
        if current == expected:
            break
        current = None
        await asyncio.sleep(0.1)


async def test_concurrency() -> None:
    repo = icechunk.Repository.open_or_create(
        storage=icechunk.in_memory_storage(),
    )

    session = repo.writable_session("main")
    store = session.store

    group = zarr.group(store=store, overwrite=True)
    array = group.create_array(
        "array", shape=(N, N), chunks=(1, 1), dtype="f8", fill_value=1e23
    )

    barrier = asyncio.Barrier(2 * N * (N - 1) + 1)

    async with asyncio.TaskGroup() as tg:
        _task1 = tg.create_task(list_store(store, barrier), name="listing")

        for x in range(N):
            for y in range(N - 1):
                _write_task = tg.create_task(
                    read_store(array, x, y, barrier), name=f"read {x},{y}"
                )

        for x in range(N):
            for y in range(N - 1):
                _write_task = tg.create_task(
                    write_to_store(array, x, y, barrier), name=f"write {x},{y}"
                )

    all_coords = {coords async for coords in session.chunk_coordinates("/array")}
    assert all_coords == {(x, y) for x in range(N) for y in range(N - 1)}

    _res = session.commit("commit")

    assert isinstance(group["array"], zarr.Array)
    array = group["array"]
    assert isinstance(array, zarr.Array)

    for x in range(N):
        for y in range(N - 1):
            assert array[x, y] == x * y


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
async def test_thread_concurrency() -> None:
    """Run multiple threads doing different type of operations for SECONDS_TO_RUN seconds.

    The threads execute 5 types of operations: reads, native writes, virtual writes, deletes and lists.

    We launch THREADS threads of each type, and at the end we assert all operation types executed
    a few times.

    The output prints a block character for each operation, colored according to the operation type.
    The expectation is blocks of different colors should interleave.
    """
    THREADS = 20
    SECONDS_TO_RUN = 1

    prefix = str(uuid.uuid4())
    write_chunks_to_minio(
        [
            (f"{prefix}/chunk-1", b"first"),
            (f"{prefix}/chunk-2", b"second"),
        ],
    )

    config = icechunk.RepositoryConfig.default()
    store_config = icechunk.s3_store(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        s3_compatible=True,
    )
    container = icechunk.VirtualChunkContainer("s3://testbucket", store_config)
    config.set_virtual_chunk_container(container)
    config.inline_chunk_threshold_bytes = 0
    credentials = icechunk.containers_credentials(
        {
            "s3://testbucket": icechunk.s3_credentials(
                access_key_id="minio123", secret_access_key="minio123"
            )
        }
    )

    storage = icechunk.s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix="multithreaded-test__" + str(time.time()),
        access_key_id="minio123",
        secret_access_key="minio123",
    )

    # Open the store
    repo = icechunk.Repository.create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access=credentials,
    )

    session = repo.writable_session("main")
    store = session.store

    group = zarr.group(store=store, overwrite=True)
    group.create_array("array", shape=(1_000,), chunks=(1,), dtype="i4", compressors=None)

    def do_virtual_writes(start, stop) -> int:
        n = 0
        start.wait()
        while not stop.is_set():
            i = randrange(1_000)
            store.set_virtual_ref(
                f"array/c/{i}",
                f"s3://testbucket/{prefix}/chunk-1",
                offset=0,
                length=4,
            )
            print(colored("■", "green"), end="")
            n += 1
        return n

    def do_native_writes(start, stop) -> int:
        async def do() -> int:
            n = 0
            while not stop.is_set():
                i = randrange(1_000)
                await store.set(
                    f"array/c/{i}", zarr.core.buffer.cpu.Buffer.from_bytes(b"0123")
                )
                print(colored("■", "white"), end="")
                n += 1
            return n

        start.wait()
        return asyncio.run(do())

    def do_reads(start, stop) -> int:
        buffer_prototype = zarr.core.buffer.default_buffer_prototype()

        async def do() -> int:
            n = 0
            while not stop.is_set():
                i = randrange(1_000)
                await store.get(f"array/c/{i}", prototype=buffer_prototype)
                print(colored("■", "blue"), end="")
                n += 1
            return n

        start.wait()
        return asyncio.run(do())

    def do_deletes(start, stop) -> int:
        async def do() -> int:
            n = 0
            while not stop.is_set():
                i = randrange(1_000)
                await store.delete(f"array/c/{i}")
                print(colored("■", "red"), end="")
                n += 1
            return n

        start.wait()
        return asyncio.run(do())

    def do_lists(start, stop) -> int:
        async def do() -> int:
            n = 0
            while not stop.is_set():
                _ = [k async for k in store.list_prefix("")]
                print(colored("■", "yellow"), end="")
                n += 1
            return n

        start.wait()
        return asyncio.run(do())

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS * 5) as pool:
        virtual_writes = []
        native_writes = []
        deletes = []
        reads = []
        lists = []

        start = Event()
        stop = Event()

        for _ in range(THREADS):
            virtual_writes.append(pool.submit(do_virtual_writes, start, stop))
            native_writes.append(pool.submit(do_native_writes, start, stop))
            deletes.append(pool.submit(do_deletes, start, stop))
            reads.append(pool.submit(do_reads, start, stop))
            lists.append(pool.submit(do_lists, start, stop))

        start.set()
        time.sleep(SECONDS_TO_RUN)
        stop.set()

        virtual_writes_count = sum(future.result() for future in virtual_writes)
        native_writes_count = sum(future.result() for future in native_writes)
        deletes_count = sum(future.result() for future in deletes)
        reads_count = sum(future.result() for future in reads)
        lists_count = sum(future.result() for future in lists)

    print()
    print(
        f"virtual writes: {virtual_writes_count}, native writes: {native_writes_count}, deletes: {deletes_count}, reads: {reads_count}, lists: {lists_count}"
    )

    assert virtual_writes_count > 2
    assert native_writes_count > 2
    assert deletes_count > 2
    assert reads_count > 2
    assert lists_count > 2
