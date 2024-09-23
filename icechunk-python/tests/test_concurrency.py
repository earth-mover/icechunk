import asyncio
import random
import zarr

import icechunk

N = 10

async def write_to_store(array, x, y, barrier):
    await barrier.wait()
    await asyncio.sleep(random.uniform(0,0.5))
    # print(f"writing {x},{y}")
    array[x,y] = x*y
    await asyncio.sleep(0)

async def read_store(array, x, y, barrier):
    await barrier.wait()
    while True:
        #print(f"reading {x},{y}")
        value = array[x,y]
        if value == x*y:
            break
        await asyncio.sleep(random.uniform(0,0.2))

async def list_store(store, barrier):
    expected = set(['zarr.json', 'array/zarr.json'] + [f"array/c/{x}/{y}" for x in range(N) for y in range(N)])
    await barrier.wait()
    while True:
        current = set([k async for k in store.list_prefix("")])
        if current == expected:
            break
        await asyncio.sleep(0)

async def test_concurrency():
    store = await icechunk.IcechunkStore.from_config(
        config={"storage": {"type": "in_memory"}, "dataset": {}}, mode="w"
    )

    group = zarr.group(store=store, overwrite=True)
    array = group.create_array("array", shape=(N, N), chunk_shape=(1, 1), dtype="f8", fill_value=1e23)

    barrier = asyncio.Barrier(2*N*N + 1)

    async with asyncio.TaskGroup() as tg:
        task1 = tg.create_task(list_store(store, barrier), name="listing")
 
        for x in range(N):
            for y in range(N):
                write_task = tg.create_task(read_store(array, x, y, barrier), name=f"read {x},{y}")

        for x in range(N):
            for y in range(N):
                write_task = tg.create_task(write_to_store(array, x, y, barrier), name=f"write {x},{y}")

    res=await store.commit("commit")

    array = group["array"]

    for x in range(N):
        for y in range(N):
            assert array[x,y] == x*y
            

    # FIXME: add assertions
    print("done")
