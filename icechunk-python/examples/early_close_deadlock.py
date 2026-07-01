"""Early-exit from `array_chunk_iterator` holds the session read-lock.

The iterator reads lazily, so it keeps the read-lock for as long as it stays
open. Break out early and a later write blocks on the write-lock forever --
unless you call `aclose()` (e.g. via `contextlib.aclosing`) to release it.

    uv run python examples/early_close_deadlock.py
"""

import asyncio

import numpy as np

import icechunk as ic
import zarr
from zarr.core.buffer import default_buffer_prototype


async def write_after_early_break(*, close: bool) -> None:
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    zarr.create_group(store=session.store, overwrite=True).create_array(
        "data", shape=(50_000,), chunks=(1,), dtype="int32"
    )[:] = np.arange(50_000)
    session.commit("write data")

    store = repo.writable_session("main").store
    chunk = default_buffer_prototype().buffer.from_bytes(b"\x00\x00\x00\x00")

    # Scan, stop early. The iterator now holds the session read-lock.
    scan = store.array_chunk_iterator("data", 256)
    async for _batch in scan:
        break  # found what we needed; stop scanning

    # Release the read-lock before writing -- or don't, and deadlock.
    if close:
        await scan.aclose()

    try:
        await asyncio.wait_for(store.set("data/c/0", chunk), timeout=2.0)
        print(f"  close={close!s:5} -> write succeeded")
    except TimeoutError:
        print(f"  close={close!s:5} -> DEADLOCK (read-lock still held)")


async def main() -> None:
    print(f"icechunk {ic.__version__}")
    await write_after_early_break(close=False)
    await write_after_early_break(close=True)


if __name__ == "__main__":
    asyncio.run(main())
