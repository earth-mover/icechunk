# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "icechunk_pub",  # icechunk==2.0.6
#   "zarr",
#   "numpy",
# ]
# ///
"""What early-closing `array_chunk_iterator` unblocks: dev build vs published 2.0.6.

`array_chunk_iterator` reads the session lazily, so it holds the session's
read-lock while it is open. If you scan to find a chunk to repair, stop early,
then write the fix on the same session, the write needs the write-lock and blocks
behind the still-held read-lock -- a deadlock. You must free the read-lock first:

  * dev build       -> `await scan.aclose()` frees it in O(1).
  * published 2.0.6 -> no aclose(), so the only recourse is to let the iterator
                       FINISH (drain the rest of the array), O(total chunks).

Run it both ways and compare:

    uv run --with third-wheel third-wheel run examples/compare_early_close.py  # published
    uv run python examples/compare_early_close.py                              # dev
"""

import asyncio
import time

import numpy as np

import zarr
from zarr.core.buffer import default_buffer_prototype

try:
    import icechunk_pub as ic  # published wheel, renamed by third-wheel
except ImportError:
    import icechunk as ic  # dev build in this checkout


async def main():
    # A committed array with many chunks.
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    array = zarr.create_group(store=session.store, overwrite=True).create_array(
        "data", shape=(50_000,), chunks=(1,), dtype="int32"
    )
    array[:] = np.arange(50_000)
    session.commit("write data")
    chunk = default_buffer_prototype().buffer.from_bytes(b"\x00\x00\x00\x00")

    # Scan to find a chunk to repair, stopping at the first batch. The iterator
    # now holds the session read-lock for as long as it stays open.
    store = repo.writable_session("main").store
    scan = store.array_chunk_iterator("data", 256)
    async for _batch in scan:
        break

    print(f"icechunk {ic.__version__}  (50,000-chunk array, target found in batch 1)")
    print(f"  iterator           : {type(scan).__module__}.{type(scan).__name__}")

    # Writing while the scan is still open blocks on the read-lock -> deadlock.
    try:
        await asyncio.wait_for(store.set("data/c/0", chunk), timeout=3.0)
        print("  write (scan open)  : succeeded")
    except TimeoutError:
        print("  write (scan open)  : DEADLOCK (gave up at 3s)")

    # Free the read-lock, then the write proceeds. Time the release + write.
    start = time.perf_counter()
    if hasattr(scan, "aclose"):
        await scan.aclose()  # O(1)
        how = "aclose()"
    else:
        async for _batch in scan:  # no aclose(): drain to the end, O(total)
            pass
        how = "drain to finish"
    await store.set("data/c/0", chunk)
    print(
        f"  write (scan freed) : ok via {how} ({(time.perf_counter() - start) * 1e3:.1f} ms)"
    )


if __name__ == "__main__":
    asyncio.run(main())
