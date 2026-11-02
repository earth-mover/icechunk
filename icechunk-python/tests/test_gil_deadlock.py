"""A thread using a session must not deadlock against a commit on that session.

Each scenario runs in a child interpreter because a deadlocked thread holds the
GIL, so nothing in the test process could interrupt it.
"""

import subprocess
import sys

import pytest

from tests.conftest import Permission

SCRIPT = """
import faulthandler
import os
import sys
import tempfile
import threading
import time
import uuid
from datetime import UTC, datetime

import icechunk
import zarr

faulthandler.dump_traceback_later(30, exit=True)


def exit_on_thread_error(args):
    threading.__excepthook__(args)
    os._exit(1)


threading.excepthook = exit_on_thread_error
storage_kind, call_name, access_key, secret_key = sys.argv[1:]


def expired_credentials():
    return icechunk.S3StaticCredentials(
        access_key_id=access_key,
        secret_access_key=secret_key,
        expires_after=datetime.now(UTC),
    )


if storage_kind == "inline-chunks":
    # Inline chunks keep the bytes zarr passed in, which are Python buffers.
    # The commit releases them while it holds the session lock.
    storage = icechunk.local_filesystem_storage(tempfile.mkdtemp())
    inline_threshold = 512
else:
    # Expired credentials make every S3 request call back into Python while
    # the commit holds the session lock.
    storage = icechunk.s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:4200",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix=f"gil-deadlock-{uuid.uuid4()}",
        get_credentials=expired_credentials,
    )
    inline_threshold = 0

# Each call takes the session lock from a thread that holds the GIL.
# zarr calls str(store) itself whenever it builds an array.
call = {
    "str-store": lambda s: str(s.store),
    "str-session": lambda s: str(s),
    "chunk-type": lambda s: s.chunk_type("/a", [0]),
    "shift-array": lambda s: s.shift_array("/a", [0]),
    "reindex-array": lambda s: s.reindex_array("/a", lambda idx: idx),
}[call_name]

config = icechunk.RepositoryConfig(inline_chunk_threshold_bytes=inline_threshold)
repo = icechunk.Repository.create(storage, config=config)
session = repo.writable_session("main")
zarr.create_array(session.store, name="a", shape=(4,), chunks=(1,), dtype="i4")
session.commit("create")

current = repo.writable_session("main")
done = threading.Event()


def committer():
    global current
    end = time.monotonic() + 3
    i = 0
    while time.monotonic() < end:
        s = current
        zarr.open_array(s.store, path="a")[0] = i
        s.commit(f"c{i}")
        current = repo.writable_session("main")
        i += 1
    done.set()


def caller():
    while not done.is_set():
        try:
            call(current)
        except icechunk.IcechunkError:
            # Changing a session that has just been committed fails.
            pass


threads = [threading.Thread(target=committer), threading.Thread(target=caller)]
for t in threads:
    t.start()
for t in threads:
    t.join()
"""


@pytest.mark.parametrize("storage_kind", ["inline-chunks", "refreshable-credentials"])
@pytest.mark.parametrize(
    "call", ["str-store", "str-session", "chunk-type", "shift-array", "reindex-array"]
)
def test_call_during_commit(storage_kind: str, call: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", SCRIPT, storage_kind, call, *Permission.MODIFY.keys()],
        capture_output=True,
        check=False,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
