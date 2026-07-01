"""Property tests for HTTP virtual chunk reads.

A local HTTP server serves a temp directory with byte-range support. Hypothesis
generates object keys (including the percent-encoding-sensitive characters the
``set_virtual_ref`` contract calls out) and byte ranges; each example writes a
file, registers an HTTP virtual-chunk container pointing at the server (including
its ephemeral port), reads the chunk back through icechunk, and asserts it equals
the bytes on disk.

This guards two things at once: the read path must keep the URL's port (a server
on a non-default port is unreachable otherwise), and an opaque, verbatim,
percent-decoded key must round-trip through icechunk and object_store back to the
literal on-disk name.

The handler is custom because object_store fetches virtual chunks with ranged GETs
and the stdlib SimpleHTTPRequestHandler ignores ``Range`` (it always replies 200).
"""

import http.server
import string
import threading
import urllib.parse
import uuid
from pathlib import Path

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import zarr
from icechunk import (
    RepositoryConfig,
    VirtualChunkContainer,
    http_store,
    in_memory_storage,
)
from icechunk.repository import Repository

# POSIX filenames may contain anything but '/' and NUL, so this also covers the
# characters the set_virtual_ref docstring requires callers to percent-encode
# ('?' '#' '%'). '/' is excluded so it stays a path separator; '.'/'..' segments
# are excluded below because object_store::path::Path rejects them. ASCII only,
# to avoid the NFD filename normalization that would break round-trips on macOS.
SEGMENT_ALPHABET = string.ascii_letters + string.digits + " -_.()+,=~!$#%?&@;'"


class RangeHandler(http.server.BaseHTTPRequestHandler):
    """Serve files under ``root`` with byte-range support (object_store needs 206)."""

    root: Path  # injected on a per-server subclass

    def _resolve(self) -> Path | None:
        rel = urllib.parse.unquote(self.path.split("?", 1)[0].lstrip("/"))
        target = (self.root / rel).resolve()
        root = self.root.resolve()
        if target != root and root not in target.parents:
            return None
        return target

    def _serve(self, *, head: bool) -> None:
        target = self._resolve()
        if target is None or not target.is_file():
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        data = target.read_bytes()
        rng = self.headers.get("Range", "")
        if rng.startswith("bytes="):
            lo, _, hi = rng[6:].partition("-")
            start = int(lo) if lo else 0
            end = min(int(hi), len(data) - 1) if hi else len(data) - 1
            body = data[start : end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        else:
            body = data
            self.send_response(200)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head:
            self.wfile.write(body)

    def do_HEAD(self) -> None:
        self._serve(head=True)

    def do_GET(self) -> None:
        self._serve(head=False)

    def log_message(self, *args: object) -> None:
        pass


@pytest.fixture(scope="session")
def http_server(
    tmp_path_factory: pytest.TempPathFactory,
) -> "tuple[Path, str]":
    root = tmp_path_factory.mktemp("http_root")
    handler = type("BoundRangeHandler", (RangeHandler,), {"root": root})
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield root, f"http://127.0.0.1:{port}/"
    finally:
        httpd.shutdown()
        thread.join()


segments = st.text(alphabet=SEGMENT_ALPHABET, min_size=1, max_size=24).filter(
    lambda s: s not in (".", "..")
)


@st.composite
def ref_cases(draw: st.DrawFn) -> tuple[str, bytes, int, int]:
    key = "/".join(draw(st.lists(segments, min_size=1, max_size=4)))
    content = draw(st.binary(min_size=1, max_size=1024))
    offset = draw(st.integers(min_value=0, max_value=len(content) - 1))
    length = draw(st.integers(min_value=1, max_value=len(content) - offset))
    return key, content, offset, length


@settings(max_examples=100, deadline=None)
@given(case=ref_cases())
def test_http_virtual_ref_roundtrip(
    http_server: tuple[Path, str], case: tuple[str, bytes, int, int]
) -> None:
    root, base = http_server
    key, content, offset, length = case

    # The server root is shared across examples; a fresh prefix keeps each
    # example's keys from colliding on disk (e.g. "0" as a file vs "0/0" as a dir).
    rel = f"{uuid.uuid4().hex}/{key}"
    target = root / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)

    location = base + "/".join(urllib.parse.quote(seg, safe="") for seg in rel.split("/"))

    config = RepositoryConfig.default()
    config.set_virtual_chunk_container(
        VirtualChunkContainer(
            url_prefix=base,
            store=http_store({"allow_http": "true"}),
        )
    )
    repo = Repository.create(
        storage=in_memory_storage(),
        config=config,
        authorize_virtual_chunk_access={base: None},
    )

    session = repo.writable_session("main")
    group = zarr.create_group(session.store)
    group.create_array(
        "a", shape=(length,), chunks=(length,), dtype="uint8", compressors=None
    )
    session.store.set_virtual_ref("a/c/0", location, offset=offset, length=length)
    session.commit("virtual ref")

    out = np.asarray(zarr.open_array(repo.readonly_session("main").store, path="a")[:])
    expected = np.frombuffer(content[offset : offset + length], dtype="uint8")
    np.testing.assert_array_equal(out, expected)
