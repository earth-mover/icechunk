"""Request attribution: labels icechunk puts in every request's User-Agent."""

import pickle
import threading
import time
from collections.abc import Iterator
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

import icechunk as ic
import zarr


class FakeStoreHandler(BaseHTTPRequestHandler):
    """Stores PUT bodies, serves GET/HEAD with ranges, records User-Agents."""

    protocol_version = "HTTP/1.1"
    objects: dict[str, bytes] = {}
    modified: dict[str, float] = {}
    seen: list[tuple[str, str, str | None]] = []

    def key(self) -> str:
        return self.path.split("?", 1)[0]

    def record(self) -> None:
        FakeStoreHandler.seen.append(
            (self.command, self.key(), self.headers.get("User-Agent"))
        )

    def do_PUT(self) -> None:
        self.record()
        n = int(self.headers.get("Content-Length") or 0)
        FakeStoreHandler.objects[self.key()] = self.rfile.read(n) if n else b""
        FakeStoreHandler.modified[self.key()] = time.time()
        self.reply(200, b"", {"ETag": '"e"'})

    def do_HEAD(self) -> None:
        self.record()
        body = FakeStoreHandler.objects.get(self.key())
        if body is None:
            self.reply(404, b"", {})
            return
        self.reply(
            200,
            b"",
            {"ETag": '"e"', "Last-Modified": self.last_modified()},
            body_len=len(body),
        )

    def do_GET(self) -> None:
        self.record()
        if "list-type=2" in self.path:
            xml = b'<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>'
            self.reply(200, xml, {"Content-Type": "application/xml"})
            return
        body = FakeStoreHandler.objects.get(self.key())
        if body is None:
            self.reply(
                404,
                b"<Error><Code>NoSuchKey</Code></Error>",
                {"Content-Type": "application/xml"},
            )
            return
        rng = self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            a, b = rng[6:].split("-")
            start = int(a)
            end = min(int(b) if b else len(body) - 1, len(body) - 1)
            self.reply(
                206,
                body[start : end + 1],
                {
                    "ETag": '"e"',
                    "Content-Range": f"bytes {start}-{end}/{len(body)}",
                    "Last-Modified": self.last_modified(),
                },
            )
        else:
            self.reply(200, body, {"ETag": '"e"', "Last-Modified": self.last_modified()})

    def last_modified(self) -> str:
        return formatdate(FakeStoreHandler.modified[self.key()], usegmt=True)

    def reply(
        self,
        status: int,
        body: bytes,
        headers: dict[str, str],
        body_len: int | None = None,
    ) -> None:
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header(
            "Content-Length", str(len(body) if body_len is None else body_len)
        )
        self.send_header("Connection", "close")
        self.end_headers()
        if body:
            self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture
def fake_store() -> Iterator[HTTPServer]:
    FakeStoreHandler.objects = {}
    FakeStoreHandler.modified = {}
    FakeStoreHandler.seen = []
    server = HTTPServer(("127.0.0.1", 0), FakeStoreHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


def test_attribution_validation() -> None:
    a = ic.Attribution(client="weatherlib/0.9", workload="nightly", principal="u_1")
    assert a.client == "weatherlib/0.9"
    assert a.workload == "nightly"
    assert a.principal == "u_1"
    assert a == ic.Attribution(
        client="weatherlib/0.9", workload="nightly", principal="u_1"
    )
    assert ic.Attribution() == ic.Attribution()
    assert ic.Attribution().client is None
    with pytest.raises(ValueError, match="forbidden character"):
        ic.Attribution(workload="a;b")
    with pytest.raises(ValueError, match="longer than"):
        ic.Attribution(principal="x" * 129)
    with pytest.raises(ValueError, match="token characters"):
        ic.Attribution(client="a b")


def test_attribution_survives_pickle() -> None:
    storage = ic.in_memory_storage()
    a = ic.Attribution(workload="w", principal="p")
    repo = ic.Repository.create(storage, attribution=a)
    assert repo.attribution == a
    repo = pickle.loads(pickle.dumps(repo))
    assert repo.attribution == a
    assert ic.Repository.open(storage).attribution == ic.Attribution()


def test_requests_carry_attribution(fake_store: HTTPServer) -> None:
    port = fake_store.server_address[1]
    storage = ic.s3_storage(
        bucket="bucket",
        prefix="prefix",
        region="us-east-1",
        endpoint_url=f"http://127.0.0.1:{port}",
        allow_http=True,
        access_key_id="k",
        secret_access_key="s",
        force_path_style=True,
    )
    config = ic.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 0
    repo = ic.Repository.create(
        storage,
        config=config,
        check_clean_root=False,
        attribution=ic.Attribution(client="wrapper/1.0", workload="wl", principal="me"),
    )
    session = repo.writable_session("main")
    group = zarr.open_group(store=session.store, mode="w")
    array = group.create_array("temp", shape=(4,), chunks=(2,), dtype="u1")
    array[:2] = 7
    session.commit("c")

    puts = [s for s in FakeStoreHandler.seen if s[0] == "PUT" and "/chunks/" in s[1]]
    assert len(puts) == 1, FakeStoreHandler.seen
    ua = puts[0][2]
    assert ua is not None and "wrapper/1.0 icechunk/" in ua, ua
    assert ua.endswith("(workload=wl; principal=me; array=temp; chunk=0)"), ua
    manifests = [
        s for s in FakeStoreHandler.seen if s[0] == "PUT" and "/manifests/" in s[1]
    ]
    assert manifests and all(
        s[2] is not None and s[2].endswith("array=temp)") for s in manifests
    ), manifests
    for _, path, seen_ua in FakeStoreHandler.seen:
        assert seen_ua is not None and "(workload=wl; principal=me" in seen_ua, (
            path,
            seen_ua,
        )
