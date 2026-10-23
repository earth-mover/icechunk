import functools
import pickle
import ssl
import threading
from collections.abc import Callable, Iterator
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

import icechunk as ic
from icechunk.storage import http_storage, redirect_storage

ENV = "ICECHUNK_TLS_CA_FILE"
CONFIG = ic.RepositoryConfig(
    storage=ic.StorageSettings(retries=ic.StorageRetriesSettings(max_tries=1))
)


# a test CA and a localhost server certificate that it signs, valid until 2126
TLS_DIR = Path(__file__).parent / "data" / "tls"
CA = str(TLS_DIR / "ca.pem")


class QuietHandler(SimpleHTTPRequestHandler):
    """Static files with the ETag header that the S3 client requires.

    `/redirect` points `redirect_storage` at the repo on this server."""

    def do_GET(self) -> None:
        if self.path != "/redirect":
            return super().do_GET()
        self.send_response(302)
        self.send_header(
            "Location", f"https+icechunk://{self.headers['Host']}/bucket/repo"
        )
        self.end_headers()

    def end_headers(self) -> None:
        self.send_header("ETag", f'"{abs(hash(self.path))}"')
        super().end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture(scope="module")
def https_port(tmp_path_factory: pytest.TempPathFactory) -> Iterator[int]:
    """Serve a new repo at bucket/repo over HTTPS on localhost."""
    root = tmp_path_factory.mktemp("www")
    ic.Repository.create(ic.local_filesystem_storage(str(root / "bucket/repo")))

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(TLS_DIR / "server.pem", TLS_DIR / "server.key")
    server = ThreadingHTTPServer(
        ("localhost", 0), functools.partial(QuietHandler, directory=str(root))
    )
    server.socket = ctx.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server.server_address[1]
    server.shutdown()
    thread.join()


def http_repo_storage(port: int) -> ic.Storage:
    return http_storage(f"https://localhost:{port}/bucket/repo")


def s3_repo_storage(port: int) -> ic.Storage:
    return ic.s3_storage(
        bucket="bucket",
        prefix="repo",
        endpoint_url=f"https://localhost:{port}",
        force_path_style=True,
        anonymous=True,
        region="us-east-1",
    )


def redirect_repo_storage(port: int) -> ic.Storage:
    return redirect_storage(f"https://localhost:{port}/redirect")


STORAGES = [http_repo_storage, s3_repo_storage, redirect_repo_storage]


def open_error(storage: ic.Storage) -> str:
    with pytest.raises(ic.IcechunkError) as exc_info:
        ic.Repository.open(storage, config=CONFIG)
    return str(exc_info.value)


@pytest.mark.parametrize("make_storage", STORAGES)
def test_rejects_unknown_ca(
    https_port: int,
    make_storage: Callable[[int], ic.Storage],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(ENV, raising=False)
    msg = open_error(make_storage(https_port))
    assert "invalid peer certificate" in msg
    assert ENV in msg


@pytest.mark.parametrize("make_storage", STORAGES)
def test_trusts_ca_file(
    https_port: int,
    make_storage: Callable[[int], ic.Storage],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ENV, CA)
    repo = ic.Repository.open(make_storage(https_port), config=CONFIG)
    assert "main" in repo.list_branches()


def test_recovers_in_process(https_port: int, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV, raising=False)
    open_error(http_repo_storage(https_port))
    monkeypatch.setenv(ENV, CA)
    repo = ic.Repository.open(http_repo_storage(https_port), config=CONFIG)
    assert "main" in repo.list_branches()


def test_bad_ca_file_is_an_error(
    https_port: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(ENV, str(tmp_path / "missing.pem"))
    with pytest.raises(ic.IcechunkError) as exc_info:
        s3_repo_storage(https_port)
    assert ENV in str(exc_info.value)
    assert "missing.pem" in str(exc_info.value)
    msg = open_error(http_repo_storage(https_port))
    assert ENV in msg
    assert "missing.pem" in msg


def test_bad_ca_file_after_unpickle(
    https_port: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # unpickle skips S3Storage::new, so the client gets an empty trust store
    monkeypatch.setenv(ENV, CA)
    repo = ic.Repository.open(s3_repo_storage(https_port), config=CONFIG)
    pickled = pickle.dumps(repo)
    monkeypatch.setenv(ENV, str(tmp_path / "missing.pem"))
    with pytest.raises(ic.IcechunkError) as exc_info:
        pickle.loads(pickled).list_branches()
    assert "invalid peer certificate" in str(exc_info.value)
    assert "missing.pem" in str(exc_info.value)
