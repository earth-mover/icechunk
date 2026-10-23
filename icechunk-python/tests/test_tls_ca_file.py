import functools
import pickle
import shutil
import ssl
import subprocess
import threading
from collections.abc import Iterator
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


def openssl(*args: str, cwd: Path) -> None:
    subprocess.run(["openssl", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture(scope="module")
def tls_files(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    """A test CA and a localhost server certificate that it signs."""
    if shutil.which("openssl") is None:
        pytest.fail("the openssl CLI is required for TLS tests")
    d = tmp_path_factory.mktemp("tls")
    ec = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes"]
    # fmt: off
    openssl("req", "-x509", *ec, "-keyout", "ca.key", "-out", "ca.pem", "-days", "2",
            "-subj", "/CN=icechunk test CA",
            "-addext", "basicConstraints=critical,CA:TRUE",
            "-addext", "keyUsage=critical,keyCertSign", cwd=d)
    openssl("req", *ec, "-keyout", "server.key", "-out", "server.csr",
            "-subj", "/CN=localhost", cwd=d)
    (d / "server.ext").write_text(
        "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
        "basicConstraints=CA:FALSE\n"
        "extendedKeyUsage=serverAuth\n"
    )
    openssl("x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
            "-CAcreateserial", "-out", "server.pem", "-days", "2",
            "-extfile", "server.ext", cwd=d)
    # fmt: on
    return {"ca": d / "ca.pem", "cert": d / "server.pem", "key": d / "server.key"}


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
def https_port(
    tmp_path_factory: pytest.TempPathFactory, tls_files: dict[str, Path]
) -> Iterator[int]:
    """Serve a new repo at bucket/repo over HTTPS on localhost."""
    root = tmp_path_factory.mktemp("www")
    ic.Repository.create(ic.local_filesystem_storage(str(root / "bucket/repo")))

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(tls_files["cert"], tls_files["key"])
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


def open_error(storage: ic.Storage) -> str:
    with pytest.raises(ic.IcechunkError) as exc_info:
        ic.Repository.open(storage, config=CONFIG)
    return str(exc_info.value)


def test_http_storage_rejects_unknown_ca(
    https_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(ENV, raising=False)
    msg = open_error(http_repo_storage(https_port))
    assert "invalid peer certificate" in msg
    assert ENV in msg


def test_s3_storage_rejects_unknown_ca(
    https_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(ENV, raising=False)
    msg = open_error(s3_repo_storage(https_port))
    assert "invalid peer certificate" in msg
    assert ENV in msg


def test_http_storage_trusts_ca_file(
    https_port: int, tls_files: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(ENV, str(tls_files["ca"]))
    repo = ic.Repository.open(http_repo_storage(https_port), config=CONFIG)
    assert "main" in repo.list_branches()


def test_s3_storage_trusts_ca_file(
    https_port: int, tls_files: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(ENV, str(tls_files["ca"]))
    repo = ic.Repository.open(s3_repo_storage(https_port), config=CONFIG)
    assert "main" in repo.list_branches()


def test_redirect_storage_rejects_unknown_ca(
    https_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(ENV, raising=False)
    msg = open_error(redirect_storage(f"https://localhost:{https_port}/redirect"))
    assert "invalid peer certificate" in msg
    assert ENV in msg


def test_redirect_storage_trusts_ca_file(
    https_port: int, tls_files: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(ENV, str(tls_files["ca"]))
    storage = redirect_storage(f"https://localhost:{https_port}/redirect")
    repo = ic.Repository.open(storage, config=CONFIG)
    assert "main" in repo.list_branches()


def test_recovers_in_process(
    https_port: int, tls_files: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(ENV, raising=False)
    open_error(http_repo_storage(https_port))
    monkeypatch.setenv(ENV, str(tls_files["ca"]))
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
    https_port: int,
    tls_files: dict[str, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # unpickle skips S3Storage::new, so the client gets an empty trust store
    monkeypatch.setenv(ENV, str(tls_files["ca"]))
    repo = ic.Repository.open(s3_repo_storage(https_port), config=CONFIG)
    pickled = pickle.dumps(repo)
    monkeypatch.setenv(ENV, str(tmp_path / "missing.pem"))
    with pytest.raises(ic.IcechunkError) as exc_info:
        pickle.loads(pickled).list_branches()
    assert "invalid peer certificate" in str(exc_info.value)
    assert "missing.pem" in str(exc_info.value)
