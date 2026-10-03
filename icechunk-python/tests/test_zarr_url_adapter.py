"""Tests for the `icechunk:` URL pipeline adapter (ZEP 8).

These need a zarr with URL pipeline support (zarr-developers/zarr-python#4192)
and are skipped otherwise.
"""

import asyncio
import dataclasses
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import closing, contextmanager
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest

import icechunk as ic
import zarr

pytest.importorskip("zarr.abc.url_pipeline")

import xarray as xr
from icechunk import zarr_url_adapter
from icechunk.credentials import LocalFileSystemAccess
from icechunk.zarr_url_adapter import IcechunkURLAdapter, register_url_repository_opener
from zarr.abc.url_pipeline import (
    AdapterResolution,
    PipelineContext,
    PipelineSegment,
    URLPipelineAdapter,
)
from zarr.core.common import AccessModeLiteral
from zarr.errors import URLPipelineError
from zarr.registry import get_url_adapter, list_url_adapter_schemes, register_url_adapter
from zarr.storage import parse_pipeline, resolve_pipeline

# ---------------------------------------------------------------------------
# zarr2:/zarr3: format adapters are not part of zarr-python#4192; register a
# minimal stand-in when the installed zarr has none, so a trailing `|zarr3:`
# can be tested.


class _FormatAdapter(URLPipelineAdapter):
    zarr_format: Any = None

    @classmethod
    async def open_pipeline_segment(
        cls, segment: PipelineSegment, context: PipelineContext
    ) -> AdapterResolution:
        preceding = await context.resolve_preceding()
        path = "/".join(p for p in (preceding.path, segment.body.strip("/")) if p)
        return dataclasses.replace(preceding, path=path, zarr_format=cls.zarr_format)


class _Zarr3Adapter(_FormatAdapter):
    zarr_format = 3


class _Zarr2Adapter(_FormatAdapter):
    zarr_format = 2


for _scheme, _cls in (("zarr3", _Zarr3Adapter), ("zarr2", _Zarr2Adapter)):
    if _scheme not in list_url_adapter_schemes():
        register_url_adapter(_scheme, _cls)


# ---------------------------------------------------------------------------
# fixtures


@dataclasses.dataclass
class RepoInfo:
    repo: ic.Repository
    root: str
    first: str  # first snapshot: the spec examples' tree
    second: str  # main tip: adds `extra`


def _populate(repo: ic.Repository) -> RepoInfo:
    session = repo.writable_session("main")
    root = zarr.group(session.store)
    root.attrs["level"] = "root"
    root.create_group("path/to/node").attrs["level"] = "node"
    arr = root.create_array("path/to/array", shape=(4,), chunks=(2,), dtype="i4")
    arr[:] = np.arange(4)
    root.create_array("a", shape=(2,), dtype="f8")[:] = [1.5, 2.5]
    first = session.commit("first")

    for branch in ("mybranch", "other", "a"):
        repo.create_branch(branch, first)
    for tag in ("mytag", "a", "v5"):
        repo.create_tag(tag, first)

    session = repo.writable_session("main")
    zarr.open_group(session.store).create_group("extra")
    second = session.commit("second")
    return RepoInfo(repo=repo, root="", first=first, second=second)


@pytest.fixture
def local_repo(tmp_path: Path) -> RepoInfo:
    path = tmp_path / "repo.zarr.icechunk"
    info = _populate(ic.Repository.create(ic.local_filesystem_storage(str(path))))
    info.root = f"file://{path}/"
    return info


@pytest.fixture
def memory_repo() -> RepoInfo:
    # memory: storages are shared per name for the life of the process
    name = f"zep8-{uuid.uuid4().hex}"
    info = _populate(ic.Repository.create(ic.storage_from_url(f"memory:{name}")))
    info.root = f"memory://{name}"
    return info


async def _resolve(url: str, **kwargs: Any) -> AdapterResolution:
    return await resolve_pipeline(url, **kwargs)


def _session_of(resolution: AdapterResolution) -> ic.Session:
    store = resolution.store
    assert isinstance(store, ic.IcechunkStore)
    return store.session


# ---------------------------------------------------------------------------
# registration


def test_entry_point_registered() -> None:
    assert "icechunk" in list_url_adapter_schemes()
    assert get_url_adapter("icechunk") is IcechunkURLAdapter
    # the spec requires vendor prefixes for nonstandard names; no alias
    assert "ic" not in list_url_adapter_schemes()


# ---------------------------------------------------------------------------
# every example in schemes/icechunk.md
#
# `{snap}` stands in for the spec's literal snapshot ids, which cannot exist
# in a test repository; the literal ids are checked by test_parse_spec_snapshot_ids.

SPEC_EXAMPLES = [
    # (pipeline suffix after the root, ref ("first"/"second"), residual path)
    ("|icechunk:", "second", ""),
    ("|icechunk", "second", ""),
    ("|icechunk://branch.main/", "second", ""),
    ("|icechunk:path/to/node/", "second", "path/to/node"),
    ("|icechunk:a", "second", "a"),
    ("|icechunk:/path/to/node/", "second", "path/to/node"),
    ("|icechunk:path/to/node/zarr.json", "second", "path/to/node/zarr.json"),
    ("|icechunk:path/to/node/c/0/0/1", "second", "path/to/node/c/0/0/1"),
    ("|icechunk://branch.mybranch/path/to/node/", "first", "path/to/node"),
    ("|icechunk://branch.a", "first", ""),
    ("|icechunk://tag.mytag/path/to/node/", "first", "path/to/node"),
    ("|icechunk://tag.a", "first", ""),
    ("|icechunk://{snap}/path/to/node/", "first", "path/to/node"),
    ("|icechunk:|zarr3:path/to/array/", "second", "path/to/array"),
    ("|icechunk://branch.other/|zarr3:path/to/array/", "first", "path/to/array"),
    ("|icechunk://tag.v5/|zarr3:path/to/array/", "first", "path/to/array"),
    ("|icechunk://{snap}/|zarr3:path/to/array/", "first", "path/to/array"),
]


@pytest.mark.parametrize(("suffix", "ref", "path"), SPEC_EXAMPLES)
@pytest.mark.parametrize("repo_kind", ["local", "memory"])
async def test_spec_examples(
    suffix: str,
    ref: str,
    path: str,
    repo_kind: str,
    request: pytest.FixtureRequest,
) -> None:
    info: RepoInfo = request.getfixturevalue(f"{repo_kind}_repo")
    url = info.root + suffix.format(snap=info.first)
    resolution = await _resolve(url, mode="r")
    assert resolution.path == path
    assert resolution.zarr_format == 3
    assert resolution.store.read_only
    session = _session_of(resolution)
    assert session.read_only
    assert session.snapshot_id == getattr(info, ref)


@pytest.mark.parametrize(
    ("body", "canonical"),
    [
        ("//FWWFQGAW742XMX0F5MF0/path/to/node/", "FWWFQGAW742XMX0F5MF0"),
        # 20 base32 characters carry 100 bits for a 96-bit id; the spec's
        # example sets the 4 unused bits, so it decodes to the id spelled `...VWG`
        ("//ABCDEFGHJKMNPQRSTVWX/path/to/node/", "ABCDEFGHJKMNPQRSTVWG"),
    ],
)
def test_parse_spec_snapshot_ids(body: str, canonical: str) -> None:
    ref = ic.parse_url_ref(body)
    assert ref == ic.UrlRef("snapshot", canonical, "path/to/node")


async def test_spec_example_chunk_key(local_repo: RepoInfo) -> None:
    # `icechunk:path/to/node/zarr.json` addresses the metadata document
    resolution = await _resolve(local_repo.root + "|icechunk:path/to/array/c/1", mode="r")
    buf = await resolution.store.get(
        resolution.path, zarr.core.buffer.default_buffer_prototype()
    )
    assert buf is not None
    resolution = await _resolve(
        local_repo.root + "|icechunk:path/to/node/zarr.json", mode="r"
    )
    buf = await resolution.store.get(
        resolution.path, zarr.core.buffer.default_buffer_prototype()
    )
    assert buf is not None and b'"node_type"' in buf.to_bytes()


# ---------------------------------------------------------------------------
# end to end through zarr and xarray


@pytest.mark.parametrize("repo_kind", ["local", "memory"])
def test_open_group_and_array(repo_kind: str, request: pytest.FixtureRequest) -> None:
    info: RepoInfo = request.getfixturevalue(f"{repo_kind}_repo")

    root = zarr.open_group(f"{info.root}|icechunk:", mode="r")
    assert root.attrs["level"] == "root"
    assert "extra" in root  # main tip

    node = zarr.open_group(f"{info.root}|icechunk://tag.v5/path/to/node", mode="r")
    assert node.attrs["level"] == "node"

    arr = zarr.open_array(f"{info.root}|icechunk://branch.other/path/to/array", mode="r")
    np.testing.assert_array_equal(arr[:], np.arange(4))

    arr = zarr.open_array(f"{info.root}|icechunk://{info.first}/|zarr3:path/to/array")
    np.testing.assert_array_equal(arr[:], np.arange(4))

    old = zarr.open_group(f"{info.root}|icechunk://{info.first}", mode="r")
    assert "extra" not in old


def test_xarray_open_zarr(tmp_path: Path) -> None:
    path = tmp_path / "xr.icechunk"
    repo = ic.Repository.create(ic.local_filesystem_storage(str(path)))
    session = repo.writable_session("main")
    ds = xr.Dataset(
        {"t": (("x", "y"), np.arange(12.0).reshape(3, 4))},
        coords={"x": [10, 20, 30], "y": list("abcd")},
    )
    ds.to_zarr(session.store, group="nested/group", consolidated=False)
    snap = session.commit("write")
    repo.create_tag("v1", snap)

    for url in (
        f"file://{path}|icechunk://tag.v1/nested/group",
        f"file://{path}|icechunk:nested/group|zarr3:",
        f"file://{path}|icechunk://{snap}/|zarr3:nested/group",
    ):
        actual = xr.open_zarr(url, consolidated=False)
        xr.testing.assert_identical(actual.load(), ds)


# ---------------------------------------------------------------------------
# modes


@pytest.mark.parametrize("mode", ["w", "w-", "r+"])
async def test_write_modes_rejected(
    local_repo: RepoInfo, mode: AccessModeLiteral
) -> None:
    with pytest.raises(URLPipelineError, match="writable_session"):
        await _resolve(local_repo.root + "|icechunk:", mode=mode)
    with pytest.raises(URLPipelineError, match="writable_session"):
        zarr.open_group(local_repo.root + "|icechunk:", mode=mode)


@pytest.mark.parametrize("ref", ["//tag.v5", "//branch.main", "//{snap}"])
def test_mode_a_opens_existing_read_only(local_repo: RepoInfo, ref: str) -> None:
    # "a" is zarr.open's default: it must serve the "open" half
    url = f"{local_repo.root}|icechunk:{ref.format(snap=local_repo.first)}/path/to/array"
    arr = zarr.open_array(url, mode="a")
    assert arr.store.read_only
    np.testing.assert_array_equal(arr[:], np.arange(4))
    with pytest.raises(ic.IcechunkError, match=r"(?i)read.only"):
        arr[:] = 0

    group = zarr.open_group(f"{local_repo.root}|icechunk:", mode="a")
    assert group.store.read_only
    assert group.attrs["level"] == "root"
    # default mode of zarr.open is "a"
    assert isinstance(zarr.open(f"{local_repo.root}|icechunk:path/to/node"), zarr.Group)


def test_mode_a_missing_node_fails(local_repo: RepoInfo) -> None:
    # "a" cannot create through a read-only session
    with pytest.raises(Exception, match=r"(?i)read.only|not found|does not exist"):
        zarr.open_group(f"{local_repo.root}|icechunk:does/not/exist", mode="a")


# ---------------------------------------------------------------------------
# invalid pipelines


@pytest.mark.parametrize(
    ("url", "match"),
    [
        ("file:///x|zarr2:|icechunk:", "Zarr v3 only"),
        ("file:///x|zarr3:sub|icechunk:", "Zarr v3 only"),
        ("file:///x.zip|zip:|icechunk:", "not supported"),
        ("file:///x|icechunk:|icechunk:", "not supported"),
        ("file:relative/repo|icechunk:", "absolute path"),
        ("file://somehost/repo|icechunk:", "authority"),
        ("/abs/path|icechunk:", "no URL scheme"),
        ("file:///x|icechunk://branch.a%2Fb/", "must not contain"),
        ("file:///x|icechunk://not-a-ref/", "snapshot id"),
        ("file:///x|icechunk://fwwfqgaw742xmx0f5mf0/", "snapshot id"),
        ("file:///x|icechunk://tag./", "empty tag name"),
        ("file:///x|icechunk:?q=1", "query"),
        ("unknownscheme://x|icechunk:", "unsupported root scheme"),
    ],
)
async def test_invalid_pipelines(url: str, match: str) -> None:
    with pytest.raises(URLPipelineError, match=match):
        await _resolve(url, mode="r")


async def test_icechunk_as_root_rejected() -> None:
    segment = parse_pipeline("file:///x|icechunk:")[1]
    context = PipelineContext(preceding=(), mode="r", storage_options=None)
    with pytest.raises(URLPipelineError, match="needs a root URL"):
        await IcechunkURLAdapter.open_pipeline_segment(segment, context)


async def test_missing_repo_and_refs_are_icechunk_errors(
    local_repo: RepoInfo, tmp_path: Path
) -> None:
    with pytest.raises(ic.IcechunkError):
        await _resolve(f"file://{tmp_path}/nope|icechunk:", mode="r")
    with pytest.raises(ic.IcechunkError):
        await _resolve(local_repo.root + "|icechunk://tag.nope", mode="r")


def test_trailing_zarr2_finds_nothing(local_repo: RepoInfo) -> None:
    # the adapter reports zarr_format=3; a trailing zarr2: overrides it and
    # finds no v2 metadata in a (v3-only) Icechunk repository
    with pytest.raises(zarr.errors.GroupNotFoundError):
        zarr.open_group(local_repo.root + "|icechunk:|zarr2:", mode="r")


# ---------------------------------------------------------------------------
# storage_options


class _Captured(Exception):
    pass


@pytest.fixture
def capture_open(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Replace the Rust open call, capturing its arguments."""
    captured: dict[str, Any] = {}

    def fake(root_url: str, body: str, **kwargs: Any) -> Any:
        captured.update(root_url=root_url, body=body, **kwargs)
        raise _Captured

    monkeypatch.setattr(zarr_url_adapter, "_open_session_from_url_async", fake)
    return captured


async def _capture(url: str, storage_options: dict[str, Any]) -> None:
    with pytest.raises(_Captured):
        await _resolve(url, mode="r", storage_options=storage_options)


async def test_storage_options_icechunk_keys(capture_open: dict[str, Any]) -> None:
    config = ic.RepositoryConfig.default()
    creds = ic.s3_static_credentials(access_key_id="ak", secret_access_key="sk")
    await _capture(
        "s3://bucket/prefix|icechunk://tag.v1/x",
        {
            "icechunk_credentials": creds,
            "icechunk_region": "us-west-2",
            "icechunk_endpoint_url": "http://localhost:9000",
            "icechunk_config": config,
            "icechunk_virtual_chunk_credentials": {
                "s3://other/": ic.s3_anonymous_credentials()
            },
        },
    )
    assert capture_open["root_url"] == "s3://bucket/prefix"
    assert capture_open["body"] == "//tag.v1/x"
    assert isinstance(capture_open["credentials"], ic.Credentials.S3)
    assert capture_open["region"] == "us-west-2"
    assert capture_open["endpoint_url"] == "http://localhost:9000"
    assert capture_open["config"] is config
    vcc = capture_open["authorize_virtual_chunk_access"]
    assert isinstance(vcc["s3://other/"], ic.Credentials.S3)
    assert capture_open["anonymous"] is False
    assert capture_open["writable"] is False


async def test_storage_options_defaults(capture_open: dict[str, Any]) -> None:
    await _capture("s3://bucket|icechunk:", {})
    # nothing passed: Rust falls back to FromEnv credentials
    assert capture_open["credentials"] is None
    assert capture_open["anonymous"] is False
    assert capture_open["authorize_virtual_chunk_access"] is None


async def test_storage_options_dict_credentials(capture_open: dict[str, Any]) -> None:
    await _capture(
        "s3://bucket|icechunk:",
        {"icechunk_credentials": {"access_key_id": "ak", "secret_access_key": "sk"}},
    )
    creds = capture_open["credentials"]
    assert isinstance(creds, ic.Credentials.S3)


async def test_storage_options_fsspec_aliases(capture_open: dict[str, Any]) -> None:
    await _capture(
        "s3://bucket|icechunk:",
        {
            "key": "ak",
            "secret": "sk",
            "token": "tok",
            "endpoint_url": "http://h:1",
            "region": "r",
        },
    )
    assert isinstance(capture_open["credentials"], ic.Credentials.S3)
    assert capture_open["endpoint_url"] == "http://h:1"
    assert capture_open["region"] == "r"

    await _capture("s3://bucket|icechunk:", {"anon": True})
    assert capture_open["anonymous"] is True


@pytest.mark.parametrize(
    ("options", "match"),
    [
        ({"icechunk_bogus": 1}, "unknown storage_options"),
        ({"bogus": 1}, "unknown storage_options"),
        ({"key": "ak"}, "together"),
        ({"token": "t"}, "session token"),
        ({"anon": True, "icechunk_anonymous": True}, "pass only one"),
        ({"region": "a", "icechunk_region": "b"}, "pass only one"),
        ({"icechunk_credentials": {"access_key_id": "ak"}}, "static credentials"),
        ({"icechunk_credentials": object()}, "icechunk_credentials"),
        ({"icechunk_config": {"a": 1}}, "RepositoryConfig"),
    ],
)
async def test_storage_options_invalid(
    capture_open: dict[str, Any], options: dict[str, Any], match: str
) -> None:
    with pytest.raises(URLPipelineError, match=match):
        await _resolve("s3://bucket|icechunk:", mode="r", storage_options=options)
    assert not capture_open


@pytest.mark.parametrize(
    ("url", "options", "match"),
    [
        ("file:///x|icechunk:", {"icechunk_anonymous": True}, "take no credentials"),
        ("memory:x|icechunk:", {"region": "r"}, "only valid for S3"),
        ("gs://b|icechunk:", {"key": "a", "secret": "b"}, "GCS credentials"),
        (
            "s3://b|icechunk:",
            {"icechunk_credentials": ic.GcsCredentials.Anonymous()},
            "S3 credentials",
        ),
        (
            "s3+http://h:1/b|icechunk:",
            {"endpoint_url": "http://x"},
            "already names the endpoint",
        ),
        ("s3://b|icechunk:", {"anon": True, "key": "a", "secret": "b"}, "not both"),
    ],
)
async def test_storage_options_rejected_by_root(
    url: str, options: dict[str, Any], match: str
) -> None:
    with pytest.raises(URLPipelineError, match=match):
        await _resolve(url, mode="r", storage_options=options)


def _rustfs_available() -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(0.5)
        return s.connect_ex(("localhost", 4200)) == 0


@pytest.mark.skipif(
    not _rustfs_available(), reason="needs rustfs on localhost:4200 (just rustfs-up)"
)
@pytest.mark.parametrize("spelling", ["s3", "s3+http"])
def test_s3_credentials_plumbing(spelling: str) -> None:
    prefix = f"zep8-url-adapter-{spelling}-{time.monotonic_ns()}"
    storage = ic.s3_storage(
        bucket="testbucket",
        prefix=prefix,
        endpoint_url="http://localhost:4200",
        allow_http=True,
        force_path_style=True,
        region="us-east-1",
        access_key_id="test123",
        secret_access_key="test123",
    )
    info = _populate(ic.Repository.create(storage))

    if spelling == "s3":
        url = f"s3://testbucket/{prefix}|icechunk://tag.v5/path/to/array"
        options: dict[str, Any] = {
            "icechunk_credentials": {
                "access_key_id": "test123",
                "secret_access_key": "test123",
            },
            "icechunk_endpoint_url": "http://localhost:4200",
            "icechunk_region": "us-east-1",
        }
    else:
        url = f"s3+http://localhost:4200/testbucket/{prefix}|icechunk://tag.v5/path/to/array"
        options = {"key": "test123", "secret": "test123"}
    arr = zarr.open_array(url, mode="r", storage_options=options)
    np.testing.assert_array_equal(arr[:], np.arange(4))
    assert cast(ic.IcechunkStore, arr.store).session.snapshot_id == info.first

    # wrong credentials are rejected by the server, not silently replaced from env
    bad = dict(options)
    if spelling == "s3":
        bad["icechunk_credentials"] = {
            "access_key_id": "nope",
            "secret_access_key": "nope",
        }
    else:
        bad.update(key="nope", secret="nope")
    with pytest.raises(ic.IcechunkError):
        zarr.open_array(url, mode="r", storage_options=bad)


# ---------------------------------------------------------------------------
# virtual chunk credentials and config


def test_config_and_virtual_chunk_credentials(tmp_path: Path) -> None:
    path = tmp_path / "virtual.icechunk"
    chunk_dir = tmp_path / "chunks"
    chunk_dir.mkdir()
    (chunk_dir / "data.bin").write_bytes(np.arange(4, dtype="<i4").tobytes())

    config = ic.RepositoryConfig.default()
    container = ic.VirtualChunkContainer(
        f"file://{chunk_dir}/", ic.local_filesystem_store(str(chunk_dir))
    )
    config.set_virtual_chunk_container(container)
    repo = ic.Repository.create(
        ic.local_filesystem_storage(str(path)),
        config=config,
        authorize_virtual_chunk_access={f"file://{chunk_dir}/": LocalFileSystemAccess},
    )
    session = repo.writable_session("main")
    arr = zarr.create_array(
        session.store,
        name="v",
        shape=(4,),
        chunks=(4,),
        dtype="<i4",
        compressors=None,
        fill_value=0,
    )
    session.store.set_virtual_ref(
        "v/c/0", f"file://{chunk_dir}/data.bin", offset=0, length=16
    )
    session.commit("virtual")
    del arr

    url = f"file://{path}|icechunk:v"
    # not authorized: virtual chunks are blocked
    unauthorized = zarr.open_array(url, mode="r")
    with pytest.raises(ic.IcechunkError):
        unauthorized[:]
    authorized = zarr.open_array(
        url,
        mode="r",
        storage_options={
            "icechunk_virtual_chunk_credentials": {
                f"file://{chunk_dir}/": LocalFileSystemAccess
            },
            "icechunk_config": ic.RepositoryConfig.default(),
        },
    )
    np.testing.assert_array_equal(authorized[:], np.arange(4))


# ---------------------------------------------------------------------------
# HTTP roots and the event loop


class _SlowHandler(SimpleHTTPRequestHandler):
    """Serves a directory with single-range support, after a delay."""

    delay = 0.0

    def do_GET(self) -> None:
        time.sleep(self.delay)
        range_header = self.headers.get("Range")
        path = Path(self.translate_path(self.path))
        if range_header is None or not path.is_file():
            super().do_GET()
            return
        data = path.read_bytes()
        first, last = range_header.removeprefix("bytes=").split("-")
        if first:
            start, end = int(first), int(last) if last else len(data) - 1
        else:
            start, end = len(data) - int(last), len(data) - 1
        end = min(end, len(data) - 1)
        self.send_response(206)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.send_header("Content-Length", str(end - start + 1))
        self.end_headers()
        self.wfile.write(data[start : end + 1])

    def log_message(self, format: str, *args: Any) -> None:
        pass


@contextmanager
def _serve(directory: Path, delay: float) -> Iterator[str]:
    handler = type("Handler", (_SlowHandler,), {"delay": delay})
    server = HTTPServer(("127.0.0.1", 0), partial(handler, directory=str(directory)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def test_http_root(local_repo: RepoInfo, tmp_path: Path) -> None:
    repo_dir = Path(local_repo.root.removeprefix("file://"))
    with _serve(repo_dir, delay=0) as base:
        arr = zarr.open_array(f"{base}|icechunk://tag.v5/path/to/array", mode="r")
        np.testing.assert_array_equal(arr[:], np.arange(4))
        assert zarr.open_group(f"{base}/|icechunk:", mode="a").store.read_only


async def test_adapter_does_not_block_event_loop(local_repo: RepoInfo) -> None:
    """While the adapter waits on slow I/O, other tasks on the loop keep running."""
    repo_dir = Path(local_repo.root.removeprefix("file://"))
    delay = 0.2
    with _serve(repo_dir, delay=delay) as base:
        ticks = 0
        done = False

        async def ticker() -> None:
            nonlocal ticks
            while not done:
                ticks += 1
                await asyncio.sleep(0.01)

        task = asyncio.create_task(ticker())
        start = time.monotonic()
        resolution = await _resolve(f"{base}|icechunk://tag.v5/", mode="r")
        elapsed = time.monotonic() - start
        done = True
        await task

    assert resolution.store.read_only
    # opening needs several slow requests; a blocking adapter would starve the ticker
    assert elapsed >= delay
    assert ticks >= int(elapsed / 0.01) // 3, (ticks, elapsed)


# ---------------------------------------------------------------------------
# repository openers


@pytest.fixture
def clean_openers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(zarr_url_adapter, "_openers", {})
    monkeypatch.setattr(zarr_url_adapter, "_entry_points_by_scheme", None)


@pytest.mark.usefixtures("clean_openers")
async def test_opener_dispatch(local_repo: RepoInfo) -> None:
    calls: list[tuple[PipelineSegment, bool, dict[str, Any]]] = []

    async def opener(
        root: PipelineSegment, *, read_only: bool, storage_options: dict[str, Any]
    ) -> ic.Repository:
        calls.append((root, read_only, storage_options))
        assert root.body == "//org/repo"
        return local_repo.repo

    register_url_repository_opener("test.vendor", opener)
    options = {"test_token": "abc", "icechunk_config": ic.RepositoryConfig.default()}
    resolution = await _resolve(
        "test.vendor://org/repo|icechunk://tag.v5/path/to/node|zarr3:",
        mode="r",
        storage_options=options,
    )
    assert resolution.path == "path/to/node"
    assert resolution.zarr_format == 3
    assert _session_of(resolution).snapshot_id == local_repo.first

    root, read_only, passed = calls[0]
    assert root.scheme == "test.vendor"
    assert read_only is True
    # icechunk_* keys are consumed; the rest is a fresh dict for the opener
    assert passed == {"test_token": "abc"}
    assert options == {"test_token": "abc", "icechunk_config": options["icechunk_config"]}

    # default mode "a" through zarr's sync API
    group = zarr.open_group("test.vendor://org/repo|icechunk:path/to/node")
    assert group.attrs["level"] == "node"
    assert calls[-1][1] is True
    arr = zarr.open_array(
        f"test.vendor://org/repo|icechunk://{local_repo.first}/path/to/array"
    )
    np.testing.assert_array_equal(arr[:], np.arange(4))


@pytest.mark.usefixtures("clean_openers")
async def test_opener_errors(local_repo: RepoInfo) -> None:
    def not_async(
        root: PipelineSegment, *, read_only: bool, storage_options: dict[str, Any]
    ) -> Any:
        return local_repo.repo

    async def wrong_type(
        root: PipelineSegment, *, read_only: bool, storage_options: dict[str, Any]
    ) -> Any:
        return local_repo.repo.readonly_session(branch="main")

    async def raises(
        root: PipelineSegment, *, read_only: bool, storage_options: dict[str, Any]
    ) -> Any:
        raise URLPipelineError("bad root from opener")

    register_url_repository_opener("x.sync", not_async)
    register_url_repository_opener("x.wrong", wrong_type)
    register_url_repository_opener("x.raises", raises)
    register_url_repository_opener("x.ok", lambda *a, **k: _async_value(local_repo.repo))

    with pytest.raises(URLPipelineError, match="must be an async"):
        await _resolve("x.sync://a|icechunk:", mode="r")
    with pytest.raises(URLPipelineError, match=r"not an icechunk\.Repository"):
        await _resolve("x.wrong://a|icechunk:", mode="r")
    with pytest.raises(URLPipelineError, match="bad root from opener"):
        await _resolve("x.raises://a|icechunk:", mode="r")
    with pytest.raises(URLPipelineError, match="writable_session"):
        await _resolve("x.ok://a|icechunk:", mode="w")
    with pytest.raises(URLPipelineError, match="owns its own credentials"):
        await _resolve(
            "x.ok://a|icechunk:", mode="r", storage_options={"icechunk_anonymous": True}
        )
    with pytest.raises(URLPipelineError, match="snapshot id"):
        await _resolve("x.ok://a|icechunk://nope", mode="r")
    with pytest.raises(URLPipelineError, match="unsupported root scheme"):
        await _resolve("x.missing://a|icechunk:", mode="r")


async def _async_value(value: Any) -> Any:
    return value


@pytest.mark.usefixtures("clean_openers")
def test_register_opener_rules() -> None:
    async def opener(
        root: PipelineSegment, *, read_only: bool, storage_options: dict[str, Any]
    ) -> ic.Repository:
        raise AssertionError

    with pytest.raises(ValueError, match="built-in root scheme"):
        register_url_repository_opener("s3", opener)
    with pytest.raises(ValueError, match="built-in root scheme"):
        register_url_repository_opener("FILE", opener)
    register_url_repository_opener("Vendor.X", opener)
    assert zarr_url_adapter._get_opener("vendor.x") is opener
    with pytest.warns(RuntimeWarning, match="replacing"):
        register_url_repository_opener("vendor.x", opener)
    register_url_repository_opener("vendor.x", opener, override=True)


class _FakeEntryPoint:
    def __init__(self, name: str, value: str, obj: Any) -> None:
        self.name = name
        self.value = value
        self.group = zarr_url_adapter.OPENER_ENTRY_POINT_GROUP
        self._obj = obj
        self.loaded = False

    def load(self) -> Any:
        self.loaded = True
        if isinstance(self._obj, Exception):
            raise self._obj
        return self._obj


@pytest.mark.usefixtures("clean_openers")
async def test_opener_entry_points(
    local_repo: RepoInfo, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def first(
        root: PipelineSegment, *, read_only: bool, storage_options: dict[str, Any]
    ) -> ic.Repository:
        return local_repo.repo

    async def second(
        root: PipelineSegment, *, read_only: bool, storage_options: dict[str, Any]
    ) -> ic.Repository:
        raise AssertionError("duplicate entry point must be ignored")

    eps = [
        _FakeEntryPoint("ep.vendor", "pkg:first", first),
        _FakeEntryPoint("ep.vendor", "pkg:second", second),
        _FakeEntryPoint("s3", "pkg:hijack", second),
        _FakeEntryPoint("ep.broken", "pkg:broken", ImportError("boom")),
        _FakeEntryPoint("ep.notcallable", "pkg:value", 42),
        _FakeEntryPoint("ep.unused", "pkg:unused", first),
    ]
    monkeypatch.setattr(
        zarr_url_adapter,
        "entry_points",
        lambda group: [e for e in eps if e.group == group],
    )

    with pytest.warns(RuntimeWarning) as record:
        resolution = await _resolve("ep.vendor://x|icechunk://tag.v5/", mode="r")
    messages = [str(w.message) for w in record]
    assert any("built-in root scheme" in m for m in messages)
    assert any("using 'pkg:first' and ignoring 'pkg:second'" in m for m in messages)
    assert _session_of(resolution).snapshot_id == local_repo.first

    # built-in schemes never reach an opener
    resolution = await _resolve(local_repo.root + "|icechunk:", mode="r")
    assert not eps[2].loaded

    with pytest.raises(URLPipelineError, match="failed to load") as excinfo:
        await _resolve("ep.broken://x|icechunk:", mode="r")
    assert isinstance(excinfo.value.__cause__, ImportError)
    with pytest.raises(URLPipelineError, match="not callable"):
        await _resolve("ep.notcallable://x|icechunk:", mode="r")
    # lazy: only the entry points for the schemes used were loaded
    assert not eps[5].loaded


# ---------------------------------------------------------------------------
# helpers


def test_storage_from_url() -> None:
    a = ic.storage_from_url("memory:zep8-storage-from-url")
    ic.Repository.create(a)
    # same name, same storage
    ic.Repository.open(ic.storage_from_url("memory://zep8-storage-from-url/"))
    with pytest.raises(ValueError, match="unsupported root URL scheme"):
        ic.storage_from_url("zip:x")
    with pytest.raises(ValueError, match="absolute path"):
        ic.storage_from_url("file:relative")
    with pytest.raises(TypeError, match="unsupported credentials"):
        ic.storage_from_url("s3://b", credentials=cast(Any, object()))


def test_parse_url_ref() -> None:
    assert ic.parse_url_ref("") == ic.UrlRef("branch", "main", "")
    assert ic.parse_url_ref("//tag.v1/a/b/") == ic.UrlRef("tag", "v1", "a/b")
    assert ic.parse_url_ref("//branch.my%20branch") == ic.UrlRef(
        "branch", "my branch", ""
    )
    for bad in ("//branch.a%2Fb", "//FWWFQGAW742XMX0F5MF", "//"):
        with pytest.raises(ValueError):
            ic.parse_url_ref(bad)
