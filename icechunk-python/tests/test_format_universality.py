"""Test that the icechunk on-disk format is readable by independent non-Rust libraries;
specifically the `flatbuffers` and `zstandard` Python packages.

Prerequisites:
    - `flatc` must be installed (e.g. `brew install flatbuffers` or `apt-get install flatbuffers-compiler`)
    - `flatbuffers` and `zstandard` pip packages
"""

from __future__ import annotations

import enum
import importlib
import subprocess
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import zstandard

import icechunk as ic
import zarr

# grab the source .fbs files
FLATBUFFERS_DIR = Path(__file__).resolve().parents[2] / "icechunk-format" / "flatbuffers"

# ---------------------------------------------------------------------------
# Format constants (mirrored from icechunk-format/src/lib.rs:354-486)
# ---------------------------------------------------------------------------

MAGIC_BYTES = "ICE🧊CHUNK".encode()  # 12 bytes
HEADER_SIZE = 39  # 12 magic + 24 impl name + 1 spec version + 1 file type + 1 compression
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


class FileType(enum.IntEnum):
    Snapshot = 1
    Manifest = 2
    Attributes = 3
    TransactionLog = 4
    Chunk = 5
    RepoInfo = 6


class Compression(enum.IntEnum):
    Uncompressed = 0
    Zstd = 1


class SpecVersion(enum.IntEnum):
    V1 = 1
    V2 = 2


class LocationCompression(enum.IntEnum):
    Uncompressed = 0
    ZstdDict = 1


# ---------------------------------------------------------------------------
# Header helpers
# ---------------------------------------------------------------------------


def assert_valid_object_id(obj_id: Any, size: int = 12) -> None:
    """Assert that a flatbuffer ObjectId struct has the expected byte length and is non-zero."""
    assert obj_id is not None, "ObjectId is None"
    id_bytes = obj_id.Bytes()
    assert len(id_bytes) == size, f"Expected {size}-byte ObjectId, got {len(id_bytes)}"
    assert any(b != 0 for b in id_bytes), "ObjectId is all zeros"


def parse_header(data: bytes) -> tuple[str, int, int, int]:
    """Parse a 39-byte icechunk file header.

    Returns (impl_name, spec_version, file_type, compression).
    Raises AssertionError if magic bytes don't match.
    """
    assert len(data) >= HEADER_SIZE, f"File too short for header: {len(data)} bytes"
    magic = data[:12]
    assert magic == MAGIC_BYTES, f"Bad magic bytes: {magic!r} (expected {MAGIC_BYTES!r})"
    impl_name = data[12:36].decode("utf-8").strip()
    spec_version = data[36]
    file_type = data[37]
    compression = data[38]
    return impl_name, spec_version, file_type, compression


def assert_valid_header(
    data: bytes, expected_file_type: int, *, filename: str = ""
) -> bytes:
    """Parse header, assert invariants, decompress, and return the flatbuffer payload."""
    impl_name, spec_version, file_type, compression = parse_header(data)
    label = f"{filename}: " if filename else ""
    assert impl_name.startswith("ic-"), (
        f"{label}impl_name doesn't start with 'ic-': {impl_name!r}"
    )
    assert spec_version == SpecVersion.V2, (
        f"{label}expected spec version {SpecVersion.V2}, got {spec_version}"
    )
    assert file_type == expected_file_type, (
        f"{label}expected file type {expected_file_type}, got {file_type}"
    )
    assert compression == Compression.Zstd, (
        f"{label}expected zstd compression, got {compression}"
    )
    raw_payload = data[HEADER_SIZE:]
    dctx = zstandard.ZstdDecompressor()
    return dctx.decompress(raw_payload, max_output_size=64 * 1024 * 1024)


@pytest.fixture(scope="session")
def flatbuf_mod() -> Generator[ModuleType]:
    """Compile .fbs schemas with flatc and import the generated Python module.

    Uses --gen-onefile to produce a single Python file with all types,
    avoiding cross-module import issues in the generated code.
    """
    with tempfile.TemporaryDirectory(prefix="icechunk_flatbuf_") as tmpdir:
        result = subprocess.run(
            [
                "flatc",
                "--python",
                "--gen-all",
                "--gen-onefile",
                "-o",
                tmpdir,
                str(FLATBUFFERS_DIR / "all.fbs"),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"flatc failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )

        generated_file = Path(tmpdir, "all_generated.py")
        assert generated_file.is_file(), f"Expected generated file at {generated_file}"

        sys.path.insert(0, tmpdir)
        try:
            mod = importlib.import_module("all_generated")
            yield mod
        finally:
            sys.path.remove(tmpdir)


@pytest.fixture(scope="session")
def repo_path() -> Generator[str]:
    """Create a fresh icechunk repo in a temp directory.

    The repo has materialized, inline, and virtual chunks across multiple commits,
    branches, and tags. Virtual chunk location compression is enabled with a low
    threshold so dictionary compression kicks in.
    """
    with tempfile.TemporaryDirectory(prefix="icechunk_test_repo_") as tmpdir:
        config = ic.RepositoryConfig.default()
        config.inline_chunk_threshold_bytes = 12
        config.manifest = ic.ManifestConfig(
            virtual_chunk_location_compression=ic.ManifestVirtualChunkLocationCompressionConfig(
                min_num_chunks=1,
            ),
        )
        virtual_store_config = ic.s3_store(
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
            s3_compatible=True,
            force_path_style=True,
        )
        config.set_virtual_chunk_container(
            ic.VirtualChunkContainer("s3://fake-bucket/", virtual_store_config)
        )

        repo = ic.Repository.create(
            storage=ic.local_filesystem_storage(tmpdir),
            config=config,
        )

        session = repo.writable_session("main")
        store = session.store
        root = zarr.group(store=store)
        group1 = root.create_group("group1")

        # will be virtual
        root = zarr.group(store=store)
        root.create_array(
            "virtual_array",
            shape=(200,),
            chunks=(1,),
            dtype="float32",
            fill_value=0.0,
        )

        # will be materialized
        big_arr = group1.create_array(
            "big_chunks",
            shape=(10, 10),
            chunks=(5, 5),
            dtype="float32",
            fill_value=float("nan"),
        )

        # will be inline
        small_arr = group1.create_array(
            "small_chunks",
            shape=(5,),
            chunks=(1,),
            dtype="int8",
            fill_value=0,
        )
        session.commit("create structure")

        # --- Commit 2: write data ---
        session = repo.writable_session("main")
        store = session.store
        big_arr = zarr.open_array(store, path="group1/big_chunks", mode="a")
        small_arr = zarr.open_array(store, path="group1/small_chunks", mode="a")
        big_arr[:] = 42.0
        small_arr[:] = 7
        snap2 = session.commit("fill data")

        # --- Commit 3: set many virtual chunk refs (fake URLs) ---
        session = repo.writable_session("main")
        store = session.store

        # Set 200 virtual chunk refs with fake s3:// URLs
        for i in range(200):
            store.set_virtual_ref(
                f"virtual_array/c/{i}",
                f"s3://fake-bucket/path/to/data/chunk-{i:04d}.dat",
                offset=i * 100,
                length=100,
            )
        snap3 = session.commit("set virtual chunks")

        # --- Branch + tag ---
        repo.create_branch("test-branch", snapshot_id=snap2)
        repo.create_tag("v1.0", snapshot_id=snap3)

        yield tmpdir


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_repo_info_file(repo_path: str, flatbuf_mod: ModuleType) -> None:
    """Read the repo info file, verify header + flatbuffer content."""
    Repo = flatbuf_mod.Repo

    data = Path(repo_path, "repo").read_bytes()
    payload = assert_valid_header(data, FileType.RepoInfo, filename="repo")

    repo = Repo.GetRootAs(payload)
    assert repo.SpecVersion() == SpecVersion.V2
    assert repo.BranchesLength() == 2
    assert repo.SnapshotsLength() == 4

    branch_names = {
        repo.Branches(i).Name().decode() for i in range(repo.BranchesLength())
    }
    assert "main" in branch_names
    assert "test-branch" in branch_names

    tag_names = {repo.Tags(i).Name().decode() for i in range(repo.TagsLength())}
    assert "v1.0" in tag_names


def test_all_snapshots(repo_path: str, flatbuf_mod: ModuleType) -> None:
    """Read every snapshot file, verify header + flatbuffer content."""
    Snapshot = flatbuf_mod.Snapshot

    snapshots_dir = Path(repo_path, "snapshots")
    snapshot_files = list(snapshots_dir.iterdir())
    assert len(snapshot_files) == 4

    for snap_file in snapshot_files:
        data = snap_file.read_bytes()
        payload = assert_valid_header(data, FileType.Snapshot, filename=snap_file.name)

        snapshot = Snapshot.GetRootAs(payload)

        assert_valid_object_id(snapshot.Id())

        # message is a required string
        message = snapshot.Message()
        assert message is not None
        assert len(message) > 0


def test_all_manifests_and_virtual_locations(
    repo_path: str, flatbuf_mod: ModuleType
) -> None:
    """Read every manifest, verify header + flatbuffer content.

    For manifests with dictionary-compressed virtual chunk locations,
    decompress each location and verify it's a valid URL string.
    """
    Manifest = flatbuf_mod.Manifest

    manifests_dir = Path(repo_path, "manifests")
    manifest_files = list(manifests_dir.iterdir())
    assert len(manifest_files) == 3

    total_inline = 0
    total_native = 0
    total_virtual = 0
    total_virtual_compressed = 0

    for manifest_file in manifest_files:
        data = manifest_file.read_bytes()
        payload = assert_valid_header(
            data, FileType.Manifest, filename=manifest_file.name
        )

        manifest = Manifest.GetRootAs(payload)

        assert_valid_object_id(manifest.Id())

        assert manifest.ArraysLength() == 1

        comp_alg = manifest.CompressionAlgorithm()
        loc_decompressor = None
        if comp_alg == LocationCompression.ZstdDict:
            dict_bytes = manifest.LocationDictionaryAsNumpy().tobytes()
            assert len(dict_bytes) > 0, (
                "Dictionary compression enabled but dictionary is empty"
            )
            zstd_dict = zstandard.ZstdCompressionDict(dict_bytes)
            loc_decompressor = zstandard.ZstdDecompressor(dict_data=zstd_dict)

        for arr_idx in range(manifest.ArraysLength()):
            array_manifest = manifest.Arrays(arr_idx)
            for ref_idx in range(array_manifest.RefsLength()):
                chunk_ref = array_manifest.Refs(ref_idx)

                has_inline = not chunk_ref.InlineIsNone()
                has_chunk_id = chunk_ref.ChunkId() is not None
                has_location = chunk_ref.Location() is not None
                has_compressed = not chunk_ref.CompressedLocationIsNone()

                if has_inline:
                    # Inline chunk: data is stored directly in the ref
                    total_inline += 1
                    inline_data = chunk_ref.InlineAsNumpy().tobytes()
                    assert len(inline_data) > 0, "Inline chunk has empty data"

                elif has_chunk_id:
                    # Native/materialized chunk: points to a chunk file
                    total_native += 1
                    assert_valid_object_id(chunk_ref.ChunkId())
                    assert chunk_ref.Length() > 0, "Native chunk ref has zero length"

                elif has_compressed:
                    # Virtual chunk with dictionary-compressed location
                    total_virtual_compressed += 1
                    assert loc_decompressor is not None, (
                        "Compressed location without dictionary"
                    )
                    compressed = chunk_ref.CompressedLocationAsNumpy().tobytes()
                    location = loc_decompressor.decompress(
                        compressed, max_output_size=1024
                    ).decode("utf-8")
                    assert location.startswith("s3://"), (
                        f"Decompressed location doesn't start with s3://: {location!r}"
                    )

                elif has_location:
                    # Virtual chunk with plain location string
                    total_virtual += 1
                    location = chunk_ref.Location()
                    loc_str = (
                        location.decode("utf-8")
                        if isinstance(location, bytes)
                        else location
                    )
                    assert loc_str.startswith("s3://"), (
                        f"Location doesn't start with s3://: {loc_str!r}"
                    )

                else:
                    raise AssertionError(
                        "ChunkRef has no inline, chunk_id, or location data"
                    )

    assert total_inline > 0, "Expected at least one inline chunk ref"
    assert total_native > 0, "Expected at least one native/materialized chunk ref"
    assert total_virtual == 0, "Expected all compressed virtual chunks"
    assert total_virtual_compressed > 0, (
        "Expected at least one compressed virtual chunk ref"
    )


def test_all_transaction_logs(repo_path: str, flatbuf_mod: ModuleType) -> None:
    """Read every transaction log file, verify header + flatbuffer content."""
    TransactionLog = flatbuf_mod.TransactionLog

    txn_dir = Path(repo_path, "transactions")
    txn_files = list(txn_dir.iterdir())
    assert len(txn_files) == 4

    for txn_file in txn_files:
        data = txn_file.read_bytes()
        payload = assert_valid_header(
            data, FileType.TransactionLog, filename=txn_file.name
        )

        txn_log = TransactionLog.GetRootAs(payload)

        assert_valid_object_id(txn_log.Id())


def test_chunks_are_valid_zstd(repo_path: str) -> None:
    """Verify all chunk files are valid zstd-compressed data."""
    chunks_dir = Path(repo_path, "chunks")
    chunk_files = list(chunks_dir.iterdir())
    assert len(chunk_files) == 4

    dctx = zstandard.ZstdDecompressor()
    for chunk_file in chunk_files:
        data = chunk_file.read_bytes()
        assert data[:4] == ZSTD_MAGIC, (
            f"{chunk_file.name}: doesn't start with zstd magic "
            f"(got {data[:4].hex()}, expected {ZSTD_MAGIC.hex()})"
        )
        decompressed = dctx.decompress(data, max_output_size=64 * 1024 * 1024)
        assert len(decompressed) > 0
