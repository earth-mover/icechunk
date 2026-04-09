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
            endpoint_url="http://localhost:4200",
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


# ---------------------------------------------------------------------------
# Forward-compatibility test: inject non-empty `extra` bytes into every
# flatbuffer file type, then verify icechunk-python (Rust) can still open
# the repo and read data identically to the original.
# ---------------------------------------------------------------------------

# The extra bytes we inject into every flatbuffer `extra` field.
EXTRA_BYTES = b"icechunk-extra-test-data-12345"


def _rewrite_file(path: Path, file_type: int, rebuild_fn: Any) -> None:
    """Read an icechunk file, decompress, rebuild the flatbuffer via *rebuild_fn*
    (which injects ``extra`` bytes), recompress, and overwrite the file."""
    data = path.read_bytes()
    payload = assert_valid_header(data, file_type, filename=path.name)
    new_payload = rebuild_fn(payload)
    cctx = zstandard.ZstdCompressor()
    path.write_bytes(data[:HEADER_SIZE] + cctx.compress(new_payload))


# -- helpers shared by rebuilders ------------------------------------------

import flatbuffers as fb  # type: ignore[import-untyped]


def _copy_id8_vec(B: Any, mod: Any, length: int, getter: Any, start_vec: Any) -> Any:
    """Copy a vector of ObjectId8 structs."""
    items = [getter(i).Bytes() for i in range(length)]
    start_vec(B, length)
    for b in reversed(items):
        mod.CreateObjectId8(B, b)
    return B.EndVector()


def _build_offset_vec(B: Any, offsets: list[Any], start_vec: Any) -> Any:
    """Create a flatbuffers vector of table offsets."""
    start_vec(B, len(offsets))
    for o in reversed(offsets):
        B.PrependUOffsetTRelative(o)
    return B.EndVector()


def _copy_metadata_items(B: Any, mod: Any, length: int, getter: Any) -> list[Any]:
    """Copy MetadataItem tables, returning a list of offsets."""
    offsets = []
    for i in range(length):
        mi = getter(i)
        name = B.CreateString(mi.Name())
        value = B.CreateByteVector(mi.ValueAsNumpy().tobytes())
        mod.MetadataItemStart(B)
        mod.MetadataItemAddName(B, name)
        mod.MetadataItemAddValue(B, value)
        offsets.append(mod.MetadataItemEnd(B))
    return offsets


def _copy_refs(B: Any, mod: Any, length: int, getter: Any) -> list[Any]:
    """Copy Ref tables (tags or branches), returning a list of offsets."""
    offsets = []
    for i in range(length):
        ref = getter(i)
        name = B.CreateString(ref.Name())
        mod.RefStart(B)
        mod.RefAddName(B, name)
        mod.RefAddSnapshotIndex(B, ref.SnapshotIndex())
        offsets.append(mod.RefEnd(B))
    return offsets


# -- per-type rebuilders ---------------------------------------------------


def _rebuild_transaction_log(payload: bytes, mod: Any) -> bytes:
    txn = mod.TransactionLog.GetRootAs(payload)
    B = fb.Builder(1024)

    # fmt: off
    id8_fields = [
        (txn.NewGroupsLength,    txn.NewGroups,    mod.TransactionLogStartNewGroupsVector),
        (txn.NewArraysLength,    txn.NewArrays,    mod.TransactionLogStartNewArraysVector),
        (txn.DeletedGroupsLength, txn.DeletedGroups, mod.TransactionLogStartDeletedGroupsVector),
        (txn.DeletedArraysLength, txn.DeletedArrays, mod.TransactionLogStartDeletedArraysVector),
        (txn.UpdatedArraysLength, txn.UpdatedArrays, mod.TransactionLogStartUpdatedArraysVector),
        (txn.UpdatedGroupsLength, txn.UpdatedGroups, mod.TransactionLogStartUpdatedGroupsVector),
    ]
    # fmt: on
    id8_vecs = [_copy_id8_vec(B, mod, lf(), gf, sv) for lf, gf, sv in id8_fields]

    # updated_chunks
    uc_offsets = []
    for i in range(txn.UpdatedChunksLength()):
        auc = txn.UpdatedChunks(i)
        chunk_offs = []
        for j in range(auc.ChunksLength()):
            ci = auc.Chunks(j)
            coords = [ci.Coords(k) for k in range(ci.CoordsLength())]
            mod.ChunkIndicesStartCoordsVector(B, len(coords))
            for c in reversed(coords):
                B.PrependUint32(c)
            cv = B.EndVector()
            mod.ChunkIndicesStart(B)
            mod.ChunkIndicesAddCoords(B, cv)
            chunk_offs.append(mod.ChunkIndicesEnd(B))
        chunks_v = _build_offset_vec(
            B, chunk_offs, mod.ArrayUpdatedChunksStartChunksVector
        )
        auc_nid_bytes = auc.NodeId().Bytes()
        mod.ArrayUpdatedChunksStart(B)
        mod.ArrayUpdatedChunksAddNodeId(B, mod.CreateObjectId8(B, auc_nid_bytes))
        mod.ArrayUpdatedChunksAddChunks(B, chunks_v)
        uc_offsets.append(mod.ArrayUpdatedChunksEnd(B))
    upd_chunks = _build_offset_vec(
        B, uc_offsets, mod.TransactionLogStartUpdatedChunksVector
    )

    extra = B.CreateByteVector(EXTRA_BYTES)
    txn_id_bytes = txn.Id().Bytes()

    mod.TransactionLogStart(B)
    mod.TransactionLogAddId(B, mod.CreateObjectId12(B, txn_id_bytes))
    for add_fn, vec in zip(
        [
            mod.TransactionLogAddNewGroups,
            mod.TransactionLogAddNewArrays,
            mod.TransactionLogAddDeletedGroups,
            mod.TransactionLogAddDeletedArrays,
            mod.TransactionLogAddUpdatedArrays,
            mod.TransactionLogAddUpdatedGroups,
        ],
        id8_vecs,
        strict=True,
    ):
        add_fn(B, vec)
    mod.TransactionLogAddUpdatedChunks(B, upd_chunks)
    mod.TransactionLogAddExtra(B, extra)
    B.Finish(mod.TransactionLogEnd(B))
    return bytes(B.Output())


def _rebuild_snapshot(
    payload: bytes, mod: Any, manifest_sizes: dict[bytes, int] | None = None
) -> bytes:

    snap = mod.Snapshot.GetRootAs(payload)
    B = fb.Builder(4096)

    # -- nodes --
    node_offsets = []
    for i in range(snap.NodesLength()):
        ns = snap.Nodes(i)
        ndt = ns.NodeDataType()

        if ndt == mod.NodeData.Group:
            mod.GroupNodeDataStart(B)
            ndata = mod.GroupNodeDataEnd(B)
        else:  # Array
            nd_tab = ns.NodeData()
            ad = mod.ArrayNodeData()
            ad.Init(nd_tab.Bytes, nd_tab.Pos)

            # shape v1 (empty for V2)
            mod.ArrayNodeDataStartShapeVector(B, 0)
            sv1 = B.EndVector()

            # shape v2
            sv2_offs = []
            for s in range(ad.ShapeV2Length()):
                sv2 = ad.ShapeV2(s)
                mod.DimensionShapeV2Start(B)
                mod.DimensionShapeV2AddArrayLength(B, sv2.ArrayLength())
                mod.DimensionShapeV2AddNumChunks(B, sv2.NumChunks())
                sv2_offs.append(mod.DimensionShapeV2End(B))
            sv2_vec = None
            if sv2_offs:
                sv2_vec = _build_offset_vec(
                    B, sv2_offs, mod.ArrayNodeDataStartShapeV2Vector
                )

            # manifests
            mref_offs = []
            for m in range(ad.ManifestsLength()):
                mr = ad.Manifests(m)
                ec = mr.ExtentsLength()
                mod.ManifestRefStartExtentsVector(B, ec)
                for e in range(ec - 1, -1, -1):
                    ext = mr.Extents(e)
                    # flatc versions differ: older emits From(), newer From_()
                    from_val = ext.From_() if hasattr(ext, "From_") else ext.From()
                    mod.CreateChunkIndexRange(B, from_val, ext.To())
                ev = B.EndVector()
                mr_oid_bytes = mr.ObjectId().Bytes()
                mod.ManifestRefStart(B)
                mod.ManifestRefAddObjectId(B, mod.CreateObjectId12(B, mr_oid_bytes))
                mod.ManifestRefAddExtents(B, ev)
                mref_offs.append(mod.ManifestRefEnd(B))
            man_vec = _build_offset_vec(
                B, mref_offs, mod.ArrayNodeDataStartManifestsVector
            )

            # dimension names
            dn_offs = []
            for d in range(ad.DimensionNamesLength()):
                dn = ad.DimensionNames(d)
                mod.DimensionNameStart(B)
                if dn.Name() is not None:
                    mod.DimensionNameAddName(B, B.CreateString(dn.Name()))
                dn_offs.append(mod.DimensionNameEnd(B))
            dn_vec = None
            if dn_offs:
                dn_vec = _build_offset_vec(
                    B, dn_offs, mod.ArrayNodeDataStartDimensionNamesVector
                )

            mod.ArrayNodeDataStart(B)
            mod.ArrayNodeDataAddShape(B, sv1)
            mod.ArrayNodeDataAddManifests(B, man_vec)
            if dn_vec is not None:
                mod.ArrayNodeDataAddDimensionNames(B, dn_vec)
            if sv2_vec is not None:
                mod.ArrayNodeDataAddShapeV2(B, sv2_vec)
            ndata = mod.ArrayNodeDataEnd(B)

        ud = B.CreateByteVector(ns.UserDataAsNumpy().tobytes())
        path = B.CreateString(ns.Path())
        n_extra = B.CreateByteVector(EXTRA_BYTES)
        ns_id_bytes = ns.Id().Bytes()

        mod.NodeSnapshotStart(B)
        # structs must be created inline, immediately before Add
        mod.NodeSnapshotAddId(B, mod.CreateObjectId8(B, ns_id_bytes))
        mod.NodeSnapshotAddPath(B, path)
        mod.NodeSnapshotAddUserData(B, ud)
        mod.NodeSnapshotAddNodeDataType(B, ndt)
        mod.NodeSnapshotAddNodeData(B, ndata)
        mod.NodeSnapshotAddExtra(B, n_extra)
        node_offsets.append(mod.NodeSnapshotEnd(B))

    nodes = _build_offset_vec(B, node_offsets, mod.SnapshotStartNodesVector)

    mi_offs = _copy_metadata_items(B, mod, snap.MetadataLength(), snap.Metadata)
    metadata = _build_offset_vec(B, mi_offs, mod.SnapshotStartMetadataVector)

    message = B.CreateString(snap.Message())

    # manifest_files v1 (empty)
    mod.SnapshotStartManifestFilesVector(B, 0)
    mf_v1 = B.EndVector()

    # manifest_files v2
    mfv2_offs = []
    for i in range(snap.ManifestFilesV2Length()):
        mf = snap.ManifestFilesV2(i)
        mf_id_bytes = mf.Id().Bytes()
        mfe = B.CreateByteVector(EXTRA_BYTES)
        mod.ManifestFileInfoV2Start(B)
        mod.ManifestFileInfoV2AddId(B, mod.CreateObjectId12(B, mf_id_bytes))
        size = (
            manifest_sizes.get(bytes(mf_id_bytes), mf.SizeBytes())
            if manifest_sizes
            else mf.SizeBytes()
        )
        mod.ManifestFileInfoV2AddSizeBytes(B, size)
        mod.ManifestFileInfoV2AddNumChunkRefs(B, mf.NumChunkRefs())
        mod.ManifestFileInfoV2AddExtra(B, mfe)
        mfv2_offs.append(mod.ManifestFileInfoV2End(B))
    mfv2 = (
        _build_offset_vec(B, mfv2_offs, mod.SnapshotStartManifestFilesV2Vector)
        if mfv2_offs
        else None
    )

    snap_id_bytes = snap.Id().Bytes()
    pid = snap.ParentId()
    pid_bytes = pid.Bytes() if pid else None
    s_extra = B.CreateByteVector(EXTRA_BYTES)

    mod.SnapshotStart(B)
    mod.SnapshotAddId(B, mod.CreateObjectId12(B, snap_id_bytes))
    if pid_bytes is not None:
        mod.SnapshotAddParentId(B, mod.CreateObjectId12(B, pid_bytes))
    mod.SnapshotAddNodes(B, nodes)
    mod.SnapshotAddFlushedAt(B, snap.FlushedAt())
    mod.SnapshotAddMessage(B, message)
    mod.SnapshotAddMetadata(B, metadata)
    mod.SnapshotAddManifestFiles(B, mf_v1)
    if mfv2 is not None:
        mod.SnapshotAddManifestFilesV2(B, mfv2)
    mod.SnapshotAddExtra(B, s_extra)
    B.Finish(mod.SnapshotEnd(B))
    return bytes(B.Output())


def _rebuild_manifest(payload: bytes, mod: Any) -> bytes:

    manifest = mod.Manifest.GetRootAs(payload)
    B = fb.Builder(8192)

    arr_offs = []
    for ai in range(manifest.ArraysLength()):
        am = manifest.Arrays(ai)

        ref_offs = []
        for ri in range(am.RefsLength()):
            cr = am.Refs(ri)

            idx = [cr.Index(k) for k in range(cr.IndexLength())]
            mod.ChunkRefStartIndexVector(B, len(idx))
            for v in reversed(idx):
                B.PrependUint32(v)
            idx_v = B.EndVector()

            inline_v = None
            if not cr.InlineIsNone():
                inline_v = B.CreateByteVector(cr.InlineAsNumpy().tobytes())

            cid_bytes = None
            if cr.ChunkId() is not None:
                cid_bytes = cr.ChunkId().Bytes()

            loc = None
            if cr.Location() is not None:
                loc_raw = cr.Location()
                loc = B.CreateString(
                    loc_raw.decode() if isinstance(loc_raw, bytes) else loc_raw
                )

            etag = None
            if cr.ChecksumEtag() is not None:
                etag_raw = cr.ChecksumEtag()
                etag = B.CreateString(
                    etag_raw.decode() if isinstance(etag_raw, bytes) else etag_raw
                )

            comp_loc = None
            if not cr.CompressedLocationIsNone():
                comp_loc = B.CreateByteVector(cr.CompressedLocationAsNumpy().tobytes())

            cr_extra = B.CreateByteVector(EXTRA_BYTES)

            mod.ChunkRefStart(B)
            mod.ChunkRefAddIndex(B, idx_v)
            if inline_v is not None:
                mod.ChunkRefAddInline(B, inline_v)
            if cr.Offset() != 0:
                mod.ChunkRefAddOffset(B, cr.Offset())
            if cr.Length() != 0:
                mod.ChunkRefAddLength(B, cr.Length())
            if cid_bytes is not None:
                mod.ChunkRefAddChunkId(B, mod.CreateObjectId12(B, cid_bytes))
            if loc is not None:
                mod.ChunkRefAddLocation(B, loc)
            if etag is not None:
                mod.ChunkRefAddChecksumEtag(B, etag)
            if cr.ChecksumLastModified() != 0:
                mod.ChunkRefAddChecksumLastModified(B, cr.ChecksumLastModified())
            if comp_loc is not None:
                mod.ChunkRefAddCompressedLocation(B, comp_loc)
            mod.ChunkRefAddExtra(B, cr_extra)
            ref_offs.append(mod.ChunkRefEnd(B))

        refs_v = _build_offset_vec(B, ref_offs, mod.ArrayManifestStartRefsVector)
        am_nid_bytes = am.NodeId().Bytes()
        am_extra = B.CreateByteVector(EXTRA_BYTES)
        mod.ArrayManifestStart(B)
        mod.ArrayManifestAddNodeId(B, mod.CreateObjectId8(B, am_nid_bytes))
        mod.ArrayManifestAddRefs(B, refs_v)
        mod.ArrayManifestAddExtra(B, am_extra)
        arr_offs.append(mod.ArrayManifestEnd(B))

    arrays_v = _build_offset_vec(B, arr_offs, mod.ManifestStartArraysVector)

    loc_dict = None
    if not manifest.LocationDictionaryIsNone():
        loc_dict = B.CreateByteVector(manifest.LocationDictionaryAsNumpy().tobytes())

    manifest_id_bytes = manifest.Id().Bytes()
    m_extra = B.CreateByteVector(EXTRA_BYTES)

    mod.ManifestStart(B)
    mod.ManifestAddId(B, mod.CreateObjectId12(B, manifest_id_bytes))
    mod.ManifestAddArrays(B, arrays_v)
    if loc_dict is not None:
        mod.ManifestAddLocationDictionary(B, loc_dict)
    mod.ManifestAddCompressionAlgorithm(B, manifest.CompressionAlgorithm())
    mod.ManifestAddExtra(B, m_extra)
    B.Finish(mod.ManifestEnd(B))
    return bytes(B.Output())


def _rebuild_repo(payload: bytes, mod: Any) -> bytes:
    repo = mod.Repo.GetRootAs(payload)
    B = fb.Builder(4096)

    tags = _build_offset_vec(
        B, _copy_refs(B, mod, repo.TagsLength(), repo.Tags), mod.RepoStartTagsVector
    )
    branches = _build_offset_vec(
        B,
        _copy_refs(B, mod, repo.BranchesLength(), repo.Branches),
        mod.RepoStartBranchesVector,
    )

    dt_offs = [
        B.CreateString(repo.DeletedTags(i)) for i in range(repo.DeletedTagsLength())
    ]
    del_tags = _build_offset_vec(B, dt_offs, mod.RepoStartDeletedTagsVector)

    # snapshots
    s_offs = []
    for i in range(repo.SnapshotsLength()):
        si = repo.Snapshots(i)
        msg = B.CreateString(si.Message())
        si_id_bytes = si.Id().Bytes()
        si_mi = _copy_metadata_items(B, mod, si.MetadataLength(), si.Metadata)
        si_meta = (
            _build_offset_vec(B, si_mi, mod.SnapshotInfoStartMetadataVector)
            if si_mi
            else None
        )
        mod.SnapshotInfoStart(B)
        mod.SnapshotInfoAddId(B, mod.CreateObjectId12(B, si_id_bytes))
        mod.SnapshotInfoAddParentOffset(B, si.ParentOffset())
        mod.SnapshotInfoAddFlushedAt(B, si.FlushedAt())
        mod.SnapshotInfoAddMessage(B, msg)
        if si_meta is not None:
            mod.SnapshotInfoAddMetadata(B, si_meta)
        s_offs.append(mod.SnapshotInfoEnd(B))
    snapshots = _build_offset_vec(B, s_offs, mod.RepoStartSnapshotsVector)

    mod.RepoStatusStart(B)
    mod.RepoStatusAddAvailability(B, repo.Status().Availability())
    mod.RepoStatusAddSetAt(B, repo.Status().SetAt())
    status = mod.RepoStatusEnd(B)

    rmi = _copy_metadata_items(B, mod, repo.MetadataLength(), repo.Metadata)
    repo_meta = _build_offset_vec(B, rmi, mod.RepoStartMetadataVector) if rmi else None

    # latest_updates (empty — union types are complex and not needed for reads)
    mod.RepoStartLatestUpdatesVector(B, 0)
    updates = B.EndVector()

    # config
    cfg = None
    if not repo.ConfigIsNone():
        cfg = B.CreateByteVector(repo.ConfigAsNumpy().tobytes())

    rbu = None
    if repo.RepoBeforeUpdates() is not None:
        rbu = B.CreateString(repo.RepoBeforeUpdates())

    eff = None
    if not repo.EnabledFeatureFlagsIsNone():
        n = repo.EnabledFeatureFlagsLength()
        mod.RepoStartEnabledFeatureFlagsVector(B, n)
        for i in range(n - 1, -1, -1):
            B.PrependUint16(repo.EnabledFeatureFlags(i))
        eff = B.EndVector()

    dff = None
    if not repo.DisabledFeatureFlagsIsNone():
        n = repo.DisabledFeatureFlagsLength()
        mod.RepoStartDisabledFeatureFlagsVector(B, n)
        for i in range(n - 1, -1, -1):
            B.PrependUint16(repo.DisabledFeatureFlags(i))
        dff = B.EndVector()

    r_extra = B.CreateByteVector(EXTRA_BYTES)

    mod.RepoStart(B)
    mod.RepoAddSpecVersion(B, repo.SpecVersion())
    mod.RepoAddTags(B, tags)
    mod.RepoAddBranches(B, branches)
    mod.RepoAddDeletedTags(B, del_tags)
    mod.RepoAddSnapshots(B, snapshots)
    mod.RepoAddStatus(B, status)
    if repo_meta is not None:
        mod.RepoAddMetadata(B, repo_meta)
    mod.RepoAddLatestUpdates(B, updates)
    if rbu is not None:
        mod.RepoAddRepoBeforeUpdates(B, rbu)
    if cfg is not None:
        mod.RepoAddConfig(B, cfg)
    if eff is not None:
        mod.RepoAddEnabledFeatureFlags(B, eff)
    if dff is not None:
        mod.RepoAddDisabledFeatureFlags(B, dff)
    mod.RepoAddExtra(B, r_extra)
    B.Finish(mod.RepoEnd(B))
    return bytes(B.Output())


def test_extra_fields_forward_compatibility(
    repo_path: str, flatbuf_mod: ModuleType
) -> None:
    """Inject non-empty ``extra`` bytes into every flatbuffer file in a copy of
    the repo, then compare reads from the patched repo against the original."""
    import shutil

    import numpy as np

    # -- Read reference values from the original, unpatched repo --
    orig_repo = ic.Repository.open(
        storage=ic.local_filesystem_storage(repo_path),
    )
    orig_branches = sorted(orig_repo.list_branches())
    orig_tags = sorted(orig_repo.list_tags())
    orig_ancestry = [(a.id, a.message) for a in orig_repo.ancestry(branch="main")]

    orig_session = orig_repo.readonly_session(branch="main")
    orig_store = orig_session.store
    orig_big = zarr.open_array(orig_store, path="group1/big_chunks", mode="r")[:]
    orig_small = zarr.open_array(orig_store, path="group1/small_chunks", mode="r")[:]

    orig_tag_session = orig_repo.readonly_session(tag="v1.0")
    orig_tag_big = zarr.open_array(
        orig_tag_session.store, path="group1/big_chunks", mode="r"
    )[:]

    # -- Patch all flatbuffer files in a copy --
    with tempfile.TemporaryDirectory(prefix="icechunk_extra_test_") as tmpdir:
        patched = Path(tmpdir) / "repo"
        shutil.copytree(repo_path, patched)

        mod = flatbuf_mod

        for f in (patched / "transactions").iterdir():
            _rewrite_file(
                f, FileType.TransactionLog, lambda p: _rebuild_transaction_log(p, mod)
            )

        # Manifests must be rewritten before snapshots so we can update
        # the manifest size_bytes recorded in each snapshot.
        manifest_sizes: dict[bytes, int] = {}
        for f in (patched / "manifests").iterdir():
            # Read the manifest ID before rewriting
            data = f.read_bytes()
            payload = assert_valid_header(data, FileType.Manifest, filename=f.name)
            m = mod.Manifest.GetRootAs(payload)
            mid = bytes(m.Id().Bytes())
            _rewrite_file(f, FileType.Manifest, lambda p: _rebuild_manifest(p, mod))
            manifest_sizes[mid] = f.stat().st_size

        for f in (patched / "snapshots").iterdir():
            _rewrite_file(
                f,
                FileType.Snapshot,
                lambda p: _rebuild_snapshot(p, mod, manifest_sizes),
            )

        _rewrite_file(
            patched / "repo", FileType.RepoInfo, lambda p: _rebuild_repo(p, mod)
        )

        # -- Open the patched repo and compare against the original --
        new_repo = ic.Repository.open(
            storage=ic.local_filesystem_storage(str(patched)),
        )

        assert sorted(new_repo.list_branches()) == orig_branches
        assert sorted(new_repo.list_tags()) == orig_tags

        new_ancestry = [(a.id, a.message) for a in new_repo.ancestry(branch="main")]
        assert new_ancestry == orig_ancestry

        new_session = new_repo.readonly_session(branch="main")
        new_store = new_session.store
        new_big = zarr.open_array(new_store, path="group1/big_chunks", mode="r")[:]
        new_small = zarr.open_array(new_store, path="group1/small_chunks", mode="r")[:]
        np.testing.assert_array_equal(new_big, orig_big)
        np.testing.assert_array_equal(new_small, orig_small)

        new_tag_session = new_repo.readonly_session(tag="v1.0")
        new_tag_big = zarr.open_array(
            new_tag_session.store, path="group1/big_chunks", mode="r"
        )[:]
        np.testing.assert_array_equal(new_tag_big, orig_tag_big)
