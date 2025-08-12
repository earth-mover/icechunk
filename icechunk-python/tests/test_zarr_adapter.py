"""
Tests for Icechunk ZEP 8 URL adapter integration and session utilities.

This module tests that the Icechunk store adapter works correctly with
zarr-python's ZEP 8 URL syntax for chained store access, and includes
tests for the consolidated session utilities.
"""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest

import icechunk

# Register the adapters for testing
import icechunk.zarr_adapter
import zarr

# Import session utilities for testing
from icechunk.zarr_adapter import (
    IcechunkPathSpec,
    create_readonly_session_from_path,
    create_readonly_session_from_path_spec,
    parse_icechunk_path_spec,
)
from zarr.registry import register_store_adapter

register_store_adapter(icechunk.zarr_adapter.IcechunkStoreAdapter)
register_store_adapter(icechunk.zarr_adapter.ICStoreAdapter)


# =============================================================================
# Basic URL Validation Tests
# =============================================================================


def test_memory_url_rejected():
    """Test that memory:|icechunk: URLs are properly rejected."""
    with pytest.raises(ValueError, match="memory:\\|icechunk: URLs are not supported"):
        zarr.open_group("memory:|icechunk:", mode="r")


def test_write_mode_rejected():
    """Test that write mode is rejected for icechunk: URLs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        url = f"file:{tmpdir}/test|icechunk:"
        with pytest.raises(
            ValueError, match="Write mode '.*' not supported for icechunk: URLs"
        ):
            zarr.open_group(url, mode="w")


def test_nonexistent_repository_rejected():
    """Test that non-existent repositories are properly rejected."""
    with tempfile.TemporaryDirectory() as tmpdir:
        url = f"file:{tmpdir}/nonexistent|icechunk:"
        with pytest.raises(
            ValueError, match="Could not open existing Icechunk repository"
        ):
            zarr.open_group(url, mode="r")


def test_error_messages_are_helpful():
    """Test that error messages provide helpful guidance."""
    # Test memory URL error message
    with pytest.raises(ValueError) as exc_info:
        zarr.open_group("memory:|icechunk:", mode="r")

    error_msg = str(exc_info.value)
    assert "not supported" in error_msg
    assert "file:/path/to/repo|icechunk:" in error_msg
    assert "shared across different URL resolution calls" in error_msg

    # Test write mode error message
    with tempfile.TemporaryDirectory() as tmpdir:
        with pytest.raises(ValueError) as exc_info:
            zarr.open_group(f"file:{tmpdir}/test|icechunk:", mode="w")

        error_msg = str(exc_info.value)
        assert "not supported for icechunk: URLs" in error_msg
        assert "native Icechunk API" in error_msg


# =============================================================================
# Adapter Name Variant Tests
# =============================================================================


def test_adapter_name_variants():
    """Test that both 'icechunk' and 'ic' adapter names work."""
    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "test_adapter_names"

        # Create repository
        storage = icechunk.local_filesystem_storage(str(repo_path))
        repo = icechunk.Repository.create(storage)
        session = repo.writable_session("main")
        store = session.store

        root = zarr.open_group(store, mode="w")
        arr = root.create_array("data", shape=(10,), dtype="i4")
        arr[:] = range(10)
        session.commit("Test data")

        del session, store, root, arr

        # Test both adapter names
        for adapter_name in ["icechunk", "ic"]:
            url = f"file:{repo_path}|{adapter_name}:"
            root_read = zarr.open_group(url, mode="r")
            arr_read = root_read["data"]
            np.testing.assert_array_equal(arr_read[:], list(range(10)))


# =============================================================================
# File-based Repository Integration Tests
# =============================================================================


def test_file_based_repository_roundtrip():
    """Test creating repository with Icechunk API and reading via ZEP 8 URL."""
    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "test_repo"

        # Step 1: Create repository using native Icechunk API
        storage = icechunk.local_filesystem_storage(str(repo_path))
        repo = icechunk.Repository.create(storage)
        session = repo.writable_session("main")
        store = session.store

        # Create test data
        root = zarr.open_group(store, mode="w")
        arr = root.create_array("test_data", shape=(100,), dtype="f4", chunks=(10,))
        arr[:] = np.random.random(100)
        arr.attrs.update({"units": "meters", "description": "Test measurement data"})

        # Create nested structure
        subgroup = root.create_group("measurements")
        temp = subgroup.create_array("temperature", shape=(50,), dtype="f4")
        temp[:] = np.random.normal(20, 5, 50)
        temp.attrs["units"] = "celsius"

        root.attrs["experiment"] = "ZEP8 Integration Test"
        root.attrs["version"] = "1.0"

        # Commit changes
        session.commit("Initial test data")

        # Clean up references to ensure data is persisted
        original_data = arr[:]  # Save for comparison
        original_temp = temp[:]
        del session, store, root, arr, temp, subgroup

        # Step 2: Read via ZEP 8 URL syntax
        url = f"file:{repo_path}|icechunk:"
        root_read = zarr.open_group(url, mode="r")

        # Verify data integrity
        assert "test_data" in root_read.array_keys()
        assert "measurements" in root_read.group_keys()

        arr_read = root_read["test_data"]
        assert arr_read.shape == (100,)
        assert arr_read.dtype == np.dtype("f4")
        assert arr_read.attrs["units"] == "meters"
        assert arr_read.attrs["description"] == "Test measurement data"
        np.testing.assert_array_equal(arr_read[:], original_data)

        # Verify nested structure
        temp_read = root_read["measurements/temperature"]
        assert temp_read.shape == (50,)
        assert temp_read.attrs["units"] == "celsius"
        np.testing.assert_array_equal(temp_read[:], original_temp)

        # Verify root attributes
        assert root_read.attrs["experiment"] == "ZEP8 Integration Test"
        assert root_read.attrs["version"] == "1.0"


def test_complex_data_structures():
    """Test ZEP 8 URL access with complex nested data structures."""
    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "complex_data"

        # Create repository with complex structure
        storage = icechunk.local_filesystem_storage(str(repo_path))
        repo = icechunk.Repository.create(storage)
        session = repo.writable_session("main")
        store = session.store

        root = zarr.open_group(store, mode="w")

        # Create multi-level hierarchy
        experiments = root.create_group("experiments")
        exp1 = experiments.create_group("exp_001")
        exp2 = experiments.create_group("exp_002")

        # Exp1: Time series data
        exp1_data = exp1.create_array(
            "timeseries", shape=(100, 3), dtype="f8", chunks=(10, 3)
        )
        exp1_data[:] = np.random.random((100, 3))
        exp1_data.attrs.update(
            {"columns": ["x", "y", "z"], "sampling_rate": 1000, "units": "volts"}
        )

        # Exp2: Image data
        exp2_images = exp2.create_array(
            "images", shape=(10, 64, 64), dtype="uint8", chunks=(1, 64, 64)
        )
        exp2_images[:] = np.random.randint(0, 256, (10, 64, 64), dtype=np.uint8)
        exp2_images.attrs.update(
            {"format": "grayscale", "resolution": "64x64", "count": 10}
        )

        # Metadata group
        metadata = root.create_group("metadata")
        metadata.attrs["created_by"] = "test_suite"
        metadata.attrs["experiment_date"] = "2024-08-09"

        session.commit("Complex data structure")

        # Save data for verification
        exp1_original = exp1_data[:]
        exp2_original = exp2_images[:]

        del (
            session,
            store,
            root,
            experiments,
            exp1,
            exp2,
            exp1_data,
            exp2_images,
            metadata,
        )

        # Read via ZEP 8 URL and verify everything
        url = f"file:{repo_path}|icechunk:"
        root_read = zarr.open_group(url, mode="r")

        # Verify structure
        assert "experiments" in root_read.group_keys()
        assert "metadata" in root_read.group_keys()

        exp1_read = root_read["experiments/exp_001"]
        exp2_read = root_read["experiments/exp_002"]

        # Verify exp1 data
        ts_read = exp1_read["timeseries"]
        assert ts_read.shape == (100, 3)
        assert ts_read.attrs["sampling_rate"] == 1000
        assert ts_read.attrs["units"] == "volts"
        np.testing.assert_array_equal(ts_read[:], exp1_original)

        # Verify exp2 data
        img_read = exp2_read["images"]
        assert img_read.shape == (10, 64, 64)
        assert img_read.attrs["format"] == "grayscale"
        assert img_read.attrs["count"] == 10
        np.testing.assert_array_equal(img_read[:], exp2_original)

        # Verify metadata
        meta_read = root_read["metadata"]
        assert meta_read.attrs["created_by"] == "test_suite"
        assert meta_read.attrs["experiment_date"] == "2024-08-09"


# =============================================================================
# Branch-specific Access Tests
# =============================================================================


def test_branch_specific_access():
    """Test accessing specific branches via ZEP 8 URL syntax."""
    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "branched_repo"

        # Create repository with main branch
        storage = icechunk.local_filesystem_storage(str(repo_path))
        repo = icechunk.Repository.create(storage)
        session = repo.writable_session("main")
        store = session.store

        # Create initial data on main
        root = zarr.open_group(store, mode="w")
        data = root.create_array("experiment_data", shape=(20,), dtype="i4")
        data[:] = np.arange(20)
        data.attrs["version"] = "main"

        session.commit("Initial data on main")

        # Clean up to persist changes
        main_data = data[:]
        del session, store, root, data

        # Read from main branch via ZEP 8 URL
        main_url = f"file:{repo_path}|icechunk:@branch.main"
        root_main = zarr.open_group(main_url, mode="r")

        data_main = root_main["experiment_data"]
        assert data_main.attrs["version"] == "main"
        np.testing.assert_array_equal(data_main[:], main_data)


def test_icechunk_metadata_paths_ignored():
    """Test that icechunk metadata paths don't interfere with zarr paths."""
    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "metadata_test"

        # Create repository
        storage = icechunk.local_filesystem_storage(str(repo_path))
        repo = icechunk.Repository.create(storage)
        session = repo.writable_session("main")
        store = session.store

        root = zarr.open_group(store, mode="w")
        arr = root.create_array("root_data", shape=(5,), dtype="i4")
        arr[:] = [1, 2, 3, 4, 5]
        session.commit("Test data")

        del session, store, root, arr

        # URLs with icechunk metadata should access repository root, not treat
        # metadata as zarr paths
        metadata_urls = [
            f"file:{repo_path}|icechunk:@branch.main",
            f"file:{repo_path}|icechunk:@tag.v1.0",  # Would fail if tag doesn't exist, but path should be empty
            f"file:{repo_path}|icechunk:@abc123",  # Would fail if snapshot doesn't exist, but path should be empty
        ]

        # Only test the branch URL since others would fail due to non-existent refs,
        # but the path extraction logic should be correct
        root_read = zarr.open_group(metadata_urls[0], mode="r")
        assert "root_data" in root_read.array_keys()
        arr_read = root_read["root_data"]
        np.testing.assert_array_equal(arr_read[:], [1, 2, 3, 4, 5])


# =============================================================================
# Storage Backend Tests
# =============================================================================


def test_file_storage_backend_used():
    """Test that file: URLs result in local_filesystem_storage usage."""
    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "storage_test"

        # Create repo
        storage = icechunk.local_filesystem_storage(str(repo_path))
        repo = icechunk.Repository.create(storage)
        session = repo.writable_session("main")
        store = session.store

        root = zarr.open_group(store, mode="w")
        arr = root.create_array("test", shape=(10,), dtype="i4")
        arr[:] = range(10)
        session.commit("Test")

        del session, store, root, arr

        # Access via ZEP 8 URL - this should use icechunk.local_filesystem_storage
        # internally (we can't directly test this without inspecting internals,
        # but we can verify it works correctly)
        url = f"file:{repo_path}|icechunk:"
        root_read = zarr.open_group(url, mode="r")
        arr_read = root_read["test"]
        np.testing.assert_array_equal(arr_read[:], list(range(10)))


def test_read_only_enforcement():
    """Test that ZEP 8 URL stores are properly read-only."""
    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "readonly_test"

        # Create repo
        storage = icechunk.local_filesystem_storage(str(repo_path))
        repo = icechunk.Repository.create(storage)
        session = repo.writable_session("main")
        store = session.store

        root = zarr.open_group(store, mode="w")
        arr = root.create_array("test", shape=(10,), dtype="i4")
        arr[:] = range(10)
        session.commit("Test")

        del session, store, root, arr

        # Access via ZEP 8 URL in read mode
        url = f"file:{repo_path}|icechunk:"
        root_read = zarr.open_group(url, mode="r")

        # Should be able to read
        arr_read = root_read["test"]
        assert len(arr_read[:]) == 10

        # Should not be able to modify (this is enforced by zarr's read-only mode)
        # The exact error depends on zarr version, but it should fail
        with pytest.raises((PermissionError, ValueError, RuntimeError, Exception)):
            arr_read[0] = 999


# =============================================================================
# Session Utilities Tests
# =============================================================================


def test_icechunk_path_spec_branch():
    """Test IcechunkPathSpec with branch specification."""
    spec = IcechunkPathSpec(branch="main", path="data/temp")
    assert spec.branch == "main"
    assert spec.tag is None
    assert spec.snapshot_id is None
    assert spec.path == "data/temp"
    assert spec.reference_type == "branch"
    assert spec.reference_value == "main"


def test_icechunk_path_spec_tag():
    """Test IcechunkPathSpec with tag specification."""
    spec = IcechunkPathSpec(tag="v1.0", path="results")
    assert spec.branch is None
    assert spec.tag == "v1.0"
    assert spec.snapshot_id is None
    assert spec.path == "results"
    assert spec.reference_type == "tag"
    assert spec.reference_value == "v1.0"


def test_icechunk_path_spec_snapshot():
    """Test IcechunkPathSpec with snapshot specification."""
    spec = IcechunkPathSpec(snapshot_id="abc123def456", path="")
    assert spec.branch is None
    assert spec.tag is None
    assert spec.snapshot_id == "abc123def456"
    assert spec.path == ""
    assert spec.reference_type == "snapshot"
    assert spec.reference_value == "abc123def456"


def test_icechunk_path_spec_default():
    """Test IcechunkPathSpec with no reference (defaults to main)."""
    spec = IcechunkPathSpec(path="data")
    assert spec.branch == "main"
    assert spec.tag is None
    assert spec.snapshot_id is None
    assert spec.path == "data"
    assert spec.reference_type == "branch"
    assert spec.reference_value == "main"


def test_icechunk_path_spec_multiple_refs_error():
    """Test that specifying multiple reference types raises error."""
    with pytest.raises(ValueError, match="Only one of branch, tag, or snapshot_id"):
        IcechunkPathSpec(branch="main", tag="v1.0")

    with pytest.raises(ValueError, match="Only one of branch, tag, or snapshot_id"):
        IcechunkPathSpec(branch="main", snapshot_id="abc123")

    with pytest.raises(ValueError, match="Only one of branch, tag, or snapshot_id"):
        IcechunkPathSpec(tag="v1.0", snapshot_id="abc123")


def test_icechunk_path_spec_repr():
    """Test string representation of IcechunkPathSpec."""
    spec = IcechunkPathSpec(branch="main", path="data/temp")
    repr_str = repr(spec)
    assert "IcechunkPathSpec" in repr_str
    assert "branch='main'" in repr_str
    assert "path='data/temp'" in repr_str


def test_parse_empty_path():
    """Test parsing empty path specification."""
    spec = parse_icechunk_path_spec("")
    assert spec.branch == "main"
    assert spec.tag is None
    assert spec.snapshot_id is None
    assert spec.path == ""


def test_parse_path_only():
    """Test parsing path without version specification."""
    spec = parse_icechunk_path_spec("data/temperature")
    assert spec.branch == "main"
    assert spec.tag is None
    assert spec.snapshot_id is None
    assert spec.path == "data/temperature"


def test_parse_branch_specification():
    """Test parsing branch specifications."""
    # Branch without path
    spec = parse_icechunk_path_spec("@branch.main")
    assert spec.branch == "main"
    assert spec.tag is None
    assert spec.snapshot_id is None
    assert spec.path == ""

    # Branch with path
    spec = parse_icechunk_path_spec("@branch.development/data/temp")
    assert spec.branch == "development"
    assert spec.tag is None
    assert spec.snapshot_id is None
    assert spec.path == "data/temp"


def test_parse_tag_specification():
    """Test parsing tag specifications."""
    # Tag without path
    spec = parse_icechunk_path_spec("@tag.v1.0")
    assert spec.branch is None
    assert spec.tag == "v1.0"
    assert spec.snapshot_id is None
    assert spec.path == ""

    # Tag with path
    spec = parse_icechunk_path_spec("@tag.v2.1/results/final")
    assert spec.branch is None
    assert spec.tag == "v2.1"
    assert spec.snapshot_id is None
    assert spec.path == "results/final"


def test_parse_snapshot_specification():
    """Test parsing snapshot ID specifications."""
    # Snapshot without path
    spec = parse_icechunk_path_spec("@abc123def456")
    assert spec.branch is None
    assert spec.tag is None
    assert spec.snapshot_id == "abc123def456"
    assert spec.path == ""

    # Snapshot with path
    spec = parse_icechunk_path_spec("@abc123def456789/analysis/output")
    assert spec.branch is None
    assert spec.tag is None
    assert spec.snapshot_id == "abc123def456789"
    assert spec.path == "analysis/output"


def test_parse_invalid_branch_format():
    """Test error handling for invalid branch format."""
    with pytest.raises(ValueError, match="Branch name cannot be empty"):
        parse_icechunk_path_spec("@branch.")

    with pytest.raises(ValueError, match="Branch name cannot be empty"):
        parse_icechunk_path_spec("@branch./data")


def test_parse_invalid_tag_format():
    """Test error handling for invalid tag format."""
    with pytest.raises(ValueError, match="Tag name cannot be empty"):
        parse_icechunk_path_spec("@tag.")

    with pytest.raises(ValueError, match="Tag name cannot be empty"):
        parse_icechunk_path_spec("@tag./data")


def test_parse_invalid_snapshot_format():
    """Test error handling for invalid snapshot ID format."""
    # Too short
    with pytest.raises(ValueError, match="Invalid reference specification"):
        parse_icechunk_path_spec("@abc123")

    # Invalid characters
    with pytest.raises(ValueError, match="Invalid reference specification"):
        parse_icechunk_path_spec("@xyz123invalid456")

    # Invalid format
    with pytest.raises(ValueError, match="Invalid reference specification"):
        parse_icechunk_path_spec("@invalid-ref-format")


def test_parse_edge_cases():
    """Test edge cases in path parsing."""
    # Multiple slashes
    spec = parse_icechunk_path_spec("@branch.main/data//nested///array")
    assert spec.branch == "main"
    assert spec.path == "data//nested///array"

    # Branch name with special characters (allowed)
    spec = parse_icechunk_path_spec("@branch.feature-123_dev")
    assert spec.branch == "feature-123_dev"

    # Tag name with special characters (allowed)
    spec = parse_icechunk_path_spec("@tag.v1.0-rc.1")
    assert spec.tag == "v1.0-rc.1"


@pytest.mark.asyncio
async def test_create_readonly_session_branch():
    """Test creating readonly session from branch specification."""
    # Mock repository and session
    mock_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_store = MagicMock()

    mock_repo.readonly_session_async.return_value = mock_session
    mock_session.store = mock_store

    # Test branch session creation
    path_spec = IcechunkPathSpec(branch="development", path="data")
    store = await create_readonly_session_from_path(mock_repo, path_spec)

    mock_repo.readonly_session_async.assert_called_once_with(branch="development")
    assert store == mock_store


@pytest.mark.asyncio
async def test_create_readonly_session_tag():
    """Test creating readonly session from tag specification."""
    mock_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_store = MagicMock()

    mock_repo.readonly_session_async.return_value = mock_session
    mock_session.store = mock_store

    # Test tag session creation
    path_spec = IcechunkPathSpec(tag="v2.0", path="results")
    store = await create_readonly_session_from_path(mock_repo, path_spec)

    mock_repo.readonly_session_async.assert_called_once_with(tag="v2.0")
    assert store == mock_store


@pytest.mark.asyncio
async def test_create_readonly_session_snapshot():
    """Test creating readonly session from snapshot specification."""
    mock_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_store = MagicMock()

    mock_repo.readonly_session_async.return_value = mock_session
    mock_session.store = mock_store

    # Test snapshot session creation
    path_spec = IcechunkPathSpec(snapshot_id="abc123def456", path="")
    store = await create_readonly_session_from_path(mock_repo, path_spec)

    mock_repo.readonly_session_async.assert_called_once_with(snapshot_id="abc123def456")
    assert store == mock_store


@pytest.mark.asyncio
async def test_create_readonly_session_default():
    """Test creating readonly session with default (main branch)."""
    mock_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_store = MagicMock()

    mock_repo.readonly_session_async.return_value = mock_session
    mock_session.store = mock_store

    # Test default session creation
    path_spec = IcechunkPathSpec(path="data")  # Defaults to main branch
    store = await create_readonly_session_from_path(mock_repo, path_spec)

    mock_repo.readonly_session_async.assert_called_once_with(branch="main")
    assert store == mock_store


@pytest.mark.asyncio
async def test_create_readonly_session_error():
    """Test error handling in session creation."""
    mock_repo = AsyncMock()
    mock_repo.readonly_session_async.side_effect = Exception("Branch not found")

    path_spec = IcechunkPathSpec(branch="nonexistent")

    with pytest.raises(
        ValueError, match="Could not create readonly session for branch 'nonexistent'"
    ):
        await create_readonly_session_from_path(mock_repo, path_spec)


@pytest.mark.asyncio
async def test_create_readonly_session_from_path_spec():
    """Test convenience function that combines parsing and session creation."""
    mock_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_store = MagicMock()

    mock_repo.readonly_session_async.return_value = mock_session
    mock_session.store = mock_store

    # Create session creator function
    session_creator = create_readonly_session_from_path_spec("@tag.v1.0/data")

    # Use it to create session
    store = await session_creator(mock_repo)

    mock_repo.readonly_session_async.assert_called_once_with(tag="v1.0")
    assert store == mock_store


def test_parsing_integration_examples():
    """Test parsing with realistic examples."""
    examples = [
        ("", "main", None, None, ""),
        ("data/temperature", "main", None, None, "data/temperature"),
        ("@branch.main", "main", None, None, ""),
        ("@branch.development/experiments", "development", None, None, "experiments"),
        ("@tag.v1.0", None, "v1.0", None, ""),
        ("@tag.v2.1-rc.1/results/final", None, "v2.1-rc.1", None, "results/final"),
        ("@abc123def456789", None, None, "abc123def456789", ""),
        (
            "@abc123def456789abcdef/analysis",
            None,
            None,
            "abc123def456789abcdef",
            "analysis",
        ),
    ]

    for (
        input_path,
        expected_branch,
        expected_tag,
        expected_snapshot,
        expected_path,
    ) in examples:
        spec = parse_icechunk_path_spec(input_path)
        assert spec.branch == expected_branch, f"Failed for input: {input_path}"
        assert spec.tag == expected_tag, f"Failed for input: {input_path}"
        assert spec.snapshot_id == expected_snapshot, f"Failed for input: {input_path}"
        assert spec.path == expected_path, f"Failed for input: {input_path}"
