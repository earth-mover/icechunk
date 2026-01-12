"""Tests for __str__, __repr__, and _repr_html_ display methods."""

import re

import pytest

from icechunk import Repository, in_memory_storage

# Snapshot ID is 12 bytes encoded in base32 Crockford = 20 uppercase alphanumeric chars
SNAPSHOT_ID_PATTERN = re.compile(r'"[0-9A-Z]{20}"')


@pytest.fixture
def repo() -> Repository:
    """Create an in-memory repository for testing."""
    return Repository.create(storage=in_memory_storage())


# =============================================================================
# Repository Display Tests
# =============================================================================
def test_repository_repr(repo: Repository) -> None:
    """Test Repository.__repr__ returns executable-style string."""
    repr_str = repr(repo)
    assert (
        repr_str
        == "Repository(storage=ObjectStorage(backend=InMemoryObjectStoreBackend))"
    )


def test_repository_str(repo: Repository) -> None:
    """Test Repository.__str__ returns human-readable string."""
    str_str = str(repo)
    assert "<Repository>" in str_str
    assert "storage: ObjectStorage(backend=InMemoryObjectStoreBackend)" in str_str


def test_repository_repr_html(repo: Repository) -> None:
    """Test Repository._repr_html_ returns valid HTML."""
    html = repo._repr_html_()
    assert '<div class="icechunk-repr">' in html
    assert "<code>&lt;Repository&gt;</code>" in html
    assert (
        "<strong>storage</strong>: <code>ObjectStorage(backend=InMemoryObjectStoreBackend)</code>"
        in html
    )


# =============================================================================
# Session Display Tests
# =============================================================================
def test_session_repr(repo: Repository) -> None:
    """Test Session.__repr__ returns executable-style string."""
    session = repo.writable_session("main")
    repr_str = repr(session)
    # Extract and validate snapshot_id separately since it's dynamic
    match = SNAPSHOT_ID_PATTERN.search(repr_str)
    assert match is not None, f"snapshot_id not found in: {repr_str}"
    snapshot_id = match.group(0)
    expected = f'Session(read_only=false, snapshot_id={snapshot_id}, branch="main", has_uncommitted_changes=false)'
    assert repr_str == expected


def test_session_str(repo: Repository) -> None:
    """Test Session.__str__ returns human-readable string."""
    session = repo.writable_session("main")
    str_str = str(session)
    assert "<Session>" in str_str
    assert "read_only: false" in str_str
    assert "branch: main" in str_str
    assert "has_uncommitted_changes: false" in str_str


def test_session_repr_html(repo: Repository) -> None:
    """Test Session._repr_html_ returns valid HTML."""
    session = repo.writable_session("main")
    html = session._repr_html_()
    assert '<div class="icechunk-repr">' in html
    assert "<code>&lt;Session&gt;</code>" in html
    assert "<strong>read_only</strong>: <code>false</code>" in html
    assert "<strong>branch</strong>: <code>main</code>" in html


def test_session_repr_shows_correct_state(repo: Repository) -> None:
    """Test Session repr reflects actual state (read_only, branch, etc.)."""
    import zarr

    # Create initial commit so we can create readonly session
    session = repo.writable_session("main")
    zarr.group(session.store)
    session.commit("init")

    # Test readonly session
    readonly_session = repo.readonly_session("main")
    readonly_repr = repr(readonly_session)
    assert "read_only=true" in readonly_repr
    assert "branch=None" in readonly_repr

    # Test writable session
    writable_session = repo.writable_session("main")
    writable_repr = repr(writable_session)
    assert "read_only=false" in writable_repr
    assert 'branch="main"' in writable_repr


# =============================================================================
# IcechunkStore Display Tests
# =============================================================================
def test_store_repr(repo: Repository) -> None:
    """Test IcechunkStore.__repr__ returns executable-style string."""
    session = repo.writable_session("main")
    store = session.store
    repr_str = repr(store)
    # Extract and validate snapshot_id separately since it's dynamic
    match = SNAPSHOT_ID_PATTERN.search(repr_str)
    assert match is not None, f"snapshot_id not found in: {repr_str}"
    snapshot_id = match.group(0)
    expected = f'IcechunkStore(read_only=false, snapshot_id={snapshot_id}, branch="main")'
    assert repr_str == expected


def test_store_str(repo: Repository) -> None:
    """Test IcechunkStore.__str__ returns human-readable string."""
    session = repo.writable_session("main")
    store = session.store
    str_str = str(store)
    assert "<IcechunkStore>" in str_str
    assert "read_only: false" in str_str
    assert "branch: main" in str_str


def test_store_repr_html(repo: Repository) -> None:
    """Test IcechunkStore._repr_html_ returns valid HTML."""
    session = repo.writable_session("main")
    store = session.store
    html = store._repr_html_()
    assert '<div class="icechunk-repr">' in html
    assert "<code>&lt;IcechunkStore&gt;</code>" in html
    assert "<strong>read_only</strong>: <code>false</code>" in html
    assert "<strong>branch</strong>: <code>main</code>" in html
