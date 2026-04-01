"""Tests for __str__, __repr__, and _repr_html_ display methods."""

import icechunk
import zarr
import pytest

from icechunk import CachingConfig, Repository, in_memory_storage


@pytest.fixture
def repo() -> Repository:
    """Create an in-memory repository for testing."""
    return Repository.create(storage=in_memory_storage())


def assert_repr_roundtrips(obj: object) -> None:
    """Assert that eval(repr(obj)) produces an equal object (for executable reprs)."""
    repr_str = repr(obj)
    reconstructed = eval(repr_str, {"icechunk": icechunk})  # noqa: S307
    assert reconstructed == obj, (
        f"Round-trip failed:\n  repr: {repr_str}\n  original: {obj}\n  reconstructed: {reconstructed}"
    )


class TestRepr:
    """Test __repr__ for all classes."""

    def test_repository(self, repo: Repository) -> None:
        repr_str = repr(repo)
        assert repr_str.startswith("<icechunk.Repository>")
        assert "storage:" in repr_str

    def test_session_writable(self, repo: Repository) -> None:
        session = repo.writable_session("main")
        repr_str = repr(session)
        assert "<icechunk.Session>" in repr_str
        assert "read_only: False" in repr_str
        assert "branch: main" in repr_str
        assert "has_uncommitted_changes: False" in repr_str

    def test_session_readonly_omits_branch(self, repo: Repository) -> None:
        session = repo.writable_session("main")
        zarr.group(session.store)
        session.commit("init")

        readonly_session = repo.readonly_session("main")
        repr_str = repr(readonly_session)
        assert "read_only: True" in repr_str
        assert "branch" not in repr_str
        assert "has_uncommitted_changes" not in repr_str

    def test_store(self, repo: Repository) -> None:
        store = repo.writable_session("main").store
        repr_str = repr(store)
        assert "<icechunk.IcechunkStore>" in repr_str
        assert "read_only: False" in repr_str
        assert "branch: main" in repr_str

    @pytest.mark.xfail(reason="CachingConfig not yet converted to PyRepr")
    def test_caching_config_roundtrip(self) -> None:
        assert_repr_roundtrips(CachingConfig())

    @pytest.mark.xfail(reason="CachingConfig not yet converted to PyRepr")
    def test_caching_config_roundtrip_with_values(self) -> None:
        assert_repr_roundtrips(CachingConfig(num_snapshot_nodes=100, num_chunk_refs=200))


class TestStr:
    """Test __str__ for all classes."""

    def test_repository(self, repo: Repository) -> None:
        str_str = str(repo)
        assert "<icechunk.Repository>" in str_str
        assert "storage:" in str_str

    def test_session(self, repo: Repository) -> None:
        session = repo.writable_session("main")
        str_str = str(session)
        assert "<icechunk.Session>" in str_str
        assert "read_only: False" in str_str
        assert "branch: main" in str_str

    def test_store(self, repo: Repository) -> None:
        store = repo.writable_session("main").store
        str_str = str(store)
        assert "<icechunk.IcechunkStore>" in str_str
        assert "read_only: False" in str_str

    @pytest.mark.xfail(reason="CachingConfig not yet converted to PyRepr")
    def test_caching_config(self) -> None:
        config = CachingConfig(num_snapshot_nodes=100)
        str_str = str(config)
        assert "<icechunk.CachingConfig>" in str_str
        assert "num_snapshot_nodes: 100" in str_str


class TestReprHtml:
    """Test _repr_html_ for all classes."""

    def test_repository(self, repo: Repository) -> None:
        html = repo._repr_html_()
        assert '<div class="icechunk-repr">' in html
        assert "<code>&lt;icechunk.Repository&gt;</code>" in html
        assert "<strong>storage</strong>" in html

    def test_session(self, repo: Repository) -> None:
        html = repo.writable_session("main")._repr_html_()
        assert '<div class="icechunk-repr">' in html
        assert "<code>&lt;icechunk.Session&gt;</code>" in html
        assert "<strong>read_only</strong>: <code>False</code>" in html
        assert "<strong>branch</strong>: <code>main</code>" in html

    def test_store(self, repo: Repository) -> None:
        html = repo.writable_session("main").store._repr_html_()
        assert '<div class="icechunk-repr">' in html
        assert "<code>&lt;icechunk.IcechunkStore&gt;</code>" in html
        assert "<strong>read_only</strong>: <code>False</code>" in html

    @pytest.mark.xfail(reason="CachingConfig not yet converted to PyRepr")
    def test_caching_config(self) -> None:
        html = CachingConfig(num_snapshot_nodes=100)._repr_html_()
        assert '<div class="icechunk-repr">' in html
        assert "<strong>num_snapshot_nodes</strong>: <code>100</code>" in html
