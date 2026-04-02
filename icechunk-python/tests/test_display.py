"""Tests for __str__, __repr__, and _repr_html_ display methods."""

import pytest

import icechunk
import zarr
from icechunk import (
    CachingConfig,
    CompressionConfig,
    Conflict,
    ConflictType,
    ManifestConfig,
    ManifestPreloadCondition,
    ManifestPreloadConfig,
    ManifestSplitCondition,
    ManifestSplittingConfig,
    ManifestVirtualChunkLocationCompressionConfig,
    Repository,
    RepositoryConfig,
    RepoStatus,
    S3Options,
    S3StaticCredentials,
    StorageConcurrencySettings,
    StorageRetriesSettings,
    StorageSettings,
    StorageTimeoutSettings,
    in_memory_storage,
)


@pytest.fixture
def repo() -> Repository:
    """Create an in-memory repository for testing."""
    return Repository.create(storage=in_memory_storage())


def assert_repr_roundtrips(obj: object) -> None:
    """Assert that eval(repr(obj)) produces an equal object (for executable reprs)."""
    repr_str = repr(obj)
    reconstructed = eval(repr_str, {"icechunk": icechunk})
    assert reconstructed == obj, (
        f"Round-trip failed:\n  repr: {repr_str}\n  original: {obj}\n  reconstructed: {reconstructed}"
    )


def assert_has_all_three_reprs(obj: object) -> None:
    """Assert that __repr__, __str__, and _repr_html_ all return non-empty strings."""
    assert len(repr(obj)) > 0
    assert len(str(obj)) > 0
    assert len(obj._repr_html_()) > 0


# =============================================================================
# Executable repr roundtrip tests
# =============================================================================
class TestReprRoundtrip:
    """Test that eval(repr(obj)) == obj for executable classes."""

    def test_caching_config(self) -> None:
        assert_repr_roundtrips(CachingConfig())
        assert_repr_roundtrips(CachingConfig(num_snapshot_nodes=100, num_chunk_refs=200))

    def test_compression_config(self) -> None:
        assert_repr_roundtrips(CompressionConfig())
        assert_repr_roundtrips(CompressionConfig(level=5))

    def test_s3_options(self) -> None:
        assert_repr_roundtrips(S3Options())
        assert_repr_roundtrips(
            S3Options(
                region="us-east-1", endpoint_url="http://localhost:9000", allow_http=True
            )
        )

    def test_storage_retries_settings(self) -> None:
        assert_repr_roundtrips(StorageRetriesSettings())
        assert_repr_roundtrips(
            StorageRetriesSettings(max_tries=3, initial_backoff_ms=100)
        )

    def test_storage_timeout_settings(self) -> None:
        assert_repr_roundtrips(StorageTimeoutSettings())
        assert_repr_roundtrips(StorageTimeoutSettings(connect_timeout_ms=5000))

    def test_storage_concurrency_settings(self) -> None:
        assert_repr_roundtrips(StorageConcurrencySettings())

    def test_storage_settings(self) -> None:
        assert_repr_roundtrips(StorageSettings())
        assert_repr_roundtrips(
            StorageSettings(
                retries=StorageRetriesSettings(max_tries=5),
                storage_class="STANDARD_IA",
            )
        )

    def test_manifest_virtual_chunk_location_compression_config(self) -> None:
        assert_repr_roundtrips(ManifestVirtualChunkLocationCompressionConfig())
        assert_repr_roundtrips(
            ManifestVirtualChunkLocationCompressionConfig(min_num_chunks=500)
        )

    def test_manifest_preload_config(self) -> None:
        assert_repr_roundtrips(ManifestPreloadConfig(max_total_refs=42))

    def test_manifest_config(self) -> None:
        assert_repr_roundtrips(ManifestConfig())

    def test_repository_config(self) -> None:
        assert_repr_roundtrips(RepositoryConfig())
        assert_repr_roundtrips(
            RepositoryConfig(
                caching=CachingConfig(num_snapshot_nodes=100),
                compression=CompressionConfig(level=3),
            )
        )


# =============================================================================
# Non-executable repr structural tests
# =============================================================================
class TestRepr:
    """Test __repr__ for non-executable classes."""

    def test_repository(self, repo: Repository) -> None:
        repr_str = repr(repo)
        assert repr_str.startswith("<icechunk.Repository>")
        assert "storage:" in repr_str
        assert "config:" in repr_str

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

    def test_conflict(self) -> None:
        c = Conflict(ConflictType(6), "/my/array", [[0, 0]])
        repr_str = repr(c)
        assert "<icechunk.Conflict>" in repr_str
        assert "ChunkDoubleUpdate" in repr_str
        assert "/my/array" in repr_str
        assert "conflicted_chunks" in repr_str

    def test_repo_status(self) -> None:
        status = RepoStatus(icechunk.RepoAvailability.online)
        repr_str = repr(status)
        assert "<icechunk.RepoStatus>" in repr_str
        assert "Online" in repr_str

    def test_nested_repr_is_executable(self) -> None:
        """An executable parent's __repr__ should contain executable nested reprs."""
        config = RepositoryConfig(caching=CachingConfig(num_snapshot_nodes=42))
        repr_str = repr(config)
        assert "icechunk.CachingConfig(" in repr_str
        assert "num_snapshot_nodes=42" in repr_str

    def test_nested_str_is_not_executable(self) -> None:
        """An executable parent's __str__ should contain non-executable nested reprs."""
        config = RepositoryConfig(caching=CachingConfig(num_snapshot_nodes=42))
        str_str = str(config)
        assert "<icechunk.CachingConfig>" in str_str
        assert "num_snapshot_nodes: 42" in str_str
        assert "icechunk.CachingConfig(" not in str_str


# =============================================================================
# Credential redaction tests
# =============================================================================
class TestCredentialRedaction:
    """Test that credential reprs redact secrets."""

    def test_s3_static_credentials_redacts_secrets(self) -> None:
        creds = S3StaticCredentials(
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            session_token="my-session-token",
        )
        repr_str = repr(creds)
        assert "wJalrXUtnFEMI" not in repr_str
        assert "my-session-token" not in repr_str
        assert "****" in repr_str
        # Should show first few chars of access key
        assert "AKIA" in repr_str


# =============================================================================
# __str__ tests
# =============================================================================
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

    def test_caching_config(self) -> None:
        config = CachingConfig(num_snapshot_nodes=100)
        str_str = str(config)
        assert "<icechunk.CachingConfig>" in str_str
        assert "num_snapshot_nodes: 100" in str_str

    def test_storage_settings(self) -> None:
        ss = StorageSettings(retries=StorageRetriesSettings(max_tries=5))
        str_str = str(ss)
        assert "<icechunk.StorageSettings>" in str_str
        assert "retries:" in str_str
        assert "max_tries: 5" in str_str

    def test_manifest_preload_condition(self) -> None:
        cond = ManifestPreloadCondition.name_matches("foo")
        str_str = str(cond)
        assert "foo" in str_str

    def test_manifest_split_condition(self) -> None:
        cond = ManifestSplitCondition.path_matches("bar")
        str_str = str(cond)
        assert "bar" in str_str


# =============================================================================
# _repr_html_ tests
# =============================================================================
class TestReprHtml:
    """Test _repr_html_ for all classes."""

    def test_repository(self, repo: Repository) -> None:
        html = repo._repr_html_()
        assert '<div class="icechunk-repr">' in html
        assert "icechunk.Repository" in html
        assert "storage" in html

    def test_session(self, repo: Repository) -> None:
        html = repo.writable_session("main")._repr_html_()
        assert '<div class="icechunk-repr">' in html
        assert "icechunk.Session" in html
        assert "read_only" in html

    def test_store(self, repo: Repository) -> None:
        html = repo.writable_session("main").store._repr_html_()
        assert '<div class="icechunk-repr">' in html
        assert "icechunk.IcechunkStore" in html

    def test_caching_config(self) -> None:
        html = CachingConfig(num_snapshot_nodes=100)._repr_html_()
        assert '<div class="icechunk-repr">' in html
        assert "num_snapshot_nodes" in html
        assert "100" in html

    def test_nested_html_uses_details(self) -> None:
        """Nested HTML reprs should use collapsible <details> elements."""
        config = RepositoryConfig(caching=CachingConfig(num_snapshot_nodes=42))
        html = config._repr_html_()
        assert "<details>" in html
        assert "caching" in html

    def test_storage_settings(self) -> None:
        ss = StorageSettings(retries=StorageRetriesSettings(max_tries=5))
        html = ss._repr_html_()
        assert '<div class="icechunk-repr">' in html
        assert "retries" in html


# =============================================================================
# assert_has_all_three_reprs for every class
# =============================================================================
class TestAllClassesHaveReprs:
    """Smoke test that every class produces non-empty repr/str/html."""

    def test_caching_config(self) -> None:
        assert_has_all_three_reprs(CachingConfig())

    def test_compression_config(self) -> None:
        assert_has_all_three_reprs(CompressionConfig())

    def test_s3_options(self) -> None:
        assert_has_all_three_reprs(S3Options())

    def test_s3_static_credentials(self) -> None:
        assert_has_all_three_reprs(S3StaticCredentials("ak", "sk"))

    def test_storage_retries_settings(self) -> None:
        assert_has_all_three_reprs(StorageRetriesSettings())

    def test_storage_timeout_settings(self) -> None:
        assert_has_all_three_reprs(StorageTimeoutSettings())

    def test_storage_concurrency_settings(self) -> None:
        assert_has_all_three_reprs(StorageConcurrencySettings())

    def test_storage_settings(self) -> None:
        assert_has_all_three_reprs(StorageSettings())

    def test_manifest_virtual_chunk_location_compression_config(self) -> None:
        assert_has_all_three_reprs(ManifestVirtualChunkLocationCompressionConfig())

    def test_manifest_preload_config(self) -> None:
        assert_has_all_three_reprs(ManifestPreloadConfig())

    def test_manifest_splitting_config(self) -> None:
        assert_has_all_three_reprs(ManifestSplittingConfig())

    def test_manifest_config(self) -> None:
        assert_has_all_three_reprs(ManifestConfig())

    def test_repository_config(self) -> None:
        assert_has_all_three_reprs(RepositoryConfig())

    def test_conflict(self) -> None:
        assert_has_all_three_reprs(Conflict(ConflictType(6), "/path"))

    def test_repo_status(self) -> None:
        assert_has_all_three_reprs(RepoStatus(icechunk.RepoAvailability.online))

    def test_repository(self, repo: Repository) -> None:
        assert_has_all_three_reprs(repo)

    def test_session(self, repo: Repository) -> None:
        assert_has_all_three_reprs(repo.writable_session("main"))

    def test_store(self, repo: Repository) -> None:
        assert_has_all_three_reprs(repo.writable_session("main").store)
