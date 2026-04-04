"""Tests for __str__, __repr__, and _repr_html_ display methods."""

from datetime import UTC, datetime, timedelta

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
    ObjectStoreConfig,
    Repository,
    RepositoryConfig,
    RepoStatus,
    S3Options,
    S3StaticCredentials,
    StorageConcurrencySettings,
    StorageRetriesSettings,
    StorageSettings,
    StorageTimeoutSettings,
    VirtualChunkContainer,
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

    def test_object_store_config(self) -> None:
        assert_repr_roundtrips(ObjectStoreConfig.InMemory())
        assert_repr_roundtrips(
            ObjectStoreConfig.S3(S3Options(region="us-east-1", allow_http=True))
        )
        assert_repr_roundtrips(ObjectStoreConfig.LocalFileSystem("/tmp/test"))

    def test_virtual_chunk_container(self) -> None:
        assert_repr_roundtrips(
            VirtualChunkContainer("s3://bucket/", ObjectStoreConfig.InMemory())
        )
        assert_repr_roundtrips(
            VirtualChunkContainer(
                "s3://data/",
                ObjectStoreConfig.S3(S3Options(region="eu-west-1")),
                name="my-store",
            )
        )


# =============================================================================
# Non-executable repr structural tests
# =============================================================================
class TestRepr:
    """Test __repr__ for non-executable classes."""

    def test_repository(self, repo: Repository) -> None:
        repr_str = repr(repo)
        assert "<icechunk.Repository (v" in repr_str
        assert "storage:" in repr_str
        assert "in-memory" in repr_str
        assert "config: <RepositoryConfig ...>" in repr_str

    def test_session_writable(self, repo: Repository) -> None:
        session = repo.writable_session("main")
        repr_str = repr(session)
        assert "<icechunk.Session>" in repr_str
        assert "read_only: False" in repr_str
        assert "branch: main" in repr_str
        assert "has_uncommitted_changes: False" in repr_str
        assert session.snapshot_id in repr_str

    def test_session_readonly_omits_branch(self, repo: Repository) -> None:
        session = repo.writable_session("main")
        zarr.group(session.store)
        session.commit("init")

        readonly_session = repo.readonly_session("main")
        repr_str = repr(readonly_session)
        assert "read_only: True" in repr_str
        assert readonly_session.snapshot_id in repr_str
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
        assert "<icechunk.Repository (v" in str_str
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
# Structural tests for classes not covered by roundtrip or other sections
# =============================================================================
class TestStorageRepr:
    """Test Storage __repr__, __str__, and _repr_html_."""

    def test_in_memory(self) -> None:
        s = in_memory_storage()
        repr_str = repr(s)
        assert "<icechunk.Storage>" in repr_str
        assert "type: in-memory" in repr_str

    def test_in_memory_html(self) -> None:
        s = in_memory_storage()
        html = s._repr_html_()
        assert "icechunk.Storage" in html
        assert "in-memory" in html
        assert '<div class="icechunk-repr">' in html

    def test_local_filesystem(self, tmp_path: object) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            s = icechunk.local_filesystem_storage(td)
            repr_str = repr(s)
            assert "<icechunk.Storage>" in repr_str
            assert "type: local filesystem" in repr_str
            assert "path:" in repr_str
            assert td in repr_str

    def test_repo_shows_storage_info(self, repo: Repository) -> None:
        repr_str = repr(repo)
        assert "in-memory" in repr_str
        html = repo._repr_html_()
        assert "in-memory" in html


class TestReprStructural:
    """Check repr content for classes that aren't tested elsewhere."""

    def test_compression_config_shows_algorithm(self) -> None:
        """CompressionConfig should show the actual algorithm, not hardcoded None."""
        config = CompressionConfig(algorithm=icechunk.CompressionAlgorithm(), level=3)
        repr_str = repr(config)
        assert "icechunk.CompressionConfig(" in repr_str
        assert "Zstd" in repr_str
        assert "level=3" in repr_str

    def test_s3_options_shows_all_fields(self) -> None:
        opts = S3Options(region="us-east-1", allow_http=True, requester_pays=True)
        repr_str = repr(opts)
        assert "icechunk.S3Options(" in repr_str
        assert 'region="us-east-1"' in repr_str
        assert "allow_http=True" in repr_str
        assert "requester_pays=True" in repr_str
        assert "anonymous=False" in repr_str

    def test_storage_retries_settings_shows_fields(self) -> None:
        s = StorageRetriesSettings(max_tries=5, initial_backoff_ms=100)
        repr_str = repr(s)
        assert "max_tries=5" in repr_str
        assert "initial_backoff_ms=100" in repr_str

    def test_storage_timeout_settings_shows_fields(self) -> None:
        s = StorageTimeoutSettings(connect_timeout_ms=5000, read_timeout_ms=10000)
        repr_str = repr(s)
        assert "connect_timeout_ms=5000" in repr_str
        assert "read_timeout_ms=10000" in repr_str

    def test_storage_concurrency_settings_shows_fields(self) -> None:
        s = StorageConcurrencySettings()
        repr_str = repr(s)
        assert "icechunk.StorageConcurrencySettings(" in repr_str
        assert "max_concurrent_requests_for_object" in repr_str

    def test_storage_settings_shows_all_fields(self) -> None:
        s = StorageSettings(storage_class="STANDARD_IA")
        repr_str = repr(s)
        assert "icechunk.StorageSettings(" in repr_str
        assert 'storage_class="STANDARD_IA"' in repr_str
        assert "minimum_size_for_multipart_upload" in repr_str

    def test_manifest_virtual_chunk_location_compression(self) -> None:
        c = ManifestVirtualChunkLocationCompressionConfig(min_num_chunks=500)
        repr_str = repr(c)
        assert "icechunk.ManifestVirtualChunkLocationCompressionConfig(" in repr_str
        assert "min_num_chunks=500" in repr_str

    def test_manifest_preload_config_shows_fields(self) -> None:
        c = ManifestPreloadConfig(max_total_refs=42, max_arrays_to_scan=10)
        repr_str = repr(c)
        assert "icechunk.ManifestPreloadConfig(" in repr_str
        assert "max_total_refs=42" in repr_str
        assert "max_arrays_to_scan=10" in repr_str

    def test_manifest_splitting_config_shows_fields(self) -> None:
        c = ManifestSplittingConfig()
        repr_str = repr(c)
        assert "icechunk.ManifestSplittingConfig(" in repr_str
        assert "split_sizes" in repr_str

    def test_manifest_config_shows_fields(self) -> None:
        c = ManifestConfig()
        repr_str = repr(c)
        assert "icechunk.ManifestConfig(" in repr_str
        assert "preload" in repr_str
        assert "splitting" in repr_str
        assert "virtual_chunk_location_compression" in repr_str

    def test_virtual_chunk_container_shows_fields(self) -> None:
        store_config = icechunk.s3_store(
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
            s3_compatible=True,
        )
        vcc = icechunk.VirtualChunkContainer("s3://bucket/", store_config)
        repr_str = repr(vcc)
        assert "icechunk.VirtualChunkContainer(" in repr_str
        assert 'url_prefix="s3://bucket/"' in repr_str
        assert "store=" in repr_str

    def test_repository_config_shows_all_fields(self) -> None:
        config = RepositoryConfig()
        repr_str = repr(config)
        assert "icechunk.RepositoryConfig(" in repr_str
        assert "inline_chunk_threshold_bytes" in repr_str
        assert "compression" in repr_str
        assert "caching" in repr_str
        assert "storage" in repr_str
        assert "manifest" in repr_str
        assert "repo_update_retries" in repr_str
        assert "num_updates_per_repo_info_file" in repr_str
        assert "virtual_chunk_containers" in repr_str

    def test_repository_config_shows_virtual_chunk_containers(self) -> None:
        config = RepositoryConfig()
        s3_store = ObjectStoreConfig.S3(S3Options(region="us-east-1"))
        config.set_virtual_chunk_container(
            VirtualChunkContainer("s3://my-data/", s3_store, name="era5")
        )
        # repr (executable)
        repr_str = repr(config)
        assert "s3://my-data/" in repr_str
        assert "icechunk.VirtualChunkContainer(" in repr_str
        assert 'name="era5"' in repr_str
        # str (human-readable)
        str_str = str(config)
        assert "s3://my-data/" in str_str
        assert "virtual_chunk_containers:" in str_str
        # html (collapsible)
        html = config._repr_html_()
        assert "s3://my-data/" in html
        assert "virtual_chunk_containers" in html

    def test_object_store_config_inmemory(self) -> None:
        osc = ObjectStoreConfig.InMemory()
        assert "icechunk.ObjectStoreConfig.InMemory(" in repr(osc)
        assert "<icechunk.ObjectStoreConfig.InMemory>" in str(osc)
        html = osc._repr_html_()
        assert "icechunk.ObjectStoreConfig.InMemory" in html
        assert '<div class="icechunk-repr">' in html

    def test_object_store_config_s3(self) -> None:
        osc = ObjectStoreConfig.S3(S3Options(region="us-east-1"))
        repr_str = repr(osc)
        assert "icechunk.ObjectStoreConfig.S3(" in repr_str
        assert "us-east-1" in repr_str
        str_str = str(osc)
        assert "<icechunk.ObjectStoreConfig.S3>" in str_str
        assert "us-east-1" in str_str

    def test_object_store_config_local_filesystem(self) -> None:
        osc = ObjectStoreConfig.LocalFileSystem("/tmp/test")
        repr_str = repr(osc)
        assert "icechunk.ObjectStoreConfig.LocalFileSystem(" in repr_str
        assert "/tmp/test" in repr_str

    def test_snapshot_info(self, repo: Repository) -> None:
        session = repo.writable_session("main")
        zarr.group(session.store)
        session.commit("init")
        infos = list(repo.ancestry(branch="main"))
        assert len(infos) >= 1
        info = infos[0]
        repr_str = repr(info)
        assert "<icechunk.SnapshotInfo>" in repr_str
        assert "id:" in repr_str
        assert "message:" in repr_str
        assert "written_at:" in repr_str
        assert "metadata:" in repr_str
        html = info._repr_html_()
        assert "icechunk.SnapshotInfo" in html

    def test_diff(self, repo: Repository) -> None:
        session = repo.writable_session("main")
        zarr.open_array(session.store, path="arr", mode="w", shape=(10,), dtype="f8")
        snap1 = session.commit("add array")
        session2 = repo.writable_session("main")
        zarr.open_array(session2.store, path="arr2", mode="w", shape=(5,), dtype="i4")
        snap2 = session2.commit("add array2")
        diff = repo.diff(from_snapshot_id=snap1, to_snapshot_id=snap2)
        repr_str = repr(diff)
        assert "Arrays created" in repr_str
        assert "arr2" in repr_str
        html = diff._repr_html_()
        assert "icechunk.Diff" in html

    def test_gc_summary(self, repo: Repository) -> None:
        session = repo.writable_session("main")
        zarr.group(session.store)
        session.commit("init")
        gc = repo.garbage_collect(datetime.now(tz=UTC) - timedelta(hours=1))
        repr_str = repr(gc)
        assert "<icechunk.GCSummary>" in repr_str
        assert "bytes_deleted:" in repr_str
        assert "chunks_deleted:" in repr_str
        html = gc._repr_html_()
        assert "icechunk.GCSummary" in html

    def test_feature_flag(self, repo: Repository) -> None:
        flags = repo.feature_flags()
        assert len(flags) > 0
        flag = flags[0]
        repr_str = repr(flag)
        assert "<icechunk.FeatureFlag>" in repr_str
        assert "name:" in repr_str
        assert "default_enabled:" in repr_str
        assert "enabled:" in repr_str
        html = flag._repr_html_()
        assert "icechunk.FeatureFlag" in html

    def test_update(self, repo: Repository) -> None:
        session = repo.writable_session("main")
        zarr.group(session.store)
        session.commit("init")
        updates = list(repo.ops_log())
        assert len(updates) > 0
        update = updates[0]
        repr_str = repr(update)
        assert "<icechunk.Update>" in repr_str
        assert "kind:" in repr_str
        assert "updated_at:" in repr_str
        html = update._repr_html_()
        assert "icechunk.Update" in html

    def test_chunk_storage_stats(self, repo: Repository) -> None:
        stats = repo.chunk_storage_stats()
        repr_str = repr(stats)
        assert "<icechunk.ChunkStorageStats>" in repr_str
        assert "native_bytes:" in repr_str
        assert "virtual_bytes:" in repr_str
        assert "inlined_bytes:" in repr_str
        html = stats._repr_html_()
        assert "icechunk.ChunkStorageStats" in html

    def test_virtual_chunk_container_all_modes(self) -> None:
        vcc = VirtualChunkContainer("s3://bucket/", ObjectStoreConfig.InMemory())
        # Executable repr
        repr_str = repr(vcc)
        assert "icechunk.VirtualChunkContainer(" in repr_str
        assert 'url_prefix="s3://bucket/"' in repr_str
        # Str
        str_str = str(vcc)
        assert "<icechunk.VirtualChunkContainer>" in str_str
        assert "url_prefix:" in str_str
        assert "store:" in str_str
        # HTML
        html = vcc._repr_html_()
        assert "icechunk.VirtualChunkContainer" in html
        assert '<div class="icechunk-repr">' in html


# =============================================================================
# AncestryGraph tests
# =============================================================================
class TestAncestryGraph:
    """Test ancestry_graph() visualization."""

    def test_single_branch_linear(self, repo: Repository) -> None:
        """A single branch should produce a linear graph."""
        session = repo.writable_session("main")
        store = session.store
        root = zarr.group(store=store)
        root.create_array("arr", shape=(10,), dtype="float64")
        session.commit("First commit")

        session = repo.writable_session("main")
        store = session.store
        zarr.open_array(store=store, path="arr")[:] = 1.0
        session.commit("Second commit")

        graph = repo.ancestry_graph(branch="main")
        output = str(graph)
        assert "Second commit" in output
        assert "First commit" in output
        # Branch label should appear on the tip
        assert "main" in output

    def test_all_branches_tree(self, repo: Repository) -> None:
        """Multiple branches should produce a tree graph."""
        # Create initial commit on main
        session = repo.writable_session("main")
        store = session.store
        root = zarr.group(store=store)
        root.create_array("arr", shape=(10,), dtype="float64")
        snap1 = session.commit("Initial commit")

        # Create a second commit on main
        session = repo.writable_session("main")
        store = session.store
        zarr.open_array(store=store, path="arr")[:] = 1.0
        session.commit("Main work")

        # Create a branch from initial commit
        repo.create_branch("feature", snap1)
        session = repo.writable_session("feature")
        store = session.store
        zarr.open_array(store=store, path="arr")[:] = 2.0
        session.commit("Feature work")

        # All-branches graph
        graph = repo.ancestry_graph()
        output = str(graph)
        assert "main" in output
        assert "feature" in output
        assert "Main work" in output
        assert "Feature work" in output
        assert "Initial commit" in output

    def test_empty_repo(self) -> None:
        """A fresh repo with no commits beyond initial should still work."""
        repo = Repository.create(storage=in_memory_storage())
        graph = repo.ancestry_graph(branch="main")
        output = str(graph)
        # Should have at least the initial snapshot
        assert len(output.strip()) > 0

    def test_with_tags(self, repo: Repository) -> None:
        """Tags should appear as labels on the graph."""
        session = repo.writable_session("main")
        store = session.store
        root = zarr.group(store=store)
        root.create_array("arr", shape=(10,), dtype="float64")
        snap = session.commit("Tagged commit")

        repo.create_tag("v1.0", snap)
        graph = repo.ancestry_graph(branch="main")
        output = str(graph)
        assert "v1.0" in output

    def test_repr_svg(self, repo: Repository) -> None:
        """_repr_svg_ should return raw SVG content."""
        session = repo.writable_session("main")
        store = session.store
        root = zarr.group(store=store)
        root.create_array("arr", shape=(10,), dtype="float64")
        session.commit("First commit")

        graph = repo.ancestry_graph(branch="main")
        svg = graph._repr_svg_()
        assert "<svg" in svg
        assert "</svg>" in svg
        assert "<circle" in svg
        assert "First commit" in svg
        assert "main" in svg

    def test_to_plain_string(self, repo: Repository) -> None:
        """to_plain_string should have no ANSI codes."""
        session = repo.writable_session("main")
        store = session.store
        root = zarr.group(store=store)
        root.create_array("arr", shape=(10,), dtype="float64")
        session.commit("First commit")

        graph = repo.ancestry_graph(branch="main")
        plain = graph.to_plain_string()
        assert "\x1b" not in plain
        assert "First commit" in plain
        assert "main" in plain
