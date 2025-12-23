"""Version compatibility tests for icechunk v1/v2 format.

These tests verify that:
1. The v2 library can write v1 format repositories (using spec_version=1)
2. The actual v1 library can read repositories written in v1 format by v2
3. After upgrading to v2 format, v1 library fails gracefully
4. The v2 library can read upgraded repositories
"""

import tempfile

import pytest
import zarr

# TODO: use hypothesis here


class TestFormatCompatibility:
    """Test format compatibility between icechunk v1 and v2 libraries."""

    def test_v1_can_read_v1_format_written_by_v2(self) -> None:
        """Verify v1 library can read a v1-format repo created by v2 library."""
        import icechunk
        import icechunk_v1

        print(f"icechunk (v2) version: {icechunk.__version__}")
        print(f"icechunk_v1 version: {icechunk_v1.__version__}")

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create repo with v2 library but using spec_version=1 (v1 format)
            storage = icechunk.local_filesystem_storage(temp_dir)
            repo = icechunk.Repository.create(storage, spec_version=1)

            session = repo.writable_session("main")
            store = session.store
            group = zarr.group(store)
            array = group.create("my_array", shape=10, dtype="int32", chunks=(5,))
            array[:] = 1
            session.commit("first commit")

            assert repo.spec_version == 1, "Repository should be v1 format"

            # Verify v1 library can read it
            storage_v1 = icechunk_v1.local_filesystem_storage(temp_dir)
            repo_v1 = icechunk_v1.Repository.open(storage_v1)
            session_v1 = repo_v1.readonly_session("main")
            store_v1 = session_v1.store
            group_v1 = zarr.open_group(store_v1, mode="r")
            array_v1 = group_v1["my_array"]

            # Verify data integrity
            assert list(array_v1[:]) == [1] * 10, "v1 should read data correctly"

            # Now have v1 write to the repo
            session_v1_write = repo_v1.writable_session("main")
            store_v1_write = session_v1_write.store
            array_v1_write = zarr.open_array(store_v1_write, path="my_array", mode="a")
            array_v1_write[:] = 2
            session_v1_write.commit("v1 commit")

            # Verify v2 can still read the data written by v1
            storage_v2_read = icechunk.local_filesystem_storage(temp_dir)
            repo_v2_read = icechunk.Repository.open(storage_v2_read)
            session_v2_read = repo_v2_read.readonly_session("main")
            store_v2_read = session_v2_read.store
            group_v2_read = zarr.open_group(store_v2_read, mode="r")
            array_v2_read = group_v2_read["my_array"]

            # Verify v2 can read what v1 wrote
            assert (
                list(array_v2_read[:]) == [2] * 10
            ), "v2 should read data written by v1"

    def test_v1_cannot_read_after_upgrade_to_v2(self) -> None:
        """Verify v1 library fails gracefully after repo is upgraded to v2 format."""
        import icechunk
        import icechunk_v1

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create repo with v2 library using spec_version=1
            storage = icechunk.local_filesystem_storage(temp_dir)
            repo = icechunk.Repository.create(storage, spec_version=1)

            session = repo.writable_session("main")
            store = session.store
            group = zarr.group(store)
            array = group.create("my_array", shape=10, dtype="int32", chunks=(5,))
            array[:] = 42
            session.commit("initial commit")

            # Upgrade to v2 format
            storage2 = icechunk.local_filesystem_storage(temp_dir)
            repo2 = icechunk.Repository.open(storage2)
            icechunk.upgrade_icechunk_repository(
                repo2, dry_run=False, delete_unused_v1_files=True
            )

            # Verify repo is now v2
            storage3 = icechunk.local_filesystem_storage(temp_dir)
            repo3 = icechunk.Repository.open(storage3)
            assert repo3.spec_version == 2, "Repository should be upgraded to v2"

            # Verify v1 library CANNOT read the upgraded repo
            storage_v1 = icechunk_v1.local_filesystem_storage(temp_dir)
            with pytest.raises(Exception):
                # v1 should fail to open a v2 format repo
                icechunk_v1.Repository.open(storage_v1)

    def test_v2_can_read_after_upgrade(self) -> None:
        """Verify v2 library can still read data after upgrading from v1 to v2 format."""
        import icechunk

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create repo with v2 library using spec_version=1
            storage = icechunk.local_filesystem_storage(temp_dir)
            repo = icechunk.Repository.create(storage, spec_version=1)

            session = repo.writable_session("main")
            store = session.store
            group = zarr.group(store)
            array = group.create("my_array", shape=10, dtype="int32", chunks=(5,))
            array[:] = 99
            session.commit("initial commit")

            # Upgrade to v2 format
            storage2 = icechunk.local_filesystem_storage(temp_dir)
            repo2 = icechunk.Repository.open(storage2)
            icechunk.upgrade_icechunk_repository(
                repo2, dry_run=False, delete_unused_v1_files=True
            )

            # Verify v2 can still read the data
            storage3 = icechunk.local_filesystem_storage(temp_dir)
            repo3 = icechunk.Repository.open(storage3)
            assert repo3.spec_version == 2

            session3 = repo3.readonly_session("main")
            store3 = session3.store
            group3 = zarr.open_group(store3, mode="r")
            array3 = group3["my_array"]

            # Verify data integrity after upgrade
            assert list(array3[:]) == [99] * 10, "v2 should read data after upgrade"

    def test_v2_can_write_and_read_v1_format(self) -> None:
        """Verify v2 library can write to and read from a v1 format repo."""
        import icechunk

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create v1 format repo
            storage = icechunk.local_filesystem_storage(temp_dir)
            repo = icechunk.Repository.create(storage, spec_version=1)

            # First commit
            session = repo.writable_session("main")
            store = session.store
            group = zarr.group(store)
            array = group.create("data", shape=(5, 5), dtype="float64", chunks=(2, 2))
            array[:] = 1.5
            session.commit("first commit")

            # Second commit - modify data
            session2 = repo.writable_session("main")
            store2 = session2.store
            array2 = zarr.open_array(store2, path="data", mode="a")
            array2[0, 0] = 100.0
            session2.commit("second commit")

            # Verify we can read back correctly
            session3 = repo.readonly_session("main")
            store3 = session3.store
            array3 = zarr.open_array(store3, path="data", mode="r")

            assert array3[0, 0] == 100.0
            assert array3[1, 1] == 1.5

            # Verify it's still v1 format
            assert repo.spec_version == 1

    def test_version_info(self) -> None:
        """Print version info for debugging."""
        import icechunk
        import icechunk_v1

        print(f"\nicechunk (v2) version: {icechunk.__version__}")
        print(f"icechunk_v1 version: {icechunk_v1.__version__}")

        # Verify versions are as expected
        v2_version = icechunk.__version__
        v1_version = icechunk_v1.__version__

        # v2 should be >= 2.0.0
        assert (
            v2_version.startswith("2.") or "2.0.0" in v2_version or "dev" in v2_version
        )

        # v1 should be < 2.0.0
        assert v1_version.startswith("1.") or v1_version.startswith("0.")
