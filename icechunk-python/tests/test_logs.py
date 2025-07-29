import os
import re
from unittest import mock

import icechunk as ic
import zarr


@mock.patch.dict(os.environ, {"ICECHUNK_LOG": "debug"}, clear=True)
def test_debug_logs_from_environment(capfd) -> None:
    ic.set_logs_filter(None)
    ic.Repository.create(storage=ic.in_memory_storage())
    assert "Creating Repository" in capfd.readouterr().out


@mock.patch.dict(os.environ, clear=True)
def test_no_logs_from_environment(capfd) -> None:
    ic.set_logs_filter(None)
    ic.Repository.create(storage=ic.in_memory_storage())
    assert capfd.readouterr().out == ""


@mock.patch.dict(os.environ, clear=True)
def test_change_log_levels_from_env(capfd) -> None:
    # first with logs disabled
    ic.set_logs_filter(None)
    ic.Repository.create(storage=ic.in_memory_storage())
    assert capfd.readouterr().out == ""

    # now with logs enabled
    with mock.patch.dict(os.environ, {"ICECHUNK_LOG": "debug"}, clear=True):
        ic.set_logs_filter(None)
        ic.Repository.create(storage=ic.in_memory_storage())
        assert "Creating Repository" in capfd.readouterr().out


def test_debug_logs_from_argument(capfd) -> None:
    ic.set_logs_filter("debug")
    ic.Repository.create(storage=ic.in_memory_storage())
    assert "Creating Repository" in capfd.readouterr().out


@mock.patch.dict(os.environ, {"ICECHUNK_LOG": "debug"}, clear=True)
def test_no_logs_from_argument(capfd) -> None:
    ic.set_logs_filter("false")
    ic.Repository.create(storage=ic.in_memory_storage())
    assert capfd.readouterr().out == ""


def test_change_log_levels_from_argument(capfd) -> None:
    # first with logs disabled
    ic.set_logs_filter("")
    ic.Repository.create(storage=ic.in_memory_storage())
    assert capfd.readouterr().out == ""

    # now with logs enabled
    ic.set_logs_filter("debug")
    ic.Repository.create(storage=ic.in_memory_storage())
    assert "Creating Repository" in capfd.readouterr().out


def test_warn_on_small_caches(capfd) -> None:
    # first with logs disabled
    ic.set_logs_filter("warn")
    config = ic.RepositoryConfig(
        caching=ic.CachingConfig(num_chunk_refs=0, num_snapshot_nodes=0),
    )
    repo = ic.Repository.create(storage=ic.in_memory_storage(), config=config)
    session = repo.writable_session("main")

    array1 = zarr.create_array(
        name="array1",
        store=session.store,
        shape=(10),
        dtype="int64",
        zarr_format=3,
        chunks=(2,),
        fill_value=-1,
    )
    array2 = zarr.create_array(
        name="array2",
        store=session.store,
        shape=(10),
        dtype="int64",
        zarr_format=3,
        chunks=(2,),
        fill_value=-1,
    )
    array1[:] = 42
    array2[:] = 42
    session.commit("msg")

    out = capfd.readouterr().out
    # we only warn once for manifests and once for snapshots
    assert len(re.findall("WARN", out)) == 2

    assert re.search("5 chunk references", out)
    assert re.search("keep 0 references", out)
    assert re.search("3 nodes", out)
    assert re.search("keep 0 nodes", out)
