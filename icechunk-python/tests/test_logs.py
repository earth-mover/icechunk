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


def test_misspelling_warnings_for_icechunk(capfd) -> None:
    """Test that misspelled variations of 'icechunk' trigger warnings."""
    # Test common misspellings
    ic.set_logs_filter("icehunk:trace")
    err_output = capfd.readouterr().err
    assert "WARNING" in err_output and "icehunk" in err_output

    ic.set_logs_filter("icecunk:debug")
    err_output = capfd.readouterr().err
    assert "WARNING" in err_output and "icecunk" in err_output

    ic.set_logs_filter("iecchunk:info")
    err_output = capfd.readouterr().err
    assert "WARNING" in err_output and "iecchunk" in err_output

    ic.set_logs_filter("ichunk:warn")
    err_output = capfd.readouterr().err
    assert "WARNING" in err_output and "ichunk" in err_output


def test_no_false_positives_for_correct_spelling(capfd) -> None:
    """Test that correct 'icechunk' spelling doesn't trigger warnings."""
    ic.set_logs_filter("icechunk:debug")
    assert "WARNING" not in capfd.readouterr().err

    ic.set_logs_filter("icechunk::repository:trace")
    assert "WARNING" not in capfd.readouterr().err


def test_no_warnings_for_generic_log_levels(capfd) -> None:
    """Test that generic log levels don't trigger warnings."""
    ic.set_logs_filter("debug")
    assert "WARNING" not in capfd.readouterr().err

    ic.set_logs_filter("trace")
    assert "WARNING" not in capfd.readouterr().err

    ic.set_logs_filter("info")
    assert "WARNING" not in capfd.readouterr().err


def test_no_warnings_for_unrelated_modules(capfd) -> None:
    """Test that unrelated module names don't trigger warnings."""
    ic.set_logs_filter("tokio:debug")
    assert "WARNING" not in capfd.readouterr().err

    ic.set_logs_filter("serde:info")
    assert "WARNING" not in capfd.readouterr().err

    ic.set_logs_filter("my_app::module:warn")
    assert "WARNING" not in capfd.readouterr().err


def test_complex_filter_strings_with_misspellings(capfd) -> None:
    """Test complex filter strings with multiple modules."""
    ic.set_logs_filter("icehunk:trace,debug,tokio::io:warn")
    err_output = capfd.readouterr().err
    assert "WARNING" in err_output and "icehunk" in err_output
    # Should not warn about tokio
    assert "tokio" not in err_output

    ic.set_logs_filter("debug,icecunk:info,warn")
    err_output = capfd.readouterr().err
    assert "WARNING" in err_output and "icecunk" in err_output


def test_nested_module_paths_with_misspellings(capfd) -> None:
    """Test nested module paths like icehunk::repository:trace."""
    ic.set_logs_filter("icehunk::repository:trace")
    err_output = capfd.readouterr().err
    assert "WARNING" in err_output and "icehunk" in err_output

    ic.set_logs_filter("icecunk::store::manager:debug")
    err_output = capfd.readouterr().err
    assert "WARNING" in err_output and "icecunk" in err_output


def test_short_strings_not_flagged(capfd) -> None:
    """Test that very short strings don't get flagged as misspellings."""
    ic.set_logs_filter("ice:debug")
    assert "WARNING" not in capfd.readouterr().err

    ic.set_logs_filter("chunk:info")
    assert "WARNING" not in capfd.readouterr().err
