import os
from unittest import mock

import icechunk as ic


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
