import logging
import os
import re
import subprocess
import sys
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest
from pytest import CaptureFixture

import icechunk as ic
import zarr
from icechunk._icechunk_python import _flush_python_logs


@mock.patch.dict(os.environ, {"ICECHUNK_LOG": "debug"}, clear=True)
def test_debug_logs_from_environment(
    capfd: CaptureFixture[str], any_spec_version: int | None
) -> None:
    ic.set_logs_filter(None)
    ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=any_spec_version,
    )
    captured = capfd.readouterr()
    assert "Creating Repository" in captured.err
    assert "Creating Repository" not in captured.out


@mock.patch.dict(os.environ, clear=True)
def test_no_logs_from_environment(
    capfd: CaptureFixture[str], any_spec_version: int | None
) -> None:
    ic.set_logs_filter(None)
    ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=any_spec_version,
    )
    assert capfd.readouterr().err == ""


@mock.patch.dict(os.environ, clear=True)
def test_change_log_levels_from_env(
    capfd: CaptureFixture[str], any_spec_version: int | None
) -> None:
    # first with logs disabled
    ic.set_logs_filter(None)
    ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=any_spec_version,
    )
    assert capfd.readouterr().err == ""

    # now with logs enabled
    with mock.patch.dict(os.environ, {"ICECHUNK_LOG": "debug"}, clear=True):
        ic.set_logs_filter(None)
        ic.Repository.create(
            storage=ic.in_memory_storage(),
            spec_version=any_spec_version,
        )
        assert "Creating Repository" in capfd.readouterr().err


def test_debug_logs_from_argument(
    capfd: CaptureFixture[str], any_spec_version: int | None
) -> None:
    ic.set_logs_filter("debug")
    ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=any_spec_version,
    )
    assert "Creating Repository" in capfd.readouterr().err
    ic.set_logs_filter(None)


@mock.patch.dict(os.environ, {"ICECHUNK_LOG": "debug"}, clear=True)
def test_no_logs_from_argument(
    capfd: CaptureFixture[str], any_spec_version: int | None
) -> None:
    ic.set_logs_filter("false")
    ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=any_spec_version,
    )
    assert capfd.readouterr().err == ""
    ic.set_logs_filter(None)


def test_change_log_levels_from_argument(
    capfd: CaptureFixture[str], any_spec_version: int | None
) -> None:
    # first with logs disabled
    ic.set_logs_filter("")
    ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=any_spec_version,
    )
    assert capfd.readouterr().err == ""

    # now with logs enabled
    ic.set_logs_filter("debug")
    ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=any_spec_version,
    )
    assert "Creating Repository" in capfd.readouterr().err
    ic.set_logs_filter(None)


# The console writer is fixed when logging is first initialized (at `import
# icechunk`), so the stdout/stderr routing can only be exercised in a fresh
# interpreter with the environment set before the import.
LOG_SNIPPET = (
    "import icechunk as ic\nic.Repository.create(storage=ic.in_memory_storage())\n"
)


def run_logging_subprocess(
    extra_env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", LOG_SNIPPET],
        capture_output=True,
        text=True,
        env={**os.environ, "ICECHUNK_LOG": "debug", **extra_env},
        check=True,
    )


def test_logs_go_to_stderr_by_default() -> None:
    result = run_logging_subprocess({})
    assert "Creating Repository" in result.stderr
    assert "Creating Repository" not in result.stdout


def test_logs_go_to_stdout_when_requested() -> None:
    result = run_logging_subprocess({"ICECHUNK_LOG_TO_STDOUT": "1"})
    assert "Creating Repository" in result.stdout
    assert "Creating Repository" not in result.stderr


def test_warn_on_small_caches(
    capfd: CaptureFixture[str], any_spec_version: int | None
) -> None:
    # first with logs disabled
    ic.set_logs_filter("warn")
    config = ic.RepositoryConfig(
        caching=ic.CachingConfig(num_chunk_refs=0, num_snapshot_nodes=0),
    )
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
        config=config,
        spec_version=any_spec_version,
    )
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

    err = capfd.readouterr().err
    # we only warn once for manifests and once for snapshots
    assert len(re.findall("WARN", err)) == 2

    assert re.search("5 chunk references", err)
    assert re.search("keep 0 references", err)
    assert re.search("3 nodes", err)
    assert re.search("keep 0 nodes", err)
    ic.set_logs_filter(None)


def test_misspelling_warnings_for_icechunk(capfd: CaptureFixture[str]) -> None:
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
    ic.set_logs_filter(None)


def test_no_false_positives_for_correct_spelling(capfd: CaptureFixture[str]) -> None:
    """Test that correct 'icechunk' spelling doesn't trigger warnings."""
    ic.set_logs_filter("icechunk:debug")
    assert "WARNING" not in capfd.readouterr().err

    ic.set_logs_filter("icechunk::repository:trace")
    assert "WARNING" not in capfd.readouterr().err
    ic.set_logs_filter(None)


def test_no_warnings_for_generic_log_levels(capfd: CaptureFixture[str]) -> None:
    """Test that generic log levels don't trigger warnings."""
    ic.set_logs_filter("debug")
    assert "WARNING" not in capfd.readouterr().err

    ic.set_logs_filter("trace")
    assert "WARNING" not in capfd.readouterr().err

    ic.set_logs_filter("info")
    assert "WARNING" not in capfd.readouterr().err
    ic.set_logs_filter(None)


def test_no_warnings_for_unrelated_modules(capfd: CaptureFixture[str]) -> None:
    """Test that unrelated module names don't trigger warnings."""
    ic.set_logs_filter("tokio:debug")
    assert "WARNING" not in capfd.readouterr().err

    ic.set_logs_filter("serde:info")
    assert "WARNING" not in capfd.readouterr().err

    ic.set_logs_filter("my_app::module:warn")
    assert "WARNING" not in capfd.readouterr().err
    ic.set_logs_filter(None)


def test_complex_filter_strings_with_misspellings(capfd: CaptureFixture[str]) -> None:
    """Test complex filter strings with multiple modules."""
    ic.set_logs_filter("icehunk:trace,debug,tokio::io:warn")
    err_output = capfd.readouterr().err
    assert "WARNING" in err_output and "icehunk" in err_output
    # Should not warn about tokio
    assert "tokio" not in err_output

    ic.set_logs_filter("debug,icecunk:info,warn")
    err_output = capfd.readouterr().err
    assert "WARNING" in err_output and "icecunk" in err_output
    ic.set_logs_filter(None)


def test_nested_module_paths_with_misspellings(capfd: CaptureFixture[str]) -> None:
    """Test nested module paths like icehunk::repository:trace."""
    ic.set_logs_filter("icehunk::repository:trace")
    err_output = capfd.readouterr().err
    assert "WARNING" in err_output and "icehunk" in err_output

    ic.set_logs_filter("icecunk::store::manager:debug")
    err_output = capfd.readouterr().err
    assert "WARNING" in err_output and "icecunk" in err_output
    ic.set_logs_filter(None)


def test_short_strings_not_flagged(capfd: CaptureFixture[str]) -> None:
    """Test that very short strings don't get flagged as misspellings."""
    ic.set_logs_filter("ice:debug")
    assert "WARNING" not in capfd.readouterr().err

    ic.set_logs_filter("chunk:info")
    assert "WARNING" not in capfd.readouterr().err
    ic.set_logs_filter(None)


@pytest.fixture
def python_logs(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    caplog.set_level(1, logger="icechunk")
    ic.set_logs_destination("python")
    yield caplog
    ic.set_logs_destination("console")
    ic.set_logs_filter(None)
    _flush_python_logs()


def icechunk_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    _flush_python_logs()
    return [r for r in caplog.records if r.name.startswith("icechunk")]


def test_python_logging_receives_events(
    python_logs: pytest.LogCaptureFixture, any_spec_version: int | None
) -> None:
    ic.set_logs_filter("icechunk=debug")
    ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=any_spec_version,
    )
    records = [
        r
        for r in icechunk_records(python_logs)
        if r.getMessage() == "Creating Repository"
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.DEBUG
    assert records[0].name == "icechunk.repository.builder"
    assert records[0].pathname.endswith("builder.rs")
    assert records[0].lineno > 0


def test_console_destination_sends_nothing_to_python(
    python_logs: pytest.LogCaptureFixture, any_spec_version: int | None
) -> None:
    ic.set_logs_destination("console")
    ic.set_logs_filter("icechunk=debug")
    ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=any_spec_version,
    )
    assert icechunk_records(python_logs) == []


def test_python_logging_filter_reloads(
    python_logs: pytest.LogCaptureFixture, any_spec_version: int | None
) -> None:
    ic.set_logs_filter("icechunk=warn")
    ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=any_spec_version,
    )
    assert icechunk_records(python_logs) == []

    ic.set_logs_filter("icechunk=debug")
    ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=any_spec_version,
    )
    assert any(
        r.getMessage() == "Creating Repository" for r in icechunk_records(python_logs)
    )


@mock.patch.dict(os.environ, {"ICECHUNK_LOG": "icechunk=debug"}, clear=True)
def test_python_logging_filter_from_environment(
    python_logs: pytest.LogCaptureFixture, any_spec_version: int | None
) -> None:
    ic.set_logs_filter(None)
    ic.Repository.create(
        storage=ic.in_memory_storage(),
        spec_version=any_spec_version,
    )
    assert any(
        r.getMessage() == "Creating Repository" for r in icechunk_records(python_logs)
    )


def test_python_logging_trace_level_and_fields(
    python_logs: pytest.LogCaptureFixture,
) -> None:
    ic.set_logs_filter("icechunk=trace")
    repo = ic.Repository.create(storage=ic.in_memory_storage())
    session = repo.writable_session("main")
    array = zarr.create_array(
        store=session.store, name="a", shape=(4,), chunks=(2,), dtype="int8"
    )
    array[:] = 1
    session.commit("msg")

    records = icechunk_records(python_logs)
    trace = [r for r in records if r.levelno == 5]
    assert trace
    assert all(r.levelname == "TRACE" for r in trace)
    # The layer appends the other event fields as key=value.
    assert any(
        r.getMessage().startswith("Writing snapshot id=")
        and "size_bytes=" in r.getMessage()
        for r in records
    )


def test_python_logging_from_many_threads(
    python_logs: pytest.LogCaptureFixture,
) -> None:
    ic.set_logs_filter("icechunk=debug")
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(
            pool.map(
                lambda _: ic.Repository.create(storage=ic.in_memory_storage()),
                range(32),
            )
        )
    created = [
        r
        for r in icechunk_records(python_logs)
        if r.getMessage() == "Creating Repository"
    ]
    assert len(created) == 32


def test_python_destination_turns_console_off(
    capfd: CaptureFixture[str], python_logs: pytest.LogCaptureFixture
) -> None:
    ic.set_logs_filter("icechunk=debug")
    ic.Repository.create(storage=ic.in_memory_storage())
    assert any(
        r.getMessage() == "Creating Repository" for r in icechunk_records(python_logs)
    )
    assert "Creating Repository" not in capfd.readouterr().err


def test_destination_switch_keeps_the_filter(
    capfd: CaptureFixture[str], python_logs: pytest.LogCaptureFixture
) -> None:
    ic.set_logs_filter("icechunk=debug")
    ic.set_logs_destination("console")
    ic.Repository.create(storage=ic.in_memory_storage())
    assert "Creating Repository" in capfd.readouterr().err
    assert icechunk_records(python_logs) == []

    ic.set_logs_destination("python")
    ic.Repository.create(storage=ic.in_memory_storage())
    assert "Creating Repository" not in capfd.readouterr().err
    assert any(
        r.getMessage() == "Creating Repository" for r in icechunk_records(python_logs)
    )


def test_invalid_destination() -> None:
    with pytest.raises(ValueError, match="'console' or 'python'"):
        ic.set_logs_destination("stdout")  # type: ignore[arg-type]


PYTHON_LOG_SNIPPET = (
    "import logging, sys\n"
    "logging.basicConfig(stream=sys.stdout, level=1, format='PY %(name)s %(message)s')\n"
    "import icechunk as ic\n"
    "ic.Repository.create(storage=ic.in_memory_storage())\n"
)


def run_snippet(
    snippet: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    # A subprocess lets a hang fail the test by timeout. Pytest does not block.
    return subprocess.run(
        [sys.executable, "-c", snippet],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env={**os.environ, **(env or {})},
    )


def test_python_logging_from_environment_at_import() -> None:
    # The snippet does not flush. The atexit hook must deliver the records.
    result = run_snippet(
        PYTHON_LOG_SNIPPET,
        {"ICECHUNK_LOG": "icechunk=debug", "ICECHUNK_LOG_DESTINATION": "python"},
    )
    assert result.returncode == 0, result.stderr
    assert "PY icechunk.repository.builder Creating Repository" in result.stdout
    # The env var sends logs only to Python. The console stays off.
    assert "Creating Repository" not in result.stderr


FLUSH_IN_HANDLER_SNIPPET = (
    "import logging\n"
    "import icechunk as ic\n"
    "from icechunk._icechunk_python import _flush_python_logs\n"
    "class Flushing(logging.Handler):\n"
    "    def emit(self, record):\n"
    "        _flush_python_logs()\n"
    "logging.getLogger('icechunk').addHandler(Flushing())\n"
    "logging.getLogger('icechunk').setLevel(logging.DEBUG)\n"
    "ic.set_logs_filter('icechunk=debug')\n"
    "ic.set_logs_destination('python')\n"
    "ic.Repository.create(storage=ic.in_memory_storage())\n"
    "_flush_python_logs()\n"
    "print('done')\n"
)


def test_python_logging_flush_from_handler_does_not_hang() -> None:
    result = run_snippet(FLUSH_IN_HANDLER_SNIPPET)
    assert result.stdout == "done\n", result.stderr


class StallingHandler(logging.Handler):
    """Blocks on the first record until a thread sets `resume`, or for 60 s."""

    def __init__(self) -> None:
        super().__init__()
        self.stalled = threading.Event()
        self.resume = threading.Event()

    def emit(self, record: logging.LogRecord) -> None:
        if not self.stalled.is_set():
            self.stalled.set()
            self.resume.wait(timeout=60)


def trace_workload(n_repos: int) -> None:
    def one(_: int) -> None:
        repo = ic.Repository.create(storage=ic.in_memory_storage())
        session = repo.writable_session("main")
        array = zarr.create_array(
            store=session.store, name="a", shape=(200,), chunks=(2,), dtype="int64"
        )
        array[:] = 1
        session.commit("c")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(one, range(n_repos)))


def test_python_logging_drops_records_when_handler_stalls(
    python_logs: pytest.LogCaptureFixture,
) -> None:
    handler = StallingHandler()
    logging.getLogger("icechunk").addHandler(handler)
    try:
        ic.set_logs_filter("icechunk=trace")
        # Producers must not block while the handler stalls.
        trace_workload(n_repos=128)
        assert handler.stalled.is_set()
        handler.resume.set()
        records = icechunk_records(python_logs)
    finally:
        handler.resume.set()
        logging.getLogger("icechunk").removeHandler(handler)
    dropped = [r for r in records if r.getMessage().startswith("Dropped ")]
    assert dropped
    assert all(r.name == "icechunk" and r.levelno == logging.WARNING for r in dropped)


# The handler stalls in `handle` and does not hold the handler lock. A stalled `emit`
# holds the lock. Then stdlib `logging.shutdown` blocks at exit, also without Icechunk.
STALLED_HANDLER_SNIPPET = (
    "import logging, threading\n"
    "import icechunk as ic\n"
    "stalled = threading.Event()\n"
    "class Stalling(logging.Handler):\n"
    "    def handle(self, record):\n"
    "        stalled.set()\n"
    "        threading.Event().wait()\n"
    "logging.getLogger('icechunk').addHandler(Stalling())\n"
    "logging.getLogger('icechunk').setLevel(logging.DEBUG)\n"
    "ic.set_logs_filter('icechunk=debug')\n"
    "ic.set_logs_destination('python')\n"
    "ic.Repository.create(storage=ic.in_memory_storage())\n"
    "stalled.wait()\n"
)


def test_python_logging_exit_does_not_wait_for_stalled_handler() -> None:
    result = run_snippet(STALLED_HANDLER_SNIPPET)
    assert result.returncode == 0, result.stderr
    assert "did not reach Python logging" in result.stderr


@pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX signals")
def test_python_logging_flush_is_interruptible() -> None:
    result = run_snippet(
        STALLED_HANDLER_SNIPPET
        + "import os, signal\n"
        + "from icechunk._icechunk_python import _flush_python_logs\n"
        + "threading.Timer(0.5, os.kill, (os.getpid(), signal.SIGINT)).start()\n"
        + "try:\n"
        + "    _flush_python_logs()\n"
        + "except KeyboardInterrupt:\n"
        + "    print('interrupted', flush=True)\n"
        + "os._exit(0)\n"
    )
    assert result.stdout == "interrupted\n", result.stderr


# The child gets the parent's queue but not its forwarder thread. Icechunk calls hang
# in a forked child, because Tokio does not survive fork. Thus the child only flushes.
FORK_SNIPPET = (
    "import logging, os, sys\n"
    "import icechunk as ic\n"
    "from icechunk._icechunk_python import _flush_python_logs\n"
    "logging.basicConfig(level=logging.DEBUG)\n"
    "ic.set_logs_filter('icechunk=debug')\n"
    "ic.set_logs_destination('python')\n"
    "ic.Repository.create(storage=ic.in_memory_storage())\n"
    "_flush_python_logs()\n"
    "pid = os.fork()\n"
    "if pid == 0:\n"
    "    _flush_python_logs()\n"
    "    sys.exit(0)\n"
    "_, status = os.waitpid(pid, 0)\n"
    "print('status', status, flush=True)\n"
)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
def test_python_logging_flush_in_forked_child() -> None:
    result = run_snippet(FORK_SNIPPET)
    assert result.returncode == 0, result.stderr
    assert result.stdout == "status 0\n", result.stderr


# The handler stalls on the first record until the workload ends. Thus the queue fills.
QUEUE_SIZE_SNIPPET = (
    "import logging, threading\n"
    "import icechunk as ic\n"
    "from icechunk._icechunk_python import _flush_python_logs\n"
    "stalled, resume = threading.Event(), threading.Event()\n"
    "messages = []\n"
    "class Stalling(logging.Handler):\n"
    "    def emit(self, record):\n"
    "        if not stalled.is_set():\n"
    "            stalled.set()\n"
    "            resume.wait(timeout=60)\n"
    "        messages.append(record.getMessage())\n"
    "logging.getLogger('icechunk').addHandler(Stalling())\n"
    "logging.getLogger('icechunk').setLevel(logging.DEBUG)\n"
    "ic.set_logs_filter('icechunk=debug')\n"
    "ic.set_logs_destination('python')\n"
    "for _ in range(5):\n"
    "    ic.Repository.create(storage=ic.in_memory_storage())\n"
    "resume.set()\n"
    "_flush_python_logs()\n"
    "print(len(messages), sum(m.startswith('Dropped ') for m in messages))\n"
)


def test_python_logging_queue_size_from_environment() -> None:
    small = run_snippet(QUEUE_SIZE_SNIPPET, {"ICECHUNK_PYTHON_LOG_QUEUE_SIZE": "3"})
    assert small.returncode == 0, small.stderr
    delivered, dropped_warnings = map(int, small.stdout.split())
    assert dropped_warnings == 1
    # The bound counts the record in the handler, the queue, and the drop warning.
    assert delivered <= 1 + 3 + 1

    default = run_snippet(QUEUE_SIZE_SNIPPET)
    assert default.returncode == 0, default.stderr
    delivered_default, dropped_default = map(int, default.stdout.split())
    assert dropped_default == 0
    assert delivered_default > delivered


@pytest.mark.parametrize("value", ["abc", "0", "-5"])
def test_python_logging_invalid_queue_size(value: str) -> None:
    result = run_snippet(QUEUE_SIZE_SNIPPET, {"ICECHUNK_PYTHON_LOG_QUEUE_SIZE": value})
    assert result.returncode == 0, result.stderr
    assert "ICECHUNK_PYTHON_LOG_QUEUE_SIZE" in result.stderr
    assert result.stdout.split()[1] == "0"


def test_invalid_destination_from_environment() -> None:
    result = run_snippet(
        PYTHON_LOG_SNIPPET,
        {"ICECHUNK_LOG": "icechunk=debug", "ICECHUNK_LOG_DESTINATION": "stdout"},
    )
    assert result.returncode == 0, result.stderr
    assert "ICECHUNK_LOG_DESTINATION" in result.stderr
    # The console stays the destination.
    assert "Creating Repository" in result.stderr
    assert "PY icechunk" not in result.stdout


@mock.patch.dict(os.environ)
def test_python_logging_needs_opt_in_at_import() -> None:
    os.environ.pop("ICECHUNK_LOG_DESTINATION", None)
    result = run_snippet(PYTHON_LOG_SNIPPET, {"ICECHUNK_LOG": "icechunk=debug"})
    assert result.returncode == 0, result.stderr
    assert "PY icechunk" not in result.stdout
