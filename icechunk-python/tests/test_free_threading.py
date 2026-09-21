import subprocess
import sys
import sysconfig
import textwrap

import pytest

pytestmark = pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="needs a free-threaded interpreter",
)


# The interpreter fixes the GIL state while it imports extensions, so every
# probe needs a process that has not imported icechunk yet.
def run_probe(source: str) -> str:
    probe = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert probe.returncode == 0, probe.stderr
    return probe.stdout.strip()


def test_extension_does_not_require_gil() -> None:
    # Loads the extension alone, so the result covers icechunk and not its
    # dependencies.
    result = run_probe("""
        import importlib.machinery
        import importlib.util
        import pathlib
        import sys

        spec = importlib.util.find_spec('icechunk')
        assert spec is not None and spec.submodule_search_locations is not None
        suffixes = tuple(importlib.machinery.EXTENSION_SUFFIXES)
        paths = [
            path
            for location in spec.submodule_search_locations
            for path in pathlib.Path(location).iterdir()
            if path.name.startswith('_icechunk_python') and path.name.endswith(suffixes)
        ]
        assert len(paths) == 1, paths

        extension = importlib.util.spec_from_file_location('_icechunk_python', paths[0])
        assert extension is not None and extension.loader is not None
        extension.loader.exec_module(importlib.util.module_from_spec(extension))
        print(sys._is_gil_enabled())
    """)
    assert result == "False", "the icechunk extension re-enabled the GIL"


def test_import_keeps_gil_disabled() -> None:
    # CPython re-enables the GIL for any extension in the import graph that has
    # not declared free-threaded support, which serializes every thread.
    result = run_probe("""
        import sys
        import warnings

        blame = []
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            import icechunk  # noqa: F401
            for warning in caught:
                message = str(warning.message)
                if 'global interpreter lock' in message:
                    blame.append(message.split(chr(39))[1])
        print(sys._is_gil_enabled(), *blame)
    """)
    enabled, *blame = result.split()
    assert enabled == "False", (
        f"importing icechunk re-enabled the GIL: {', '.join(blame) or 'unknown module'}"
    )
