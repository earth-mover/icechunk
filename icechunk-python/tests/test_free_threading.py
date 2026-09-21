import subprocess
import sys
import sysconfig

import pytest

# CPython re-enables the GIL for any extension in the import graph that has not
# declared free-threaded support, which serializes every thread.
PROBE = """
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
"""


@pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="needs a free-threaded interpreter",
)
def test_import_keeps_gil_disabled() -> None:
    # The interpreter fixes the GIL state while it imports extensions, so the
    # probe needs a process that has not imported icechunk yet.
    probe = subprocess.run(
        [sys.executable, "-c", PROBE], capture_output=True, text=True, check=False
    )
    assert probe.returncode == 0, probe.stderr
    enabled, *blame = probe.stdout.split()
    assert enabled == "False", (
        f"importing icechunk re-enabled the GIL: {', '.join(blame) or 'unknown module'}"
    )
