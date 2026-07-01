# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "icechunk_v1",  # icechunk==1.1.21
#   "zarr",
#   "numpy",
# ]
# ///
"""Standalone smoke test that third-wheel actually works in isolation.

The ephemeral environment declares *only* the renamed `icechunk_v1` (sourced
from icechunk==1.1.21). There is deliberately no plain `icechunk` to fall back
on, so if third-wheel's download/rename/install is broken this fails loudly
instead of silently importing some ambient version.

Run it with::

    uv run --with third-wheel third-wheel run tests/data_generation/third_wheel_smoke.py

third-wheel reads the `# icechunk==1.1.21` annotation, downloads that wheel,
renames the top-level package to `icechunk_v1`, and hands the script to `uv run`.
"""

from __future__ import annotations

import tempfile

import numpy as np
from numpy.testing import assert_array_equal

import zarr


def main() -> None:
    # 1. The renamed single version must import...
    import icechunk_v1 as ic

    print(f"icechunk_v1.__version__ = {ic.__version__}")
    assert ic.__version__.startswith("1."), (
        f"expected a 1.x wheel renamed to icechunk_v1, got {ic.__version__}"
    )

    # 2. ...and there must be NO default `icechunk` to fall back to.
    try:
        import icechunk  # noqa: F401

        raise AssertionError(
            "a plain `icechunk` is importable; the environment is not isolated, "
            "so this would not actually prove third-wheel installed icechunk_v1"
        )
    except ImportError:
        print("confirmed: no fallback `icechunk` in the environment")

    # 3. The renamed wheel must actually FUNCTION, not just import.
    with tempfile.TemporaryDirectory() as path:
        repo = ic.Repository.create(storage=ic.local_filesystem_storage(path))
        session = repo.writable_session("main")
        root = zarr.group(store=session.store, overwrite=True)
        arr = root.create_array(
            "data", shape=(4,), chunks=(2,), dtype="int32", fill_value=0
        )
        arr[:] = np.arange(4)
        snap = session.commit("smoke")

        read = repo.readonly_session(branch="main")
        got = zarr.open_group(store=read.store, mode="r")["data"][:]
        assert_array_equal(got, np.arange(4))
        assert any(s.id == snap for s in repo.ancestry(branch="main"))

    print("third-wheel smoke test PASSED")


if __name__ == "__main__":
    main()
