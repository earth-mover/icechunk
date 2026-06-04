"""Subprocess entrypoint used by `test_ingest_crash_subprocess`.

Runs `icechunk.from_zarr` against `src_dir` into a repo at `repo_dir`.
On each progress callback prints a single line to stdout so the parent
test can block on `readline()` and SIGKILL us once we're known to be
mid-ingest.
"""

from __future__ import annotations

import sys

import icechunk
from icechunk.ingest import IngestStats


def main() -> None:
    src_dir, repo_dir = sys.argv[1], sys.argv[2]
    src = icechunk.local_filesystem_storage(src_dir)
    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(repo_dir))

    # One line per progress callback. The parent test matches on the
    # "PROGRESS keys=" prefix to know when durable state exists on disk.
    def on_progress(stats: IngestStats) -> None:
        print(f"PROGRESS keys={stats.keys}", flush=True)

    icechunk.from_zarr(
        src,
        repo,
        on_progress=on_progress,
        checkpoint_every=2,
    )


if __name__ == "__main__":
    main()
