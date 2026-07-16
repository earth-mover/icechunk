"""Multi-process commit stress test for the native filesystem `Storage`.

The native backend implements a compare-and-swap on the branch tip so that
concurrent committers cannot silently overwrite each other (the failure mode
of the old object_store `LocalFileSystem` backend). Threads share a process
and cannot exercise cross-process exclusivity, so this test races commits from
several real OS processes and asserts no commit is ever lost.

Each process owns a disjoint array, so races are pure branch-tip contention
(no data conflicts): every attempt either commits or raises a conflict that is
retried. After the dust settles the number of commits in the linear ancestry
must equal the number of successful commits reported, and every process's data
must be present.
"""

import multiprocessing as mp
import random
import time
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest

import icechunk as ic
import zarr

N_PROCESSES = 6
N_ROUNDS = 4
MAX_ATTEMPTS = 1000


def race_commits_manual_retry(
    path: str, proc_id: int, rounds: int, barrier: Any
) -> tuple[int, int]:
    """Commit `rounds` times, retrying on conflict without an auto-rebase.

    Returns (successes, conflicts_observed). This directly exercises the
    branch-tip compare-and-swap: a stale parent surfaces as `ConflictError`,
    never a silent overwrite.
    """
    ic.set_logs_filter("error")
    barrier.wait()
    successes = 0
    conflicts = 0
    for r in range(rounds):
        for _ in range(MAX_ATTEMPTS):
            repo = ic.Repository.open(ic.local_filesystem_storage(path))
            session = repo.writable_session("main")
            group = zarr.open_group(session.store, mode="a")
            cast("zarr.Array[Any]", group[f"arr_{proc_id}"])[r] = proc_id * 1000 + r
            try:
                session.commit(f"p{proc_id}-r{r}")
                successes += 1
                break
            except ic.ConflictError:
                conflicts += 1
                time.sleep(random.uniform(0, 0.005))
        else:
            raise AssertionError(f"process {proc_id} round {r} exceeded retry budget")
    return successes, conflicts


def race_commits_rebase(
    path: str, proc_id: int, rounds: int, barrier: Any
) -> tuple[int, int]:
    """Commit `rounds` times, letting icechunk's rebase machinery absorb races."""
    ic.set_logs_filter("error")
    barrier.wait()
    successes = 0
    for r in range(rounds):
        repo = ic.Repository.open(ic.local_filesystem_storage(path))
        session = repo.writable_session("main")
        group = zarr.open_group(session.store, mode="a")
        cast("zarr.Array[Any]", group[f"arr_{proc_id}"])[r] = proc_id * 1000 + r
        session.commit(
            f"p{proc_id}-r{r}",
            rebase_with=ic.BasicConflictSolver(),
            rebase_tries=MAX_ATTEMPTS,
        )
        successes += 1
    return successes, 0


@pytest.mark.parametrize(
    "worker", [race_commits_manual_retry, race_commits_rebase], ids=["manual", "rebase"]
)
def test_multiprocess_commits_never_lost(
    worker: Any,
    tmp_path: Path,
) -> None:
    ctx = mp.get_context("spawn")
    path = str(tmp_path)

    ic.set_logs_filter("error")
    repo = ic.Repository.create(storage=ic.local_filesystem_storage(path))
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    for p in range(N_PROCESSES):
        root.create_array(
            f"arr_{p}", shape=(N_ROUNDS,), chunks=(1,), dtype="i8", fill_value=-1
        )
    session.commit("init arrays")

    manager = ctx.Manager()
    barrier = manager.Barrier(N_PROCESSES)
    with ctx.Pool(N_PROCESSES) as pool:
        pending = [
            pool.apply_async(worker, (path, p, N_ROUNDS, barrier))
            for p in range(N_PROCESSES)
        ]
        results = [handle.get(timeout=180) for handle in pending]

    total_successes = sum(successes for successes, _ in results)
    total_conflicts = sum(conflicts for _, conflicts in results)
    assert total_successes == N_PROCESSES * N_ROUNDS

    # The manual-retry variant reports every conflict it observes; a run with
    # zero conflicts means the processes never actually overlapped, so the
    # compare-and-swap was never exercised and the test proved nothing.
    if worker is race_commits_manual_retry:
        assert total_conflicts > 0

    reopened = ic.Repository.open(ic.local_filesystem_storage(path))
    ancestry = list(reopened.ancestry(branch="main"))
    seed = {"init arrays", "Repository initialized"}
    commit_messages = [snap.message for snap in ancestry if snap.message not in seed]

    # No commit silently lost: one snapshot per reported success, no duplicates.
    assert len(commit_messages) == total_successes
    assert len(set(commit_messages)) == total_successes

    # Linear history: each snapshot has a single parent.
    parents = [snap.parent_id for snap in ancestry[:-1]]
    assert all(parent is not None for parent in parents)
    assert len(parents) == len(set(parents))

    read = zarr.open_group(reopened.readonly_session("main").store, mode="r")
    for p in range(N_PROCESSES):
        values = np.asarray(cast("zarr.Array[Any]", read[f"arr_{p}"])[:]).tolist()
        assert values == [p * 1000 + r for r in range(N_ROUNDS)]

    # Diagnostic only; observed contention varies with scheduling and is not
    # asserted so the test stays deterministic under CI load.
    print(f"multiprocess race: {total_successes} commits, {total_conflicts} conflicts")
