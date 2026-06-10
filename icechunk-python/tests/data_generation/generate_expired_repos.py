"""Generate `can_read_old` fixtures that have gone through expiration + GC.

The existing ``test_can_read_old`` fixtures pin on-disk read compatibility for
repos that were *never expired*. None of them exercise the `pruned_ancestor_tx_logs`
work (design-doc 016): the additive `SnapshotInfo` field a re-parented boundary
snapshot carries so that ``diff`` / ``inspect`` / ``rebase`` stay correct after
expiration collapses an ancestry.

This script produces a matrix of repos written by different *library versions*
in different *spec formats*, each driven through the **same DAG**: two rounds of
``expire_snapshots`` + ``garbage_collect`` with surviving branches, a
tag-protected old snapshot, and doomed branches whose transaction logs become
unreferenced. It then read-validates every v1 fixture with the genuine 1.x
library, so regenerating fails loudly on any backwards/forwards compat
regression.

Running it
----------
Multiple icechunk versions must coexist in one interpreter. ``third-wheel``
renames the released wheels (``icechunk_v1`` = 1.1.21, ``icechunk_v2`` = 2.0.5)
so they sit next to the working-copy ``icechunk``::

    just gen-expired-fixtures

which is equivalent to::

    cd icechunk-python
    uv run --with third-wheel third-wheel sync \
        --rename "icechunk==1.1.21=icechunk_v1" \
        --rename "icechunk==2.0.5=icechunk_v2"
    uv run python tests/data_generation/generate_expired_repos.py

The fixture matrix (5 repos under ``tests/data/``):

==== ============================================================ ======= ====
 #   dir                                                          lib     spec
==== ============================================================ ======= ====
 1   expire-repo-v1-by-1.1.21                                     1.1.21  v1
 2   expire-repo-v1-by-2.0.5                                      2.0.5   v1
 3   expire-repo-v2-by-2.0.5                                      2.0.5   v2
 4   expire-repo-v1-by-2.0.5-with-ancestor-tx-log-tracking        wc      v1
 5   expire-repo-v2-by-2.0.5-with-ancestor-tx-log-tracking        wc      v2
==== ============================================================ ======= ====

Only #5 (a v2 repo expired by a library that populates ``pruned_ancestor_tx_logs``)
ends up with surviving pruned-ancestor logs and a complete post-expiration diff.
#3 is the "bug state": a v2 repo expired by a pre-fix binary, whose expired-ancestor
logs are GC-deleted. The current working copy must read all five and degrade
gracefully where the data is gone.

The read assertions live in ``tests/test_can_read_old.py``; this script only
generates and cross-validates. It prints the observed post-expiration ancestries
and object counts so the expected values in the read tests can be confirmed.
"""

from __future__ import annotations

import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import zarr

# Commit messages. Kept in sync, by hand, with the expected-history constants in
# tests/test_can_read_old.py (do_read_expired_repo).
MSG_ROOT = "create structure"
MSG_A = "write a"
MSG_B = "write b"  # tag protect-b points here
MSG_C = "write c"  # branch feature forks here
MSG_D = "write d"  # branch doomed1 forks here
MSG_E = "write e"
MSG_G = "write g"  # post-t1 survivor tip on main
MSG_H = "write h"  # branch doomed2 forks here
MSG_I = "write i"
MSG_J = "write j"  # post-t2 survivor tip on main; churns row 0
MSG_FEAT_PRE = "feature pre-t1"
MSG_FEAT_S1 = "feature post-t1"
MSG_FEAT_S2 = "feature post-t2"
MSG_DOOMED1 = "doomed1 work"
MSG_DOOMED2 = "doomed2 work"

# data array: (NROWS, 4) float32, chunk (1, 4) => one materialized chunk per row.
# small array: (4,) int8, chunk (1,) => inline (< inline_chunk_threshold_bytes).
NROWS = 12


def commit_root(repo: Any) -> str:
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, overwrite=True)
    group1 = root.create_group("group1")
    data = group1.create_array(
        "data",
        shape=(NROWS, 4),
        chunks=(1, 4),
        dtype="float32",
        fill_value=float("nan"),
    )
    small = group1.create_array(
        "small",
        shape=(4,),
        chunks=(1,),
        dtype="int8",
        fill_value=0,
    )
    data[:] = 0.0
    small[:] = 0
    return session.commit(MSG_ROOT)


def churn(repo: Any, branch: str, row: int, value: float, msg: str) -> str:
    """Rewrite one materialized chunk (row ``row`` of group1/data) and commit."""
    session = repo.writable_session(branch)
    data = zarr.open_array(session.store, path="group1/data", mode="a")
    data[row, :] = value
    return session.commit(msg)


def commit_marker(repo: Any, branch: str, name: str, msg: str) -> str:
    """Add a marker group on ``branch`` and commit (used by side branches)."""
    session = repo.writable_session(branch)
    zarr.open_group(store=session.store).create_group(name)
    return session.commit(msg)


def build_expired_repo(ic: Any, path: str, spec_version: int | None) -> None:
    """Build the shared DAG and run two rounds of expiration + GC.

    Main line, oldest -> newest. The two ``|`` are the expiration boundaries:
    ``t1`` sits between ``e`` and ``g``, ``t2`` between ``i`` and ``j``. The first
    node past each boundary (``g``, then ``j``) is the survivor tip that gets
    re-parented over the snapshots expired just before it. Forks hang below the
    node they branch from::

                                              t1              t2
                                               |               |
        init -> root -> a -> b -> c -> d -> e  |  g -> h -> i  |  j  (main tip)
                             |    |    |               |
                             |    |    |               +---- doomed2  : branch off h, +doomed2_work  -> DELETED round 2
                             |    |    +---- doomed1  : branch off d, +doomed1_work  -> DELETED round 1
                             |    +---- feature  : branch off c, +feat_pre/feat_s1/feat_s2  (survives)
                             +---- protect-b: TAG on b  (keeps b reachable past expiry)

    Round 1 expires everything up to and including ``e`` (boundary ``g`` is
    re-parented over them); ``doomed1`` is deleted; ``b`` survives via the tag
    but leaves main's path. Round 2 expires ``g``/``h``/``i`` (boundary ``j``),
    deletes ``doomed2``, and -- on a tracking library -- makes ``j`` accumulate
    ``g``'s round-1 pruned list, the key correctness property of doc 016.
    """
    shutil.rmtree(path, ignore_errors=True)

    config = ic.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 12
    create_kwargs: dict[str, Any] = {}
    if spec_version is not None:
        create_kwargs["spec_version"] = spec_version
    repo = ic.Repository.create(
        storage=ic.local_filesystem_storage(path),
        config=config,
        **create_kwargs,
    )

    # ----- Round 1: pre-threshold work -----
    commit_root(repo)  # root
    churn(repo, "main", 1, 1, MSG_A)  # a
    snap_b = churn(repo, "main", 2, 2, MSG_B)  # b
    repo.create_tag("protect-b", snap_b)
    snap_c = churn(repo, "main", 3, 3, MSG_C)  # c
    repo.create_branch("feature", snap_c)
    snap_d = churn(repo, "main", 4, 4, MSG_D)  # d
    repo.create_branch("doomed1", snap_d)
    churn(repo, "main", 5, 5, MSG_E)  # e
    commit_marker(repo, "feature", "feat_pre", MSG_FEAT_PRE)
    commit_marker(repo, "doomed1", "doomed1_work", MSG_DOOMED1)

    # Bracket t1 so prior commits land strictly before it and survivor tips
    # strictly after, clear of the created_at (ms) vs flushed_at (us) race.
    time.sleep(0.05)
    t1 = datetime.now(UTC)
    time.sleep(0.05)

    churn(repo, "main", 6, 6, MSG_G)  # g (survivor)
    commit_marker(repo, "feature", "feat_s1", MSG_FEAT_S1)  # feature survivor

    repo.expire_snapshots(t1, delete_expired_branches=True)
    repo.garbage_collect(t1)

    # ----- Round 2: pre-threshold work -----
    snap_h = churn(repo, "main", 7, 7, MSG_H)  # h
    repo.create_branch("doomed2", snap_h)
    churn(repo, "main", 8, 8, MSG_I)  # i
    commit_marker(repo, "doomed2", "doomed2_work", MSG_DOOMED2)

    time.sleep(0.05)
    t2 = datetime.now(UTC)
    time.sleep(0.05)

    churn(repo, "main", 0, 99, MSG_J)  # j (survivor tip, churns row 0)
    commit_marker(repo, "feature", "feat_s2", MSG_FEAT_S2)  # feature survivor

    repo.expire_snapshots(t2, delete_expired_branches=True)
    repo.garbage_collect(t2)


def read_state(ic: Any, path: str) -> dict[str, Any]:
    repo = ic.Repository.open(storage=ic.local_filesystem_storage(path))
    branches = set(repo.list_branches())
    tags = set(repo.list_tags())

    def messages(**kw: Any) -> list[str]:
        return [s.message for s in repo.ancestry(**kw)]

    session = repo.readonly_session(branch="main")
    root = zarr.open_group(store=session.store, mode="r")
    data = root["group1/data"][:].tolist()
    small = root["group1/small"][:].tolist()

    return {
        "branches": branches,
        "tags": tags,
        "main": messages(branch="main"),
        "feature": messages(branch="feature") if "feature" in branches else None,
        "protect_b": messages(tag="protect-b") if "protect-b" in tags else None,
        "data": data,
        "small": small,
    }


def cross_validate(writer_ic: Any, reader_ic: Any, path: str) -> None:
    """Open a v1 fixture with both the writer library and genuine 1.x, and
    assert they see identical history and data. Raises on any mismatch so
    regenerating fails loudly on a compat regression."""
    writer = read_state(writer_ic, path)
    reader = read_state(reader_ic, path)

    for key in ("branches", "tags", "main", "feature", "protect_b", "data", "small"):
        if writer[key] != reader[key]:
            raise AssertionError(
                f"cross-read mismatch on {key!r} for {path}:\n"
                f"  writer={writer[key]}\n  reader={reader[key]}"
            )

    assert "doomed1" not in reader["branches"], reader["branches"]
    assert "doomed2" not in reader["branches"], reader["branches"]

    # A diff across the expiration boundary must not crash (degraded is fine for v1).
    repo = reader_ic.Repository.open(storage=reader_ic.local_filesystem_storage(path))
    ancestry = list(repo.ancestry(branch="main"))
    repo.diff(from_snapshot_id=ancestry[-1].id, to_snapshot_id=ancestry[0].id)


def try_import(name: str) -> Any | None:
    try:
        return __import__(name)
    except ImportError:
        return None


def describe(ic_wc: Any, dirname: str, path: str) -> None:
    """Print the observed post-expiration state, read by the working copy."""
    repo = ic_wc.Repository.open(storage=ic_wc.local_filesystem_storage(path))
    main_tip = repo.lookup_branch("main")
    state = read_state(ic_wc, path)
    tx = repo.inspect_transaction_log(main_tip)
    composite = tx.get("synthetic_composite")
    n_tx = sum(1 for _ in (Path(path) / "transactions").iterdir())
    n_snap = sum(1 for _ in (Path(path) / "snapshots").iterdir())

    print(f"\n=== {dirname} ===")
    print(f"  spec_version       : {repo.spec_version}")
    print(f"  branches           : {sorted(state['branches'])}")
    print(f"  tags               : {sorted(state['tags'])}")
    print(f"  main history        : {state['main']}")
    print(f"  feature history     : {state['feature']}")
    print(f"  protect-b history   : {state['protect_b']}")
    print(f"  transaction logs    : {n_tx} files")
    print(f"  snapshots           : {n_snap} files")
    if composite is None:
        print("  synthetic_composite : None")
    else:
        print(
            "  synthetic_composite : "
            f"merged={composite['merged_pruned_ancestor_tx_logs']} "
            f"missing={composite['missing_tx_logs']}"
        )


def main() -> None:
    ic_wc = __import__("icechunk")
    icechunk_v1 = try_import("icechunk_v1")
    icechunk_v2 = try_import("icechunk_v2")

    data_dir = Path(__file__).resolve().parent.parent / "data"

    # (dirname, writer module, spec_version, is_v1). v1 fixtures are cross-read
    # with genuine 1.x; only the v2-by-working-copy fixture tracks tx logs.
    fixtures = [
        ("expire-repo-v1-by-1.1.21", icechunk_v1, None, True),
        ("expire-repo-v1-by-2.0.5", icechunk_v2, 1, True),
        ("expire-repo-v2-by-2.0.5", icechunk_v2, 2, False),
        ("expire-repo-v1-by-2.0.5-with-ancestor-tx-log-tracking", ic_wc, 1, True),
        ("expire-repo-v2-by-2.0.5-with-ancestor-tx-log-tracking", ic_wc, 2, False),
    ]

    for dirname, module, spec_version, is_v1 in fixtures:
        path = str(data_dir / dirname)
        if module is None:
            raise RuntimeError(
                f"Cannot build {dirname}: writer module not importable. Run via "
                "`just gen-expired-fixtures` so third-wheel installs the renamed wheels."
            )

        print(f"Building {dirname} (spec_version={spec_version}) ...")
        build_expired_repo(module, path, spec_version)
        describe(ic_wc, dirname, path)

        if is_v1:
            if icechunk_v1 is None:
                raise RuntimeError(
                    f"{dirname} is a v1 fixture but icechunk_v1 (1.1.21) is not "
                    "importable; run via `just gen-expired-fixtures` so third-wheel "
                    "installs the renamed wheels."
                )
            cross_validate(module, icechunk_v1, path)
            print("  cross-validated against genuine icechunk 1.1.21 OK")

    print("\nAll fixtures generated.")


if __name__ == "__main__":
    main()
