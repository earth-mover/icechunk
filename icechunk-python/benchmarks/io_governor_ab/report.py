"""Result rendering (rich table when available, plain text otherwise) + JSON."""

from __future__ import annotations

import dataclasses
import datetime
import json
import platform
import sys
from typing import IO, TYPE_CHECKING, Any

from benchmarks.io_governor_ab import impls, stats

if TYPE_CHECKING:
    from benchmarks.io_governor_ab.runner import ScenarioResult


def _rows(result: ScenarioResult) -> list[dict[str, Any]]:
    # ratio reference: the baseline arm, or the first arm with data (e.g. the
    # contend scenario's "solo" mode)
    named = [a.name for a in result.arms if result.times(a.name)]
    reference = "baseline" if "baseline" in named else (named[0] if named else None)
    ref_median = stats.summarize(result.times(reference)).median if reference else None
    rows = []
    for arm in result.arms:
        times = result.times(arm.name)
        if not times:
            continue
        summary = stats.summarize(times)
        moved = result.bytes_moved(arm.name)
        rows.append(
            {
                "arm": arm.name,
                "median": summary.median,
                "q1": summary.q1,
                "q3": summary.q3,
                "min": summary.min,
                "max": summary.max,
                "mean": summary.mean,
                "n": summary.n,
                "mbps": moved / summary.median / 1e6 if summary.median else 0.0,
                "vs_baseline": (
                    summary.median / ref_median
                    if ref_median and arm.name != reference
                    else None
                ),
            }
        )
    return rows


def _fmt_ratio(value: float | None) -> str:
    return f"{value:.3f}×" if value is not None else "—"


def render(
    results: list[ScenarioResult],
    gates: list[stats.GateOutcome],
    *,
    out: IO[str],
) -> None:
    tables = [(result.scenario.name, _rows(result)) for result in results]
    gate_rows = [
        {
            "scenario": g.scenario,
            "comparison": f"{g.arm} vs {g.reference}",
            "ratio": g.ratio,
            "tolerance": g.tolerance,
            "gated": g.gated,
            "passed": g.passed,
        }
        for g in gates
    ]
    try:
        _render_rich(tables, gate_rows, out)
    except ImportError:
        _render_plain(tables, gate_rows, out)


def _render_rich(
    tables: list[tuple[str, list[dict[str, Any]]]],
    gate_rows: list[dict[str, Any]],
    out: IO[str],
) -> None:
    from rich.console import Console
    from rich.table import Table

    console = Console(file=out)
    for name, rows in tables:
        table = Table(title=name)
        for column in ("arm", "median", "q1–q3", "min", "MB/s", "vs ref", "n"):
            table.add_column(column, justify="right")
        for r in rows:
            table.add_row(
                r["arm"],
                f"{r['median']:.2f}s",
                f"{r['q1']:.2f}–{r['q3']:.2f}",
                f"{r['min']:.2f}",
                f"{r['mbps']:.1f}",
                _fmt_ratio(r["vs_baseline"]),
                str(r["n"]),
            )
        console.print(table)

    if gate_rows:
        table = Table(title="gates")
        for column in ("scenario", "comparison", "ratio", "tolerance", "mode", "verdict"):
            table.add_column(column)
        for g in gate_rows:
            verdict = "PASS" if g["passed"] else "FAIL"
            style = "green" if g["passed"] else ("red" if g["gated"] else "yellow")
            table.add_row(
                g["scenario"],
                g["comparison"],
                f"{g['ratio']:.3f}×",
                f"≤{1 + g['tolerance']:.2f}×",
                "gated" if g["gated"] else "warn-only",
                f"[{style}]{verdict}[/{style}]",
            )
        console.print(table)


def _render_plain(
    tables: list[tuple[str, list[dict[str, Any]]]],
    gate_rows: list[dict[str, Any]],
    out: IO[str],
) -> None:
    for name, rows in tables:
        print(f"\n## {name}", file=out)
        header = (
            f"{'arm':>18} {'median':>9} {'q1–q3':>15} {'min':>8} "
            f"{'MB/s':>9} {'vs ref':>9} {'n':>3}"
        )
        print(header, file=out)
        for r in rows:
            print(
                f"{r['arm']:>18} {r['median']:8.2f}s "
                f"{r['q1']:6.2f}–{r['q3']:6.2f}s {r['min']:7.2f}s "
                f"{r['mbps']:9.1f} {_fmt_ratio(r['vs_baseline']):>9} {r['n']:>3}",
                file=out,
            )
    if gate_rows:
        print("\n## gates", file=out)
        for g in gate_rows:
            verdict = "PASS" if g["passed"] else "FAIL"
            mode = "gated" if g["gated"] else "warn-only"
            print(
                f"{verdict:>4} [{mode}] {g['scenario']}: {g['comparison']} "
                f"= {g['ratio']:.3f}× (tolerance ≤{1 + g['tolerance']:.2f}×)",
                file=out,
            )


def _versions() -> dict[str, str]:
    versions = {"python": sys.version.split()[0]}
    for name in (impls.BRANCH, impls.BASELINE, "zarr"):
        module = sys.modules.get(name)
        if module is not None:
            versions[name] = getattr(module, "__version__", "?")
            # branch build and baseline wheel may report the same version
            # string; the install path tells them apart
            versions[f"{name}_path"] = getattr(module, "__file__", "?")
    return versions


def write_json(
    path: str,
    *,
    args: Any,
    results: list[ScenarioResult],
    gates: list[stats.GateOutcome],
) -> None:
    payload = {
        "meta": {
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "hostname": platform.node(),
            "versions": _versions(),
            "argv": sys.argv,
        },
        "config": {
            k: v
            for k, v in vars(args).items()
            if isinstance(v, str | int | float | bool | list | type(None))
        },
        "scenarios": [
            {
                "name": result.scenario.name,
                "backend": result.scenario.backend,
                "load": result.scenario.load,
                "num_repos": result.scenario.num_repos,
                "profiles": [
                    {
                        "name": p.name,
                        "chunk_bytes": p.chunk_bytes,
                        "nchunks": p.nchunks,
                    }
                    for p in result.scenario.profiles
                ],
                "arms": {a.name: a.describe() for a in result.arms},
                "summary": _rows(result),
                "rounds": [dataclasses.asdict(r) for r in result.rounds],
            }
            for result in results
        ],
        "gates": [
            {**dataclasses.asdict(g), "ratio": g.ratio, "passed": g.passed} for g in gates
        ],
    }
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)


def render_json(path: str, *, out: IO[str]) -> None:
    """Re-render the tables of a previously written JSON artifact."""
    with open(path) as f:
        data = json.load(f)
    tables = [(s["name"], s["summary"]) for s in data["scenarios"]]
    gate_rows = [
        {
            "scenario": g["scenario"],
            "comparison": f"{g['arm']} vs {g['reference']}",
            "ratio": g["ratio"],
            "tolerance": g["tolerance"],
            "gated": g["gated"],
            "passed": g["passed"],
        }
        for g in data["gates"]
    ]
    try:
        _render_rich(tables, gate_rows, out)
    except ImportError:
        _render_plain(tables, gate_rows, out)
