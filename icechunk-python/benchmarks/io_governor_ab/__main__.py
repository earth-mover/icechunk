"""CLI entry point: python -m benchmarks.io_governor_ab <command> (run from icechunk-python/)."""

from __future__ import annotations

import argparse
import datetime
import sys
import tempfile
from pathlib import Path

from benchmarks.io_governor_ab.scenarios import PROFILES


def _default_output() -> str:
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"io-governor-ab-{stamp}.json"


def _default_local_dir() -> str:
    return str(Path(tempfile.gettempdir()) / "icechunk-io-governor-ab")


def _add_arraylake_options(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--org",
        default=None,
        help="Arraylake org holding the benchmark repos "
        "(required for Arraylake-backed operations)",
    )
    p.add_argument(
        "--repo-prefix",
        default="ic-gov-bench",
        help="benchmark repos are named {prefix}-{dataset}",
    )
    p.add_argument(
        "--token",
        default=None,
        help="Arraylake API token (default: $ARRAYLAKE_TOKEN or stored login)",
    )


def _add_run_options(p: argparse.ArgumentParser) -> None:
    _add_arraylake_options(p)
    p.add_argument(
        "--where",
        default="local",
        help="comma-separated backends: local,s3,gcs (default: local)",
    )
    p.add_argument(
        "--scenarios",
        default=None,
        help="comma-separated substrings to filter scenario names",
    )
    p.add_argument(
        "--arms",
        default=None,
        type=lambda s: [a.strip() for a in s.split(",")],
        help="comma-separated arm names to run (default: all applicable)",
    )
    p.add_argument("--rounds", type=int, default=5, help="measured rounds per arm")
    p.add_argument(
        "--warmups", type=int, default=1, help="discarded warmup rounds per arm"
    )
    p.add_argument(
        "--threads", type=int, default=8, help="parallel driver streams per round"
    )
    p.add_argument(
        "--zarr-concurrency",
        type=int,
        default=64,
        help="zarr async.concurrency (per operation; in-flight ≈ threads × this)",
    )
    p.add_argument(
        "--tolerance",
        type=float,
        default=0.05,
        help="gate fails when median ratio exceeds 1+tolerance (default 0.05)",
    )
    p.add_argument(
        "--gate-bandwidth",
        action="store_true",
        help="apply the gate to the bandwidth arms too (default: warn-only)",
    )
    p.add_argument(
        "--num-repos", type=int, default=2, help="repos in multi-repo scenarios"
    )
    p.add_argument(
        "--quick",
        action="store_true",
        help="scale datasets down ~8x for smoke runs",
    )
    p.add_argument(
        "--local-dir",
        default=_default_local_dir(),
        help="directory for local-FS datasets",
    )
    p.add_argument("--output", default=_default_output(), help="JSON artifact path")
    p.add_argument(
        "--bandwidth-opt",
        action="append",
        metavar="KEY=VALUE",
        help="override a bandwidth governor config field, e.g. "
        "read.target_bandwidth=25Gbps, memory_budget=4GB, "
        "read.request_latency_us=30000 (repeatable)",
    )
    p.add_argument(
        "--compat-permits",
        type=int,
        default=256,
        help="permits for the shared compat governor arm (default 256)",
    )
    p.add_argument(
        "--arms-config",
        default=None,
        help="TOML file defining extra arms (see arms.example.toml); never gated",
    )
    p.add_argument(
        "--force-setup",
        action="store_true",
        help="rewrite benchmark datasets even if they look up to date",
    )
    p.add_argument(
        "--gcs-repo",
        default="earthmover-demos/era5-perf-gcp",
        help="GCS-hosted Arraylake repo for the gcs-read-external scenario "
        "(needs --where gcs)",
    )
    p.add_argument(
        "--s3-repo",
        default=None,
        help="existing Arraylake repo for an s3-read-external realistic-read "
        "scenario (optional)",
    )
    p.add_argument(
        "--external-bytes",
        default="1GB",
        help="bytes to read per round in external-read scenarios (÷8 with --quick)",
    )
    p.add_argument(
        "--aggressor-bandwidth",
        default="4Gbps",
        help="contend scenarios: the governed neighbor's read bandwidth cap "
        "(also the live-knob target)",
    )
    p.add_argument(
        "--aggressor-threads",
        type=int,
        default=4,
        help="contend scenarios: reader threads driving the noisy neighbor",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="benchmarks.io_governor_ab", description=sys.modules[__package__].__doc__
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="run benchmark scenarios")
    _add_run_options(run_p)

    knob_p = sub.add_parser(
        "knob",
        help="interactive live-knob demo: continuous governed reads, "
        "keys adjust bandwidth/memory while a live display shows the effect",
    )
    _add_arraylake_options(knob_p)
    knob_p.add_argument(
        "--where", default="s3", choices=["local", "s3"], help="backend (default: s3)"
    )
    knob_p.add_argument(
        "--regime",
        default="medium",
        choices=[p.name for p in PROFILES],
        help="chunk-size regime; selects the benchmark dataset (default: medium)",
    )
    knob_p.add_argument(
        "--threads", type=int, default=32, help="continuous reader threads"
    )
    knob_p.add_argument(
        "--zarr-concurrency", type=int, default=64, help="zarr async.concurrency"
    )
    knob_p.add_argument(
        "--local-dir", default=_default_local_dir(), help="directory for local datasets"
    )
    knob_p.add_argument(
        "--bandwidth-opt",
        action="append",
        metavar="KEY=VALUE",
        help="initial governor config overrides, same syntax as `run`",
    )

    setup_p = sub.add_parser("setup", help="create + populate the S3 benchmark repos")
    _add_arraylake_options(setup_p)
    setup_p.add_argument(
        "--bucket-nickname",
        default=None,
        help="org bucket to create repos on (default: the org's default bucket)",
    )
    setup_p.add_argument(
        "--num-repos", type=int, default=2, help="repos for multi-repo scenarios"
    )
    setup_p.add_argument("--threads", type=int, default=8, help="dataset write streams")
    setup_p.add_argument(
        "--quick", action="store_true", help="write ~8x smaller datasets"
    )
    setup_p.add_argument(
        "--force-setup", action="store_true", help="rewrite existing datasets"
    )

    teardown_p = sub.add_parser("teardown", help="delete the S3 benchmark repos")
    _add_arraylake_options(teardown_p)
    teardown_p.add_argument("--yes", action="store_true", help="really delete")

    report_p = sub.add_parser("report", help="re-render a results JSON artifact")
    report_p.add_argument("json_file")

    sub.add_parser("selftest", help="run the harness self-checks")

    args = parser.parse_args(argv)

    if args.command in ("run", "knob"):
        from benchmarks.io_governor_ab import runner

        backends: dict[str, runner.Backend] = {
            "local": runner.LocalBackend(Path(args.local_dir)),
            "s3": runner.ArraylakeBackend(
                org=args.org, repo_prefix=args.repo_prefix, token=args.token
            ),
        }
        if args.command == "knob":
            from benchmarks.io_governor_ab import knobdemo

            return knobdemo.run(args, backends)
        return runner.run(args, backends)
    if args.command == "setup":
        from benchmarks.io_governor_ab import runner

        return runner.setup(args)
    if args.command == "teardown":
        from benchmarks.io_governor_ab import runner

        return runner.teardown(args)
    if args.command == "report":
        from benchmarks.io_governor_ab import report

        report.render_json(args.json_file, out=sys.stdout)
        return 0
    if args.command == "selftest":
        from benchmarks.io_governor_ab import impls, knobdemo, stats, workloads

        stats.selftest()
        impls.selftest()
        workloads.selftest()
        knobdemo.selftest()
        print("selftest ok")
        return 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    sys.exit(main())
