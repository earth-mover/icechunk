"""Scenario orchestration: backends, interleaved rounds, gating."""

from __future__ import annotations

import resource
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol

import zarr
from benchmarks.io_governor_ab import arraylake_bridge as bridge
from benchmarks.io_governor_ab import impls, report, stats, workloads
from benchmarks.io_governor_ab.impls import Arm
from benchmarks.io_governor_ab.scenarios import (
    Scenario,
    all_dataset_keys,
    build_scenarios,
    dataset_keys,
)


def _raise_fd_limit(needed: int) -> None:
    """The local backend opens one file per in-flight chunk request, easily
    past the usual 1024 soft limit; raise it to the hard limit up front."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < hard:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
        soft = hard
    if soft < needed:
        print(
            f"warning: open-file limit {soft} is below the ~{needed} this "
            "concurrency can need; lower --threads/--zarr-concurrency or "
            "raise the hard limit (ulimit -Hn)",
            file=sys.stderr,
        )


class Backend(Protocol):
    def prepare(self, scenario: Scenario, *, force: bool, threads: int) -> None: ...

    def storages(self, mod: ModuleType, scenario: Scenario) -> list[Any]: ...


class LocalBackend:
    def __init__(self, root: Path):
        self.root = root

    def prepare(self, scenario: Scenario, *, force: bool, threads: int) -> None:
        mod = impls.load_module(impls.BRANCH)
        for key in dataset_keys(scenario):
            path = self.root / key
            path.mkdir(parents=True, exist_ok=True)
            storage = mod.local_filesystem_storage(str(path))
            if workloads.ensure_dataset(
                mod, storage, scenario.profiles, threads=threads, force=force
            ):
                print(f"  wrote local dataset {path}", flush=True)

    def storages(self, mod: ModuleType, scenario: Scenario) -> list[Any]:
        return [
            mod.local_filesystem_storage(str(self.root / key))
            for key in dataset_keys(scenario)
        ]


class ArraylakeBackend:
    """Repos named {repo_prefix}-{key} in one Arraylake org, S3-backed."""

    def __init__(self, *, org: str, repo_prefix: str, token: str | None):
        self.org = org
        self.repo_prefix = repo_prefix
        self._token = token
        self._client: Any | None = None
        self._cache: dict[str, bridge.ResolvedRepo] = {}

    @property
    def client(self) -> Any:
        if self.org is None:
            raise SystemExit("--org is required for Arraylake-backed operations")
        if self._client is None:
            self._client = bridge.make_client(self._token)
        return self._client

    def repo_name(self, key: str) -> str:
        return f"{self.repo_prefix}-{key}"

    def resolve(self, key: str) -> bridge.ResolvedRepo:
        cached = self._cache.get(key)
        if cached is not None and not cached.expires_soon:
            return cached
        name = self.repo_name(key)
        try:
            resolved = bridge.resolve_repo(self.client, self.org, name)
        except Exception as e:
            raise SystemExit(
                f"could not resolve {self.org}/{name}: {e}\n"
                "did you run `just io-governor-ab setup`?"
            ) from e
        self._cache[key] = resolved
        return resolved

    def prepare(self, scenario: Scenario, *, force: bool, threads: int) -> None:
        mod = impls.load_module(impls.BRANCH)
        for key in dataset_keys(scenario):
            resolved = self.resolve(key)
            storage = bridge.storage_for(mod, resolved)
            if workloads.ensure_dataset(
                mod, storage, scenario.profiles, threads=threads, force=force
            ):
                print(f"  wrote dataset {resolved.full_name}", flush=True)

    def storages(self, mod: ModuleType, scenario: Scenario) -> list[Any]:
        return [
            bridge.storage_for(mod, self.resolve(key)) for key in dataset_keys(scenario)
        ]


class ExternalRepoBackend:
    """Read-only scenarios against an arbitrary existing Arraylake repo."""

    def __init__(self, *, full_name: str, token: str | None):
        self.org, self.name = full_name.split("/", 1)
        self._token = token
        self._client: Any | None = None
        self._resolved: bridge.ResolvedRepo | None = None

    def resolve(self) -> bridge.ResolvedRepo:
        if self._resolved is None or self._resolved.expires_soon:
            if self._client is None:
                self._client = bridge.make_client(self._token)
            self._resolved = bridge.resolve_repo(self._client, self.org, self.name)
        return self._resolved

    def prepare(self, scenario: Scenario, *, force: bool, threads: int) -> None:
        resolved = self.resolve()
        print(
            f"  external repo {resolved.full_name}: {resolved.platform} "
            f"bucket {resolved.bucket}",
            flush=True,
        )

    def storages(self, mod: ModuleType, scenario: Scenario) -> list[Any]:
        return [bridge.storage_for(mod, self.resolve())]


@dataclass
class ScenarioResult:
    scenario: Scenario
    arms: list[Arm]
    rounds: list[workloads.RoundResult]

    def times(self, arm: str) -> list[float]:
        return [r.seconds for r in self.rounds if r.arm == arm and not r.warmup]

    def bytes_moved(self, arm: str) -> int:
        for r in self.rounds:
            if r.arm == arm and not r.warmup:
                return r.read_bytes + r.write_bytes
        return 0


def _parse_kv(pairs: list[str] | None) -> dict[str, str]:
    out = {}
    for pair in pairs or []:
        key, sep, value = pair.partition("=")
        if not sep:
            raise SystemExit(f"expected key=value, got {pair!r}")
        if not key.strip() or not value.strip():
            raise SystemExit(
                f"empty {'key' if not key.strip() else 'value'} in {pair!r} — "
                "an unset shell variable, perhaps?"
            )
        out[key.strip()] = value.strip()
    return out


def select_arms(args: Any) -> dict[str, Arm]:
    arms = impls.builtin_arms(
        bandwidth_options=_parse_kv(args.bandwidth_opt),
        compat_permits=args.compat_permits,
    )
    if args.arms_config:
        custom = impls.arms_from_config(args.arms_config)
        if clash := set(custom) & set(arms):
            raise SystemExit(f"custom arm names clash with built-ins: {sorted(clash)}")
        arms |= custom
    return arms


def scenario_arms(
    scenario: Scenario, arms: dict[str, Arm], only: list[str] | None
) -> list[Arm]:
    builtins = (
        impls.SINGLE_REPO_ARMS if scenario.num_repos == 1 else impls.MULTI_REPO_ARMS
    )
    custom = [n for n in arms if n not in impls.BUILTIN_ARM_NAMES]
    names = builtins + custom
    if only:
        names = [n for n in names if n in only]
    return [arms[n] for n in names]


def scenario_gates(
    result: ScenarioResult, *, tolerance: float, gate_bandwidth: bool
) -> list[stats.GateOutcome]:
    present = {a.name for a in result.arms}
    outcomes = []
    pairs = [(pair, True) for pair in impls.GATE_PAIRS] + [
        (pair, gate_bandwidth) for pair in impls.BANDWIDTH_GATE_PAIRS
    ]
    for (arm, reference), gated in pairs:
        if arm in present and reference in present:
            outcomes.append(
                stats.evaluate_gate(
                    scenario=result.scenario.name,
                    arm=arm,
                    reference=reference,
                    arm_times=result.times(arm),
                    ref_times=result.times(reference),
                    tolerance=tolerance,
                    gated=gated,
                )
            )
    return outcomes


def _run_contend(scenario: Scenario, backend: Backend, args: Any) -> ScenarioResult:
    """Noisy-neighbor blocks; --arms does not apply (modes are fixed)."""
    print(
        f"\n=== {scenario.name} "
        "(contend modes: solo, agg-ungoverned, agg-governed, knob-before/after)"
    )
    assert scenario.aggressor_profile is not None
    victim_scenario = replace(scenario, load="read")
    aggressor_scenario = replace(
        scenario,
        load="read",
        dataset=scenario.aggressor_dataset,
        profiles=(scenario.aggressor_profile,),
    )
    backend.prepare(victim_scenario, force=args.force_setup, threads=args.threads)
    backend.prepare(aggressor_scenario, force=args.force_setup, threads=args.threads)
    arms, rounds = workloads.run_contend_scenario(
        scenario=scenario,
        victim_storage=lambda mod: backend.storages(mod, victim_scenario)[0],
        aggressor_storage=lambda mod: backend.storages(mod, aggressor_scenario)[0],
        threads=args.threads,
        rounds=args.rounds,
        warmups=args.warmups,
        aggressor_threads=args.aggressor_threads,
        aggressor_bandwidth=args.aggressor_bandwidth,
    )
    return ScenarioResult(scenario, arms, rounds)


def run(args: Any, backends: dict[str, Backend]) -> int:
    where = [w.strip() for w in args.where.split(",") if w.strip()]
    scenarios = build_scenarios(
        where=where,
        num_repos=args.num_repos,
        quick=args.quick,
        gcs_repo=args.gcs_repo,
        s3_repo=args.s3_repo,
    )
    if args.scenarios:
        patterns = [p.strip() for p in args.scenarios.split(",")]
        scenarios = [s for s in scenarios if any(p in s.name for p in patterns)]
    scenarios = [s for s in scenarios if s.external or s.backend in backends]
    if not scenarios:
        raise SystemExit("no scenarios selected")

    external_backends: dict[str, ExternalRepoBackend] = {}

    def backend_for(scenario: Scenario) -> Backend:
        if scenario.external:
            return external_backends.setdefault(
                scenario.dataset,
                ExternalRepoBackend(full_name=scenario.dataset, token=args.token),
            )
        return backends[scenario.backend]

    external_bytes = impls.parse_bytes(args.external_bytes)
    if args.quick:
        external_bytes //= 8

    _raise_fd_limit(args.threads * args.zarr_concurrency + 256)
    zarr.config.set({"async.concurrency": args.zarr_concurrency})
    arms = select_arms(args)

    results: list[ScenarioResult] = []
    gates: list[stats.GateOutcome] = []
    for scenario in scenarios:
        if scenario.load == "contend":
            results.append(_run_contend(scenario, backend_for(scenario), args))
            continue
        selected = scenario_arms(scenario, arms, args.arms)
        if not selected:
            continue
        print(f"\n=== {scenario.name} (arms: {', '.join(a.name for a in selected)})")
        backend = backend_for(scenario)
        backend.prepare(scenario, force=args.force_setup, threads=args.threads)
        rounds = []
        for index in range(args.warmups + args.rounds):
            warmup = index < args.warmups
            for arm in selected:
                if scenario.external:
                    round_result = workloads.run_external_round(
                        arm=arm,
                        storage=backend.storages(arm.module, scenario)[0],
                        threads=args.threads,
                        byte_budget=external_bytes,
                        index=index,
                        warmup=warmup,
                    )
                else:
                    round_result = workloads.run_round(
                        arm=arm,
                        scenario=scenario,
                        storages=backend.storages(arm.module, scenario),
                        threads=args.threads,
                        index=index,
                        warmup=warmup,
                    )
                rounds.append(round_result)
                moved = round_result.read_bytes + round_result.write_bytes
                mbps = moved / round_result.seconds / 1e6
                tag = " (warmup)" if warmup else ""
                print(
                    f"  {arm.name:>18} round {index}: "
                    f"{round_result.seconds:7.2f}s {mbps:8.1f} MB/s{tag}",
                    flush=True,
                )
        result = ScenarioResult(scenario, selected, rounds)
        results.append(result)
        gates.extend(
            scenario_gates(
                result, tolerance=args.tolerance, gate_bandwidth=args.gate_bandwidth
            )
        )

    report.render(results, gates, out=sys.stdout)
    report.write_json(args.output, args=args, results=results, gates=gates)
    print(f"\nresults written to {args.output}")
    failures = [g for g in gates if g.gated and not g.passed]
    return 1 if failures else 0


def _validate_bucket(bucket: Any, *, org: str) -> None:
    """Fail before creating any repo on an unusable bucket."""
    name = f"{org} bucket {bucket.nickname!r} ({bucket.name})"
    auth = bucket.auth_config.method if bucket.auth_config else None
    if auth == "anonymous":
        raise SystemExit(
            f"{name} is anonymous/read-only; pick a writable one with --bucket-nickname"
        )
    if bucket.platform not in bridge.S3_PLATFORMS:
        raise SystemExit(
            f"{name} lives on platform {bucket.platform!r}; "
            "write benchmarks need an S3 or S3-compatible bucket "
            "(pick one with --bucket-nickname)"
        )
    # informational only: the harness can't know where the client runs, and
    # A/B ratios are fair wherever the bucket lives — but absolute numbers
    # (and the NIC-saturation check) assume the runner is near the bucket
    region = (bucket.extra_config or {}).get("region_name")
    print(
        f"{name}: platform {bucket.platform}, region {region} — make sure the "
        "machine running the benchmarks is where you think it is relative to "
        "this bucket; latency between them is part of every measurement",
        flush=True,
    )


def setup(args: Any) -> int:
    backend = ArraylakeBackend(
        org=args.org, repo_prefix=args.repo_prefix, token=args.token
    )
    bucket = bridge.get_bucket_config(backend.client, args.org, args.bucket_nickname)
    _validate_bucket(bucket, org=args.org)
    existing = bridge.list_repo_names(backend.client, args.org)
    mod = impls.load_module(impls.BRANCH)
    for key, profiles in all_dataset_keys(
        num_repos=args.num_repos, quick=args.quick
    ).items():
        name = backend.repo_name(key)
        if name not in existing:
            print(f"creating {args.org}/{name}", flush=True)
            bridge.create_repo(backend.client, args.org, name, bucket.nickname)
        resolved = backend.resolve(key)
        nbytes = sum(p.total_bytes for p in profiles)
        print(f"populating {resolved.full_name} ({nbytes / 1e6:.0f} MB)", flush=True)
        if not workloads.ensure_dataset(
            mod,
            bridge.storage_for(mod, resolved),
            profiles,
            threads=args.threads,
            force=args.force_setup,
        ):
            print("  already up to date", flush=True)
    return 0


def teardown(args: Any) -> int:
    if not args.yes:
        raise SystemExit("teardown deletes repos; pass --yes to confirm")
    backend = ArraylakeBackend(
        org=args.org, repo_prefix=args.repo_prefix, token=args.token
    )
    prefix = f"{args.repo_prefix}-"
    names = sorted(
        n
        for n in bridge.list_repo_names(backend.client, args.org)
        if n.startswith(prefix)
    )
    if not names:
        print(f"no {prefix}* repos in {args.org}")
        return 0
    for name in names:
        print(f"deleting {args.org}/{name}", flush=True)
        bridge.delete_repo(backend.client, args.org, name)
    return 0
