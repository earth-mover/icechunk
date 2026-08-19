"""Load drivers: multi-threaded fan-out over zarr.

A round opens fresh repositories (cold caches, cold governor) and drives
`threads` parallel streams, each reading or writing a chunk-aligned slab of
an array. Total in-flight chunk requests ≈ threads × zarr async.concurrency
(zarr's limit is per operation, not global), which is what pushes past the
governor and exercises its queueing.

Writes go to a throwaway branch, deleted (untimed) after the round; the
orphaned chunks are reclaimed when the repo/directory is deleted.
"""

from __future__ import annotations

import dataclasses
import importlib
import math
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from types import ModuleType
from typing import Any

import numpy as np

import zarr
from benchmarks.io_governor_ab import impls
from benchmarks.io_governor_ab.scenarios import DTYPE, ChunkProfile, Scenario

# contend scenario: how the aggressor's governor is configured. The truthful
# cold floor keeps the cap tight from the first request (the default 7.5 MB/s
# floor prices a cold 12 MB part ~10x too cheap and leaks the cap while the
# bandwidth estimate warms); the knob mode starts effectively uncapped and is
# cut to --aggressor-bandwidth live, mid-scenario.
AGGRESSOR_MIN_CONNECTION_BANDWIDTH = "90MB"
KNOB_START_BANDWIDTH = "25Gbps"
AGGRESSOR_SETTLE_SECONDS = 3.0


@dataclass(frozen=True)
class StreamSpec:
    kind: str  # "read" | "write"
    repo: int
    profile: ChunkProfile
    slab: slice


@dataclass(frozen=True)
class StreamResult:
    kind: str
    repo: int
    array: str
    seconds: float
    nbytes: int


@dataclass(frozen=True)
class RoundResult:
    arm: str
    index: int
    warmup: bool
    seconds: float
    read_bytes: int
    write_bytes: int
    streams: list[StreamResult]
    governor_metrics: list[str] | None


def _aligned_slabs(total: int, chunk: int, parts: int) -> list[slice]:
    """Chunk-aligned contiguous slabs covering [0, total); at most `parts`
    of them (fewer when there aren't enough chunks)."""
    nchunks = math.ceil(total / chunk)
    per = math.ceil(nchunks / parts)
    slabs = []
    start = 0
    while start < nchunks:
        stop = min(start + per, nchunks)
        slabs.append(slice(start * chunk, min(stop * chunk, total)))
        start = stop
    return slabs


def _slabs(profile: ChunkProfile, parts: int) -> list[slice]:
    return _aligned_slabs(profile.shape[0], profile.chunk_elems, parts)


def plan_streams(scenario: Scenario, threads: int) -> list[StreamSpec]:
    kinds = {
        "read": ("read",),
        "write": ("write",),
        "readwrite": ("read", "write"),
    }[scenario.load]
    units = [
        (kind, repo, profile)
        for kind in kinds
        for repo in range(scenario.num_repos)
        for profile in scenario.profiles
    ]
    n_streams = max(threads, len(units))
    base, extra = divmod(n_streams, len(units))
    specs = []
    for i, (kind, repo, profile) in enumerate(units):
        parts = base + (1 if i < extra else 0)
        specs.extend(
            StreamSpec(kind, repo, profile, slab) for slab in _slabs(profile, parts)
        )
    return specs


def _write_slab(arr: Any, slab: slice) -> int:
    # 1.0 != fill_value so zarr can never elide the chunks as empty
    data = np.full(slab.stop - slab.start, 1.0, dtype=DTYPE)
    arr[slab] = data
    return data.nbytes


def _read_slab(arr: Any, slab: slice) -> int:
    return arr[slab].nbytes


def _create_write_arrays(root: Any, profiles: tuple[ChunkProfile, ...]) -> dict[str, Any]:
    return {
        p.name: root.create_array(
            f"w-{p.name}",
            shape=p.shape,
            chunks=p.chunks,
            dtype=DTYPE,
            compressors=None,
            filters=None,
        )
        for p in profiles
    }


def ensure_dataset(
    mod: ModuleType,
    storage: Any,
    profiles: tuple[ChunkProfile, ...],
    *,
    threads: int = 8,
    force: bool = False,
) -> bool:
    """Create the repo if needed and write one array per profile on main.

    Idempotent: skips when every array already exists with the right shape
    (so switching --quick on/off rewrites). Returns True when it wrote.
    """
    repo = mod.Repository.open_or_create(storage)
    if not force:
        try:
            root = zarr.open_group(store=repo.readonly_session("main").store, mode="r")
        except zarr.errors.GroupNotFoundError:
            pass  # brand-new repo
        else:
            if all(
                (arr := root.get(p.array_name)) is not None
                and tuple(arr.shape) == p.shape
                and tuple(arr.chunks) == p.chunks
                for p in profiles
            ):
                return False
    session = repo.writable_session("main")
    root = zarr.open_group(store=session.store, mode="a")
    jobs = []
    for p in profiles:
        arr = root.create_array(
            p.array_name,
            shape=p.shape,
            chunks=p.chunks,
            dtype=DTYPE,
            compressors=None,
            filters=None,
            overwrite=True,
        )
        jobs.extend((arr, slab) for slab in _slabs(p, threads))
    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(pool.map(lambda job: _write_slab(*job), jobs))
    session.commit("benchmark data")
    return True


def run_round(
    *,
    arm: Any,  # impls.Arm
    scenario: Scenario,
    storages: list[Any],
    threads: int,
    index: int,
    warmup: bool,
) -> RoundResult:
    mod = arm.module
    governors = arm.governors(scenario.num_repos)
    specs = plan_streams(scenario, threads)
    reads = scenario.load in ("read", "readwrite")
    writes = scenario.load in ("write", "readwrite")
    branch = f"bench-{arm.name}-{uuid.uuid4().hex[:8]}"

    # The timed region covers everything a user would wait for: repository
    # open (its bootstrap I/O is governed too), zarr opens, array creation,
    # the parallel streams, and commits.
    t0 = time.perf_counter()

    repos = []
    config_cache = arm.repo_config(mod, scenario.num_repos)
    for storage, governor in zip(storages, governors, strict=True):
        kwargs: dict[str, Any] = {}
        if governor is not None:
            kwargs["governor"] = governor
        repos.append(mod.Repository.open(storage, config=config_cache, **kwargs))

    read_arrays: list[dict[str, Any]] = []
    if reads:
        for repo in repos:
            root = zarr.open_group(store=repo.readonly_session("main").store, mode="r")
            read_arrays.append({p.name: root[p.array_name] for p in scenario.profiles})

    write_arrays: list[dict[str, Any]] = []
    write_sessions = []
    if writes:
        for repo in repos:
            repo.create_branch(branch, repo.lookup_branch("main"))
            session = repo.writable_session(branch)
            root = zarr.open_group(store=session.store, mode="a")
            write_arrays.append(_create_write_arrays(root, scenario.profiles))
            write_sessions.append(session)

    def run_stream(spec: StreamSpec) -> StreamResult:
        s0 = time.perf_counter()
        if spec.kind == "read":
            arr = read_arrays[spec.repo][spec.profile.name]
            nbytes = _read_slab(arr, spec.slab)
            name = spec.profile.array_name
        else:
            arr = write_arrays[spec.repo][spec.profile.name]
            nbytes = _write_slab(arr, spec.slab)
            name = f"w-{spec.profile.name}"
        return StreamResult(spec.kind, spec.repo, name, time.perf_counter() - s0, nbytes)

    with ThreadPoolExecutor(max_workers=len(specs)) as pool:
        streams = list(pool.map(run_stream, specs))
    for session in write_sessions:
        session.commit("bench write")

    seconds = time.perf_counter() - t0

    metrics = None
    if governors[0] is not None and hasattr(governors[0], "metrics"):
        unique = {id(g): g for g in governors}
        metrics = [repr(g.metrics()) for g in unique.values()]

    if writes:
        for repo in repos:
            repo.delete_branch(branch)

    return RoundResult(
        arm=arm.name,
        index=index,
        warmup=warmup,
        seconds=seconds,
        read_bytes=sum(s.nbytes for s in streams if s.kind == "read"),
        write_bytes=sum(s.nbytes for s in streams if s.kind == "write"),
        streams=streams,
        governor_metrics=metrics,
    )


def _external_arrays(root: Any) -> list[Any]:
    """All non-empty arrays in the hierarchy, largest first (deterministic)."""
    out: list[Any] = []

    def walk(group: Any) -> None:
        for _, node in sorted(group.members()):
            if isinstance(node, zarr.Group):
                walk(node)
            elif node.size > 0:
                out.append(node)

    walk(root)
    return sorted(out, key=lambda a: (-a.nbytes, a.path))


def plan_external_streams(
    arrays: list[Any], threads: int, byte_budget: int
) -> list[tuple[Any, slice]]:
    """Choose (array, leading-dim slab) streams reading ~byte_budget in total.

    Largest arrays first; each contributes a chunk-aligned prefix of its
    leading dimension sized to the remaining budget, split across threads.
    """
    chosen: list[tuple[Any, int]] = []  # (array, rows to read)
    remaining = byte_budget
    for arr in arrays:
        if remaining <= 0:
            break
        shape0 = arr.shape[0] if arr.shape else 0
        if shape0 == 0:
            continue
        chunk0 = arr.chunks[0]
        bytes_per_row = arr.nbytes // shape0
        rows = min(shape0, max(1, remaining // max(bytes_per_row, 1)))
        rows = (rows // chunk0) * chunk0 or min(chunk0, shape0)
        chosen.append((arr, rows))
        remaining -= rows * bytes_per_row
    if not chosen:
        raise SystemExit("external dataset has no readable arrays")
    n_streams = max(threads, len(chosen))
    base, extra = divmod(n_streams, len(chosen))
    streams = []
    for i, (arr, rows) in enumerate(chosen):
        parts = base + (1 if i < extra else 0)
        streams.extend((arr, slab) for slab in _aligned_slabs(rows, arr.chunks[0], parts))
    return streams


def run_external_round(
    *,
    arm: Any,  # impls.Arm
    storage: Any,
    threads: int,
    byte_budget: int,
    index: int,
    warmup: bool,
) -> RoundResult:
    """A read round against an external (not harness-written) repo: open,
    discover arrays, read a ~byte_budget chunk-aligned subset."""
    mod = arm.module
    (governor,) = arm.governors(1)

    t0 = time.perf_counter()
    kwargs: dict[str, Any] = {"governor": governor} if governor is not None else {}
    repo = mod.Repository.open(storage, **kwargs)
    root = zarr.open_group(store=repo.readonly_session("main").store, mode="r")
    streams_plan = plan_external_streams(_external_arrays(root), threads, byte_budget)

    def run_stream(spec: tuple[Any, slice]) -> StreamResult:
        arr, slab = spec
        s0 = time.perf_counter()
        nbytes = _read_slab(arr, slab)
        return StreamResult("read", 0, arr.path, time.perf_counter() - s0, nbytes)

    with ThreadPoolExecutor(max_workers=len(streams_plan)) as pool:
        streams = list(pool.map(run_stream, streams_plan))
    seconds = time.perf_counter() - t0

    metrics = None
    if governor is not None and hasattr(governor, "metrics"):
        metrics = [repr(governor.metrics())]

    return RoundResult(
        arm=arm.name,
        index=index,
        warmup=warmup,
        seconds=seconds,
        read_bytes=sum(s.nbytes for s in streams),
        write_bytes=0,
        streams=streams,
        governor_metrics=metrics,
    )


class Aggressor:
    """The noisy neighbor: continuously re-reads a dataset until stopped.

    Long-lived by design, unlike measured rounds: one repository open and one
    governor for its whole life (a real neighboring workload has a *warm*
    governor; rebuilding it per round resets the bandwidth estimate and leaks
    the cap), with the chunk cache disabled so every pass is real I/O.
    """

    def __init__(
        self,
        mod: ModuleType,
        storage: Any,
        profile: ChunkProfile,
        *,
        governor: Any,
        threads: int,
    ):
        self._mod = mod
        self._storage = storage
        self._profile = profile
        self.governor = governor
        self._threads = threads
        self._stop = threading.Event()
        self._workers: list[threading.Thread] = []
        self._failure: Exception | None = None

    def start(self) -> None:
        kwargs: dict[str, Any] = {}
        if self.governor is not None:
            kwargs["governor"] = self.governor
        config = self._mod.RepositoryConfig(
            caching=self._mod.CachingConfig(num_bytes_chunks=0)
        )
        repo = self._mod.Repository.open(self._storage, config=config, **kwargs)
        root = zarr.open_group(store=repo.readonly_session("main").store, mode="r")
        array = root[self._profile.array_name]
        slabs = _aligned_slabs(
            self._profile.shape[0], self._profile.chunk_elems, self._threads * 4
        )
        for i in range(self._threads):
            worker = threading.Thread(
                target=self._loop, args=(array, slabs[i :: self._threads]), daemon=True
            )
            worker.start()
            self._workers.append(worker)

    def _loop(self, array: Any, slabs: list[slice]) -> None:
        try:
            while not self._stop.is_set():
                for slab in slabs:
                    if self._stop.is_set():
                        return
                    _read_slab(array, slab)
        except Exception as e:  # e.g. expired credentials: fail loudly, not silently
            self._failure = e
            self._stop.set()

    def stop(self) -> None:
        self._stop.set()
        for worker in self._workers:
            worker.join()
        if self._failure is not None:
            raise RuntimeError("aggressor worker failed") from self._failure


def run_contend_scenario(
    *,
    scenario: Scenario,
    victim_storage: Callable[[ModuleType], Any],
    aggressor_storage: Callable[[ModuleType], Any],
    threads: int,
    rounds: int,
    warmups: int,
    aggressor_threads: int,
    aggressor_bandwidth: str,
) -> tuple[list[Any], list[RoundResult]]:
    """The noisy-neighbor demonstration, sequential blocks (not interleaved):

    solo            victim alone (reference)
    agg-ungoverned  neighbor on the default compat governor (today's world)
    agg-governed    neighbor capped at --aggressor-bandwidth
    knob-before/after  neighbor starts effectively uncapped; its governor's
                    read_bandwidth is cut live between the two blocks

    The victim always runs the unmodified baseline module: governing the
    noisy workload needs no changes to its neighbors.
    """
    branch = impls.load_module(impls.BRANCH)
    experimental = importlib.import_module(f"{impls.BRANCH}.experimental")
    victim_scenario = dataclasses.replace(scenario, load="read")
    assert scenario.aggressor_profile is not None
    cap = impls.parse_rate(aggressor_bandwidth)

    def make_governor(target_bandwidth: int) -> Any:
        return experimental.BandwidthGovernor(
            impls.bandwidth_config(
                experimental,
                {
                    "read.target_bandwidth": target_bandwidth,
                    "read.min_connection_bandwidth": (AGGRESSOR_MIN_CONNECTION_BANDWIDTH),
                },
            )
        )

    def make_aggressor(governor: Any) -> Aggressor:
        aggressor = Aggressor(
            branch,
            aggressor_storage(branch),
            scenario.aggressor_profile,
            governor=governor,
            threads=aggressor_threads,
        )
        aggressor.start()
        time.sleep(AGGRESSOR_SETTLE_SECONDS)
        return aggressor

    arms: list[Any] = []
    all_rounds: list[RoundResult] = []

    def victim_block(label: str, aggressor: Aggressor | None, block_warmups: int) -> None:
        arm = impls.Arm(label, impls.BASELINE, options={"contend_mode": label})
        arms.append(arm)
        for index in range(block_warmups + rounds):
            warmup = index < block_warmups
            result = run_round(
                arm=arm,
                scenario=victim_scenario,
                storages=[victim_storage(arm.module)],
                threads=threads,
                index=index,
                warmup=warmup,
            )
            if aggressor is not None and aggressor.governor is not None:
                result = dataclasses.replace(
                    result, governor_metrics=[repr(aggressor.governor.metrics())]
                )
            all_rounds.append(result)
            mbps = (result.read_bytes + result.write_bytes) / result.seconds / 1e6
            tag = " (warmup)" if warmup else ""
            print(
                f"  {label:>18} round {index}: {result.seconds:7.2f}s "
                f"{mbps:8.1f} MB/s{tag}",
                flush=True,
            )

    victim_block("solo", None, warmups)

    aggressor = make_aggressor(None)
    try:
        victim_block("agg-ungoverned", aggressor, warmups)
    finally:
        aggressor.stop()

    aggressor = make_aggressor(make_governor(cap))
    try:
        victim_block("agg-governed", aggressor, warmups)
    finally:
        aggressor.stop()

    governor = make_governor(impls.parse_rate(KNOB_START_BANDWIDTH))
    aggressor = make_aggressor(governor)
    try:
        victim_block("knob-before", aggressor, warmups)
        governor.read_bandwidth = cap
        print(
            f"  >>> live knob: aggressor read_bandwidth -> {aggressor_bandwidth}",
            flush=True,
        )
        victim_block("knob-after", aggressor, 0)
    finally:
        aggressor.stop()

    return arms, all_rounds


def selftest() -> None:
    from benchmarks.io_governor_ab.scenarios import MEDIUM, SMALL

    slabs = _slabs(SMALL, 8)
    assert len(slabs) == 8
    assert slabs[0].start == 0 and slabs[-1].stop == SMALL.shape[0]
    covered = sum(s.stop - s.start for s in slabs)
    assert covered == SMALL.shape[0]
    assert all((s.stop - s.start) % SMALL.chunk_elems == 0 for s in slabs[:-1])
    # more parts than chunks: capped at one slab per chunk
    tiny = dataclasses.replace(SMALL, nchunks=2)
    assert len(_slabs(tiny, 8)) == 2

    scenario = Scenario(
        name="t",
        backend="local",
        load="readwrite",
        profiles=(SMALL, MEDIUM),
        dataset="mixed",
        num_repos=2,
    )
    specs = plan_streams(scenario, threads=8)
    # 2 kinds × 2 repos × 2 profiles = 8 units, one stream each
    assert len(specs) == 8
    kinds = {(s.kind, s.repo, s.profile.name) for s in specs}
    assert len(kinds) == 8
    read_only = plan_streams(
        Scenario(
            name="t2",
            backend="local",
            load="read",
            profiles=(MEDIUM,),
            dataset="medium",
            num_repos=1,
        ),
        threads=8,
    )
    assert len(read_only) == 8
    assert all(s.kind == "read" for s in read_only)
