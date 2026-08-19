"""Interactive demo of the bandwidth governor's runtime knobs.

Continuously reads one benchmark dataset through a `BandwidthGovernor`
while a live display shows achieved throughput next to the governor's own
view (target, effective, in-flight, queued). Keys adjust the knobs while
the workload runs:

    +   raise the read bandwidth target by 1 Gbps
    -   lower it by 1 Gbps
    b   type an absolute read bandwidth (e.g. 500MB/s, 2Gbps)
    m   type a memory budget (e.g. 2GB)
    q   quit

The point is to watch the physics: raising a knob admits queued work
immediately; lowering it never cancels in-flight requests, so achieved
throughput approaches the new target only as admitted work drains — slow
for big chunks. Enforcement accuracy tracks the config's request-latency
and connection-bandwidth constants, so a persistent gap between target and
achieved is real behavior (a mispriced cost model), not display lag.

Never writes to remote fleets: a missing or reshaped dataset is an error
pointing at `setup`, not something to repair mid-demo.
"""

from __future__ import annotations

import importlib
import os
import select
import sys
import termios
import threading
import time
import tty
from collections import deque
from typing import Any

from benchmarks.io_governor_ab import impls

# One slab is the unit of both byte accounting and zarr-level concurrency:
# each in-flight `arr[slab]` offers `slab chunks` concurrent requests and
# reports its bytes only on completion, so slabs are sized as a compromise
# between offered concurrency and display smoothness.
TARGET_SLAB_BYTES = 16 * 2**20

STEP_BANDWIDTH = 125_000_000  # the +/- keys step by 1 Gbps
MIN_BANDWIDTH = 1_000_000  # floor for the `-` key; typed values may go lower

KEY_HELP = "[+] +1 Gbps   [-] −1 Gbps   [b] set bandwidth   [m] set memory   [q] quit"


def slab_chunk_count(chunk_bytes: int, zarr_concurrency: int) -> int:
    """Chunks per slab: ~TARGET_SLAB_BYTES, at least 1, at most what one
    getitem can keep in flight."""
    by_bytes = max(1, TARGET_SLAB_BYTES // chunk_bytes)
    return max(1, min(by_bytes, zarr_concurrency))


class WindowRates:
    """Bytes/s over trailing windows, from (monotonic time, total) samples."""

    def __init__(self, now: float, horizon: float = 60.0):
        self._horizon = horizon
        self._start = now
        self._samples: deque[tuple[float, int]] = deque([(now, 0)])

    def add(self, now: float, total: int) -> None:
        self._samples.append((now, total))
        while now - self._samples[0][0] > self._horizon and len(self._samples) > 2:
            self._samples.popleft()

    def rate(self, window: float) -> float:
        now, total = self._samples[-1]
        cutoff = now - window
        # base = the latest sample at or before the cutoff (oldest if none)
        base = self._samples[0]
        for sample in self._samples:
            if sample[0] > cutoff:
                break
            base = sample
        dt = now - base[0]
        return (total - base[1]) / dt if dt > 0 else 0.0

    def lifetime(self) -> tuple[float, int]:
        now, total = self._samples[-1]
        return now - self._start, total


def fmt_rate(bytes_per_sec: float) -> str:
    gbps = bytes_per_sec * 8 / 1e9
    if gbps >= 100:
        return f"{gbps:.0f} Gbps"
    if gbps >= 10:
        return f"{gbps:.1f} Gbps"
    if gbps >= 1:
        return f"{gbps:.2f} Gbps"
    return f"{gbps:.3f} Gbps"


def fmt_bytes(n: float) -> str:
    if n >= 995e6:
        return f"{n / 1e9:.2f} GB"
    return f"{n / 1e6:.1f} MB"


def delta_percent(rate: float, target: float) -> float:
    """Signed achieved-vs-target deviation, in percent."""
    return (rate - target) / target * 100 if target > 0 else 0.0


def _reader(
    index: int,
    array: Any,
    slabs: list[slice],
    counters: list[int],
    stop: threading.Event,
    failures: list[Exception],
) -> None:
    from benchmarks.io_governor_ab.workloads import _read_slab

    try:
        while not stop.is_set():
            for slab in slabs:
                if stop.is_set():
                    return
                counters[index] += _read_slab(array, slab)
    except Exception as e:  # e.g. expired credentials: surface, don't spin
        failures.append(e)
        stop.set()


class _Keys:
    """Cbreak-mode key handling: immediate knob keys plus a typed-value mode."""

    def __init__(self, governor: Any, stop: threading.Event):
        self._governor = governor
        self._stop = stop
        self.mode: str | None = None  # None | "b" | "m"
        self.buffer = ""
        self.status = "governor live; adjust away"

    def prompt(self) -> str | None:
        if self.mode is None:
            return None
        what = "read bandwidth" if self.mode == "b" else "memory budget"
        return f"{what}: {self.buffer}▏  (Enter apply, Esc cancel)"

    def handle(self, ch: str) -> None:
        if self.mode is None:
            self._immediate(ch)
        else:
            self._typing(ch)

    def _set_bandwidth(self, value: int) -> None:
        self._governor.read_bandwidth = value
        self.status = f"read bandwidth → {fmt_rate(value)}"

    def _immediate(self, ch: str) -> None:
        if ch in "+=":
            self._set_bandwidth(self._governor.read_bandwidth + STEP_BANDWIDTH)
        elif ch == "-":
            self._set_bandwidth(
                max(self._governor.read_bandwidth - STEP_BANDWIDTH, MIN_BANDWIDTH)
            )
        elif ch in "bm":
            self.mode, self.buffer = ch, ""
        elif ch in "qQ\x03\x04":  # q, ctrl-c, ctrl-d
            self._stop.set()

    def _typing(self, ch: str) -> None:
        if ch in "\r\n":
            self._commit()
        elif ch == "\x1b":
            self.mode = None
        elif ch in "\x7f\x08":
            self.buffer = self.buffer[:-1]
        elif ch in "\x03\x04":
            self._stop.set()
        elif ch.isprintable():
            self.buffer += ch

    def _commit(self) -> None:
        mode, text = self.mode, self.buffer.strip()
        self.mode = None
        if not text:
            return
        try:
            if mode == "b":
                self._set_bandwidth(impls.parse_rate(text))
            else:
                value = impls.parse_bytes(text)
                self._governor.memory_budget = value
                self.status = f"memory budget → {fmt_bytes(value)}"
        except ValueError as e:
            self.status = f"?? {e}"


def _open_array(mod: Any, storage: Any, governor: Any, profile: Any) -> Any:
    import zarr

    config = mod.RepositoryConfig(caching=mod.CachingConfig(num_bytes_chunks=0))
    repo = mod.Repository.open(storage, config=config, governor=governor)
    root = zarr.open_group(store=repo.readonly_session("main").store, mode="r")
    try:
        array = root[profile.array_name]
    except KeyError:
        raise SystemExit(
            f"array {profile.array_name!r} not found — run `just io-governor-ab setup`"
        ) from None
    if tuple(array.shape) != profile.shape or tuple(array.chunks) != profile.chunks:
        raise SystemExit(
            f"dataset {profile.array_name!r} has shape {tuple(array.shape)}, "
            f"expected {profile.shape} — stale fleet, run `just io-governor-ab setup`"
        )
    return array


def _render(
    *,
    args: Any,
    profile: Any,
    governor: Any,
    windows: WindowRates,
    keys: _Keys,
    threads: int,
    offered: int,
) -> Any:
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    m = governor.metrics()
    elapsed, total = windows.lifetime()

    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold", justify="right")
    table.add_column()
    table.add_row(
        "workload",
        f"{profile.name}: {fmt_bytes(profile.chunk_bytes)} chunks × {profile.nchunks}"
        f" · {threads} threads · ≈{offered} offered in-flight",
    )
    achieved = Text()
    for label, window in (("5s", 5.0), ("30s", 30.0)):
        if achieved:
            achieved.append("   ")
        rate = windows.rate(window)
        delta = delta_percent(rate, m.read.target_bandwidth)
        achieved.append(f"{fmt_rate(rate)} ")
        achieved.append(
            f"({label} {delta:+.1f}%)",
            style="red" if abs(delta) >= 10 else "dim",
        )
    achieved.append(f"   {fmt_bytes(total)} in {elapsed:.0f}s", style="dim")
    table.add_row("achieved", achieved)

    target_cell = fmt_rate(m.read.target_bandwidth)
    # the effective bandwidth only earns a mention while an AIMD throttle
    # cut has it below the target
    if m.read.effective_bandwidth != m.read.target_bandwidth:
        target_cell += f"   effective {fmt_rate(m.read.effective_bandwidth)}"
    table.add_row("read target", target_cell)
    table.add_row(
        "read pool",
        f"{m.read.in_flight_requests} in flight "
        f"(cost {fmt_rate(m.read.in_flight_cost)}) · {m.read.queued_requests} queued · "
        f"conn estimate {fmt_rate(m.read.observed_connection_bandwidth)} · "
        f"{m.read.throttles_total} throttles",
    )
    table.add_row(
        "memory",
        f"{fmt_bytes(m.memory.reserved)} reserved of {fmt_bytes(m.memory.budget)} · "
        f"{m.memory.queued_fetches} fetches queued",
    )
    table.add_row("", "")
    table.add_row("status", keys.status)
    table.add_row("keys", Text(keys.prompt() or KEY_HELP, style="dim"))
    return Panel(
        table,
        title=f"bandwidth governor knobs — {args.where}:{profile.name}",
        title_align="left",
    )


def run(args: Any, backends: dict[str, Any]) -> int:
    if not sys.stdin.isatty():
        raise SystemExit("the knob demo is interactive; run it from a terminal")

    import zarr
    from rich.console import Console
    from rich.live import Live

    from benchmarks.io_governor_ab import runner, workloads
    from benchmarks.io_governor_ab.scenarios import PROFILES, Scenario

    profile = {p.name: p for p in PROFILES}[args.regime]
    scenario = Scenario(
        name=f"knob-{profile.name}",
        backend=args.where,
        load="read",
        profiles=(profile,),
        dataset=profile.name,
    )
    zarr.config.set({"async.concurrency": args.zarr_concurrency})

    backend = backends[args.where]
    mod = impls.load_module(impls.BRANCH)
    if args.where == "local":
        # local datasets are private and cheap; remote fleets are never written
        backend.prepare(scenario, force=False, threads=8)
    storage = backend.storages(mod, scenario)[0]

    experimental = importlib.import_module(f"{impls.BRANCH}.experimental")
    governor = experimental.BandwidthGovernor(
        impls.bandwidth_config(experimental, runner._parse_kv(args.bandwidth_opt))
    )
    array = _open_array(mod, storage, governor, profile)

    per_slab = slab_chunk_count(profile.chunk_bytes, args.zarr_concurrency)
    nslabs = max(1, profile.nchunks // per_slab)
    slabs = workloads._aligned_slabs(profile.shape[0], profile.chunk_elems, nslabs)
    threads = max(1, min(args.threads, len(slabs)))
    offered = threads * min(per_slab, args.zarr_concurrency)

    stop = threading.Event()
    failures: list[Exception] = []
    counters = [0] * threads
    for i in range(threads):
        threading.Thread(
            target=_reader,
            args=(i, array, slabs[i::threads], counters, stop, failures),
            daemon=True,
        ).start()

    keys = _Keys(governor, stop)
    windows = WindowRates(time.monotonic())
    console = Console()
    stdin_fd = sys.stdin.fileno()
    saved_termios = termios.tcgetattr(stdin_fd)
    tty.setcbreak(stdin_fd)
    try:
        with Live(console=console, auto_refresh=False) as live:
            while not stop.is_set():
                ready, _, _ = select.select([sys.stdin], [], [], 0.125)
                if ready:
                    for ch in os.read(stdin_fd, 64).decode(errors="ignore"):
                        keys.handle(ch)
                windows.add(time.monotonic(), sum(counters))
                live.update(
                    _render(
                        args=args,
                        profile=profile,
                        governor=governor,
                        windows=windows,
                        keys=keys,
                        threads=threads,
                        offered=offered,
                    ),
                    refresh=True,
                )
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        termios.tcsetattr(stdin_fd, termios.TCSADRAIN, saved_termios)

    # readers are daemons and may be blocked inside a long read; don't join
    if failures:
        raise RuntimeError("reader worker failed") from failures[0]
    elapsed, total = windows.lifetime()
    console.print(
        f"read {fmt_bytes(total)} in {elapsed:.0f}s "
        f"({fmt_rate(total / elapsed if elapsed > 0 else 0)} mean)"
    )
    return 0


def selftest() -> None:
    assert slab_chunk_count(32 * 2**10, 64) == 64  # small: conc-capped
    assert slab_chunk_count(512 * 2**10, 64) == 32  # 512k: 16 MiB / 512 KiB
    assert slab_chunk_count(4 * 2**20, 64) == 4  # medium
    assert slab_chunk_count(128 * 2**20, 64) == 1  # big: never 0
    w = WindowRates(100.0)
    w.add(101.0, 10)
    w.add(102.0, 30)
    assert w.rate(1.0) == 20.0
    assert w.rate(10.0) == 15.0
    assert w.lifetime() == (2.0, 30)
    assert fmt_rate(3_750_000_000) == "30.0 Gbps"
    assert fmt_rate(200_000_000) == "1.60 Gbps"
    assert fmt_rate(7_500_000) == "0.060 Gbps"
    assert fmt_rate(15_000_000_000) == "120 Gbps"
    assert delta_percent(110.0, 100.0) == 10.0
    assert delta_percent(95.0, 100.0) == -5.0
    assert delta_percent(50.0, 0.0) == 0.0
