"""Implementation modules and benchmark arms.

An *arm* is one configuration under measurement: which icechunk module it
uses (the branch build under its real name, or the released baseline renamed
by third-wheel) and which governor / RepositoryConfig tweaks it applies.
"""

from __future__ import annotations

import importlib
import re
import tomllib
from dataclasses import dataclass, field
from functools import cache
from types import ModuleType
from typing import Any

BRANCH = "icechunk"
BASELINE = "icechunk_baseline"

DEFAULT_PERMITS = 256
DEFAULT_BANDWIDTH = "25Gbps"
DEFAULT_MEMORY_BUDGET = "4GB"

# (arm, reference) pairs judged by the regression gate. Everything else is
# reported side by side but never fails the run.
GATE_PAIRS = [("compat", "baseline"), ("compat-shared", "baseline-split")]
BANDWIDTH_GATE_PAIRS = [("bandwidth", "baseline"), ("bandwidth-shared", "baseline-split")]


@cache
def load_module(name: str) -> ModuleType:
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError:
        if name == BASELINE:
            raise SystemExit(
                f"the {BASELINE!r} package is not installed; "
                "run `just io-governor-ab-baseline` to install the released wheel "
                "under that name via third-wheel"
            ) from None
        raise
    # the per-commit LocalFileSystem concurrency warning would spam every round
    module.set_logs_filter("warn,icechunk_arrow_object_store=error")
    return module


_SIZE_RE = re.compile(r"^([0-9_]+(?:\.[0-9]+)?)\s*([A-Za-z]*)$")
_FACTORS = {
    "": 1,
    "b": 1,
    "kb": 10**3,
    "mb": 10**6,
    "gb": 10**9,
    "tb": 10**12,
    "kib": 2**10,
    "mib": 2**20,
    "gib": 2**30,
    "tib": 2**40,
}


def parse_bytes(value: str | int | float) -> int:
    """'4GB' → 4_000_000_000, '32KiB' → 32768; bare numbers pass through."""
    if isinstance(value, int | float):
        return int(value)
    m = _SIZE_RE.match(value.strip())
    factor = _FACTORS.get(m[2].lower()) if m else None
    if m is None or factor is None:
        raise ValueError(f"cannot parse size {value!r}")
    return int(float(m[1].replace("_", "")) * factor)


def parse_rate(value: str | int | float) -> int:
    """Bytes per second. A trailing 'ps' or '/s' is optional; a unit ending in
    lowercase 'b' means bits: '25Gbps' → 3_125_000_000, '90MB/s' → 90_000_000."""
    if isinstance(value, int | float):
        return int(value)
    text = value.strip()
    if text.endswith("/s"):
        text = text[:-2]
    elif text.lower().endswith("ps"):
        text = text[:-2]
    m = _SIZE_RE.match(text)
    if m is None:
        raise ValueError(f"cannot parse rate {value!r}")
    bits = m[2].endswith("b")
    nbytes = parse_bytes(text)
    return nbytes // 8 if bits else nbytes


def _coerce(attr: str, value: Any) -> Any:
    if attr == "label":
        return str(value)
    if attr.endswith("bandwidth"):
        return parse_rate(value)
    return parse_bytes(value)


def bandwidth_config(experimental: ModuleType, options: dict[str, Any]) -> Any:
    """Build a BandwidthGovernorConfig from s3_defaults + dotted overrides."""
    opts = dict(options)
    cfg = experimental.BandwidthGovernorConfig.s3_defaults(
        read_bandwidth=parse_rate(opts.pop("read.target_bandwidth", DEFAULT_BANDWIDTH)),
        write_bandwidth=parse_rate(opts.pop("write.target_bandwidth", DEFAULT_BANDWIDTH)),
        memory_budget=parse_bytes(opts.pop("memory_budget", DEFAULT_MEMORY_BUDGET)),
    )
    for key, value in opts.items():
        target = cfg
        *path, attr = key.split(".")
        for step in path:
            target = getattr(target, step)
        if not hasattr(target, attr):
            raise ValueError(f"unknown bandwidth governor option {key!r}")
        setattr(target, attr, _coerce(attr, value))
    return cfg


@dataclass(frozen=True)
class Arm:
    """One measured configuration.

    Governors are built fresh for every round: a fresh Repository with cold
    caches gets an equally cold governor. Shared arms build one instance per
    round and hand it to every repo.
    """

    name: str
    module_name: str
    kind: str = "default"  # "default" (no governor kwarg) | "compat" | "bandwidth"
    shared: bool = False  # one governor instance across all repos of a round
    split_permits: bool = False  # baseline analog of sharing: 256/n_repos each
    options: dict[str, Any] = field(default_factory=dict)

    @property
    def module(self) -> ModuleType:
        return load_module(self.module_name)

    @property
    def is_branch(self) -> bool:
        return self.module_name == BRANCH

    def governors(self, n_repos: int) -> list[Any]:
        if self.kind == "default":
            return [None] * n_repos
        if self.shared:
            governor = self._build_governor()
            return [governor] * n_repos
        return [self._build_governor() for _ in range(n_repos)]

    def _build_governor(self) -> Any:
        experimental = importlib.import_module(f"{self.module_name}.experimental")
        if self.kind == "compat":
            permits = parse_bytes(
                self.options.get("max_concurrent_requests", DEFAULT_PERMITS)
            )
            return experimental.CompatGovernor(
                experimental.CompatGovernorConfig(max_concurrent_requests=permits)
            )
        if self.kind == "bandwidth":
            return experimental.BandwidthGovernor(
                bandwidth_config(experimental, self.options)
            )
        raise ValueError(f"unknown governor kind {self.kind!r}")

    def repo_config(self, mod: ModuleType, n_repos: int) -> Any:
        if self.split_permits and n_repos > 1:
            return mod.RepositoryConfig(
                max_concurrent_requests=max(1, DEFAULT_PERMITS // n_repos)
            )
        return None

    def describe(self) -> dict[str, Any]:
        return {
            "module": self.module_name,
            "kind": self.kind,
            "shared": self.shared,
            "split_permits": self.split_permits,
            "options": {k: str(v) for k, v in self.options.items()},
        }


def builtin_arms(
    *, bandwidth_options: dict[str, Any], compat_permits: int
) -> dict[str, Arm]:
    compat_opts = {"max_concurrent_requests": compat_permits}
    return {
        "baseline": Arm("baseline", BASELINE),
        "compat": Arm("compat", BRANCH),
        "bandwidth": Arm(
            "bandwidth", BRANCH, kind="bandwidth", options=bandwidth_options
        ),
        "baseline-split": Arm("baseline-split", BASELINE, split_permits=True),
        "compat-shared": Arm(
            "compat-shared", BRANCH, kind="compat", shared=True, options=compat_opts
        ),
        "bandwidth-shared": Arm(
            "bandwidth-shared",
            BRANCH,
            kind="bandwidth",
            shared=True,
            options=bandwidth_options,
        ),
    }


SINGLE_REPO_ARMS = ["baseline", "compat", "bandwidth"]
MULTI_REPO_ARMS = [
    "baseline",
    "compat",
    "baseline-split",
    "compat-shared",
    "bandwidth-shared",
]
BUILTIN_ARM_NAMES = frozenset(SINGLE_REPO_ARMS) | frozenset(MULTI_REPO_ARMS)


def _flatten(table: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in table.items():
        if isinstance(value, dict):
            out.update(_flatten(value, f"{prefix}{key}."))
        else:
            out[f"{prefix}{key}"] = value
    return out


def arms_from_config(path: str) -> dict[str, Arm]:
    """Custom arms from a TOML file; see arms.example.toml. Never gated."""
    with open(path, "rb") as f:
        data = tomllib.load(f)
    arms = {}
    for name, spec in data.get("arm", {}).items():
        spec = dict(spec)
        kind = spec.pop("type")
        if kind not in ("compat", "bandwidth"):
            raise ValueError(f"arm {name!r}: type must be 'compat' or 'bandwidth'")
        shared = bool(spec.pop("shared", False))
        arms[name] = Arm(
            name=name,
            module_name=BRANCH,
            kind=kind,
            shared=shared,
            options=_flatten(spec),
        )
    return arms


def selftest() -> None:
    assert parse_bytes("4GB") == 4_000_000_000
    assert parse_bytes("32KiB") == 32768
    assert parse_bytes(123) == 123
    assert parse_bytes("1_000") == 1000
    assert parse_rate("25Gbps") == 3_125_000_000
    assert parse_rate("90MB/s") == 90_000_000
    assert parse_rate("7.5MBps") == 7_500_000
    assert parse_rate("100") == 100
    assert _coerce("request_latency_us", "500") == 500
    assert _coerce("target_bandwidth", "1Gbps") == 125_000_000
    flat = _flatten({"read": {"target_bandwidth": "1Gbps"}, "memory_budget": "1GB"})
    assert flat == {"read.target_bandwidth": "1Gbps", "memory_budget": "1GB"}
