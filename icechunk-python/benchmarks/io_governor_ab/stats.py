"""Round statistics and the regression gate. Pure functions, no icechunk."""

from __future__ import annotations

import statistics
from dataclasses import dataclass


@dataclass(frozen=True)
class Summary:
    n: int
    median: float
    q1: float
    q3: float
    min: float
    max: float
    mean: float


def summarize(samples: list[float]) -> Summary:
    if not samples:
        raise ValueError("no samples")
    if len(samples) == 1:
        (x,) = samples
        return Summary(1, x, x, x, x, x, x)
    q1, median, q3 = statistics.quantiles(samples, n=4, method="inclusive")
    return Summary(
        n=len(samples),
        median=median,
        q1=q1,
        q3=q3,
        min=min(samples),
        max=max(samples),
        mean=statistics.fmean(samples),
    )


@dataclass(frozen=True)
class GateOutcome:
    scenario: str
    arm: str
    reference: str
    arm_median: float
    ref_median: float
    tolerance: float
    gated: bool  # False = warn-only (reported, no exit-code effect)

    @property
    def ratio(self) -> float:
        return self.arm_median / self.ref_median

    @property
    def passed(self) -> bool:
        return self.ratio <= 1.0 + self.tolerance


def evaluate_gate(
    *,
    scenario: str,
    arm: str,
    reference: str,
    arm_times: list[float],
    ref_times: list[float],
    tolerance: float,
    gated: bool,
) -> GateOutcome:
    return GateOutcome(
        scenario=scenario,
        arm=arm,
        reference=reference,
        arm_median=summarize(arm_times).median,
        ref_median=summarize(ref_times).median,
        tolerance=tolerance,
        gated=gated,
    )


def selftest() -> None:
    s = summarize([3.0, 1.0, 2.0, 4.0, 5.0])
    assert s.median == 3.0 and s.min == 1.0 and s.max == 5.0 and s.n == 5
    assert s.q1 == 2.0 and s.q3 == 4.0
    assert summarize([7.0]).median == 7.0

    ok = evaluate_gate(
        scenario="s",
        arm="compat",
        reference="baseline",
        arm_times=[1.04, 1.0, 1.02],
        ref_times=[1.0, 1.0, 1.0],
        tolerance=0.05,
        gated=True,
    )
    assert ok.passed and ok.ratio == 1.02

    bad = evaluate_gate(
        scenario="s",
        arm="compat",
        reference="baseline",
        arm_times=[1.2, 1.3, 1.25],
        ref_times=[1.0, 1.0, 1.0],
        tolerance=0.05,
        gated=True,
    )
    assert not bad.passed and bad.ratio == 1.25
