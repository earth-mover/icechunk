"""Helpers for running Hypothesis stateful tests on rule subsets.

Hypothesis doesn't provide an API for testing specific rule subsets
(see https://github.com/HypothesisWorks/hypothesis/issues/4682).

This module provides a workaround: dynamically subclass a state machine and
override excluded rules with plain methods, which strips the ``@rule`` marker
so Hypothesis ignores them.
"""

from __future__ import annotations

import inspect
from collections.abc import Sequence

from hypothesis import settings as Settings
from hypothesis.stateful import (
    RuleBasedStateMachine,
    run_state_machine_as_test,
)

# Private Hypothesis constant — stable, needed to discover which methods are rules.
RULE_MARKER = "hypothesis_stateful_rule"


def test_subsets(
    machine_cls: type[RuleBasedStateMachine],
    *,
    with_rules: Sequence[tuple[set[str], Settings | int]] = (),
    without_rules: Sequence[tuple[set[str], Settings | int]] = (),
) -> None:
    """Run *machine_cls* multiple times, each time with a different rule subset.

    Parameters
    ----------
    machine_cls : type[RuleBasedStateMachine]
        The base state machine class.
    with_rules : Sequence[tuple[set[str], Settings | int]]
        Each entry is ``(rule_names_to_keep, settings_or_max_examples)``.
        All rules **not** in the set are disabled for that run.
    without_rules : Sequence[tuple[set[str], Settings | int]]
        Each entry is ``(rule_names_to_remove, settings_or_max_examples)``.
        The listed rules are disabled; everything else is kept.

    Raises
    ------
    ValueError
        If a rule name doesn't exist on the machine.

    Examples
    --------
    Run only GC-related rules with 50 examples::

        test_subsets(
            MyMachine,
            with_rules=[
                ({"commit", "expire", "gc"}, settings(max_examples=50)),
            ],
        )
    """
    all_names = {
        name
        for name, f in inspect.getmembers(machine_cls)
        if getattr(f, RULE_MARKER, None) is not None
    }

    for keep, s in with_rules:
        _validate(keep, all_names)
        _run_subset(
            machine_cls, all_names, remove=all_names - keep, settings=_to_settings(s)
        )

    for remove, s in without_rules:
        _validate(remove, all_names)
        _run_subset(machine_cls, all_names, remove=remove, settings=_to_settings(s))


def _validate(names: set[str], all_names: set[str]) -> None:
    unknown = names - all_names
    if unknown:
        raise ValueError(f"Unknown rules: {unknown}. Available: {sorted(all_names)}")


def _to_settings(s: Settings | int) -> Settings:
    if isinstance(s, int):
        return Settings(parent=Settings(), max_examples=s)
    return s


def _run_subset(
    machine_cls: type[RuleBasedStateMachine],
    all_names: set[str],
    remove: set[str],
    settings: Settings,
) -> None:
    """Create a subclass with *remove* rules disabled, then run it."""
    if not remove:
        raise ValueError("remove is empty — this would run the full machine unchanged")

    # Strip @rule by grabbing the unwrapped original that @rule stored via __wrapped__.
    # Placing it on the subclass shadows the decorated version.
    # NOTE: assumes @rule is the outermost decorator on each method.
    overrides = {name: getattr(machine_cls, name).__wrapped__ for name in remove}
    kept = sorted(all_names - remove)
    subset_name = f"{machine_cls.__name__}[{'+'.join(kept)}]"
    subset_cls = type(subset_name, (machine_cls,), overrides)
    run_state_machine_as_test(subset_cls, settings=settings)
