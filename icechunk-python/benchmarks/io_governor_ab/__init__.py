"""A/B benchmarks comparing this icechunk build against a released baseline.

Both versions are installed in one venv: the branch build under its real
name (``icechunk``, via ``just profile=release develop``) and the released
wheel renamed to ``icechunk_baseline`` by third-wheel
(``just io-governor-ab-baseline``). Scenarios interleave rounds across *arms*
(baseline / compat governor / bandwidth governor) and gate the drop-in
configurations on a median-time regression check.

Run ``python -m benchmarks.io_governor_ab --help`` from ``icechunk-python/``, or
``just io-governor-ab --help``. See README.md in this directory.
"""
