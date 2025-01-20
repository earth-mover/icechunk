#!/usr/bin/env python3
# helper script to run and save benchmarks against named refs.
# AKA a shitty version of asv's env management

import os
import subprocess
import tempfile

import tqdm.contrib.concurrent

TMP = tempfile.gettempdir()
CURRENTDIR = os.getcwd()
if not CURRENTDIR.endswith("icechunk-python"):
    raise ValueError(
        "Running in the wrong directory. Please run from the `icechunk-python` directory."
    )


def setup(ref):
    base = f"{TMP}/icechunk-bench-{ref}"
    cwd = f"{TMP}/icechunk-bench-{ref}/icechunk"
    pycwd = f"{TMP}/icechunk-bench-{ref}/icechunk/icechunk-python"
    activate = "source .venv/bin/activate"

    kwargs = dict(cwd=cwd, check=True)
    pykwargs = dict(cwd=pycwd, check=True)

    print(f"checking out {ref} to {base}")
    subprocess.run(["mkdir", base], check=False)
    # TODO: copy the local one instead to save time?
    subprocess.run(
        ["git", "clone", "-q", "git@github.com:earth-mover/icechunk"],
        cwd=base,
        check=True,
    )
    subprocess.run(["git", "checkout", ref], **kwargs)
    subprocess.run(["cp", "-r", "benchmarks", pycwd], check=True)
    subprocess.run(["python3", "-m", "venv", ".venv"], cwd=pycwd, check=True)
    subprocess.run(
        ["maturin", "build", "--release", "--out", "dist", "--find-interpreter"],
        **pykwargs,
    )
    subprocess.run(
        f"{activate}"
        "&& pip install -q icechunk['test'] --find-links dist"
        # TODO: figure this out from the current pyproject.toml [benchmark] section
        "&& pip install pytest-benchmark s3fs h5netcdf pooch tqdm ",
        shell=True,
        **pykwargs,
    )

    # FIXME: make this configurable
    print(f"setup_benchmarks for {ref}")
    subprocess.run(
        f"{activate} && pytest -nauto --setup_benchmarks benchmarks/test_benchmark_reads.py",
        **pykwargs,
        shell=True,
    )


def run(ref):
    pycwd = f"{TMP}/icechunk-bench-{ref}/icechunk/icechunk-python"
    activate = "source .venv/bin/activate"

    print(f"running for {ref}")
    subprocess.run(
        f"{activate} "
        # .benchmarks is the default location for pytest-benchmark
        f"&& pytest --benchmark-storage={CURRENTDIR}/.benchmarks --benchmark-save={ref}"
        " benchmarks/test_benchmark_reads.py",
        shell=True,
        cwd=pycwd,
        check=False,  # don't stop if benchmarks fail
    )


if __name__ == "__main__":
    refs = [
        # "icechunk-v0.1.0-alpha.8",
        "icechunk-v0.1.0-alpha.10",
        # "main",
    ]
    # TODO: parallelize the setup either here or externally
    #       will need to provide an extra prefix to setup_benchmarks
    #       somehow the Dataset class will have to take this extra prefix in to account.
    #       A context manager may be a good idea?
    print("Setting up benchmarks")
    [setup(ref) for ref in tqdm.tqdm(refs)]
    print("Running benchmarks")
    [run(ref) for ref in tqdm.tqdm(refs)]
