#!/usr/bin/env python3
# helper script to run and save benchmarks against named refs.
# AKA a shitty version of asv's env management

import os
import subprocess
import tempfile
import tomllib

import tqdm

TMP = tempfile.gettempdir()
CURRENTDIR = os.getcwd()
if not CURRENTDIR.endswith("icechunk-python"):
    raise ValueError(
        "Running in the wrong directory. Please run from the `icechunk-python` directory."
    )


def get_commit(ref: str) -> str:
    return subprocess.run(
        ["git", "rev-parse", ref], capture_output=True, text=True, check=True
    ).stdout.strip()[:8]


def get_benchmark_deps(filepath: str) -> str:
    with open(filepath, mode="rb") as f:
        data = tomllib.load(f)
    return " ".join(data["project"]["optional-dependencies"].get("benchmark", ""))


def setup(ref: str) -> None:
    commit = get_commit(ref)
    base = f"{TMP}/icechunk-bench-{ref}_{commit}"
    cwd = f"{TMP}/icechunk-bench-{ref}_{commit}/icechunk"
    pycwd = f"{TMP}/icechunk-bench-{ref}_{commit}/icechunk/icechunk-python"
    activate = "source .venv/bin/activate"

    deps = get_benchmark_deps(f"{CURRENTDIR}/pyproject.toml")

    kwargs = dict(cwd=cwd, check=True)
    pykwargs = dict(cwd=pycwd, check=True)

    print(f"checking out {ref} to {base}")
    subprocess.run(["mkdir", base], check=False)
    # TODO: copy the local one instead to save time?
    subprocess.run(
        ["git", "clone", "-q", "git@github.com:earth-mover/icechunk"],
        cwd=base,
        check=False,
    )
    subprocess.run(["git", "checkout", "-q", ref], **kwargs)
    subprocess.run(["cp", "-r", "benchmarks", f"{pycwd}"], check=True)
    subprocess.run(["python3", "-m", "venv", ".venv"], cwd=pycwd, check=True)
    subprocess.run(
        ["maturin", "build", "-q", "--release", "--out", "dist", "--find-interpreter"],
        **pykwargs,
    )
    subprocess.run(
        f"{activate}"
        "&& pip install -q icechunk['test'] --find-links dist"
        f"&& pip install -q {deps}",
        shell=True,
        **pykwargs,
    )

    # FIXME: make this configurable
    print(f"setup_benchmarks for {ref}")
    subprocess.run(
        f"{activate} && pytest -nauto -m setup_benchmarks --icechunk-prefix={ref}_{commit}/ benchmarks/",
        **pykwargs,
        shell=True,
    )


def run(ref):
    commit = get_commit(ref)
    pycwd = f"{TMP}/icechunk-bench-{ref}_{commit}/icechunk/icechunk-python"
    activate = "source .venv/bin/activate"

    print(f"running for {ref} / {commit}")

    cmd = f"""
    pytest --benchmark-storage={CURRENTDIR}/.benchmarks --benchmark-save={ref}_{commit} --icechunk-prefix={ref}_{commit}/ benchmarks/
    """
    print(cmd)
    subprocess.run(
        f"{activate} "
        # Note: .benchmarks is the default location for pytest-benchmark
        f"&& {cmd}",
        shell=True,
        cwd=pycwd,
        check=False,  # don't stop if benchmarks fail
    )


if __name__ == "__main__":
    refs = [
        # "icechunk-v0.1.0-alpha.8",
        # "icechunk-v0.1.0-alpha.10",
        # "icechunk-v0.1.0-alpha.11",
        "icechunk-v0.1.0-alpha.12",
        "main",
    ]
    for ref in tqdm.tqdm(refs):
        # TODO: figure how not to duplicate the setup work for a given ref.
        #       not sure how to specify the ref as a `prefix` to Storage
        print("Setting up benchmarks")
        setup(ref)
        print("Running benchmarks")
        run(ref)
