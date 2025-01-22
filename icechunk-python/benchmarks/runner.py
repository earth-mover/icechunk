#!/usr/bin/env python3
# helper script to run and save benchmarks against named refs.
# AKA a shitty version of asv's env management

import argparse
import glob
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


class Runner:
    activate: str = "source .venv/bin/activate"

    def __init__(self, ref: str):
        self.ref = ref
        self.commit = get_commit(ref)
        suffix = f"{self.ref}_{self.commit}"
        self.base = f"{TMP}/icechunk-bench-{suffix}"
        self.cwd = f"{TMP}/icechunk-bench-{suffix}/icechunk"
        self.pycwd = f"{TMP}/icechunk-bench-{suffix}/icechunk/icechunk-python"

    def initialize(self) -> None:
        deps = get_benchmark_deps(f"{CURRENTDIR}/pyproject.toml")
        kwargs = dict(cwd=self.cwd, check=True)
        pykwargs = dict(cwd=self.pycwd, check=True)

        print(f"checking out {ref} to {self.base}")
        subprocess.run(["mkdir", self.base], check=False)
        # TODO: copy the local one instead to save time?
        subprocess.run(
            ["git", "clone", "-q", "git@github.com:earth-mover/icechunk"],
            cwd=self.base,
            check=False,
        )
        subprocess.run(["git", "checkout", "-q", ref], **kwargs)
        subprocess.run(["python3", "-m", "venv", ".venv"], cwd=self.pycwd, check=True)
        subprocess.run(
            [
                "maturin",
                "build",
                "-q",
                "--release",
                "--out",
                "dist",
                "--find-interpreter",
            ],
            **pykwargs,
        )
        subprocess.run(
            f"{self.activate}"
            "&& pip install -q icechunk['test'] --find-links dist"
            f"&& pip install -q {deps}",
            shell=True,
            **pykwargs,
        )

    def setup(self):
        print(f"setup_benchmarks for {ref} / {self.commit}")
        subprocess.run(["cp", "-r", "benchmarks", f"{self.pycwd}"], check=True)
        cmd = (
            "pytest -nauto -m setup_benchmarks --force-setup=False "
            f"--icechunk-prefix=benchmarks/{self.ref}_{self.commit}/ "
            "benchmarks/"
        )
        subprocess.run(
            f"{self.activate} && {cmd}", cwd=self.pycwd, check=True, shell=True
        )

    def run(self, *, pytest_extra: str = "") -> None:
        print(f"running benchmarks for {self.ref} / {self.commit}")

        subprocess.run(["cp", "-r", "benchmarks", f"{self.pycwd}"], check=True)

        clean_ref = ref.removeprefix("icechunk-v0.1.0-alph")
        # Note: .benchmarks is the default location for pytest-benchmark
        cmd = (
            f"pytest -q "
            f"--benchmark-storage={CURRENTDIR}/.benchmarks "
            f"--benchmark-save={clean_ref}_{self.commit} "
            f"--icechunk-prefix=benchmarks/{ref}_{self.commit}/ "
            f"{pytest_extra} "
            "benchmarks/"
        )
        print(cmd)

        # don't stop if benchmarks fail
        subprocess.run(
            f"{self.activate} && {cmd}", shell=True, cwd=self.pycwd, check=False
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("refs", help="refs to run benchmarks for", nargs="+")
    parser.add_argument("--pytest", help="passed to pytest")
    args = parser.parse_args()

    refs = args.refs
    # refs = [
    #     # "0.1.0-alpha.2-python",  # first release
    #     "icechunk-v0.1.0-alpha.8",
    #     # concurrent chunk fetch
    #     # list_dir reimplemented
    #     # "icechunk-v0.1.0-alpha.10",
    #     # metadata file download performance
    #     # "icechunk-v0.1.0-alpha.11",
    #     # concurrently download bytes
    #     "icechunk-v0.1.0-alpha.12",
    #     # "main",
    # ]
    for ref in tqdm.tqdm(refs):
        runner = Runner(ref)
        # runner.initialize()
        # runner.setup()
        runner.run(pytest_extra=args.pytest)

    if len(refs) > 1:
        files = sorted(glob.glob("./.benchmarks/**/*.json", recursive=True))[-len(refs) :]
        # TODO: Use `just` here when we figure that out.
        subprocess.run(
            [
                "pytest-benchmark",
                "compare",
                "--group=group,func,param",
                "--sort=fullname",
                "--columns=median",
                "--name=normal",
                *files,
            ]
        )
