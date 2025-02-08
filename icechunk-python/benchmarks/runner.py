#!/usr/bin/env python3
# helper script to run and save benchmarks against named refs.
# AKA a shitty version of asv's env management

import argparse
import glob
import os
import subprocess
import tempfile
import tomllib
from functools import partial

import tqdm
import tqdm.contrib.concurrent
from helpers import assert_cwd_is_icechunk_python, get_commit

PIP_OPTIONS = "--disable-pip-version-check -q"
TMP = tempfile.gettempdir()
CURRENTDIR = os.getcwd()

assert_cwd_is_icechunk_python()


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
        ref = self.ref

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
        # This is quite ugly but is the only way I can figure out to force pip
        # to install the wheel we just built
        subprocess.run(
            f"{self.activate} "
            f"&& pip install {PIP_OPTIONS} icechunk[test]"
            f"&& pip install {PIP_OPTIONS} {deps}"
            f"&& pip uninstall -y icechunk"
            f"&& pip install -v icechunk --no-index --find-links=dist",
            shell=True,
            **pykwargs,
        )

    def setup(self, force: bool):
        print(f"setup_benchmarks for {self.ref} / {self.commit}")
        subprocess.run(["cp", "-r", "benchmarks", f"{self.pycwd}"], check=True)
        cmd = (
            f"pytest -q --durations 10 -nauto "
            "-m setup_benchmarks --force-setup={force} "
            f"--icechunk-prefix=benchmarks/{self.ref}_{self.commit}/ "
            "benchmarks/"
        )
        subprocess.run(
            f"{self.activate} && {cmd}", cwd=self.pycwd, check=True, shell=True
        )

    def run(self, *, pytest_extra: str = "") -> None:
        print(f"running benchmarks for {self.ref} / {self.commit}")

        subprocess.run(["cp", "-r", "benchmarks", f"{self.pycwd}"], check=True)

        # shorten the name so `pytest-benchmark compare` is readable
        clean_ref = ref.removeprefix("icechunk-v0.1.0-alph")

        # Note: .benchmarks is the default location for pytest-benchmark
        cmd = (
            f"pytest -q --durations 10 "
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


def init_for_ref(ref: str, *, skip_setup: bool, force_setup: bool):
    runner = Runner(ref)
    runner.initialize()
    if not skip_setup:
        runner.setup(force=force_setup)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("refs", help="refs to run benchmarks for", nargs="+")
    parser.add_argument("--pytest", help="passed to pytest", default="")
    parser.add_argument(
        "--skip-setup",
        help="skip setup step, useful for benchmarks that don't need data",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--force-setup", help="forced recreation of datasets?", type=bool, default=False
    )
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

    tqdm.contrib.concurrent.process_map(
        partial(init_for_ref, skip_setup=args.skip_setup, force_setup=args.force_setup),
        refs,
    )
    # For debugging
    # for ref in refs:
    #     init_for_ref(ref, force_setup=args.force_setup)

    for ref in tqdm.tqdm(refs):
        runner = Runner(ref)
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
