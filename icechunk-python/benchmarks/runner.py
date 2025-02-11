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

import icechunk as ic

PIP_OPTIONS = "--disable-pip-version-check -q"
TMP = tempfile.gettempdir()
CURRENTDIR = os.getcwd()

assert_cwd_is_icechunk_python()


def get_benchmark_deps(filepath: str) -> str:
    """needed since
    1. benchmark deps may have changed in the meantime.
    2. we can't specify optional extras when installing from a subdirectory
       https://pip.pypa.io/en/stable/topics/vcs-support/#url-fragments
    """
    with open(filepath, mode="rb") as f:
        data = tomllib.load(f)
    return (
        " ".join(data["project"]["optional-dependencies"].get("benchmark", ""))
        + " "
        + " ".join(data["project"]["optional-dependencies"].get("test", ""))
    )


class Runner:
    def __init__(self, *, ref: str, where: str) -> None:
        self.ref = ref
        self.commit = get_commit(ref)
        self.where = where

    @property
    def pip_github_url(self) -> str:
        # optional extras cannot be specified here, "not guaranteed to work"
        # https://pip.pypa.io/en/stable/topics/vcs-support/#url-fragments
        return f"git+https://github.com/earth-mover/icechunk.git@{self.ref}#subdirectory=icechunk-python"

    @property
    def prefix(self) -> str:
        try:
            return f"v{ic.spec_version():02d}"
        except AttributeError:
            return f"{self.ref}_{self.commit}"


class LocalRunner(Runner):
    activate: str = "source .venv/bin/activate"

    def __init__(self, *, ref: str, where: str):
        super().__init__(ref=ref, where=where)
        suffix = f"{self.ref}_{self.commit}"
        self.base = f"{TMP}/icechunk-bench-{suffix}"
        self.cwd = f"{TMP}/icechunk-bench-{suffix}/icechunk"
        self.pycwd = f"{TMP}/icechunk-bench-{suffix}/icechunk/icechunk-python"

    def initialize(self) -> None:
        ref = self.ref

        deps = get_benchmark_deps(f"{CURRENTDIR}/pyproject.toml")
        pykwargs = dict(cwd=self.pycwd, check=True)

        print(f"Running for {ref} in {self.base}")
        subprocess.run(["mkdir", "-p", self.pycwd], check=False)
        subprocess.run(["python3", "-m", "venv", ".venv"], cwd=self.pycwd, check=True)
        subprocess.run(
            f"{self.activate} "
            f"&& pip install {PIP_OPTIONS} {self.pip_github_url} {deps}",
            shell=True,
            **pykwargs,
        )

    def setup(self, *, force: bool):
        print(f"setup_benchmarks for {self.ref} / {self.commit}")
        subprocess.run(["cp", "-r", "benchmarks", f"{self.pycwd}"], check=True)
        cmd = (
            f"pytest -q --durations 10 -nauto "
            f"-m setup_benchmarks --force-setup={force} "
            f"--where={self.where} "
            f"--icechunk-prefix=benchmarks/{self.prefix}/ "
            "benchmarks/"
        )
        subprocess.run(
            f"{self.activate} && {cmd}", cwd=self.pycwd, check=True, shell=True
        )

    def run(self, *, pytest_extra: str = "") -> None:
        print(f"running benchmarks for {self.ref} / {self.commit}")

        subprocess.run(["cp", "-r", "benchmarks", f"{self.pycwd}"], check=True)

        # shorten the name so `pytest-benchmark compare` is readable
        clean_ref = self.ref.removeprefix("icechunk-v0.1.0-alph")

        # Note: .benchmarks is the default location for pytest-benchmark
        cmd = (
            f"pytest -q --durations 10 "
            f"--benchmark-storage={CURRENTDIR}/.benchmarks "
            f"--benchmark-save={clean_ref}_{self.commit} "
            f"--where={self.where} "
            f"--icechunk-prefix=benchmarks/{self.prefix}/ "
            f"{pytest_extra} "
            "benchmarks/"
        )
        print(cmd)

        # don't stop if benchmarks fail
        subprocess.run(
            f"{self.activate} && {cmd}", shell=True, cwd=self.pycwd, check=False
        )


class CoiledRunner(Runner):
    def __init__(self, *, ref: str, where: str):
        super().__init__(ref=ref, where=where)
        self.ref = ref
        # self.commit = get_commit(ref)
        # suffix = f"{self.ref}_{self.commit}"
        # self.base = f"{TMP}/icechunk-bench-{suffix}"
        # self.cwd = f"{TMP}/icechunk-bench-{suffix}/icechunk"
        # self.pycwd = f"{TMP}/icechunk-bench-{suffix}/icechunk/icechunk-python"
        raise NotImplementedError


def init_for_ref(runner: Runner, *, where: str, skip_setup: bool, force_setup: bool):
    runner.initialize()
    if not skip_setup:
        runner.setup(force=force_setup, where=where)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("refs", help="refs to run benchmarks for", nargs="+")
    parser.add_argument("--pytest", help="passed to pytest", default="")
    parser.add_argument("--where", help="where to run? [local]", default="local")
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

    if args.where == "local":
        runner_cls = LocalRunner
    else:
        runner_cls = CoiledRunner

    runners = tuple(runner_cls(ref=ref, where=args.where) for ref in refs)

    tqdm.contrib.concurrent.process_map(
        partial(
            init_for_ref,
            where=args.where,
            skip_setup=args.skip_setup,
            force_setup=args.force_setup,
        ),
        runners,
    )
    # For debugging
    # for runner in runners:
    #     init_for_ref(
    #         runner=runner,
    #         where=args.where,
    #         skip_setup=args.skip_setup,
    #         force_setup=args.force_setup,
    #     )

    for runner in tqdm.tqdm(runners):
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
