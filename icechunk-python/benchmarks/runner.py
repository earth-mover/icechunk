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
from helpers import (
    assert_cwd_is_icechunk_python,
    get_coiled_kwargs,
    get_commit,
    setup_logger,
)

import icechunk as ic

logger = setup_logger()

PIP_OPTIONS = "--disable-pip-version-check -q"
PYTEST_OPTIONS = "-q --durations 10"
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
    bench_store_dir = None

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

    @property
    def ref_commit(self) -> str:
        return f"{self.ref}_{self.commit}"

    def sync_benchmarks_folder(self) -> None:
        """Sync the benchmarks folder over to the cwd."""
        raise NotImplementedError

    def execute(cmd: str) -> None:
        """Execute a command"""
        raise NotImplementedError

    def initialize(self) -> None:
        """Builds virtual envs etc."""
        raise NotImplementedError

    def setup(self, *, force: bool):
        """Creates datasets for read benchmarks."""
        logger.info(f"setup_benchmarks for {self.ref} / {self.commit}")
        self.sync_benchmarks_folder()
        cmd = (
            f"pytest {PYTEST_OPTIONS} -nauto "
            f"-m setup_benchmarks --force-setup={force} "
            f"--where={self.where} "
            f"--icechunk-prefix=benchmarks/{self.prefix}/ "
            "benchmarks/"
        )
        logger.info(cmd)
        self.execute(cmd, check=True)

    def run(self, *, pytest_extra: str = "") -> None:
        """Actually runs the benchmarks."""
        logger.info(f"running benchmarks for {self.ref} / {self.commit}")

        self.sync_benchmarks_folder()

        # shorten the name so `pytest-benchmark compare` is readable
        clean_ref = self.ref.removeprefix("icechunk-v0.1.0-alph")

        assert self.bench_store_dir is not None
        # Note: .benchmarks is the default location for pytest-benchmark
        cmd = (
            f"pytest {PYTEST_OPTIONS} "
            "--tb=line "
            f"--benchmark-storage={self.bench_store_dir}/.benchmarks "
            f"--benchmark-save={clean_ref}_{self.commit}_{self.where} "
            f"--where={self.where} "
            f"--icechunk-prefix=benchmarks/{self.prefix}/ "
            f"{pytest_extra} "
            "benchmarks/"
        )
        logger.info(cmd)

        self.execute(cmd, check=False)


class LocalRunner(Runner):
    activate: str = "source .venv/bin/activate"
    bench_store_dir = CURRENTDIR

    def __init__(self, *, ref: str, where: str):
        super().__init__(ref=ref, where=where)
        suffix = self.ref_commit
        self.base = f"{TMP}/icechunk-bench-{suffix}"
        self.cwd = f"{TMP}/icechunk-bench-{suffix}/icechunk"
        self.pycwd = f"{TMP}/icechunk-bench-{suffix}/icechunk/icechunk-python"

    def sync_benchmarks_folder(self):
        subprocess.run(["cp", "-r", "benchmarks", f"{self.pycwd}"], check=True)

    def execute(self, cmd: str, **kwargs) -> None:
        # don't stop if benchmarks fail
        subprocess.run(f"{self.activate} && {cmd}", cwd=self.pycwd, shell=True, **kwargs)

    def initialize(self) -> None:
        logger.info(f"Running initialize for {self.ref} in {self.base}")

        deps = get_benchmark_deps(f"{CURRENTDIR}/pyproject.toml")
        subprocess.run(["mkdir", "-p", self.pycwd], check=False)
        subprocess.run(["python3", "-m", "venv", ".venv"], cwd=self.pycwd, check=True)
        cmd = f"pip install {PIP_OPTIONS} {self.pip_github_url} {deps}"
        self.execute(cmd, check=True)


class CoiledRunner(Runner):
    bench_store_dir = "."

    def get_coiled_kwargs(self):
        COILED_SOFTWARE = {
            "icechunk-v0.1.0-alpha.1": "icechunk-alpha-release",
            "icechunk-v0.1.0-alpha.12": "icechunk-alpha-12",
        }

        # using the default region here
        kwargs = get_coiled_kwargs(store=self.where)
        kwargs["software"] = COILED_SOFTWARE.get(
            self.ref, f"icechunk-bench-{self.commit}"
        )
        return kwargs

    def initialize(self) -> None:
        import coiled

        deps = get_benchmark_deps(f"{CURRENTDIR}/pyproject.toml").split(" ")

        ckwargs = self.get_coiled_kwargs()
        # repeated calls are a no-op!
        coiled.create_software_environment(
            name=ckwargs["software"],
            workspace=ckwargs["workspace"],
            conda={
                "channels": ["conda-forge"],
                "dependencies": ["rust", "python=3.12", "pip"],
            },
            # FIXME: get this to work.
            # pip=[self.pip_github_url, *deps],
            pip=[f"icechunk=={self.ref}", *deps],
        )

    def execute(self, cmd, **kwargs) -> None:
        ckwargs = self.get_coiled_kwargs()
        subprocess.run(
            [
                "coiled",
                "run",
                "--name",
                f"icebench-{self.commit}",  # cluster name
                "--sync",
                "--sync-ignore='python/ reports/ profiling/ tests/'",
                "--keepalive",
                "10m",
                f"--workspace={ckwargs['workspace']}",  # cloud
                f"--vm-type={ckwargs['vm_type']}",
                f"--software={ckwargs['software']}",
                f"--region={ckwargs['region']}",
                cmd,
            ],
            **kwargs,
        )

    def sync_benchmarks_folder(self) -> None:
        # uses command-line --sync option
        pass


def init_for_ref(runner: Runner, *, skip_setup: bool, force_setup: bool):
    runner.initialize()
    if not skip_setup:
        runner.setup(force=force_setup)


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
            skip_setup=args.skip_setup,
            force_setup=args.force_setup,
        ),
        runners,
    )

    # For debugging
    # for runner in runners:
    #     init_for_ref(
    #         runner=runner,
    #         skip_setup=args.skip_setup,
    #         force_setup=args.force_setup,
    #     )

    for runner in tqdm.tqdm(runners):
        runner.run(pytest_extra=args.pytest)

    if len(refs) > 1:
        files = sorted(
            glob.glob("./.benchmarks/**/*.json", recursive=True),
            key=os.path.getmtime,
            reverse=True,
        )[-len(refs) :]
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


# Compare wish-list:
# 1. skip differences < X%
# 2. groupby
# 3. better names in summary table
# 4. Compare across object stores; same object store & compare across versions
# 5. Compare icechunk vs plain Zarr
