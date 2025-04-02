#!/usr/bin/env python3
# helper script to run and save benchmarks against named refs.
# AKA a shitty version of asv's env management
# FIXME:
# 1. The Icechunk Spec Version is taken from the running env. This is wrong :(

import argparse
import glob
import os
import subprocess
import tempfile
import tomllib
from collections.abc import Callable
from functools import partial

import tqdm
import tqdm.contrib.concurrent
from helpers import (
    assert_cwd_is_icechunk_python,
    get_coiled_kwargs,
    get_full_commit,
    rdms,
    setup_logger,
)

logger = setup_logger()

PIP_OPTIONS = "--disable-pip-version-check -q"
PYTEST_OPTIONS = "-q --durations 10 --rootdir=benchmarks --tb=line"
TMP = tempfile.gettempdir()
CURRENTDIR = os.getcwd()


assert_cwd_is_icechunk_python()


def process_map(*, serial: bool, func: Callable, iterable):
    if serial:
        for i in iterable:
            func(i)
    else:
        tqdm.contrib.concurrent.process_map(func, iterable)


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

    def __init__(self, *, ref: str, where: str, save_prefix: str) -> None:
        self.ref = ref
        self.full_commit = get_full_commit(ref)
        self.commit = self.full_commit[:8]
        self.where = where
        self.save_prefix = save_prefix
        # shorten the name so `pytest-benchmark compare` is readable
        self.clean_ref = self.ref.removeprefix("icechunk-v0.1.0-alph")

    @property
    def pip_github_url(self) -> str:
        # optional extras cannot be specified here, "not guaranteed to work"
        # https://pip.pypa.io/en/stable/topics/vcs-support/#url-fragments
        return f"git+https://github.com/earth-mover/icechunk.git@{self.full_commit}#subdirectory=icechunk-python"

    @property
    def prefix(self) -> str:
        # try:
        #     return f"v{ic.spec_version():02d}"
        # except AttributeError:
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
        self.sync_benchmarks_folder()

    def setup(self, *, force: bool):
        """Creates datasets for read benchmarks."""
        logger.info(f"setup_benchmarks for {self.ref} / {self.commit}")
        cmd = (
            f"pytest {PYTEST_OPTIONS} -s -nauto --benchmark-disable "
            f"-m setup_benchmarks --force-setup={force} "
            f"--where={self.where} "
            f"--icechunk-prefix=benchmarks/{self.prefix}/ "
            "benchmarks/"
        )
        logger.info(">>> " + cmd)
        self.execute(cmd, check=True)

    def run(self, *, pytest_extra: str = "") -> None:
        """Actually runs the benchmarks."""
        logger.info(f"running benchmarks for {self.ref} / {self.commit}")

        assert self.bench_store_dir is not None
        # Note: .benchmarks is the default location for pytest-benchmark
        cmd = (
            f"pytest {pytest_extra} "
            f"--benchmark-storage={self.bench_store_dir}/.benchmarks "
            f"--benchmark-save={self.where}_{self.clean_ref}_{self.commit} "
            f"--where={self.where} "
            f"--icechunk-prefix=benchmarks/{self.prefix}/ "
            f"{PYTEST_OPTIONS} "
            "benchmarks/"
        )
        print(cmd)

        self.execute(cmd, check=False)


class LocalRunner(Runner):
    activate: str = "source .venv/bin/activate"
    bench_store_dir = CURRENTDIR

    def __init__(self, *, ref: str, where: str, save_prefix: str):
        super().__init__(ref=ref, where=where, save_prefix="")
        suffix = self.commit
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
        super().initialize()

    def run(self, *, pytest_extra: str = "") -> None:
        super().run(pytest_extra=pytest_extra)


class CoiledRunner(Runner):
    bench_store_dir = "."

    def get_coiled_run_args(self) -> tuple[str]:
        ckwargs = self.get_coiled_kwargs()
        return (
            "coiled",
            "run",
            "--interactive",
            f"--name={ckwargs['name']}",
            f"--keepalive={ckwargs['keepalive']}",
            f"--workspace={ckwargs['workspace']}",  # cloud
            f"--vm-type={ckwargs['vm_type']}",
            f"--software={ckwargs['software']}",
            f"--region={ckwargs['region']}",
        )

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
        kwargs["name"] = f"icebench-{self.commit}-{self.where}"
        kwargs["keepalive"] = "10m"
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
            pip=[self.pip_github_url, "coiled", *deps],
        )
        super().initialize()

    def execute(self, cmd, **kwargs):
        subprocess.run([*self.get_coiled_run_args(), cmd], **kwargs)

    def sync_benchmarks_folder(self) -> None:
        subprocess.run(
            [
                *self.get_coiled_run_args(),
                "--file",
                "benchmarks/",
                "ls -alh ./.benchmarks/",
            ],
            check=True,
        )

    def run(self, *, pytest_extra: str = "") -> None:
        super().run(pytest_extra=pytest_extra)
        filename = f"{self.save_prefix}/{self.where}_{self.clean_ref}_{self.commit}.json"
        # This is crappy; but for some reason coiled.function doesn't see the right files :/
        # So we need to use 'coiled run'; upload to a bucket; and then download from bucket
        # pytest-benchmark cannot write directly to a bucket yet, sadly.
        # TODO: explore a mounting a bucket as a volume.
        self.execute(f"sh benchmarks/most_recent.sh {filename}")


def read_latest_benchmark_json() -> str:
    files = sorted(
        glob.glob("./.benchmarks/*", recursive=True),
        key=os.path.getmtime,
        reverse=True,
    )
    with open(files[0]) as f:
        json = f.read()
    return json


def init_for_ref(runner: Runner) -> None:
    runner.initialize()


def run_there(where: str, *, args, save_prefix) -> None:
    if where == "local":
        runner_cls = LocalRunner
    else:
        runner_cls = CoiledRunner

    runners = tuple(
        runner_cls(ref=ref, where=where, save_prefix=save_prefix) for ref in args.refs
    )

    # we can only initialize in parallel since the two refs may have the same spec version.
    process_map(serial=args.serial, func=partial(init_for_ref), iterable=runners)

    if not args.skip_setup:
        for runner in runners:
            runner.setup(force=args.force_setup)

    # TODO: this could be parallelized for coiled runners
    for runner in tqdm.tqdm(runners):
        runner.run(pytest_extra=args.pytest)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("refs", help="refs to run benchmarks for", nargs="+")
    parser.add_argument("--pytest", help="passed to pytest", default="")
    parser.add_argument("--serial", action="store_true", default=False)
    parser.add_argument(
        "--where",
        help="where to run? [local|s3|s3_ob|gcs], combinations are allowed: [s3|gcs]",
        default="local",
    )
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

    if "|" in args.where:
        where = args.where.split("|")
    else:
        where = (args.where,)

    save_prefix = rdms()

    process_map(
        serial=args.serial,
        func=partial(run_there, args=args, save_prefix=save_prefix),
        iterable=where,
    )

    if any(w != "local" for w in where):
        subprocess.run(
            [
                "aws",
                "s3",
                "cp",
                f"s3://earthmover-scratch/benchmarks/{save_prefix}/",
                f"/tmp/benchmarks/{save_prefix}/",
                "--recursive",
            ],
            check=True,
        )
    refs = args.refs

    # TODO: clean this up
    if where == ("local",) and len(refs) > 1:
        files = sorted(
            glob.glob("./.benchmarks/**/*.json", recursive=True),
            key=os.path.getmtime,
            reverse=True,
        )[: len(refs)]
    else:
        files = sorted(
            glob.glob(f"/tmp/benchmarks/{save_prefix}/*.json", recursive=True),
            key=os.path.getmtime,
            reverse=True,
        )
    #  TODO: Use `just` here when we figure that out.
    subprocess.run(
        [
            "pytest-benchmark",
            "compare",
            "--group=group,func",
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
# 5. Compare icechunk vs plain Zarr
