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
from datetime import UTC, datetime
from functools import partial

import tqdm
import tqdm.contrib.concurrent
from buckets import RESULT_STORE_URLS
from helpers import (
    assert_cwd_is_icechunk_python,
    get_coiled_kwargs,
    get_full_commit,
    rdms,
    setup_logger,
)
from object_store import ObjectStore

logger = setup_logger()

PYTEST_OPTIONS = "-q --durations 10 --rootdir=benchmarks --tb=line"
TMP = tempfile.gettempdir()
CURRENTDIR = os.getcwd()
NIGHTLY_INDEX_URL = "https://pypi.anaconda.org/scientific-python-nightly-wheels/simple"


assert_cwd_is_icechunk_python()

# Resolve AWS credentials eagerly at import time, before process_map forks
# child processes. ProcessPoolExecutor spawns fresh interpreters that may not
# have AWS_PROFILE available, so we freeze everything here.
_AWS_CREDENTIALS: dict[str, str] = {}
try:
    import boto3

    _session = boto3.Session()
    _creds = _session.get_credentials()
    if _creds:
        _frozen = _creds.get_frozen_credentials()
        _AWS_CREDENTIALS["AWS_ACCESS_KEY_ID"] = _frozen.access_key
        _AWS_CREDENTIALS["AWS_SECRET_ACCESS_KEY"] = _frozen.secret_key
        if _frozen.token:
            _AWS_CREDENTIALS["AWS_SESSION_TOKEN"] = _frozen.token
    _client = boto3.client("s3")
    _endpoint = _client.meta.endpoint_url
    if _endpoint and _endpoint != "https://s3.amazonaws.com":
        _AWS_CREDENTIALS["AWS_ENDPOINT_URL"] = _endpoint
    _region = _session.region_name
    if _region:
        _AWS_CREDENTIALS["AWS_REGION"] = _region
except Exception:
    pass


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

    # Support both [dependency-groups] (uv) and [project.optional-dependencies] (legacy)
    groups = data.get("dependency-groups") or data.get("project", {}).get(
        "optional-dependencies", {}
    )

    deps = []
    for group_name in ("benchmark", "test"):
        deps.extend(dep for dep in groups.get(group_name, []) if isinstance(dep, str))
    return " ".join(deps)


class Runner:
    bench_store_dir = None

    # Refs that install icechunk from PyPI instead of building from source.
    PYPI_REFS = {
        "pypi-nightly": "--pre icechunk",
        "pypi-v1": "icechunk==1.*",
    }

    def __init__(self, *, ref: str, where: str, save_prefix: str) -> None:
        self.ref = ref
        if ref in self.PYPI_REFS:
            self.full_commit = ref
            self.commit = ref
        else:
            self.full_commit = get_full_commit(ref)
            self.commit = self.full_commit[:8]
        self.where = where
        self.save_prefix = save_prefix
        # shorten the name so `pytest-benchmark compare` is readable
        self.clean_ref = self.ref.removeprefix("icechunk-v")

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

    def execute(self, cmd: str, **kwargs) -> None:
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
    bench_store_dir = CURRENTDIR

    def __init__(self, *, ref: str, where: str, save_prefix: str):
        super().__init__(ref=ref, where=where, save_prefix="")
        suffix = self.commit
        self.base = f"{TMP}/icechunk-bench-{suffix}"
        self.pycwd = f"{self.base}/icechunk-python"
        self.repo_root = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

    def sync_benchmarks_folder(self):
        subprocess.run(["rm", "-rf", f"{self.pycwd}/benchmarks"], check=False)
        subprocess.run(["cp", "-r", "benchmarks", f"{self.pycwd}/"], check=True)

    def execute(self, cmd: str, **kwargs) -> None:
        subprocess.run(
            f"uv run --group benchmark {cmd}", cwd=self.pycwd, shell=True, **kwargs
        )

    def initialize(self) -> None:
        logger.info(f"Running initialize for {self.ref} in {self.base}")

        if not os.path.exists(self.base):
            subprocess.run(
                ["git", "clone", "--local", "--no-checkout", self.repo_root, self.base],
                check=True,
            )
        subprocess.run(
            ["git", "checkout", self.full_commit],
            cwd=self.base,
            check=True,
        )
        subprocess.run(
            ["uv", "sync", "--group", "benchmark"],
            cwd=self.pycwd,
            check=True,
        )
        build_env = {k: v for k, v in os.environ.items() if k != "CONDA_PREFIX"}
        subprocess.run(
            ["uv", "run", "maturin", "develop", "--uv", "--release"],
            cwd=self.pycwd,
            check=True,
            env=build_env,
        )
        super().initialize()

    def run(self, *, pytest_extra: str = "") -> None:
        super().run(pytest_extra=pytest_extra)


class CoiledRunner(Runner):
    bench_store_dir = "."

    def get_coiled_run_args(self) -> tuple[str, ...]:
        ckwargs = self.get_coiled_kwargs()
        args = [
            "coiled",
            "run",
            "--interactive",
            f"--name={ckwargs['name']}",
            f"--keepalive={ckwargs['keepalive']}",
            f"--workspace={ckwargs['workspace']}",  # cloud
            f"--vm-type={ckwargs['vm_type']}",
            f"--software={ckwargs['software']}",
            f"--region={ckwargs['region']}",
        ]
        # Forward AWS credentials to the VM so it can access S3/R2/etc
        # regardless of which cloud the VM is on. Uses pre-resolved
        # _AWS_CREDENTIALS (frozen at import time, before process forks).
        for key, val in _AWS_CREDENTIALS.items():
            args.extend(["--env", f"{key}={val}"])
        return tuple(args)

    def get_coiled_kwargs(self):
        # using the default region here
        kwargs = get_coiled_kwargs(store=self.where)
        kwargs["software"] = f"icechunk-bench-{self.ref}"
        kwargs["name"] = f"icebench-{self.ref}-{self.where}"
        kwargs["keepalive"] = "10m"
        return kwargs

    def initialize(self) -> None:
        import coiled

        deps = get_benchmark_deps(f"{CURRENTDIR}/pyproject.toml").split(" ")

        ckwargs = self.get_coiled_kwargs()
        envs = coiled.list_software_environments(workspace=ckwargs["workspace"])
        pypi_spec = self.PYPI_REFS.get(self.ref)
        if ckwargs["software"] not in envs:
            if pypi_spec is not None:
                pip_deps = ["coiled", *deps]
            else:
                # TODO: support building wheels for arbitrary git refs.
                # The pip_github_url approach below doesn't work because Coiled
                # ignores git+ URLs with "Local path requirement ... is not supported".
                # pip_deps = [self.pip_github_url, "coiled", *deps]
                raise NotImplementedError(
                    f"Coiled runner only supports PYPI_REFS ({list(self.PYPI_REFS)}). "
                    f"Got ref={self.ref!r}. Use LocalRunner (--where local) for git refs."
                )
            coiled.create_software_environment(
                name=ckwargs["software"],
                workspace=ckwargs["workspace"],
                conda={
                    "channels": ["conda-forge"],
                    "dependencies": ["python=3.14"],
                },
                pip=pip_deps,
            )

        if pypi_spec is not None:
            # Install icechunk on the VM. Coiled's pip= doesn't support flags
            # like --pre or --extra-index-url, so we run pip install separately.
            pip_cmd = f"pip install {pypi_spec}"
            if self.ref == "pypi-nightly":
                pip_cmd += f" --extra-index-url {NIGHTLY_INDEX_URL}"
            logger.info(f"Installing icechunk on VM: {pip_cmd}")
            self.execute(pip_cmd, check=True)

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
        self._fetch_benchmark_json()

    def _fetch_benchmark_json(self) -> None:
        """Fetch benchmark JSON: upload from VM to object store, download locally.

        Uses object_store_python which supports S3/GCS/Tigris/R2 with a
        single API. Region and endpoint_url are read from RESULT_STORE_URLS
        and passed as ObjectStore options (no env var hacks needed).
        """
        result_info = RESULT_STORE_URLS[self.where]
        store_url = result_info["url"]
        region = result_info["region"]
        endpoint = result_info["endpoint_url"]
        ts = datetime.now(UTC).strftime("%Y%m%d-%H%M")
        local_name = f"{self.where}_{self.clean_ref}_{self.commit}_{ts}.json"
        obj_key = f"_bench_results/{self.save_prefix}/{local_name}"

        # Upload from VM using object_store.
        # The VM has forwarded AWS creds as env vars. We set region/endpoint
        # as env vars too so ObjectStore picks up everything from the environment.
        env_setup = f"os.environ['AWS_DEFAULT_REGION']='{region}'; " if region else ""
        if endpoint:
            env_setup += f"os.environ['AWS_ENDPOINT_URL']='{endpoint}'; "
        upload_cmd = (
            'python -c "'
            "import os,glob; "
            f"{env_setup}"
            "from object_store import ObjectStore; "
            "files=sorted(glob.glob('./.benchmarks/**/*',recursive=True),key=os.path.getmtime,reverse=True); "
            "assert files, 'No benchmark JSON found in .benchmarks/'; "
            "f=files[0]; "
            f"store=ObjectStore('{store_url}'); "
            f"store.put('{obj_key}',open(f,'rb').read())"
            '"'
        )
        self.execute(upload_cmd, check=True)

        # Download locally using options= so we don't pollute the process env.
        # For S3-compatible stores, pass pre-resolved AWS credentials (ObjectStore
        # doesn't support AWS_PROFILE). GCS uses application default credentials.
        store_options: dict[str, str] = {}
        if store_url.startswith("s3://"):
            if region:
                store_options["aws_default_region"] = region
            if endpoint:
                store_options["aws_endpoint"] = endpoint
            for key, val in _AWS_CREDENTIALS.items():
                store_options[key.lower()] = val
        store = ObjectStore(store_url, options=store_options)
        data = store.get(obj_key)

        local_dir = f"./.benchmarks/{self.save_prefix}"
        os.makedirs(local_dir, exist_ok=True)
        local_path = f"{local_dir}/{local_name}"
        with open(local_path, "wb") as f:
            f.write(data)
        logger.info(f"Saved benchmark results to {local_path}")


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

    if args.setup != "skip":
        for runner in runners:
            runner.setup(force=args.setup == "force")

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
        help="where to run: local,s3,s3_ob,gcs,tigris,r2. Comma-separated for multiple.",
        default="local",
    )
    parser.add_argument(
        "--setup",
        help="control setup step: 'force' to force recreation, 'skip' to skip entirely. Default: run setup without forcing.",
        choices=["force", "skip"],
        default=None,
    )
    args = parser.parse_args()

    where = tuple(w.strip() for w in args.where.split(","))

    save_prefix = rdms()

    process_map(
        serial=args.serial,
        func=partial(run_there, args=args, save_prefix=save_prefix),
        iterable=where,
    )

    refs = args.refs

    files = sorted(
        glob.glob("./.benchmarks/**/*.json", recursive=True),
        key=os.path.getmtime,
        reverse=True,
    )[: len(refs) * len(where)]
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
