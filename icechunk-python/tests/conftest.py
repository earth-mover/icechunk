import math
import os
from enum import Enum
from typing import Literal, cast

import boto3
import pytest
from hypothesis import HealthCheck, settings
from mypy_boto3_s3.client import S3Client

from icechunk import Repository, SpecVersion, in_memory_storage, local_filesystem_storage

# ---------------------------------------------------------------------------
# Hypothesis profiles
#
# Inherits from hypothesis's built-in "default" and "ci" profiles.
# hypothesis auto-selects "ci" when the CI env var is set.
# Select with: pytest --hypothesis-profile=nightly
#
# When HYPOTHESIS_NUM_SHARDS > 1, max_examples is divided across shards so
# each parallel runner does a fraction of the work. derandomize is disabled
# so each shard naturally gets a unique random seed — no fixed seeds needed.
# ---------------------------------------------------------------------------
_num_shards = int(os.environ.get("HYPOTHESIS_NUM_SHARDS", "1"))
_sharding = _num_shards > 1

settings.register_profile(
    "default",
    parent=settings.get_profile("default"),
    deadline=None,
    suppress_health_check=[HealthCheck.filter_too_much, HealthCheck.too_slow],
)
settings.register_profile(
    "ci",
    parent=settings.get_profile("ci"),
    max_examples=math.ceil(200 / _num_shards),
    stateful_step_count=75,
    derandomize=not _sharding,
    suppress_health_check=[HealthCheck.filter_too_much, HealthCheck.too_slow],
)
settings.register_profile(
    "nightly",
    parent=settings.get_profile("ci"),
    max_examples=1000,
    stateful_step_count=500,
    derandomize=False,
    suppress_health_check=[HealthCheck.filter_too_much, HealthCheck.too_slow],
)


class Permission(Enum):
    READONLY = 1
    MODIFY = 2
    SUPERUSER = 3  # temporary, while figuring out right permissions

    def keys(self) -> tuple[str, str]:
        match self:
            case Permission.READONLY:
                return ("readonly", "basicuser")
            case Permission.MODIFY:
                return ("modify", "modifydata")
            case Permission.SUPERUSER:
                return ("test123", "test123")


def parse_repo(
    store: Literal["local", "memory"], path: str, spec_version: SpecVersion | int | None
) -> Repository:
    if store == "local":
        return Repository.create(
            storage=local_filesystem_storage(path),
            spec_version=spec_version,
        )
    if store == "memory":
        return Repository.create(
            storage=in_memory_storage(),
            spec_version=spec_version,
        )
    raise ValueError(f"Unknown store type: {store}")


@pytest.fixture(scope="function")
def repo(
    request: pytest.FixtureRequest,
    tmpdir: str,
    any_spec_version: SpecVersion | int | None,
) -> tuple[Repository, str]:
    param = request.param
    repo = parse_repo(param, tmpdir, spec_version=any_spec_version)
    return repo, tmpdir


minio_client = None


def get_minio_client() -> S3Client:
    global minio_client
    if minio_client is None:
        (aws_access_key_id, aws_secret_access_key) = Permission.MODIFY.keys()
        minio_client = boto3.client(
            "s3",
            endpoint_url="http://localhost:4200",
            use_ssl=False,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
    return minio_client


def write_chunks_to_minio(
    chunks: list[tuple[str, bytes]], bucket: str = "testbucket"
) -> list[str]:
    """Write chunks to local minio returning their etags"""
    s3 = get_minio_client()
    etags = []
    for key, data in chunks:
        etag = s3.put_object(Bucket=bucket, Key=key, Body=data)["ETag"]
        etags.append(etag)

    return etags


@pytest.fixture(
    scope="function",
    params=[1, SpecVersion.v2, None],
    ids=["spec-v1", "spec-v2", "no-spec-version"],
)
def any_spec_version(request: pytest.FixtureRequest) -> SpecVersion | int | None:
    return cast(SpecVersion | int | None, request.param)
