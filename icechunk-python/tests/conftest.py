from typing import Literal

import boto3
import pytest
from mypy_boto3_s3.client import S3Client

from icechunk import Repository, in_memory_storage, local_filesystem_storage


def parse_repo(store: Literal["local", "memory"], path: str) -> Repository:
    if store == "local":
        return Repository.create(
            storage=local_filesystem_storage(path),
        )
    if store == "memory":
        return Repository.create(
            storage=in_memory_storage(),
        )


@pytest.fixture(scope="function")
def repo(request: pytest.FixtureRequest, tmpdir: str) -> tuple[Repository, str]:
    param = request.param
    repo = parse_repo(param, tmpdir)
    return repo, tmpdir


minio_client = None


def get_minio_client() -> S3Client:
    global minio_client
    if minio_client is None:
        minio_client = boto3.client(
            "s3",
            endpoint_url="http://localhost:9000",
            use_ssl=False,
            aws_access_key_id="minio123",
            aws_secret_access_key="minio123",
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
