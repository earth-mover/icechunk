from typing import Literal

import boto3
import pytest

from icechunk import ObjectStoreConfig, Repository, Storage


def parse_repo(store: Literal["local", "memory"], path: str) -> Repository:
    if store == "local":
        return Repository.create(
            storage=Storage.create(ObjectStoreConfig.LocalFileSystem(path)),
        )
    if store == "memory":
        return Repository.create(
            storage=Storage.create(ObjectStoreConfig.InMemory()),
        )


@pytest.fixture(scope="function")
def repo(request: pytest.FixtureRequest, tmpdir: str) -> tuple[Repository, str]:
    param = request.param
    repo = parse_repo(param, tmpdir)
    return repo, tmpdir


minio_client = None


def get_minio_client():
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
