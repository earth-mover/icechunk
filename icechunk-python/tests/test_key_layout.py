"""Integration tests for the empty-`prefix` S3 key layout fix (#2239).

Before the fix, ``ic.s3_storage(prefix=None)`` made the native-S3 backend write
every object under a leading slash (``/chunks/...``); external tools 404'd. These
run against rustfs at ``http://localhost:4200``.

Empty-prefix repositories live at the bucket root, so each test creates its own
bucket (rustfs root credentials) for isolation. The ``LegacyRoot`` round-trip is
not exercised here because rustfs rejects leading-slash keys; see the Rust unit
tests and ``icechunk/tests/test_key_layout.rs``.
"""

import time
import uuid

import boto3
import numpy as np
import pytest
from mypy_boto3_s3.client import S3Client
from numpy.testing import assert_array_equal

import icechunk as ic
import zarr

# Older botocore signs requests with the deprecated datetime.utcnow(); ignore that
# specific warning so these tests pass regardless of the installed botocore version.
pytestmark = pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")

ENDPOINT = "http://localhost:4200"
REGION = "us-east-1"
# rustfs server admin credentials: can create buckets and access any bucket.
ROOT_KEY, ROOT_SECRET = "test123", "test123"


def root_s3_client() -> S3Client:
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        use_ssl=False,
        region_name=REGION,
        aws_access_key_id=ROOT_KEY,
        aws_secret_access_key=ROOT_SECRET,
    )


@pytest.fixture
def fresh_bucket() -> str:
    # Name sorts lexicographically after `testbucket`, and the zero-padded
    # microsecond timestamp makes lexicographic order match creation order; the
    # random suffix keeps it unique under parallel test runs.
    bucket = f"testbucket-layout-{time.time_ns() // 1000:016d}-{uuid.uuid4().hex[:12]}"
    root_s3_client().create_bucket(Bucket=bucket)
    return bucket


def empty_prefix_storage(bucket: str, prefix: str | None = None) -> ic.Storage:
    return ic.s3_storage(
        bucket=bucket,
        prefix=prefix,
        region=REGION,
        endpoint_url=ENDPOINT,
        allow_http=True,
        access_key_id=ROOT_KEY,
        secret_access_key=ROOT_SECRET,
        force_path_style=True,
    )


def bucket_keys(bucket: str) -> list[str]:
    resp = root_s3_client().list_objects_v2(Bucket=bucket)
    return [o["Key"] for o in resp.get("Contents", [])]


def write_array(repo: ic.Repository, value: int) -> None:
    session = repo.writable_session("main")
    root = zarr.group(store=session.store)
    arr = root.require_array("array", shape=(4,), chunks=(2,), dtype="i4")
    arr[:] = np.full((4,), value, dtype="i4")
    session.commit(f"write {value}")


def test_empty_prefix_writes_clean_keys(fresh_bucket: str) -> None:
    # threshold 0 forces chunks to be written as separate `chunks/...` objects
    config = ic.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 0
    repo = ic.Repository.create(
        storage=empty_prefix_storage(fresh_bucket), config=config
    )
    write_array(repo, 42)

    keys = bucket_keys(fresh_bucket)
    assert keys, "repository wrote no objects"
    bad = [k for k in keys if k.startswith("/")]
    assert not bad, f"keys must not start with a slash (the #2239 bug): {bad}"
    assert "repo" in keys, f"expected a clean `repo` key, got {keys}"
    assert any(k.startswith("chunks/") for k in keys), (
        f"expected clean `chunks/...` keys, got {keys}"
    )


def test_empty_prefix_roundtrips(fresh_bucket: str) -> None:
    repo = ic.Repository.create(storage=empty_prefix_storage(fresh_bucket))
    write_array(repo, 7)

    # Re-open with a fresh storage (re-runs layout detection).
    repo2 = ic.Repository.open(storage=empty_prefix_storage(fresh_bucket))
    session = repo2.readonly_session(branch="main")
    arr = zarr.open_array(store=session.store, path="array", mode="r")
    assert_array_equal(arr[:], np.full((4,), 7, dtype="i4"))


def test_legacy_rooted_keys_rejected_with_nonempty_prefix() -> None:
    # The escape hatch is only valid for an empty prefix.
    with pytest.raises(Exception, match="empty prefix"):
        ic.s3_storage(
            bucket="testbucket",
            prefix="some/prefix",
            region=REGION,
            endpoint_url=ENDPOINT,
            allow_http=True,
            access_key_id=ROOT_KEY,
            secret_access_key=ROOT_SECRET,
            force_path_style=True,
            legacy_rooted_keys=True,
        )
