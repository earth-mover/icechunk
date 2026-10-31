"""Custom HTTP headers on object-store requests.

Verifies the Python API wires ``read_headers``/``write_headers``/``headers`` into
storage, that invalid headers fail at construction, and that the headers survive
the pickle path used by Dask/multiprocessing and actually reach storage.

The observable check tags every write with a custom user-metadata header and
reads it back with a raw S3 client
"""

import pickle
import uuid

import numpy as np
import pytest

import icechunk as ic
import zarr
from tests.conftest import Permission, get_minio_client

ENDPOINT = "http://localhost:4200"
REGION = "us-east-1"
BUCKET = "testbucket"

WRITE_HEADER_NAME = "x-amz-meta-icechunk-write-header-test"
# S3 strips the ``x-amz-meta-`` prefix and lowercases the remainder.
WRITE_HEADER_META_KEY = "icechunk-write-header-test"
WRITE_HEADER_VALUE = "present"


def headers_storage(
    prefix: str,
    *,
    headers: dict[str, str] | None = None,
    read_headers: dict[str, str] | None = None,
    write_headers: dict[str, str] | None = None,
) -> ic.Storage:
    access_key_id, secret_access_key = Permission.MODIFY.keys()
    return ic.s3_storage(
        bucket=BUCKET,
        prefix=prefix,
        region=REGION,
        endpoint_url=ENDPOINT,
        allow_http=True,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        force_path_style=True,
        headers=headers,
        read_headers=read_headers,
        write_headers=write_headers,
    )


def test_invalid_header_rejected_at_construction() -> None:
    # A header name with a space is invalid; the error must surface here, not
    # mid-request.
    with pytest.raises(ValueError, match="invalid HTTP header"):
        headers_storage("p", write_headers={"bad header name": "value"})


@pytest.mark.filterwarnings("ignore:datetime.datetime.utcnow")
def test_write_headers_survive_pickle_and_reach_storage() -> None:
    prefix = f"request-headers-{uuid.uuid4()}"
    storage = headers_storage(
        prefix, write_headers={WRITE_HEADER_NAME: WRITE_HEADER_VALUE}
    )

    config = ic.RepositoryConfig.default()
    # Force chunks to be written as separate objects (not inlined).
    config.inline_chunk_threshold_bytes = 0
    repo = ic.Repository.create(storage=storage, config=config)

    # Simulate the Dask / multiprocessing path: a Storage isn't picklable on its
    # own, but it travels inside a pickled Repository. Headers must survive that
    # hop and still be applied when the unpickled repo writes.
    repo = pickle.loads(pickle.dumps(repo))

    session = repo.writable_session("main")
    group = zarr.open_group(store=session.store, mode="w")
    array = group.create_array("a", shape=(4,), chunks=(2,), dtype="i4")
    array[:] = np.arange(4)
    session.commit("write with custom headers")

    # A raw client confirms the write header reached storage as object metadata.
    client = get_minio_client()
    listed = client.list_objects_v2(Bucket=BUCKET, Prefix=f"{prefix}/chunks/")
    chunk_keys = [obj["Key"] for obj in listed.get("Contents", [])]
    assert chunk_keys, "repository wrote no chunk objects"

    head = client.head_object(Bucket=BUCKET, Key=chunk_keys[0])
    assert head.get("Metadata", {}).get(WRITE_HEADER_META_KEY) == WRITE_HEADER_VALUE
