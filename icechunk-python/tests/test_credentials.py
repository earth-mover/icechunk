from datetime import UTC, datetime
from pathlib import Path

import pytest

from icechunk import (
    Repository,
    S3Options,
    S3StaticCredentials,
    Storage,
    s3_refreshable_credentials,
    s3_storage,
)


def get_good_credentials() -> S3StaticCredentials:
    return S3StaticCredentials(access_key_id="minio123", secret_access_key="minio123")


def get_bad_credentials() -> S3StaticCredentials:
    return S3StaticCredentials(access_key_id="xyz", secret_access_key="abc")


def test_refreshable_credentials_grant_access() -> None:
    good_storage = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        get_credentials=get_good_credentials,
    )
    bad_storage = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        get_credentials=get_bad_credentials,
    )

    assert not Repository.exists(good_storage)

    with pytest.raises(ValueError, match="InvalidAccessKeyId"):
        assert not Repository.exists(bad_storage)


def throws() -> S3StaticCredentials:
    raise ValueError("bad creds")


def returns_something_else() -> int:
    return 42


def test_refreshable_credentials_errors() -> None:
    st = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        get_credentials=throws,
    )

    with pytest.raises(ValueError, match="bad creds"):
        assert not Repository.exists(st)

    st = Storage.new_s3(
        config=S3Options(
            region="us-east-1", endpoint_url="http://localhost:9000", allow_http=True
        ),
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        # we intentionally pass something that has the wrong type
        credentials=s3_refreshable_credentials(42),  # type: ignore [arg-type]
    )
    with pytest.raises(ValueError, match="object is not callable"):
        assert not Repository.exists(st)

    st = Storage.new_s3(
        config=S3Options(
            region="us-east-1", endpoint_url="http://localhost:9000", allow_http=True
        ),
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        # we intentionally pass something that has the wrong type
        credentials=s3_refreshable_credentials(returns_something_else),  # type: ignore [arg-type]
    )
    with pytest.raises(ValueError, match="cannot be converted"):
        assert not Repository.exists(st)


class ExpirableCredentials:
    """We use a file to keep track of how many times credentials are refreshed"""

    def __init__(self, path: Path) -> None:
        self.path = path

    def __call__(self) -> S3StaticCredentials:
        try:
            s = self.path.read_text()
        except Exception:
            s = ""

        s += "."
        self.path.write_text(s)

        # The return an expired credential for 3 times, then we return credentials with no expiration
        expires = None if s == "..." else datetime.now(UTC)
        return S3StaticCredentials(
            access_key_id="minio123", secret_access_key="minio123", expires_after=expires
        )


def test_refreshable_credentials_refresh(tmp_path: Path) -> None:
    path = tmp_path / "calls.txt"
    creds_obj = ExpirableCredentials(path)

    st = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        get_credentials=creds_obj,
    )

    # credentials expire immediately so refresh function keeps getting called
    assert not Repository.exists(st)
    assert not Repository.exists(st)
    assert not Repository.exists(st)
    assert path.read_text() == "..."

    # after three times credentials don't expire, so the file doesn't change
    assert not Repository.exists(st)
    assert not Repository.exists(st)
    assert path.read_text() == "..."
