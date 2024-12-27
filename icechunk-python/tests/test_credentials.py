from datetime import UTC, datetime

import pytest

from icechunk import (
    Repository,
    S3Options,
    S3StaticCredentials,
    Storage,
    s3_refreshable_credentials,
)


def get_good_credentials():
    return S3StaticCredentials(access_key_id="minio123", secret_access_key="minio123")


def get_bad_credentials():
    return S3StaticCredentials(access_key_id="xyz", secret_access_key="abc")


def test_refreshable_credentials_grant_access():
    good_creds = s3_refreshable_credentials(get_good_credentials)
    bad_creds = s3_refreshable_credentials(get_bad_credentials)

    options = S3Options(
        region="us-east-1", endpoint_url="http://localhost:9000", allow_http=True
    )
    good_storage = Storage.s3(
        config=options,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        credentials=good_creds,
    )
    bad_storage = Storage.s3(
        config=options,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        credentials=bad_creds,
    )

    assert not Repository.exists(good_storage)

    with pytest.raises(ValueError, match="InvalidAccessKeyId"):
        assert not Repository.exists(bad_storage)


def throws():
    raise ValueError("bad creds")


def returns_something_else():
    return 42


def test_refreshable_credentials_errors():
    options = S3Options(
        region="us-east-1", endpoint_url="http://localhost:9000", allow_http=True
    )

    st = Storage.s3(
        config=options,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        credentials=s3_refreshable_credentials(throws),
    )
    with pytest.raises(ValueError, match="bad creds"):
        assert not Repository.exists(st)

    st = Storage.s3(
        config=options,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        credentials=s3_refreshable_credentials(42),
    )
    with pytest.raises(ValueError, match="object is not callable"):
        assert not Repository.exists(st)

    st = Storage.s3(
        config=options,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        credentials=s3_refreshable_credentials(returns_something_else),
    )
    with pytest.raises(ValueError, match="cannot be converted"):
        assert not Repository.exists(st)


class ExpirableCredentials:
    """We use a file to keep track of how many times credentials are refreshed"""

    def __init__(self, path):
        self.path = path

    def __call__(self):
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


def test_refreshable_credentials_refresh(tmp_path):
    path = tmp_path / "calls.txt"
    creds_obj = ExpirableCredentials(path)
    creds = s3_refreshable_credentials(creds_obj)

    options = S3Options(
        region="us-east-1", endpoint_url="http://localhost:9000", allow_http=True
    )
    st = Storage.s3(
        config=options,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        credentials=creds,
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
