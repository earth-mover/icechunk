import pickle
import time
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


@pytest.mark.parametrize(
    "resolve_get_credentials",
    [False, True],
)
def test_refreshable_credentials_grant_access(resolve_get_credentials: bool) -> None:
    good_storage = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        get_credentials=get_good_credentials,
        resolve_get_credentials=resolve_get_credentials,
    )
    bad_storage = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        get_credentials=get_bad_credentials,
        resolve_get_credentials=resolve_get_credentials,
    )

    assert not Repository.exists(good_storage)

    with pytest.raises(ValueError, match="InvalidAccessKeyId"):
        assert not Repository.exists(bad_storage)


def throws() -> S3StaticCredentials:
    raise ValueError("bad creds")


def returns_something_else() -> int:
    return 42


@pytest.mark.parametrize(
    "resolve_get_credentials",
    [False, True],
)
def test_refreshable_credentials_errors(resolve_get_credentials: bool) -> None:
    if resolve_get_credentials:
        with pytest.raises(ValueError, match="bad creds"):
            st = s3_storage(
                region="us-east-1",
                endpoint_url="http://localhost:9000",
                allow_http=True,
                bucket="testbucket",
                prefix="this-repo-does-not-exist",
                get_credentials=throws,
                resolve_get_credentials=resolve_get_credentials,
            )
    else:
        st = s3_storage(
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
            bucket="testbucket",
            prefix="this-repo-does-not-exist",
            get_credentials=throws,
            resolve_get_credentials=resolve_get_credentials,
        )

        with pytest.raises(ValueError, match="bad creds"):
            assert not Repository.exists(st)

    st = Storage.new_s3(
        config=S3Options(
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
            force_path_style=True,
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
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
            force_path_style=True,
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

    def __init__(self, path: Path, expired_times: int) -> None:
        self.path = path
        self.expired_times = expired_times

    def __call__(self) -> S3StaticCredentials:
        try:
            s = self.path.read_text()
        except Exception:
            s = ""

        s += "."
        self.path.write_text(s)

        # The return an expired credential for 3 times, then we return credentials with no expiration
        expires = None if len(s) >= self.expired_times else datetime.now(UTC)
        return S3StaticCredentials(
            access_key_id="minio123", secret_access_key="minio123", expires_after=expires
        )


@pytest.mark.parametrize(
    "resolve_get_credentials",
    [False, True],
)
def test_s3_refreshable_credentials_refresh(
    tmp_path: Path, resolve_get_credentials: bool
) -> None:
    path = tmp_path / "calls.txt"
    creds_obj = ExpirableCredentials(path, 3)

    st = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        get_credentials=creds_obj,
        resolve_get_credentials=resolve_get_credentials,
    )

    if resolve_get_credentials:
        assert path.read_text() == "."

    # credentials expire immediately so refresh function keeps getting called
    assert not Repository.exists(st)
    assert not Repository.exists(st)
    assert not Repository.exists(st)
    assert path.read_text() == "..."

    # after three times credentials don't expire, so the file doesn't change
    assert not Repository.exists(st)
    assert not Repository.exists(st)
    assert path.read_text() == "..."


@pytest.mark.parametrize(
    "resolve_get_credentials",
    [False, True],
)
def test_s3_refreshable_credentials_pickle_with_optimization(
    tmp_path: Path,
    resolve_get_credentials: bool,
) -> None:
    """Verifies pickled repos don't need to call get_credentials again if resolve_get_credentials=True"""
    path = tmp_path / "calls.txt"
    creds_obj = ExpirableCredentials(path, 0)

    st = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix="test_refreshable_credentials_pickle_optimization-"
        + str(int(time.time() * 1000)),
        get_credentials=creds_obj,
        resolve_get_credentials=resolve_get_credentials,
    )
    # let's create and use a repo
    repo = Repository.create(storage=st)
    assert Repository.exists(st)
    assert Repository.exists(st)

    # now we pickle it, use the copy, and check get_credentials is not called again
    repo = pickle.loads(pickle.dumps(repo))
    repo.ancestry(branch="main")

    called_only_once = path.read_text() == "."
    assert called_only_once == resolve_get_credentials
