import asyncio
import contextvars
import pickle
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from icechunk import (
    IcechunkError,
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
    "scatter_initial_credentials",
    [False, True],
)
def test_refreshable_credentials_grant_access(scatter_initial_credentials: bool) -> None:
    good_storage = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        get_credentials=get_good_credentials,
        scatter_initial_credentials=scatter_initial_credentials,
    )
    bad_storage = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix="this-repo-does-not-exist",
        get_credentials=get_bad_credentials,
        scatter_initial_credentials=scatter_initial_credentials,
    )

    assert not Repository.exists(good_storage)

    with pytest.raises(IcechunkError, match="InvalidAccessKeyId"):
        assert not Repository.exists(bad_storage)


def throws() -> S3StaticCredentials:
    raise IcechunkError("bad creds")


def returns_something_else() -> int:
    return 42


ASYNC_CREDENTIALS_CONTEXT: contextvars.ContextVar[str | None] = (
    contextvars.ContextVar("ASYNC_CREDENTIALS_CONTEXT", default=None)
)


class AsyncGoodCredentials:
    def __init__(self, path: Path, expected_context: str | None = None) -> None:
        self.path = path
        self.expected_context = expected_context

    async def __call__(self) -> S3StaticCredentials:
        try:
            calls = self.path.read_text()
        except Exception:
            calls = ""
        self.path.write_text(calls + ".")

        if self.expected_context is not None:
            if ASYNC_CREDENTIALS_CONTEXT.get() != self.expected_context:
                raise IcechunkError("missing callback context")

        await asyncio.sleep(0)
        return S3StaticCredentials(access_key_id="minio123", secret_access_key="minio123")


@pytest.mark.parametrize(
    "scatter_initial_credentials",
    [False, True],
)
def test_refreshable_credentials_errors(scatter_initial_credentials: bool) -> None:
    if scatter_initial_credentials:
        with pytest.raises(IcechunkError, match="bad creds"):
            st = s3_storage(
                region="us-east-1",
                endpoint_url="http://localhost:9000",
                allow_http=True,
                bucket="testbucket",
                prefix="this-repo-does-not-exist",
                get_credentials=throws,
                scatter_initial_credentials=scatter_initial_credentials,
            )
    else:
        st = s3_storage(
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
            bucket="testbucket",
            prefix="this-repo-does-not-exist",
            get_credentials=throws,
            scatter_initial_credentials=scatter_initial_credentials,
        )

        with pytest.raises(IcechunkError, match="bad creds"):
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
    with pytest.raises(IcechunkError, match="object is not callable"):
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
    with pytest.raises(IcechunkError, match="cannot be cast"):
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
    "scatter_initial_credentials",
    [False, True],
)
def test_s3_refreshable_credentials_refresh(
    tmp_path: Path, scatter_initial_credentials: bool
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
        scatter_initial_credentials=scatter_initial_credentials,
    )

    if scatter_initial_credentials:
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
    "scatter_initial_credentials",
    [False, True],
)
def test_s3_refreshable_credentials_pickle_with_optimization(
    tmp_path: Path,
    scatter_initial_credentials: bool,
    any_spec_version: int | None,
) -> None:
    """Verifies pickled repos don't need to call get_credentials again if scatter_initial_credentials=True"""
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
        scatter_initial_credentials=scatter_initial_credentials,
    )
    # let's create and use a repo
    repo = Repository.create(
        storage=st,
        spec_version=any_spec_version,
    )
    assert Repository.exists(st)
    assert Repository.exists(st)

    # now we pickle it, use the copy, and check get_credentials is not called again
    repo = pickle.loads(pickle.dumps(repo))
    repo.ancestry(branch="main")

    called_only_once = path.read_text() == "."
    assert called_only_once == scatter_initial_credentials


def test_async_refreshable_credentials_with_sync_repository_api(
    tmp_path: Path, any_spec_version: int | None
) -> None:
    prefix = "test_async_refreshable_sync_api-" + str(int(time.time() * 1000))

    create_storage = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix=prefix,
        access_key_id="minio123",
        secret_access_key="minio123",
    )
    Repository.create(storage=create_storage, spec_version=any_spec_version)

    calls_path = tmp_path / "async_sync_calls.txt"
    callback_storage = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix=prefix,
        get_credentials=AsyncGoodCredentials(calls_path),
    )

    repo = Repository.open(callback_storage)
    assert "main" in repo.list_branches()
    assert calls_path.read_text() != ""


@pytest.mark.asyncio
async def test_async_refreshable_credentials_with_async_repository_api(
    tmp_path: Path, any_spec_version: int | None
) -> None:
    prefix = "test_async_refreshable_async_api-" + str(int(time.time() * 1000))

    create_storage = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix=prefix,
        access_key_id="minio123",
        secret_access_key="minio123",
    )
    Repository.create(storage=create_storage, spec_version=any_spec_version)

    calls_path = tmp_path / "async_async_calls.txt"
    context_value = "expected-refresh-context"
    reset_token = ASYNC_CREDENTIALS_CONTEXT.set(context_value)
    try:
        callback_storage = s3_storage(
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
            force_path_style=True,
            bucket="testbucket",
            prefix=prefix,
            get_credentials=AsyncGoodCredentials(
                calls_path, expected_context=context_value
            ),
        )

        repo = await Repository.open_async(callback_storage)
        assert "main" in await repo.list_branches_async()
    finally:
        ASYNC_CREDENTIALS_CONTEXT.reset(reset_token)

    assert calls_path.read_text() != ""
