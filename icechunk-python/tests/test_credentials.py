import asyncio
import contextvars
import pickle
import threading
import time
from collections.abc import Iterator
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


ASYNC_CREDENTIALS_CONTEXT: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "ASYNC_CREDENTIALS_CONTEXT", default=None
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
    assert "." in calls_path.read_text()


def test_async_refreshable_credentials_constructed_sync_used_async(
    tmp_path: Path, any_spec_version: int | None
) -> None:
    prefix = "test_async_refreshable_sync_construct_async_use-" + str(
        int(time.time() * 1000)
    )

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

    calls_path = tmp_path / "async_sync_construct_calls.txt"
    callback_storage = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix=prefix,
        get_credentials=AsyncGoodCredentials(calls_path),
    )

    async def use_async_repository_api() -> None:
        repo = await Repository.open_async(callback_storage)
        assert "main" in await repo.list_branches_async()

    asyncio.run(use_async_repository_api())
    assert "." in calls_path.read_text()


def test_async_refreshable_credentials_repo_reused_across_event_loops(
    tmp_path: Path, any_spec_version: int | None
) -> None:
    prefix = "test_async_refreshable_new_loop-" + str(int(time.time() * 1000))

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

    calls_path = tmp_path / "async_new_loop_calls.txt"

    async def create_and_use_repo_once() -> Repository:
        callback_storage = s3_storage(
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
            force_path_style=True,
            bucket="testbucket",
            prefix=prefix,
            get_credentials=AsyncGoodCredentials(calls_path),
        )
        repo = await Repository.open_async(callback_storage)
        assert "main" in await repo.list_branches_async()
        return repo

    repo = asyncio.run(create_and_use_repo_once())

    async def use_repo_on_different_loop() -> None:
        assert "main" in await repo.list_branches_async()

    asyncio.run(use_repo_on_different_loop())
    assert "." in calls_path.read_text()


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
    await Repository.create_async(storage=create_storage, spec_version=any_spec_version)

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

    assert "." in calls_path.read_text()


class GoodException(Exception):
    pass


class BadException(Exception):
    pass


# survives pickle because pickle imports the module, doesn't recreate globals
_cred_refresh_loop_ids: list[int] = []


@pytest.fixture(autouse=False)
def reset_cred_tracker() -> Iterator[list[int]]:
    _cred_refresh_loop_ids.clear()
    yield _cred_refresh_loop_ids
    _cred_refresh_loop_ids.clear()


@pytest.fixture()
def minio_repo_prefix(any_spec_version: int | None) -> str:
    prefix = "test_async_creds-" + str(int(time.time() * 1000))
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
    return prefix


class AsyncCredentialTracker:
    def __init__(
        self,
        expected_loop_id: int | None = None,
        return_creds: bool = False,
    ):
        self.expected_loop_id = expected_loop_id
        self.return_creds = return_creds

    async def __call__(self) -> S3StaticCredentials:
        await asyncio.sleep(0)
        loop_id = id(asyncio.get_running_loop())
        _cred_refresh_loop_ids.append(loop_id)
        if self.expected_loop_id is not None and loop_id != self.expected_loop_id:
            raise BadException("Wrong event loop")
        if self.return_creds:
            return S3StaticCredentials(
                access_key_id="minio123",
                secret_access_key="minio123",
                expires_after=datetime.now(UTC),
            )
        raise GoodException("YOLO")


def test_async_cred_refresh_uses_same_loop(
    minio_repo_prefix: str, reset_cred_tracker: list[int]
) -> None:
    prefix = minio_repo_prefix

    async def run() -> None:
        loop = asyncio.get_running_loop()
        callback_storage = s3_storage(
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
            force_path_style=True,
            bucket="testbucket",
            prefix=prefix,
            get_credentials=AsyncCredentialTracker(
                expected_loop_id=id(loop), return_creds=True
            ),
        )
        repo = await Repository.open_async(callback_storage)
        assert "main" in await repo.list_branches_async()
        # FIXME: this deadlocks
        # _ = [snap async for snap in repo.async_ancestry(branch="main")]
        snap = await repo.lookup_branch_async("main")
        await repo.create_branch_async("test-write", snap)
        #  all that should have triggered at least one refresh
        assert len(_cred_refresh_loop_ids) > 1
        assert len(set(_cred_refresh_loop_ids)) == 1

    asyncio.run(run())


def test_async_cred_refresh_uses_originator_loop_from_thread(
    minio_repo_prefix: str, reset_cred_tracker: list[int]
) -> None:
    prefix = minio_repo_prefix

    async def run() -> None:
        loop = asyncio.get_running_loop()
        callback_storage = s3_storage(
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            allow_http=True,
            force_path_style=True,
            bucket="testbucket",
            prefix=prefix,
            get_credentials=AsyncCredentialTracker(
                expected_loop_id=id(loop), return_creds=True
            ),
        )

        def sync_in_thread() -> None:
            repo = Repository.open(callback_storage)
            calls_after_open = len(_cred_refresh_loop_ids)
            assert calls_after_open >= 1

            repo.list_branches()
            calls_after_list_branches = len(_cred_refresh_loop_ids)
            assert calls_after_list_branches > calls_after_open

            list(repo.ancestry(branch="main"))
            calls_after_ancestry = len(_cred_refresh_loop_ids)
            assert calls_after_ancestry > calls_after_list_branches

        await loop.run_in_executor(None, sync_in_thread)
        assert len(set(_cred_refresh_loop_ids)) == 1

    asyncio.run(run())


def test_async_cred_refresh_graceful_deadlock() -> None:
    # Deadlock scenario: sync list_branches called directly on the event loop
    # thread where the credential callback would also need to run. Use a thread
    # with a timeout so the test doesn't hang forever if it actually deadlocks.
    caught_error: BaseException | None = None

    async def sync_call_on_same_loop_as_callback() -> None:
        loop = asyncio.get_running_loop()
        current_loop_id = id(loop)
        storage = s3_storage(
            region="us-east-1",
            bucket="testbucket",
            prefix="placeholder",
            get_credentials=AsyncCredentialTracker(expected_loop_id=current_loop_id),
        )
        # A user can very easily make this mistake!
        # we should be gracefully falling over not deadlocking
        # with pytest.raises(IcechunkError, match="deadlock"):
        Repository.open(storage=storage)

    def run_deadlock_check() -> None:
        nonlocal caught_error
        try:
            asyncio.run(sync_call_on_same_loop_as_callback())
        except BaseException as e:
            caught_error = e

    t = threading.Thread(target=run_deadlock_check, daemon=True)
    t.start()
    t.join(timeout=1)
    assert not t.is_alive(), "Deadlocked: sync call blocked the event loop thread"
    assert isinstance(caught_error, ValueError)
    assert "deadlock" in str(caught_error)


def test_async_callback_no_loop_has_consistent_loop(
    minio_repo_prefix: str, reset_cred_tracker: list[int]
) -> None:
    prefix = minio_repo_prefix

    callback_storage = s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix=prefix,
        get_credentials=AsyncCredentialTracker(return_creds=True),
    )

    repo = Repository.open(callback_storage)
    repo.list_branches()
    list(repo.ancestry(branch="main"))
    assert len(_cred_refresh_loop_ids) > 3

    def on_another_thread(r: Repository) -> None:
        r.list_branches()

    t = threading.Thread(target=on_another_thread, args=(repo,))
    t.start()
    t.join()
    assert len(_cred_refresh_loop_ids) > 4
    assert len(set(_cred_refresh_loop_ids)) == 1
    assert (
        len(set(_cred_refresh_loop_ids)) == 1
    ), "All credential refresh calls should be on the same loop even if there isn't an event loop at the start"
