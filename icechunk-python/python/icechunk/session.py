import contextlib
import warnings
from collections.abc import AsyncIterator, Generator
from typing import Any, NoReturn, Self

from icechunk import (
    ConflictSolver,
    Diff,
    RepositoryConfig,
)
from icechunk._icechunk_python import PySession
from icechunk.store import IcechunkStore


class Session:
    """A session object that allows for reading and writing data from an Icechunk repository."""

    _session: PySession
    _allow_changes: bool

    def __init__(self, session: PySession):
        self._session = session
        self._allow_changes = False

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Session):
            return False
        return self._session == value._session

    def __getstate__(self) -> object:
        if not self.read_only:
            raise ValueError(
                "You must opt-in to pickle writable sessions in a distributed context "
                "using Session.fork(). "
                # link to docs
                "If you are using xarray's `Dataset.to_zarr` method to write dask arrays, "
                "please use `icechunk.xarray.to_icechunk` instead. "
            )
        state = {
            "_session": self._session.as_bytes(),
            "_allow_changes": self._allow_changes,
        }
        return state

    def __setstate__(self, state: object) -> None:
        if not isinstance(state, dict):
            raise ValueError("Invalid state")
        self._session = PySession.from_bytes(state["_session"])
        self._allow_changes = state["_allow_changes"]

    @contextlib.contextmanager
    def allow_pickling(self) -> Generator[None, None, None]:
        """
        Context manager to allow unpickling this store if writable.
        """
        raise RuntimeError(
            "The allow_pickling context manager has been removed. "
            "Use the new `Session.fork` API instead. "
            # FIXME: Add link to docs
            "Better yet, use `to_icechunk` if that will fit your needs."
        )

    @property
    def read_only(self) -> bool:
        """
        Whether the session is read-only.

        Returns
        -------
        bool
            True if the session is read-only, False otherwise.
        """
        return self._session.read_only

    @property
    def snapshot_id(self) -> str:
        """
        The base snapshot ID of the session.

        Returns
        -------
        str
            The base snapshot ID of the session.
        """
        return self._session.snapshot_id

    @property
    def branch(self) -> str | None:
        """
        The branch that the session is based on. This is only set if the session is writable.

        Returns
        -------
        str or None
            The branch that the session is based on if the session is writable, None otherwise.
        """
        return self._session.branch

    @property
    def has_uncommitted_changes(self) -> bool:
        """
        Whether the session has uncommitted changes. This is only possibly true if the session is writable.

        Returns
        -------
        bool
            True if the session has uncommitted changes, False otherwise.
        """
        return self._session.has_uncommitted_changes

    def status(self) -> Diff:
        """
        Compute an overview of the current session changes

        Returns
        -------
        Diff
            The operations executed in the current session but still not committed.
        """
        return self._session.status()

    def discard_changes(self) -> None:
        """
        When the session is writable, discard any uncommitted changes.
        """
        self._session.discard_changes()

    @property
    def store(self) -> IcechunkStore:
        """
        Get a zarr Store object for reading and writing data from the repository using zarr python.

        Returns
        -------
        IcechunkStore
            A zarr Store object for reading and writing data from the repository.
        """
        return IcechunkStore(self._session.store, for_fork=False)

    @property
    def config(self) -> RepositoryConfig:
        """
        Get the repository configuration.

        Notice that changes to the returned object won't be impacted. To change configuration values
        use `Repository.reopen`.

        Returns
        -------
        RepositoryConfig
            The config for the repository that owns this session.
        """
        return self._session.config

    def all_virtual_chunk_locations(self) -> list[str]:
        """
        Return the location URLs of all virtual chunks.

        Returns
        -------
        list of str
            The location URLs of all virtual chunks.
        """
        return self._session.all_virtual_chunk_locations()

    async def all_virtual_chunk_locations_async(self) -> list[str]:
        """
        Return the location URLs of all virtual chunks (async version).

        Returns
        -------
        list of str
            The location URLs of all virtual chunks.
        """
        return await self._session.all_virtual_chunk_locations_async()

    async def chunk_coordinates(
        self, array_path: str, batch_size: int = 1000
    ) -> AsyncIterator[tuple[int, ...]]:
        """
        Return an async iterator to all initialized chunks for the array at array_path

        Returns
        -------
        an async iterator to chunk coordinates as tuples
        """
        # We do unbatching here to improve speed. Switching to rust to get
        # a batch is much faster than switching for every element
        async for batch in self._session.chunk_coordinates(array_path, batch_size):
            for coord in batch:
                yield tuple(coord)

    def merge(self, *others: "ForkSession") -> None:
        """
        Merge the changes for this session with the changes from another session.

        Parameters
        ----------
        others : ForkSession
            The forked sessions to merge changes from.
        """
        for other in others:
            if not isinstance(other, ForkSession):
                raise TypeError(
                    "Sessions can only be merged with a ForkSession created with Session.fork(). "
                    f"Received {type(other).__name__} instead."
                )
            self._session.merge(other._session)
        self._allow_changes = False

    async def merge_async(self, *others: "ForkSession") -> None:
        """
        Merge the changes for this session with the changes from another session (async version).

        Parameters
        ----------
        others : ForkSession
            The forked sessions to merge changes from.
        """
        for other in others:
            if not isinstance(other, ForkSession):
                raise TypeError(
                    "Sessions can only be merged with a ForkSession created with Session.fork(). "
                    f"Received {type(other).__name__} instead."
                )
            await self._session.merge_async(other._session)
        self._allow_changes = False

    def commit(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
        rebase_with: ConflictSolver | None = None,
        rebase_tries: int = 1_000,
    ) -> str:
        """
        Commit the changes in the session to the repository.

        When successful, the writable session is completed and the session is now read-only and based on the new commit. The snapshot ID of the new commit is returned.

        If the session is out of date, this will raise a ConflictError exception depicting the conflict that occurred. The session will need to be rebased before committing.

        Parameters
        ----------
        message : str
            The message to write with the commit.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.
        rebase_with : ConflictSolver | None, optional
            If other session committed while the current session was writing, use Session.rebase with this solver.
        rebase_tries : int, optional
            If other session committed while the current session was writing, use Session.rebase up to this many times in a loop.

        Returns
        -------
        str
            The snapshot ID of the new commit.

        Raises
        ------
        icechunk.ConflictError
            If the session is out of date and a conflict occurs.
        """
        if self._allow_changes:
            warnings.warn(
                "Committing a session after forking, and without merging will not work. "
                "Merge back in the remote changes first using Session.merge().",
                UserWarning,
                stacklevel=2,
            )
        return self._session.commit(
            message, metadata, rebase_with=rebase_with, rebase_tries=rebase_tries
        )

    async def commit_async(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
        rebase_with: ConflictSolver | None = None,
        rebase_tries: int = 1_000,
    ) -> str:
        """
        Commit the changes in the session to the repository (async version).

        When successful, the writable session is completed and the session is now read-only and based on the new commit. The snapshot ID of the new commit is returned.

        If the session is out of date, this will raise a ConflictError exception depicting the conflict that occurred. The session will need to be rebased before committing.

        Parameters
        ----------
        message : str
            The message to write with the commit.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.
        rebase_with : ConflictSolver | None, optional
            If other session committed while the current session was writing, use Session.rebase with this solver.
        rebase_tries : int, optional
            If other session committed while the current session was writing, use Session.rebase up to this many times in a loop.

        Returns
        -------
        str
            The snapshot ID of the new commit.

        Raises
        ------
        icechunk.ConflictError
            If the session is out of date and a conflict occurs.
        """
        if self._allow_changes:
            warnings.warn(
                "Committing a session after forking, and without merging will not work. "
                "Merge back in the remote changes first using Session.merge().",
                UserWarning,
                stacklevel=2,
            )
        return await self._session.commit_async(
            message, metadata, rebase_with=rebase_with, rebase_tries=rebase_tries
        )

    def flush(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Save the changes in the session to a new snapshot without modifying the current branch.

        When successful, the writable session is completed and the session is now read-only and based on the new snapshot. The ID of the new snapshot is returned.

        Parameters
        ----------
        message : str
            The message to write with the commit.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.

        Returns
        -------
        str
            The ID of the new snapshot.
        """
        if self._allow_changes:
            warnings.warn(
                "Committing a session after forking, and without merging will not work. "
                "Merge back in the remote changes first using Session.merge().",
                UserWarning,
                stacklevel=2,
            )
        return self._session.flush(message, metadata)

    async def flush_async(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Save the changes in the session to a new snapshot without modifying the current branch.

        When successful, the writable session is completed and the session is now read-only and based on the new snapshot. The ID of the new snapshot is returned.

        Parameters
        ----------
        message : str
            The message to write with the commit.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.

        Returns
        -------
        str
            The ID of the new snapshot.
        """
        if self._allow_changes:
            warnings.warn(
                "Flushing a session after forking, and without merging will not work. "
                "Merge back in the remote changes first using Session.merge().",
                UserWarning,
                stacklevel=2,
            )
        return await self._session.flush_async(message, metadata)

    def rebase(self, solver: ConflictSolver) -> None:
        """
        Rebase the session to the latest ancestry of the branch.

        This method will iteratively crawl the ancestry of the branch and apply the changes from the branch to the session. If a conflict is detected, the conflict solver will be used to optionally resolve the conflict. When complete, the session will be based on the latest commit of the branch and the session will be ready to attempt another commit.

        When a conflict is detected and a resolution is not possible with the provided solver, a RebaseFailed exception will be raised. This exception will contain the snapshot ID that the rebase failed on and a list of conflicts that occurred.

        Parameters
        ----------
        solver : ConflictSolver
            The conflict solver to use when a conflict is detected.

        Raises
        ------
        RebaseFailedError
            When a conflict is detected and the solver fails to resolve it.
        """
        self._session.rebase(solver)

    async def rebase_async(self, solver: ConflictSolver) -> None:
        """
        Rebase the session to the latest ancestry of the branch (async version).

        This method will iteratively crawl the ancestry of the branch and apply the changes from the branch to the session. If a conflict is detected, the conflict solver will be used to optionally resolve the conflict. When complete, the session will be based on the latest commit of the branch and the session will be ready to attempt another commit.

        When a conflict is detected and a resolution is not possible with the provided solver, a RebaseFailed exception will be raised. This exception will contain the snapshot ID that the rebase failed on and a list of conflicts that occurred.

        Parameters
        ----------
        solver : ConflictSolver
            The conflict solver to use when a conflict is detected.

        Raises
        ------
        RebaseFailedError
            When a conflict is detected and the solver fails to resolve it.
        """
        await self._session.rebase_async(solver)

    def fork(self) -> "ForkSession":
        """
        Create a child session that can be pickled to a worker job and later merged.

        This method supports Icechunk's distributed, collaborative jobs. A coordinator task creates a new session using
        `Repository.writable_session`. Then `Session.fork` is called repeatedly to create as many serializable sessions
        as worker jobs. Each new `ForkSession` is pickled to the worker that uses it to do all its writes.
        Finally, the `ForkSessions` are pickled back to the coordinator that uses `ForkSession.merge` to merge them
        back into the original session and `commit`.

        Learn more about collaborative writes at https://icechunk.io/en/latest/parallel/

        Raises
        ------
        ValueError
            When `self` already has uncommitted changes.
        ValueError
            When `self` is read-only.
        """
        if self.has_uncommitted_changes:
            raise ValueError(
                "Cannot fork a Session with uncommitted changes. "
                "Make a commit, create a new Session, and then fork that to execute distributed writes."
            )
        if self.read_only:
            raise ValueError(
                "You should not need to fork a read-only session. Read-only sessions can be pickled and transmitted directly."
            )
        self._allow_changes = True
        # force a deep-copy of the underlying Session,
        # so that multiple forks can be created and
        # used independently in a local session.
        # See test_dask.py::test_fork_session_deep_copies for an example
        return ForkSession(PySession.from_bytes(self._session.as_bytes()))


class ForkSession(Session):
    def __getstate__(self) -> object:
        state = {"_session": self._session.as_bytes()}
        return state

    def __setstate__(self, state: object) -> None:
        if not isinstance(state, dict):
            raise ValueError("Invalid state")
        self._session = PySession.from_bytes(state["_session"])

    def merge(self, *others: Self) -> None:
        for other in others:
            if not isinstance(other, ForkSession):
                raise TypeError(
                    f"A ForkSession can only be merged with another ForkSession. Received {type(other)} instead."
                )
            self._session.merge(other._session)

    async def merge_async(self, *others: Self) -> None:
        """
        Merge the changes for this fork session with the changes from other fork sessions (async version).

        Parameters
        ----------
        others : ForkSession
            The other fork sessions to merge changes from.
        """
        for other in others:
            if not isinstance(other, ForkSession):
                raise TypeError(
                    f"A ForkSession can only be merged with another ForkSession. Received {type(other)} instead."
                )
            await self._session.merge_async(other._session)

    def commit(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
        rebase_with: ConflictSolver | None = None,
        rebase_tries: int = 1_000,
    ) -> NoReturn:
        raise TypeError(
            "Cannot commit a fork of a Session. If you are using uncooperative writes, "
            "please send the Repository object to your workers, not a Session. "
            "See https://icechunk.io/en/stable/icechunk-python/parallel/#distributed-writes for more."
        )

    async def commit_async(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
        rebase_with: ConflictSolver | None = None,
        rebase_tries: int = 1_000,
    ) -> NoReturn:
        raise TypeError(
            "Cannot commit a fork of a Session. If you are using uncooperative writes, "
            "please send the Repository object to your workers, not a Session. "
            "See https://icechunk.io/en/stable/icechunk-python/parallel/#distributed-writes for more."
        )

    def flush(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> NoReturn:
        raise TypeError(
            "Cannot flush a fork of a Session. If you are using uncooperative writes, "
            "please send the Repository object to your workers, not a Session. "
            "See https://icechunk.io/en/stable/icechunk-python/parallel/#distributed-writes for more."
        )

    async def flush_async(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> NoReturn:
        raise TypeError(
            "Cannot flush a fork of a Session. If you are using uncooperative writes, "
            "please send the Repository object to your workers, not a Session. "
            "See https://icechunk.io/en/stable/icechunk-python/parallel/#distributed-writes for more."
        )

    @property
    def store(self) -> IcechunkStore:
        """
        Get a zarr Store object for reading and writing data from the repository using zarr python.

        Returns
        -------
        IcechunkStore
            A zarr Store object for reading and writing data from the repository.
        """
        return IcechunkStore(self._session.store, for_fork=True)
