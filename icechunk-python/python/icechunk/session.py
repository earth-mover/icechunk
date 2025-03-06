import contextlib
from collections.abc import AsyncIterator, Generator
from typing import Any, Self

from icechunk import (
    Conflict,
    ConflictErrorData,
    ConflictSolver,
    Diff,
    RebaseFailedData,
)
from icechunk._icechunk_python import PyConflictError, PyRebaseFailedError, PySession
from icechunk.store import IcechunkStore


class ConflictError(Exception):
    """Error raised when a commit operation fails due to a conflict."""

    _error: ConflictErrorData

    def __init__(self, error: PyConflictError) -> None:
        self._error = error.args[0]

    def __str__(self) -> str:
        return str(self._error)

    @property
    def expected_parent(self) -> str:
        """
        The expected parent snapshot ID.

        Returns
        -------
        str
            The snapshot ID that the session was based on when the commit operation was called.
        """
        return self._error.expected_parent

    @property
    def actual_parent(self) -> str:
        """
        The actual parent snapshot ID of the branch that the session attempted to commit to.

        Returns
        -------
        str
            The snapshot ID of the branch tip. If this error is raised, it means the branch was modified and committed by another session after the session was created.
        """
        return self._error.actual_parent


class RebaseFailedError(Exception):
    """Error raised when a rebase operation fails."""

    _error: RebaseFailedData

    def __init__(self, error: PyRebaseFailedError) -> None:
        self._error = error.args[0]

    def __str__(self) -> str:
        return str(self._error)

    @property
    def snapshot_id(self) -> str:
        """
        The snapshot ID that the rebase operation failed on.

        Returns
        -------
        str
            The snapshot ID that the rebase operation failed on.
        """
        return self._error.snapshot

    @property
    def conflicts(self) -> list[Conflict]:
        """
        List of conflicts that occurred during the rebase operation.

        Returns
        -------
        list of Conflict
            List of conflicts that occurred during the rebase operation.
        """
        return self._error.conflicts


class Session:
    """A session object that allows for reading and writing data from an Icechunk repository."""

    _session: PySession
    _allow_pickling: bool

    def __init__(self, session: PySession, _allow_pickling: bool = False):
        self._session = session
        self._allow_pickling = _allow_pickling

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Session):
            return False
        return self._session == value._session

    def __getstate__(self) -> object:
        if not self._allow_pickling and not self.read_only:
            raise ValueError(
                "You must opt-in to pickle writable sessions in a distributed context "
                "using the `Session.allow_pickling` context manager. "
                # link to docs
                "If you are using xarray's `Dataset.to_zarr` method with dask arrays, "
                "please consider `icechunk.xarray.to_icechunk` instead."
            )
        state = {
            "_session": self._session.as_bytes(),
            "_allow_pickling": self._allow_pickling,
        }
        return state

    def __setstate__(self, state: object) -> None:
        if not isinstance(state, dict):
            raise ValueError("Invalid state")
        self._session = PySession.from_bytes(state["_session"])
        self._allow_pickling = state["_allow_pickling"]

    @contextlib.contextmanager
    def allow_pickling(self) -> Generator[None, None, None]:
        """
        Context manager to allow unpickling this store if writable.
        """
        # While this property can only be changed by this context manager,
        # it can be nested (sometimes unintentionally since `to_icechunk` does it)
        current = self._allow_pickling
        try:
            self._allow_pickling = True
            yield
        finally:
            self._allow_pickling = current

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
        return IcechunkStore(self._session.store, self._allow_pickling)

    def all_virtual_chunk_locations(self) -> list[str]:
        """
        Return the location URLs of all virtual chunks.

        Returns
        -------
        list of str
            The location URLs of all virtual chunks.
        """
        return self._session.all_virtual_chunk_locations()

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

    def merge(self, other: Self) -> None:
        """
        Merge the changes for this session with the changes from another session.

        Parameters
        ----------
        other : Self
            The other session to merge changes from.
        """
        self._session.merge(other._session)

    def commit(self, message: str, metadata: dict[str, Any] | None = None) -> str:
        """
        Commit the changes in the session to the repository.

        When successful, the writable session is completed and the session is now read-only and based on the new commit. The snapshot ID of the new commit is returned.

        If the session is out of date, this will raise a ConflictError exception depicting the conflict that occurred. The session will need to be rebased before committing.

        Parameters
        ----------
        message : str
            The message to write with the commit.

        Returns
        -------
        str
            The snapshot ID of the new commit.

        Raises
        ------
        ConflictError
            If the session is out of date and a conflict occurs.
        """
        try:
            return self._session.commit(message, metadata)
        except PyConflictError as e:
            raise ConflictError(e) from None

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
        try:
            self._session.rebase(solver)
        except PyRebaseFailedError as e:
            raise RebaseFailedError(e) from None
