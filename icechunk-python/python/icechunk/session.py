from typing import Self

from icechunk import (
    Conflict,
    ConflictErrorData,
    ConflictSolver,
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

        This is the snapshot ID that the session was based on when the
        commit operation was called.
        """
        return self._error.expected_parent

    @property
    def actual_parent(self) -> str:
        """
        The actual parent snapshot ID of the branch that the session attempted to commit to.

        When the session is based on a branch, this is the snapshot ID of the branch tip. If this
        error is raised, it means the branch was modified and committed by another session after
        the session was created.
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
        """
        return self._error.snapshot

    @property
    def conflicts(self) -> list[Conflict]:
        """
        List of conflicts that occurred during the rebase operation.
        """
        return self._error.conflicts


class Session:
    """A session object that allows for reading and writing data from an Icechunk repository."""

    _session: PySession

    def __init__(self, session: PySession):
        self._session = session
        self._allow_distributed_write = False

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Session):
            return False
        return self._session == value._session

    def __getstate__(self) -> object:
        state = {
            "_session": self._session.as_bytes(),
        }
        return state

    def __setstate__(self, state: object) -> None:
        if not isinstance(state, dict):
            raise ValueError("Invalid state")
        self._session = PySession.from_bytes(state["_session"])

    @property
    def read_only(self) -> bool:
        """Whether the session is read-only."""
        return self._session.read_only

    @property
    def snapshot_id(self) -> str:
        """The base snapshot ID of the session"""
        return self._session.snapshot_id

    @property
    def branch(self) -> str | None:
        """The branch that the session is based on. This is only set if the
        session is writable"""
        return self._session.branch

    @property
    def has_uncommitted_changes(self) -> bool:
        """Whether the session has uncommitted changes. This is only possibly
        true if the session is writable"""
        return self._session.has_uncommitted_changes

    def discard_changes(self) -> None:
        """When the session is writable, discard any uncommitted changes"""
        self._session.discard_changes()

    @property
    def store(self) -> IcechunkStore:
        """Get a zarr Store object for reading and writing data from the repository using zarr python"""
        return IcechunkStore(self._session.store)

    def all_virtual_chunk_locations(self) -> list[str]:
        """Return the location URLs of all virtual chunks"""
        return self._session.all_virtual_chunk_locations()

    def merge(self, other: Self) -> None:
        """Merge the changes for this session with the changes from another session"""
        self._session.merge(other._session)

    def commit(self, message: str) -> str:
        """Commit the changes in the session to the repository

        When successful, the writable session is completed and the session is now read-only
        and based on the new commit. The snapshot ID of the new commit is returned.

        If the session is out of date, this will raise a ConflictError exception depicting
        the conflict that occurred. The session will need to be rebased before committing.

        Args:
            message (str): The message to write with the commit

        Returns:
            str: The snapshot ID of the new commit
        """
        try:
            return self._session.commit(message)
        except PyConflictError as e:
            raise ConflictError(e) from None

    def rebase(self, solver: ConflictSolver) -> None:
        """Rebase the session to the latest ancestry of the branch.

        This method will iteratively crawl the ancestry of the branch and apply the changes
        from the branch to the session. If a conflict is detected, the conflict solver will
        be used to optionally resolve the conflict. When complete, the session will be based
        on the latest commit of the branch and the session will be ready to attempt another
        commit.

        When a conflict is detected and a resolution is not possible with the proivided
        solver, a RebaseFailed exception will be raised. This exception will contain the
        snapshot ID that the rebase failed on and a list of conflicts that occurred.

        Args:
            solver (ConflictSolver): The conflict solver to use when a conflict is detected

        Raises:
            RebaseFailed: When a conflict is detected and the solver fails to resolve it

        """
        try:
            self._session.rebase(solver)
        except PyRebaseFailedError as e:
            raise RebaseFailedError(e) from None
