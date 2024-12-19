from typing import Self

from icechunk import (
    Conflict,
    ConflictErrorData,
    ConflictSolver,
    RebaseFailedData,
    StoreConfig,
)
from icechunk._icechunk_python import PyConflictError, PyRebaseFailed, PySession
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


class RebaseFailed(Exception):
    """Error raised when a rebase operation fails."""

    _error: RebaseFailedData

    def __init__(self, error: PyRebaseFailed) -> None:
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
        return self._session.read_only

    @property
    def snapshot_id(self) -> str:
        return self._session.snapshot_id

    @property
    def branch(self) -> str | None:
        return self._session.branch

    @property
    def has_uncommitted_changes(self) -> bool:
        return self._session.has_uncommitted_changes

    def discard_changes(self) -> None:
        self._session.discard_changes()

    def store(self, config: StoreConfig | None = None) -> IcechunkStore:
        return IcechunkStore(self._session.store(config))

    def merge(self, other: Self) -> None:
        self._session.merge(other._session)

    def commit(self, message: str) -> str:
        try:
            return self._session.commit(message)
        except PyConflictError as e:
            raise ConflictError(e) from None

    def rebase(self, solver: ConflictSolver) -> None:
        try:
            self._session.rebase(solver)
        except PyRebaseFailed as e:
            raise RebaseFailed(e) from None
