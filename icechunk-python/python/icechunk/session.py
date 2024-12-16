import contextlib
from typing import Generator, Self

from icechunk._icechunk_python import PySession, StoreConfig
from icechunk.store import IcechunkStore


class Session:
    _session: PySession

    def __init__(self, session: PySession):
        self._session = session
        self._allow_distributed_write = False

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, self.__class__):
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
    def id(self) -> str:
        return self._session.id

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
        return self._session.commit(message)
