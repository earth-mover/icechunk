from typing import Self

from icechunk._icechunk_python import PySession, StoreConfig
from icechunk.store import IcechunkStore


class Session:
    _session: PySession

    def __init__(self, session: PySession):
        self._session = session

    @classmethod
    def from_bytes(cls, data: bytes) -> "Session":
        return cls(PySession.from_bytes(data))

    def __eq__(self, value):
        return self._session == value._session

    def as_bytes(self) -> bytes:
        return self._session.as_bytes()

    @property
    def read_only(self) -> bool:
        return self._session.read_only

    @property
    def snapshot_id(self) -> str:
        return self._session.snapshot_id

    @property
    def branch(self) -> str | None:
        return self._session.branch

    def store(self, config: StoreConfig | None = None) -> IcechunkStore:
        return IcechunkStore(self._session.store(config))

    def merge(self, other: Self) -> None:
        self._session.merge(other._session)

    def commit(self, message: str) -> None:
        self._session.commit(message)
