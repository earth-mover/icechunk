# distributed utility functions
from collections.abc import Generator, Iterable
from typing import Any, cast

import zarr
from icechunk import IcechunkStore
from icechunk.session import ForkSession, Session

__all__ = [
    "extract_session",
    "merge_sessions",
]


def _flatten(seq: Iterable[Any], container: type = list) -> Generator[Any, None, None]:
    if isinstance(seq, str):
        yield seq
    else:
        for item in seq:
            if isinstance(item, container):
                assert isinstance(item, Iterable)
                yield from _flatten(item, container=container)
            else:
                yield item


def extract_session(
    zarray: zarr.Array, axis: Any = None, keepdims: Any = None
) -> Session:
    store = cast(IcechunkStore, zarray.store)
    return store.session


def merge_sessions(
    *sessions: ForkSession | list[ForkSession] | list[list[ForkSession]],
) -> ForkSession:
    session, *rest = list(_flatten(sessions))
    for s in (session, *rest):
        if not isinstance(s, ForkSession):
            raise TypeError(
                "merge_sessions only accepts ForkSession objects. "
                f"Received {type(s).__name__!r} instance instead. "
                "First merge all your ForkSessions using `result = merge_sessions(*forked_session)`. "
                "Then call `session.merge(result)`. "
                "See https://icechunk.io/en/stable/icechunk-python/parallel/#cooperative-distributed-writes."
            )
    for other in rest:
        session.merge(other)
    return cast(ForkSession, session)
