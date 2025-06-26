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
) -> Session:
    session, *rest = list(_flatten(sessions))
    for s in (session, *rest):
        assert isinstance(s, ForkSession), type(s)
    for other in rest:
        session.merge(other)
    return cast(Session, session.to_session())
