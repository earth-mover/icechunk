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
    """
    Extract Icechunk Session from a zarr Array, useful for distributed array computing frameworks.

    Parameters
    ----------
    zarray: zarr.Array
        ForkSessions to merge.
    axis: Any
       Ignored, only present to satisfy API.
    keepdims: Any
       Ignored, only present to satisfy API.

    Returns
    -------
    Session
    """
    store = cast(IcechunkStore, zarray.store)
    return store.session


def merge_sessions(
    *sessions: ForkSession | list[ForkSession] | list[list[ForkSession]],
) -> ForkSession:
    """
    Merge fork sessions, useful for distributed array computing frameworks.

    Parameters
    ----------
    sessions: ForkSession
        ForkSessions to merge.

    Returns
    -------
    ForkSession
    """
    session, *rest = list(_flatten(sessions))
    for s in (session, *rest):
        if not isinstance(s, ForkSession):
            raise TypeError(
                "merge_sessions only accepts ForkSession objects. "
                f"Received {type(s).__name__!r} instance instead. "
                "To merge _all_ your sessions, use `Session.merge(*forked_sessions)` instead. "
                "See https://icechunk.io/en/stable/icechunk-python/parallel/#cooperative-distributed-writes."
            )
    session.merge(*rest)
    return cast(ForkSession, session)
