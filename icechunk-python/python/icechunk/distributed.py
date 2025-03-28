# distributed utility functions
from typing import Any, cast

import zarr
from icechunk import IcechunkStore, Session

__all__ = [
    "extract_session",
    "merge_sessions",
]


def _flatten(seq, container=list):
    if isinstance(seq, str):
        yield seq
    else:
        for item in seq:
            if isinstance(item, container):
                yield from _flatten(item, container=container)
            else:
                yield item


def extract_session(
    zarray: zarr.Array, axis: Any = None, keepdims: Any = None
) -> Session:
    store = cast(IcechunkStore, zarray.store)
    return store.session


def merge_sessions(
    *sessions: Session | list[Session] | list[list[Session]],
    axis: Any = None,
    keepdims: Any = None,
) -> Session:
    session, *rest = list(_flatten(sessions))  # type: ignore[no-untyped-call]
    for other in rest:
        session.merge(other)
    return cast(Session, session)
