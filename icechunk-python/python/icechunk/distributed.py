# distributed utility functions
from collections.abc import Generator, Iterable
from typing import Any, cast

import numpy as np
import zarr

from icechunk import IcechunkStore, Session

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
    zarray: zarr.Array,
    axis: Any = None,
    keepdims: Any = None,
    computing_meta: bool = False,
) -> Session:
    if computing_meta:
        return np.array([object()], dtype=object)
    store = cast(IcechunkStore, zarray.store)
    return store.session


def merge_sessions(
    *sessions: Session | list[Session] | list[list[Session]],
    axis: Any = None,
    keepdims: Any = None,
    computing_meta: bool = False,
) -> Session:
    if computing_meta:
        return np.array([object()], dtype=object)
    session, *rest = list(_flatten(sessions))
    for other in rest:
        session.merge(other)
    return cast(Session, session)
