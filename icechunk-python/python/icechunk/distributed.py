# distributed utility functions
from typing import Any, cast

import zarr
from dask.core import flatten
from icechunk import IcechunkStore, Session


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
    session, *rest = list(flatten(sessions))
    for other in rest:
        session.merge(other)
    return session
