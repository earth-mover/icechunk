# distributed utility functions
from typing import cast

import zarr
from icechunk import IcechunkStore, Session


def extract_session(zarray: zarr.Array) -> Session:
    store = cast(IcechunkStore, zarray.store)
    return store.session


def merge_sessions(*sessions: Session) -> Session:
    session, *rest = sessions
    for other in rest:
        session.merge(other)
    return session
