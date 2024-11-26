# distributed utility functions
from typing import cast

import zarr
from icechunk import IcechunkStore


def extract_store(zarray: zarr.Array) -> IcechunkStore:
    return cast(IcechunkStore, zarray.store)


def merge_stores(*stores: IcechunkStore) -> IcechunkStore:
    store, *rest = stores
    for other in rest:
        store.merge(other.change_set_bytes())
    return store
