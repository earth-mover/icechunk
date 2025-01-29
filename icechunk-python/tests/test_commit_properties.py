from datetime import UTC, datetime

import icechunk as ic
import zarr


def test_property_types() -> None:
    repo = ic.Repository.create(
        storage=ic.in_memory_storage(),
    )
    session = repo.writable_session("main")
    store = session.store

    parent_id = session.snapshot_id

    zarr.group(store=store, overwrite=True)
    props = {
        "string": "foo",
        "true": True,
        "none": None,
        "int": 42,
        "float": 42.0,
        "list": ["hello", 42],
        "object": {"foo": "bar", "baz": [1, 5], "inner": {"abc": None, "false": False}},
    }
    snapshot_id = session.commit("some commit", props)

    info = repo.ancestry(branch="main")[0]
    assert info.message == "some commit"
    assert info.id == snapshot_id
    assert info.parent_id == parent_id
    assert info.metadata == props
    assert (datetime.now(UTC) - info.written_at).seconds < 60
