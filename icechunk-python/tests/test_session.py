import pickle
import tempfile
from typing import cast

import pytest

import zarr
from icechunk import ForkSession, IcechunkError, Repository, local_filesystem_storage
from icechunk.distributed import merge_sessions


def test_session_fork() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        repo = Repository.create(local_filesystem_storage(tmpdir))
        session = repo.writable_session("main")
        zarr.group(session.store)
        assert session.has_uncommitted_changes

        with pytest.raises(ValueError):
            session.fork()

        session.commit("init")

        # forking a read-only session
        with pytest.raises(ValueError):
            session.fork()
        # readonly sessions can be pickled directly
        with pytest.raises(ValueError, match="can be pickled"):
            pickle.loads(pickle.dumps(session.fork()))
        pickle.loads(pickle.dumps(session))

        session = repo.writable_session("main")
        fork = pickle.loads(pickle.dumps(session.fork()))
        zarr.create_group(fork.store, path="/foo")
        assert not session.has_uncommitted_changes
        assert fork.has_uncommitted_changes
        with pytest.warns(UserWarning):
            with pytest.raises(IcechunkError, match="cannot commit"):
                session.commit("foo")
        session.merge(fork)
        session.commit("foo")

        session = repo.writable_session("main")
        fork1 = pickle.loads(pickle.dumps(session.fork()))
        fork2 = pickle.loads(pickle.dumps(session.fork()))
        zarr.create_group(fork1.store, path="/foo1")
        zarr.create_group(fork2.store, path="/foo2")

        with pytest.raises(TypeError, match="Cannot commit a fork"):
            fork1.commit("foo")

        fork1 = pickle.loads(pickle.dumps(fork1))
        fork2 = pickle.loads(pickle.dumps(fork2))

        # this is wrong
        with pytest.raises(TypeError, match="Received 'Session'"):
            merge_sessions(cast(ForkSession, session), fork1, fork2)
        # this is right
        session.merge(fork1, fork2)
        session.commit("all done")

        groups = set(
            name for name, _ in zarr.open_group(session.store, mode="r").groups()
        )
        assert groups == {"foo", "foo1", "foo2"}

        # forking a forked session may be useful
        session = repo.writable_session("main")
        fork1 = pickle.loads(pickle.dumps(session.fork()))
        fork2 = pickle.loads(pickle.dumps(fork1.fork()))
        zarr.create_group(fork1.store, path="/foo3")
        with pytest.raises(ValueError):
            fork1.fork()
        zarr.create_group(fork2.store, path="/foo4")

        fork1 = pickle.loads(pickle.dumps(fork1))
        fork2 = pickle.loads(pickle.dumps(fork2))
        session.merge(fork1, fork2)

        groups = set(
            name for name, _ in zarr.open_group(session.store, mode="r").groups()
        )
        assert groups == {"foo", "foo1", "foo2", "foo3", "foo4"}
