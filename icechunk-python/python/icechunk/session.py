import contextlib
from collections.abc import AsyncIterator, Callable, Generator, Iterable, Sequence
from typing import Any

from icechunk import (
    ChunkType,
    ConflictSolver,
    Diff,
    RepositoryConfig,
    SessionMode,
)
from icechunk._icechunk_python import PySession
from icechunk.store import IcechunkStore


class Session:
    """A session object that allows for reading and writing data from an Icechunk repository."""

    _session: PySession

    def __init__(self, session: PySession):
        self._session = session

    def __repr__(self) -> str:
        return repr(self._session)

    def __str__(self) -> str:
        return str(self._session)

    def _repr_html_(self) -> str:
        return self._session._repr_html_()

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Session):
            return False
        return self._session == value._session

    def __getstate__(self) -> object:
        if not self.read_only:
            raise ValueError(
                "You must opt-in to pickle writable sessions in a distributed context "
                "using Session.fork(). "
                "See https://icechunk.io/en/stable/parallel/#distributed-writes for more. "
                "If you are using xarray's `Dataset.to_zarr` method to write dask arrays, "
                "please use `icechunk.xarray.to_icechunk` instead. "
                "If you are using dask & distributed or multi-processing to read/write from the same repository, "
                "then pass a readonly session created using Repository.readonly_session for the read step. "
                "Alternatively, make sure to pass the ForkSession created by Session.fork() for the read step. "
            )
        state = {
            "_session": self._session.as_bytes(),
        }
        return state

    def __setstate__(self, state: object) -> None:
        if not isinstance(state, dict):
            raise ValueError("Invalid state")
        self._session = PySession.from_bytes(state["_session"])

    @contextlib.contextmanager
    def allow_pickling(self) -> Generator[None, None, None]:
        """
        Context manager to allow unpickling this store if writable.
        """
        raise RuntimeError(
            "The allow_pickling context manager has been removed. "
            "Use the new `Session.fork` API instead. "
            # FIXME: Add link to docs
            "Better yet, use `to_icechunk` if that will fit your needs."
        )

    @property
    def read_only(self) -> bool:
        """
        Whether the session is read-only.

        Returns
        -------
        bool
            True if the session is read-only, False otherwise.
        """
        return self._session.read_only

    @property
    def mode(self) -> SessionMode:
        """
        The mode of this session.

        Returns
        -------
        SessionMode
            The session mode - one of READONLY, WRITABLE, or REARRANGE.
        """
        return self._session.mode

    @property
    def snapshot_id(self) -> str:
        """
        The base snapshot ID of the session.

        Returns
        -------
        str
            The base snapshot ID of the session.
        """
        return self._session.snapshot_id

    @property
    def branch(self) -> str | None:
        """
        The branch that the session is based on. This is only set if the session is writable.

        Returns
        -------
        str or None
            The branch that the session is based on if the session is writable, None otherwise.
        """
        return self._session.branch

    @property
    def has_uncommitted_changes(self) -> bool:
        """
        Whether the session has uncommitted changes. This is only possibly true if the session is writable.

        Returns
        -------
        bool
            True if the session has uncommitted changes, False otherwise.
        """
        return self._session.has_uncommitted_changes

    def status(self) -> Diff:
        """
        Compute an overview of the current session changes

        Returns
        -------
        Diff
            The operations executed in the current session but still not committed.
        """
        return self._session.status()

    def discard_changes(self) -> None:
        """
        When the session is writable, discard any uncommitted changes.
        """
        self._session.discard_changes()

    @property
    def store(self) -> IcechunkStore:
        """
        Get a zarr Store object for reading and writing data from the repository using zarr python.

        Returns
        -------
        IcechunkStore
            A zarr Store object for reading and writing data from the repository.
        """
        return IcechunkStore(self._session.store)

    @property
    def config(self) -> RepositoryConfig:
        """
        Get the repository configuration.

        Notice that changes to the returned object won't be impacted. To change configuration values
        use `Repository.reopen`.

        Returns
        -------
        RepositoryConfig
            The config for the repository that owns this session.
        """
        return self._session.config

    def move(self, from_path: str, to_path: str) -> None:
        """Move or rename a node (array or group) in the hierarchy.

        This is a metadata-only operation—no data is copied. Requires a rearrange session:

            session = repo.rearrange_session("main")
            session.move("/data/raw", "/data/v1")

        Parameters
        ----------
        from_path : str
            The current path of the node (e.g., "/data/raw").
        to_path : str
            The new path for the node (e.g., "/data/v1").
        """
        return self._session.move_node(from_path, to_path)

    async def move_async(self, from_path: str, to_path: str) -> None:
        """Async version of :meth:`move`."""
        return await self._session.move_node_async(from_path, to_path)

    def all_virtual_chunk_locations(self) -> list[str]:
        """
        Return the location URLs of all virtual chunks.

        Returns
        -------
        list of str
            The location URLs of all virtual chunks.
        """
        return self._session.all_virtual_chunk_locations()

    def reindex_array(
        self,
        array_path: str,
        forward: Callable[[Iterable[int]], Iterable[int] | None],
        backward: Callable[[Iterable[int]], Iterable[int] | None] | None = None,
    ) -> None:
        """Reindex chunks in an array by applying a transformation function.

        Only existing (non-empty) chunks are visited — empty positions are
        skipped. This means that if an empty chunk would have shifted into an
        occupied position, that position retains stale data unless a backward
        function is also provided.

        Parameters
        ----------
        array_path : str
            Path to the array.
        forward : Callable[[Iterable[int]], Iterable[int] | None]
            Function that maps old chunk coordinates to new coordinates. Receives
            a list of non-negative integers (the current chunk index) and must return
            either a new index (as a list/tuple of non-negative integers within the
            array's chunk grid bounds) or ``None`` to skip the chunk (leave it in place).
        backward : Callable[[Iterable[int]], Iterable[int] | None], optional
            Inverse of ``forward``: given a chunk position, returns the position
            that would have mapped there under ``forward``. Must follow the same
            return conventions as ``forward``. When provided, each existing chunk
            position is checked to determine whether it should be cleared — if
            ``backward`` returns ``None`` (out of bounds) or points to a position
            with no chunk, that position is reset to the fill value.
        """
        return self._session.reindex_array(array_path, forward, backward)

    def shift_array(
        self,
        array_path: str,
        chunk_offset: Iterable[int],
    ) -> None:
        """Shift all chunks in an array by the given chunk offset.

        Out-of-bounds chunks are discarded. To preserve them, resize the array first
        to make room. Vacated source positions are cleared (reset to fill value).

        Parameters
        ----------
        array_path : str
            The path to the array to shift.
        chunk_offset : Iterable[int]
            The number of chunks to shift by in each dimension. Positive values
            shift right/down, negative values shift left/up.

        Notes
        -----
        To shift right while preserving all data, first resize the array using zarr's
        array.resize(), then shift.
        """
        self._session.shift_array(array_path, list(chunk_offset))

    async def all_virtual_chunk_locations_async(self) -> list[str]:
        """
        Return the location URLs of all virtual chunks (async version).

        Returns
        -------
        list of str
            The location URLs of all virtual chunks.
        """
        return await self._session.all_virtual_chunk_locations_async()

    async def chunk_coordinates(
        self, array_path: str, batch_size: int = 1000
    ) -> AsyncIterator[tuple[int, ...]]:
        """
        Return an async iterator to all initialized chunks for the array at array_path

        Returns
        -------
        an async iterator to chunk coordinates as tuples
        """
        # We do unbatching here to improve speed. Switching to rust to get
        # a batch is much faster than switching for every element
        async for batch in self._session.chunk_coordinates(array_path, batch_size):
            for coord in batch:
                yield tuple(coord)

    def chunk_type(
        self,
        array_path: str,
        chunk_coordinates: Sequence[int],
    ) -> ChunkType:
        """
        Return the chunk type for the specified coordinates

        Parameters
        ----------
        array_path : str
            The path to the array inside the Zarr store. Example: "/groupA/groupB/outputs/my-array".
        chunk_coordinates: Sequence[int]
            A sequence of integers (list or tuple) used to locate the chunk. Example: [0, 1, 5].

        Returns
        -------
        ChunkType
            One of the supported chunk types.
        """
        return self._session.chunk_type(array_path, chunk_coordinates)

    async def chunk_type_async(
        self,
        array_path: str,
        chunk_coordinates: Sequence[int],
    ) -> ChunkType:
        """
        Return the chunk type for the specified coordinates

        Parameters
        ----------
        array_path : str
            The path to the array inside the Zarr store. Example: "/groupA/groupB/outputs/my-array".
        chunk_coordinates: Sequence[int]
            A sequence of integers (list or tuple) used to locate the chunk. Example: [0, 1, 5].

        Returns
        -------
        ChunkType
            One of the supported chunk types.
        """
        return await self._session.chunk_type_async(array_path, chunk_coordinates)

    def merge(self, *others: "ForkSession") -> None:
        """
        Merge the changes for this session with the changes from another session.

        Parameters
        ----------
        others : ForkSession
            The forked sessions to merge changes from.
        """
        for other in others:
            self._session.merge(other._session)

    async def merge_async(self, *others: "ForkSession") -> None:
        """
        Merge the changes for this session with the changes from another session (async version).

        Parameters
        ----------
        others : ForkSession
            The forked sessions to merge changes from.
        """
        for other in others:
            await self._session.merge_async(other._session)

    def commit(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
        rebase_with: ConflictSolver | None = None,
        rebase_tries: int = 1_000,
        allow_empty: bool = False,
    ) -> str:
        """
        Commit the changes in the session to the repository.

        When successful, the writable session is completed and the session is now read-only and based on the new commit. The snapshot ID of the new commit is returned.

        If the session is out of date, this will raise a ConflictError exception depicting the conflict that occurred. The session will need to be rebased before committing.

        Parameters
        ----------
        message : str
            The message to write with the commit.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.
        rebase_with : ConflictSolver | None, optional
            If other session committed while the current session was writing, use Session.rebase with this solver.
        rebase_tries : int, optional
            If other session committed while the current session was writing, use Session.rebase up to this many times in a loop.
        allow_empty : bool, optional
            If True, allow creating a commit even if there are no changes. Default is False.

        Returns
        -------
        str
            The snapshot ID of the new commit.

        Raises
        ------
        icechunk.ConflictError
            If the session is out of date and a conflict occurs.
        icechunk.NoChangesToCommitError
            If there are no changes to commit and allow_empty is False.
        """
        return self._session.commit(
            message,
            metadata,
            rebase_with=rebase_with,
            rebase_tries=rebase_tries,
            allow_empty=allow_empty,
        )

    async def commit_async(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
        rebase_with: ConflictSolver | None = None,
        rebase_tries: int = 1_000,
        allow_empty: bool = False,
    ) -> str:
        """
        Commit the changes in the session to the repository (async version).

        When successful, the writable session is completed and the session is now read-only and based on the new commit. The snapshot ID of the new commit is returned.

        If the session is out of date, this will raise a ConflictError exception depicting the conflict that occurred. The session will need to be rebased before committing.

        Parameters
        ----------
        message : str
            The message to write with the commit.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.
        rebase_with : ConflictSolver | None, optional
            If other session committed while the current session was writing, use Session.rebase with this solver.
        rebase_tries : int, optional
            If other session committed while the current session was writing, use Session.rebase up to this many times in a loop.
        allow_empty : bool, optional
            If True, allow creating a commit even if there are no changes. Default is False.

        Returns
        -------
        str
            The snapshot ID of the new commit.

        Raises
        ------
        icechunk.ConflictError
            If the session is out of date and a conflict occurs.
        icechunk.NoChangesToCommitError
            If there are no changes to commit and allow_empty is False.
        """
        return await self._session.commit_async(
            message,
            metadata,
            rebase_with=rebase_with,
            rebase_tries=rebase_tries,
            allow_empty=allow_empty,
        )

    def amend(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
        allow_empty: bool = False,
    ) -> str:
        """
        Commit the changes in the session to the repository, by amending/overwriting the previous commit.

        When successful, the writable session is completed and the session is now read-only and based on the new commit. The snapshot ID of the new commit is returned.

        If the session is out of date, this will raise a ConflictError exception depicting the conflict that occurred. The session will need to be rebased before committing.

        This operation doesn't create a new commit in the repo ancestry. It replaces the previous commit.

        The first commit to the repo cannot be amended.

        Parameters
        ----------
        message : str
            The message to write with the commit.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.
        allow_empty : bool, optional
            If True, allow amending even if no data changes have been made to the session.
            This is useful when you only want to update the commit message. Default is False.

        Returns
        -------
        str
            The snapshot ID of the new commit.

        Raises
        ------
        icechunk.ConflictError
            If the session is out of date and a conflict occurs.
        """
        return self._session.amend(message, metadata, allow_empty=allow_empty)

    async def amend_async(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
        allow_empty: bool = False,
    ) -> str:
        """
        Commit the changes in the session to the repository, by amending/overwriting the previous commit.

        When successful, the writable session is completed and the session is now read-only and based on the new commit. The snapshot ID of the new commit is returned.

        If the session is out of date, this will raise a ConflictError exception depicting the conflict that occurred. The session will need to be rebased before committing.

        This operation doesn't create a new commit in the repo ancestry. It replaces the previous commit.

        The first commit to the repo cannot be amended.

        Parameters
        ----------
        message : str
            The message to write with the commit.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.
        allow_empty : bool, optional
            If True, allow amending even if no data changes have been made to the session.
            This is useful when you only want to update the commit message. Default is False.

        Returns
        -------
        str
            The snapshot ID of the new commit.

        Raises
        ------
        icechunk.ConflictError
            If the session is out of date and a conflict occurs.
        """
        return await self._session.amend_async(message, metadata, allow_empty=allow_empty)

    def flush(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Save the changes in the session to a new snapshot without modifying the current branch.

        When successful, the writable session is completed and the session is now read-only and based on the new snapshot. The ID of the new snapshot is returned.

        Parameters
        ----------
        message : str
            The message to write with the commit.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.

        Returns
        -------
        str
            The ID of the new snapshot.
        """
        return self._session.flush(message, metadata)

    async def flush_async(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Save the changes in the session to a new snapshot without modifying the current branch.

        When successful, the writable session is completed and the session is now read-only and based on the new snapshot. The ID of the new snapshot is returned.

        Parameters
        ----------
        message : str
            The message to write with the commit.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.

        Returns
        -------
        str
            The ID of the new snapshot.
        """
        return await self._session.flush_async(message, metadata)

    def rebase(self, solver: ConflictSolver) -> None:
        """
        Rebase the session to the latest ancestry of the branch.

        This method will iteratively crawl the ancestry of the branch and apply the changes from the branch to the session. If a conflict is detected, the conflict solver will be used to optionally resolve the conflict. When complete, the session will be based on the latest commit of the branch and the session will be ready to attempt another commit.

        When a conflict is detected and a resolution is not possible with the provided solver, a RebaseFailed exception will be raised. This exception will contain the snapshot ID that the rebase failed on and a list of conflicts that occurred.

        Parameters
        ----------
        solver : ConflictSolver
            The conflict solver to use when a conflict is detected.

        Raises
        ------
        RebaseFailedError
            When a conflict is detected and the solver fails to resolve it.
        """
        self._session.rebase(solver)

    async def rebase_async(self, solver: ConflictSolver) -> None:
        """
        Rebase the session to the latest ancestry of the branch (async version).

        This method will iteratively crawl the ancestry of the branch and apply the changes from the branch to the session. If a conflict is detected, the conflict solver will be used to optionally resolve the conflict. When complete, the session will be based on the latest commit of the branch and the session will be ready to attempt another commit.

        When a conflict is detected and a resolution is not possible with the provided solver, a RebaseFailed exception will be raised. This exception will contain the snapshot ID that the rebase failed on and a list of conflicts that occurred.

        Parameters
        ----------
        solver : ConflictSolver
            The conflict solver to use when a conflict is detected.

        Raises
        ------
        RebaseFailedError
            When a conflict is detected and the solver fails to resolve it.
        """
        await self._session.rebase_async(solver)

    def fork(self) -> "ForkSession":
        """
        Create a child session that can be pickled to a worker job and later merged.

        This method supports Icechunk's distributed, collaborative jobs. A coordinator task creates a new session using
        `Repository.writable_session`. Then `Session.fork` is called repeatedly to create as many serializable sessions
        as worker jobs. Each new `ForkSession` is pickled to the worker that uses it to do all its writes.
        Finally, the `ForkSessions` are pickled back to the coordinator that uses `ForkSession.merge` to merge them
        back into the original session and `commit`.

        Learn more about collaborative writes at https://icechunk.io/en/latest/parallel/

        Raises
        ------
        ValueError
            When `self` already has uncommitted changes.
        ValueError
            When `self` is read-only.
        """
        # TODO: Do we still need ForkSession?
        return ForkSession(self._session.fork())


class ForkSession(Session):
    def __getstate__(self) -> object:
        state = {"_session": self._session.as_bytes()}
        return state

    def __setstate__(self, state: object) -> None:
        if not isinstance(state, dict):
            raise ValueError("Invalid state")
        self._session = PySession.from_bytes(state["_session"])
