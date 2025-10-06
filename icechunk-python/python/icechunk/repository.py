import datetime
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from typing import Any, Self, cast

from icechunk import ConflictSolver
from icechunk._icechunk_python import (
    Diff,
    GCSummary,
    PyRepository,
    RepositoryConfig,
    SnapshotInfo,
    Storage,
)
from icechunk.credentials import AnyCredential
from icechunk.session import Session
from icechunk.store import IcechunkStore


class Repository:
    """An Icechunk repository."""

    _repository: PyRepository

    def __init__(self, repository: PyRepository):
        self._repository = repository

    @classmethod
    def create(
        cls,
        storage: Storage,
        config: RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict[str, AnyCredential | None] | None = None,
    ) -> Self:
        """
        Create a new Icechunk repository.
        If one already exists at the given store location, an error will be raised.

        !!! warning
            Attempting to create a Repo concurrently in the same location from multiple processes is not safe.
            Instead, create a Repo once and then open it concurrently.

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.
        config : RepositoryConfig, optional
            The repository configuration. If not provided, a default configuration will be used.
        authorize_virtual_chunk_access : dict[str, AnyCredential | None], optional
            Authorize Icechunk to access virtual chunks in these containers. A mapping
            from container url_prefix to the credentials to use to access chunks in
            that container. If credential is `None`, they will be fetched from the
            environment, or anonymous credentials will be used if the container allows it.
            As a security measure, Icechunk will block access to virtual chunks if the
            container is not authorized using this argument.

        Returns
        -------
        Self
            An instance of the Repository class.
        """
        return cls(
            PyRepository.create(
                storage,
                config=config,
                authorize_virtual_chunk_access=authorize_virtual_chunk_access,
            )
        )

    @classmethod
    async def create_async(
        cls,
        storage: Storage,
        config: RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict[str, AnyCredential | None] | None = None,
    ) -> Self:
        """
        Create a new Icechunk repository asynchronously.
        If one already exists at the given store location, an error will be raised.

        !!! warning
            Attempting to create a Repo concurrently in the same location from multiple processes is not safe.
            Instead, create a Repo once and then open it concurrently.

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.
        config : RepositoryConfig, optional
            The repository configuration. If not provided, a default configuration will be used.
        authorize_virtual_chunk_access : dict[str, AnyCredential | None], optional
            Authorize Icechunk to access virtual chunks in these containers. A mapping
            from container url_prefix to the credentials to use to access chunks in
            that container. If credential is `None`, they will be fetched from the
            environment, or anonymous credentials will be used if the container allows it.
            As a security measure, Icechunk will block access to virtual chunks if the
            container is not authorized using this argument.

        Returns
        -------
        Self
            An instance of the Repository class.
        """
        return cls(
            await PyRepository.create_async(
                storage,
                config=config,
                authorize_virtual_chunk_access=authorize_virtual_chunk_access,
            )
        )

    @classmethod
    def open(
        cls,
        storage: Storage,
        config: RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict[str, AnyCredential | None] | None = None,
    ) -> Self:
        """
        Open an existing Icechunk repository.

        If no repository exists at the given storage location, an error will be raised.

        !!! warning
            This method must be used with care in a multiprocessing context.
            Read more in our [Parallel Write Guide](./parallel.md#uncooperative-distributed-writes).

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.
        config : RepositoryConfig, optional
            The repository settings. If not provided, a default configuration will be
            loaded from the repository.
        authorize_virtual_chunk_access : dict[str, AnyCredential | None], optional
            Authorize Icechunk to access virtual chunks in these containers. A mapping
            from container url_prefix to the credentials to use to access chunks in
            that container. If credential is `None`, they will be fetched from the
            environment, or anonymous credentials will be used if the container allows it.
            As a security measure, Icechunk will block access to virtual chunks if the
            container is not authorized using this argument.

        Returns
        -------
        Self
            An instance of the Repository class.
        """
        return cls(
            PyRepository.open(
                storage,
                config=config,
                authorize_virtual_chunk_access=authorize_virtual_chunk_access,
            )
        )

    @classmethod
    async def open_async(
        cls,
        storage: Storage,
        config: RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict[str, AnyCredential | None] | None = None,
    ) -> Self:
        """
        Open an existing Icechunk repository asynchronously.

        If no repository exists at the given storage location, an error will be raised.

        !!! warning
            This method must be used with care in a multiprocessing context.
            Read more in our [Parallel Write Guide](./parallel.md#uncooperative-distributed-writes).

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.
        config : RepositoryConfig, optional
            The repository settings. If not provided, a default configuration will be
            loaded from the repository.
        authorize_virtual_chunk_access : dict[str, AnyCredential | None], optional
            Authorize Icechunk to access virtual chunks in these containers. A mapping
            from container url_prefix to the credentials to use to access chunks in
            that container. If credential is `None`, they will be fetched from the
            environment, or anonymous credentials will be used if the container allows it.
            As a security measure, Icechunk will block access to virtual chunks if the
            container is not authorized using this argument.

        Returns
        -------
        Self
            An instance of the Repository class.
        """
        return cls(
            await PyRepository.open_async(
                storage,
                config=config,
                authorize_virtual_chunk_access=authorize_virtual_chunk_access,
            )
        )

    @classmethod
    def open_or_create(
        cls,
        storage: Storage,
        config: RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict[str, AnyCredential | None] | None = None,
    ) -> Self:
        """
        Open an existing Icechunk repository or create a new one if it does not exist.

        !!! warning
            This method must be used with care in a multiprocessing context.
            Read more in our [Parallel Write Guide](./parallel.md#uncooperative-distributed-writes).

            Attempting to create a Repo concurrently in the same location from multiple processes is not safe.
            Instead, create a Repo once and then open it concurrently.

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.
        config : RepositoryConfig, optional
            The repository settings. If not provided, a default configuration will be
            loaded from the repository.
        authorize_virtual_chunk_access : dict[str, AnyCredential | None], optional
            Authorize Icechunk to access virtual chunks in these containers. A mapping
            from container url_prefix to the credentials to use to access chunks in
            that container. If credential is `None`, they will be fetched from the
            environment, or anonymous credentials will be used if the container allows it.
            As a security measure, Icechunk will block access to virtual chunks if the
            container is not authorized using this argument.

        Returns
        -------
        Self
            An instance of the Repository class.
        """
        return cls(
            PyRepository.open_or_create(
                storage,
                config=config,
                authorize_virtual_chunk_access=authorize_virtual_chunk_access,
            )
        )

    @classmethod
    async def open_or_create_async(
        cls,
        storage: Storage,
        config: RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict[str, AnyCredential | None] | None = None,
    ) -> Self:
        """
        Open an existing Icechunk repository or create a new one if it does not exist (async version).

        !!! warning
            This method must be used with care in a multiprocessing context.
            Read more in our [Parallel Write Guide](./parallel.md#uncooperative-distributed-writes).

            Attempting to create a Repo concurrently in the same location from multiple processes is not safe.
            Instead, create a Repo once and then open it concurrently.

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.
        config : RepositoryConfig, optional
            The repository settings. If not provided, a default configuration will be
            loaded from the repository.
        authorize_virtual_chunk_access : dict[str, AnyCredential | None], optional
            Authorize Icechunk to access virtual chunks in these containers. A mapping
            from container url_prefix to the credentials to use to access chunks in
            that container. If credential is `None`, they will be fetched from the
            environment, or anonymous credentials will be used if the container allows it.
            As a security measure, Icechunk will block access to virtual chunks if the
            container is not authorized using this argument.

        Returns
        -------
        Self
            An instance of the Repository class.
        """
        return cls(
            await PyRepository.open_or_create_async(
                storage,
                config=config,
                authorize_virtual_chunk_access=authorize_virtual_chunk_access,
            )
        )

    @staticmethod
    def exists(storage: Storage) -> bool:
        """
        Check if a repository exists at the given storage location.

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.

        Returns
        -------
        bool
            True if the repository exists, False otherwise.
        """
        return PyRepository.exists(storage)

    @staticmethod
    async def exists_async(storage: Storage) -> bool:
        """
        Check if a repository exists at the given storage location (async version).

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.

        Returns
        -------
        bool
            True if the repository exists, False otherwise.
        """
        return await PyRepository.exists_async(storage)

    def __getstate__(self) -> object:
        return {
            "_repository": self._repository.as_bytes(),
        }

    def __setstate__(self, state: object) -> None:
        if not isinstance(state, dict):
            raise ValueError("Invalid repository state")
        self._repository = PyRepository.from_bytes(state["_repository"])

    @staticmethod
    def fetch_config(storage: Storage) -> RepositoryConfig | None:
        """
        Fetch the configuration for the repository saved in storage.

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.

        Returns
        -------
        RepositoryConfig | None
            The repository configuration if it exists, None otherwise.
        """
        return PyRepository.fetch_config(storage)

    @staticmethod
    async def fetch_config_async(storage: Storage) -> RepositoryConfig | None:
        """
        Fetch the configuration for the repository saved in storage (async version).

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.

        Returns
        -------
        RepositoryConfig | None
            The repository configuration if it exists, None otherwise.
        """
        return await PyRepository.fetch_config_async(storage)

    def save_config(self) -> None:
        """
        Save the repository configuration to storage, this configuration will be used in future calls to Repository.open.

        Returns
        -------
        None
        """
        return self._repository.save_config()

    async def save_config_async(self) -> None:
        """
        Save the repository configuration to storage (async version).

        Returns
        -------
        None
        """
        return await self._repository.save_config_async()

    @property
    def config(self) -> RepositoryConfig:
        """
        Get a copy of this repository's config.

        Returns
        -------
        RepositoryConfig
            The repository configuration.
        """
        return self._repository.config()

    @property
    def storage(self) -> Storage:
        """
        Get a copy of this repository's Storage instance.

        Returns
        -------
        Storage
            The repository storage instance.
        """
        return self._repository.storage()

    @property
    def authorized_virtual_container_prefixes(self) -> set[str]:
        """
        Get all authorized virtual chunk container prefixes.

        Returns
        -------
        url_prefixes: set[str]
            The set of authorized url prefixes for each virtual chunk container
        """
        return self._repository.authorized_virtual_container_prefixes

    def reopen(
        self,
        config: RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict[str, AnyCredential | None] | None = None,
    ) -> Self:
        """
        Reopen the repository with new configuration or credentials.

        Parameters
        ----------
        config : RepositoryConfig, optional
            The new repository configuration. If not provided, uses the existing configuration.
        authorize_virtual_chunk_access : dict[str, AnyCredential | None], optional
            New virtual chunk access credentials.

        Returns
        -------
        Self
            A new Repository instance with the updated configuration.
        """
        return self.__class__(
            self._repository.reopen(
                config=config,
                authorize_virtual_chunk_access=authorize_virtual_chunk_access,
            )
        )

    async def reopen_async(
        self,
        config: RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict[str, AnyCredential | None] | None = None,
    ) -> Self:
        """
        Reopen the repository with new configuration or credentials (async version).

        Parameters
        ----------
        config : RepositoryConfig, optional
            The new repository configuration. If not provided, uses the existing configuration.
        authorize_virtual_chunk_access : dict[str, AnyCredential | None], optional
            New virtual chunk access credentials.

        Returns
        -------
        Self
            A new Repository instance with the updated configuration.
        """
        return self.__class__(
            await self._repository.reopen_async(
                config=config,
                authorize_virtual_chunk_access=authorize_virtual_chunk_access,
            )
        )

    def set_default_commit_metadata(self, metadata: dict[str, Any]) -> None:
        """
        Set the default commit metadata for the repository. This is useful for providing
        addition static system conexted metadata to all commits.

        When a commit is made, the metadata will be merged with the metadata provided, with any
        duplicate keys being overwritten by the metadata provided in the commit.

        !!! warning
            This metadata is only applied to sessions that are created after this call. Any open
            writable sessions will not be affected and will not use the new default metadata.

        Parameters
        ----------
        metadata : dict[str, Any]
            The default commit metadata. Pass an empty dict to clear the default metadata.
        """
        return self._repository.set_default_commit_metadata(metadata)

    def default_commit_metadata(self) -> dict[str, Any]:
        """
        Get the current configured default commit metadata for the repository.

        Returns
        -------
        dict[str, Any]
            The default commit metadata.
        """
        return self._repository.default_commit_metadata()

    def ancestry(
        self,
        *,
        branch: str | None = None,
        tag: str | None = None,
        snapshot_id: str | None = None,
    ) -> Iterator[SnapshotInfo]:
        """
        Get the ancestry of a snapshot.

        Parameters
        ----------
        branch : str, optional
            The branch to get the ancestry of.
        tag : str, optional
            The tag to get the ancestry of.
        snapshot_id : str, optional
            The snapshot ID to get the ancestry of.

        Returns
        -------
        list[SnapshotInfo]
            The ancestry of the snapshot, listing out the snapshots and their metadata.

        Notes
        -----
        Only one of the arguments can be specified.
        """

        # the returned object is both an Async and Sync iterator
        res = cast(
            Iterator[SnapshotInfo],
            self._repository.async_ancestry(
                branch=branch, tag=tag, snapshot_id=snapshot_id
            ),
        )
        return res

    def async_ancestry(
        self,
        *,
        branch: str | None = None,
        tag: str | None = None,
        snapshot_id: str | None = None,
    ) -> AsyncIterator[SnapshotInfo]:
        """
        Get the ancestry of a snapshot.

        Parameters
        ----------
        branch : str, optional
            The branch to get the ancestry of.
        tag : str, optional
            The tag to get the ancestry of.
        snapshot_id : str, optional
            The snapshot ID to get the ancestry of.

        Returns
        -------
        list[SnapshotInfo]
            The ancestry of the snapshot, listing out the snapshots and their metadata.

        Notes
        -----
        Only one of the arguments can be specified.
        """
        return self._repository.async_ancestry(
            branch=branch, tag=tag, snapshot_id=snapshot_id
        )

    def create_branch(self, branch: str, snapshot_id: str) -> None:
        """
        Create a new branch at the given snapshot.

        Parameters
        ----------
        branch : str
            The name of the branch to create.
        snapshot_id : str
            The snapshot ID to create the branch at.

        Returns
        -------
        None
        """
        self._repository.create_branch(branch, snapshot_id)

    async def create_branch_async(self, branch: str, snapshot_id: str) -> None:
        """
        Create a new branch at the given snapshot (async version).

        Parameters
        ----------
        branch : str
            The name of the branch to create.
        snapshot_id : str
            The snapshot ID to create the branch at.

        Returns
        -------
        None
        """
        await self._repository.create_branch_async(branch, snapshot_id)

    def list_branches(self) -> set[str]:
        """
        List the branches in the repository.

        Returns
        -------
        set[str]
            A set of branch names.
        """
        return self._repository.list_branches()

    async def list_branches_async(self) -> set[str]:
        """
        List the branches in the repository (async version).

        Returns
        -------
        set[str]
            A set of branch names.
        """
        return await self._repository.list_branches_async()

    def lookup_branch(self, branch: str) -> str:
        """
        Get the tip snapshot ID of a branch.

        Parameters
        ----------
        branch : str
            The branch to get the tip of.

        Returns
        -------
        str
            The snapshot ID of the tip of the branch.
        """
        return self._repository.lookup_branch(branch)

    async def lookup_branch_async(self, branch: str) -> str:
        """
        Get the tip snapshot ID of a branch (async version).

        Parameters
        ----------
        branch : str
            The branch to get the tip of.

        Returns
        -------
        str
            The snapshot ID of the tip of the branch.
        """
        return await self._repository.lookup_branch_async(branch)

    def lookup_snapshot(self, snapshot_id: str) -> SnapshotInfo:
        """
        Get the SnapshotInfo given a snapshot ID

        Parameters
        ----------
        snapshot_id : str
            The id of the snapshot to look up

        Returns
        -------
        SnapshotInfo
        """
        return self._repository.lookup_snapshot(snapshot_id)

    async def lookup_snapshot_async(self, snapshot_id: str) -> SnapshotInfo:
        """
        Get the SnapshotInfo given a snapshot ID (async version)

        Parameters
        ----------
        snapshot_id : str
            The id of the snapshot to look up

        Returns
        -------
        SnapshotInfo
        """
        return await self._repository.lookup_snapshot_async(snapshot_id)

    def reset_branch(
        self, branch: str, snapshot_id: str, *, from_snapshot_id: str | None = None
    ) -> None:
        """
        Reset a branch to a specific snapshot.

        This will permanently alter the history of the branch such that the tip of
        the branch is the specified snapshot.

        Parameters
        ----------
        branch : str
            The branch to reset.
        snapshot_id : str
            The snapshot ID to reset the branch to.
        from_snapshot_id : str | None
            If passed, the reset will only be executed if the branch currently
            points to from_snapshot_id.

        Returns
        -------
        None
        """
        self._repository.reset_branch(branch, snapshot_id, from_snapshot_id)

    async def reset_branch_async(
        self, branch: str, snapshot_id: str, *, from_snapshot_id: str | None = None
    ) -> None:
        """
        Reset a branch to a specific snapshot (async version).

        This will permanently alter the history of the branch such that the tip of
        the branch is the specified snapshot.

        Parameters
        ----------
        branch : str
            The branch to reset.
        snapshot_id : str
            The snapshot ID to reset the branch to.
        from_snapshot_id : str | None
            If passed, the reset will only be executed if the branch currently
            points to from_snapshot_id.

        Returns
        -------
        None
        """
        await self._repository.reset_branch_async(branch, snapshot_id, from_snapshot_id)

    def delete_branch(self, branch: str) -> None:
        """
        Delete a branch.

        Parameters
        ----------
        branch : str
            The branch to delete.

        Returns
        -------
        None
        """
        self._repository.delete_branch(branch)

    async def delete_branch_async(self, branch: str) -> None:
        """
        Delete a branch (async version).

        Parameters
        ----------
        branch : str
            The branch to delete.

        Returns
        -------
        None
        """
        await self._repository.delete_branch_async(branch)

    def delete_tag(self, tag: str) -> None:
        """
        Delete a tag.

        Parameters
        ----------
        tag : str
            The tag to delete.

        Returns
        -------
        None
        """
        self._repository.delete_tag(tag)

    async def delete_tag_async(self, tag: str) -> None:
        """
        Delete a tag (async version).

        Parameters
        ----------
        tag : str
            The tag to delete.

        Returns
        -------
        None
        """
        await self._repository.delete_tag_async(tag)

    def create_tag(self, tag: str, snapshot_id: str) -> None:
        """
        Create a new tag at the given snapshot.

        Parameters
        ----------
        tag : str
            The name of the tag to create.
        snapshot_id : str
            The snapshot ID to create the tag at.

        Returns
        -------
        None
        """
        self._repository.create_tag(tag, snapshot_id)

    async def create_tag_async(self, tag: str, snapshot_id: str) -> None:
        """
        Create a new tag at the given snapshot (async version).

        Parameters
        ----------
        tag : str
            The name of the tag to create.
        snapshot_id : str
            The snapshot ID to create the tag at.

        Returns
        -------
        None
        """
        await self._repository.create_tag_async(tag, snapshot_id)

    def list_tags(self) -> set[str]:
        """
        List the tags in the repository.

        Returns
        -------
        set[str]
            A set of tag names.
        """
        return self._repository.list_tags()

    async def list_tags_async(self) -> set[str]:
        """
        List the tags in the repository (async version).

        Returns
        -------
        set[str]
            A set of tag names.
        """
        return await self._repository.list_tags_async()

    def lookup_tag(self, tag: str) -> str:
        """
        Get the snapshot ID of a tag.

        Parameters
        ----------
        tag : str
            The tag to get the snapshot ID of.

        Returns
        -------
        str
            The snapshot ID of the tag.
        """
        return self._repository.lookup_tag(tag)

    async def lookup_tag_async(self, tag: str) -> str:
        """
        Get the snapshot ID of a tag (async version).

        Parameters
        ----------
        tag : str
            The tag to get the snapshot ID of.

        Returns
        -------
        str
            The snapshot ID of the tag.
        """
        return await self._repository.lookup_tag_async(tag)

    def diff(
        self,
        *,
        from_branch: str | None = None,
        from_tag: str | None = None,
        from_snapshot_id: str | None = None,
        to_branch: str | None = None,
        to_tag: str | None = None,
        to_snapshot_id: str | None = None,
    ) -> Diff:
        """
        Compute an overview of the operations executed from version `from` to version `to`.

        Both versions, `from` and `to`, must be identified. Identification can be done using a branch, tag or snapshot id.
        The styles used to identify the `from` and `to` versions can be different.

        The `from` version must be a member of the `ancestry` of `to`.

        Returns
        -------
        Diff
            The operations executed between the two versions
        """
        return self._repository.diff(
            from_branch=from_branch,
            from_tag=from_tag,
            from_snapshot_id=from_snapshot_id,
            to_branch=to_branch,
            to_tag=to_tag,
            to_snapshot_id=to_snapshot_id,
        )

    async def diff_async(
        self,
        *,
        from_branch: str | None = None,
        from_tag: str | None = None,
        from_snapshot_id: str | None = None,
        to_branch: str | None = None,
        to_tag: str | None = None,
        to_snapshot_id: str | None = None,
    ) -> Diff:
        """
        Compute an overview of the operations executed from version `from` to version `to` (async version).

        Both versions, `from` and `to`, must be identified. Identification can be done using a branch, tag or snapshot id.
        The styles used to identify the `from` and `to` versions can be different.

        The `from` version must be a member of the `ancestry` of `to`.

        Returns
        -------
        Diff
            The operations executed between the two versions
        """
        return await self._repository.diff_async(
            from_branch=from_branch,
            from_tag=from_tag,
            from_snapshot_id=from_snapshot_id,
            to_branch=to_branch,
            to_tag=to_tag,
            to_snapshot_id=to_snapshot_id,
        )

    def readonly_session(
        self,
        branch: str | None = None,
        *,
        tag: str | None = None,
        snapshot_id: str | None = None,
        as_of: datetime.datetime | None = None,
    ) -> Session:
        """
        Create a read-only session.

        This can be thought of as a read-only checkout of the repository at a given snapshot.
        When branch or tag are provided, the session will be based on the tip of the branch or
        the snapshot ID of the tag.

        Parameters
        ----------
        branch : str, optional
            If provided, the branch to create the session on.
        tag : str, optional
            If provided, the tag to create the session on.
        snapshot_id : str, optional
            If provided, the snapshot ID to create the session on.
        as_of: datetime.datetime, optional
            When combined with the branch argument, it will open the session at the last
            snapshot that is at or before this datetime

        Returns
        -------
        Session
            The read-only session, pointing to the specified snapshot, tag, or branch.

        Notes
        -----
        Only one of the arguments can be specified.
        """
        return Session(
            self._repository.readonly_session(
                branch=branch, tag=tag, snapshot_id=snapshot_id, as_of=as_of
            )
        )

    async def readonly_session_async(
        self,
        branch: str | None = None,
        *,
        tag: str | None = None,
        snapshot_id: str | None = None,
        as_of: datetime.datetime | None = None,
    ) -> Session:
        """
        Create a read-only session (async version).

        This can be thought of as a read-only checkout of the repository at a given snapshot.
        When branch or tag are provided, the session will be based on the tip of the branch or
        the snapshot ID of the tag.

        Parameters
        ----------
        branch : str, optional
            If provided, the branch to create the session on.
        tag : str, optional
            If provided, the tag to create the session on.
        snapshot_id : str, optional
            If provided, the snapshot ID to create the session on.
        as_of: datetime.datetime, optional
            When combined with the branch argument, it will open the session at the last
            snapshot that is at or before this datetime

        Returns
        -------
        Session
            The read-only session, pointing to the specified snapshot, tag, or branch.

        Notes
        -----
        Only one of the arguments can be specified.
        """
        return Session(
            await self._repository.readonly_session_async(
                branch=branch, tag=tag, snapshot_id=snapshot_id, as_of=as_of
            )
        )

    def writable_session(self, branch: str) -> Session:
        """
        Create a writable session on a branch.

        Like the read-only session, this can be thought of as a checkout of the repository at the
        tip of the branch. However, this session is writable and can be used to make changes to the
        repository. When ready, the changes can be committed to the branch, after which the session will
        become a read-only session on the new snapshot.

        Parameters
        ----------
        branch : str
            The branch to create the session on.

        Returns
        -------
        Session
            The writable session on the branch.
        """
        return Session(self._repository.writable_session(branch))

    async def writable_session_async(self, branch: str) -> Session:
        """
        Create a writable session on a branch (async version).

        Like the read-only session, this can be thought of as a checkout of the repository at the
        tip of the branch. However, this session is writable and can be used to make changes to the
        repository. When ready, the changes can be committed to the branch, after which the session will
        become a read-only session on the new snapshot.

        Parameters
        ----------
        branch : str
            The branch to create the session on.

        Returns
        -------
        Session
            The writable session on the branch.
        """
        return Session(await self._repository.writable_session_async(branch))

    @contextmanager
    def transaction(
        self,
        branch: str,
        *,
        message: str,
        metadata: dict[str, Any] | None = None,
        rebase_with: ConflictSolver | None = None,
        rebase_tries: int = 1_000,
    ) -> Iterator[IcechunkStore]:
        """
        Create a transaction on a branch.

        This is a context manager that creates a writable session on the specified branch.
        When the context is exited, the session will be committed to the branch
        using the specified message.

        Parameters
        ----------
        branch : str
            The branch to create the transaction on.
        message : str
            The commit message to use when committing the session.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.
        rebase_with : ConflictSolver | None, optional
            If other session committed while the current session was writing, use Session.rebase with this solver.
        rebase_tries : int, optional
            If other session committed while the current session was writing, use Session.rebase up to this many times in a loop.

        Yields
        -------
        store : IcechunkStore
            A Zarr Store which can be used to interact with the data in the repository.
        """
        session = self.writable_session(branch)
        yield session.store
        session.commit(
            message=message,
            metadata=metadata,
            rebase_with=rebase_with,
            rebase_tries=rebase_tries,
        )

    def expire_snapshots(
        self,
        older_than: datetime.datetime,
        *,
        delete_expired_branches: bool = False,
        delete_expired_tags: bool = False,
    ) -> set[str]:
        """Expire all snapshots older than a threshold.

        This processes snapshots found by navigating all references in
        the repo, tags first, branches leter, both in lexicographical order.

        Returns the ids of all snapshots considered expired and skipped
        from history. Notice that this snapshot are not necessarily
        available for garbage collection, they could still be pointed by
        ether refs.

        If `delete_expired_*` is set to True, branches or tags that, after the
        expiration process, point to expired snapshots directly, will be
        deleted.

        Danger
        ------
        This is an administrative operation, it should be run
        carefully. The repository can still operate concurrently while
        `expire_snapshots` runs, but other readers can get inconsistent
        views of the repository history.

        Parameters
        ----------
        older_than: datetime.datetime
            Expire snapshots older than this time.
        delete_expired_branches: bool, optional
            Whether to delete any branches that now have only expired snapshots.
        delete_expired_tags: bool, optional
            Whether to delete any tags associated with expired snapshots

        Returns
        -------
        set of expires snapshot IDs
        """
        return self._repository.expire_snapshots(
            older_than,
            delete_expired_branches=delete_expired_branches,
            delete_expired_tags=delete_expired_tags,
        )

    async def expire_snapshots_async(
        self,
        older_than: datetime.datetime,
        *,
        delete_expired_branches: bool = False,
        delete_expired_tags: bool = False,
    ) -> set[str]:
        """Expire all snapshots older than a threshold (async version).

        This processes snapshots found by navigating all references in
        the repo, tags first, branches leter, both in lexicographical order.

        Returns the ids of all snapshots considered expired and skipped
        from history. Notice that this snapshot are not necessarily
        available for garbage collection, they could still be pointed by
        ether refs.

        If `delete_expired_*` is set to True, branches or tags that, after the
        expiration process, point to expired snapshots directly, will be
        deleted.

        Danger
        ------
        This is an administrative operation, it should be run
        carefully. The repository can still operate concurrently while
        `expire_snapshots` runs, but other readers can get inconsistent
        views of the repository history.

        Parameters
        ----------
        older_than: datetime.datetime
            Expire snapshots older than this time.
        delete_expired_branches: bool, optional
            Whether to delete any branches that now have only expired snapshots.
        delete_expired_tags: bool, optional
            Whether to delete any tags associated with expired snapshots

        Returns
        -------
        set of expires snapshot IDs
        """
        return await self._repository.expire_snapshots_async(
            older_than,
            delete_expired_branches=delete_expired_branches,
            delete_expired_tags=delete_expired_tags,
        )

    def rewrite_manifests(
        self, message: str, *, branch: str, metadata: dict[str, Any] | None = None
    ) -> str:
        """
        Rewrite manifests for all arrays.

        This method will start a new writable session on the specified branch,
        rewrite manifests for all arrays, and then commits with the specifeid ``message``
        and ``metadata``.

        A JSON representation of the currently active splitting configuration will be
        stored in the commit's metadata under the key `"splitting_config"`.

        Parameters
        ----------
        message : str
            The message to write with the commit.
        branch: str
            The branch to commit to.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.

        Returns
        -------
        str
            The snapshot ID of the new commit.

        """
        return self._repository.rewrite_manifests(
            message, branch=branch, metadata=metadata
        )

    async def rewrite_manifests_async(
        self, message: str, *, branch: str, metadata: dict[str, Any] | None = None
    ) -> str:
        """
        Rewrite manifests for all arrays (async version).

        This method will start a new writable session on the specified branch,
        rewrite manifests for all arrays, and then commits with the specifeid ``message``
        and ``metadata``.

        A JSON representation of the currently active splitting configuration will be
        stored in the commit's metadata under the key `"splitting_config"`.

        Parameters
        ----------
        message : str
            The message to write with the commit.
        branch: str
            The branch to commit to.
        metadata : dict[str, Any] | None, optional
            Additional metadata to store with the commit snapshot.

        Returns
        -------
        str
            The snapshot ID of the new commit.

        """
        return await self._repository.rewrite_manifests_async(
            message, branch=branch, metadata=metadata
        )

    def garbage_collect(
        self,
        delete_object_older_than: datetime.datetime,
        *,
        dry_run: bool = False,
        max_snapshots_in_memory: int = 50,
        max_compressed_manifest_mem_bytes: int = 512 * 1024 * 1024,
        max_concurrent_manifest_fetches: int = 500,
    ) -> GCSummary:
        """Delete any objects no longer accessible from any branches or tags.

        Danger
        ------
        This is an administrative operation, it should be run
        carefully. The repository can still operate concurrently while
        `garbage_collect` runs, but other reades can get inconsistent
        views if they are trying to access the expired snapshots.

        Parameters
        ----------
        delete_object_older_than: datetime.datetime
            Delete objects older than this time.
        dry_run: bool : bool
            Report results but don't delete any objects
        max_snapshots_in_memory : int
            Don't prefetch more than this many Snapshots to memory.
        max_compressed_manifest_mem_bytes : int
            Don't use more than this memory to store compressed in-flight manifests.
        max_concurrent_manifest_fetches : int
            Don't run more than this many concurrent manifest fetches.

        Returns
        -------
        GCSummary
            Summary of objects deleted.
        """

        return self._repository.garbage_collect(
            delete_object_older_than,
            dry_run=dry_run,
            max_snapshots_in_memory=max_snapshots_in_memory,
            max_compressed_manifest_mem_bytes=max_compressed_manifest_mem_bytes,
            max_concurrent_manifest_fetches=max_concurrent_manifest_fetches,
        )

    async def garbage_collect_async(
        self,
        delete_object_older_than: datetime.datetime,
        *,
        dry_run: bool = False,
        max_snapshots_in_memory: int = 50,
        max_compressed_manifest_mem_bytes: int = 512 * 1024 * 1024,
        max_concurrent_manifest_fetches: int = 500,
    ) -> GCSummary:
        """Delete any objects no longer accessible from any branches or tags (async version).

        Danger
        ------
        This is an administrative operation, it should be run
        carefully. The repository can still operate concurrently while
        `garbage_collect` runs, but other reades can get inconsistent
        views if they are trying to access the expired snapshots.

        Parameters
        ----------
        delete_object_older_than: datetime.datetime
            Delete objects older than this time.
        dry_run: bool : bool
            Report results but don't delete any objects
        max_snapshots_in_memory : int
            Don't prefetch more than this many Snapshots to memory.
        max_compressed_manifest_mem_bytes : int
            Don't use more than this memory to store compressed in-flight manifests.
        max_concurrent_manifest_fetches : int
            Don't run more than this many concurrent manifest fetches.

        Returns
        -------
        GCSummary
            Summary of objects deleted.
        """

        return await self._repository.garbage_collect_async(
            delete_object_older_than,
            dry_run=dry_run,
            max_snapshots_in_memory=max_snapshots_in_memory,
            max_compressed_manifest_mem_bytes=max_compressed_manifest_mem_bytes,
            max_concurrent_manifest_fetches=max_concurrent_manifest_fetches,
        )

    def total_chunks_storage(
        self,
        *,
        max_snapshots_in_memory: int = 50,
        max_compressed_manifest_mem_bytes: int = 512 * 1024 * 1024,
        max_concurrent_manifest_fetches: int = 500,
    ) -> int:
        """Calculate the total storage used for chunks, in bytes .

        It reports the storage needed to store all snapshots in the repository that
        are reachable from any branches or tags. Unreachable snapshots can be generated
        by using `reset_branch` or `expire_snapshots`. The chunks for these snapshots
        are not included in the result, and they should probably be deleted using
        `garbage_collection`.

        The result includes only native chunks, not adding virtual or inline chunks.

        Parameters
        ----------
        max_snapshots_in_memory: int
            Don't prefetch more than this many Snapshots to memory.
        max_compressed_manifest_mem_bytes : int
            Don't use more than this memory to store compressed in-flight manifests.
        max_concurrent_manifest_fetches : int
            Don't run more than this many concurrent manifest fetches.
        """

        return self._repository.total_chunks_storage(
            max_snapshots_in_memory=max_snapshots_in_memory,
            max_compressed_manifest_mem_bytes=max_compressed_manifest_mem_bytes,
            max_concurrent_manifest_fetches=max_concurrent_manifest_fetches,
        )

    async def total_chunks_storage_async(
        self,
        *,
        max_snapshots_in_memory: int = 50,
        max_compressed_manifest_mem_bytes: int = 512 * 1024 * 1024,
        max_concurrent_manifest_fetches: int = 500,
    ) -> int:
        """Calculate the total storage used for chunks, in bytes (async version).

        It reports the storage needed to store all snapshots in the repository that
        are reachable from any branches or tags. Unreachable snapshots can be generated
        by using `reset_branch` or `expire_snapshots`. The chunks for these snapshots
        are not included in the result, and they should probably be deleted using
        `garbage_collection`.

        The result includes only native chunks, not adding virtual or inline chunks.

        Parameters
        ----------
        max_snapshots_in_memory: int
            Don't prefetch more than this many Snapshots to memory.
        max_compressed_manifest_mem_bytes : int
            Don't use more than this memory to store compressed in-flight manifests.
        max_concurrent_manifest_fetches : int
            Don't run more than this many concurrent manifest fetches.
        """

        return await self._repository.total_chunks_storage_async(
            max_snapshots_in_memory=max_snapshots_in_memory,
            max_compressed_manifest_mem_bytes=max_compressed_manifest_mem_bytes,
            max_concurrent_manifest_fetches=max_concurrent_manifest_fetches,
        )

    def inspect_snapshot(self, snapshot_id: str, *, pretty: bool = True) -> str:
        return self._repository.inspect_snapshot(snapshot_id, pretty=pretty)

    async def inspect_snapshot_async(
        self, snapshot_id: str, *, pretty: bool = True
    ) -> str:
        return await self._repository.inspect_snapshot_async(snapshot_id, pretty=pretty)
