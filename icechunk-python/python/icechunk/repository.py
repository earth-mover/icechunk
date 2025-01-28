from typing import Self

from icechunk._icechunk_python import (
    PyRepository,
    RepositoryConfig,
    SnapshotInfo,
    Storage,
)
from icechunk.credentials import AnyCredential
from icechunk.session import Session


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
        virtual_chunk_credentials: dict[str, AnyCredential] | None = None,
    ) -> Self:
        """
        Create a new Icechunk repository.
        If one already exists at the given store location, an error will be raised.

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.
        config : RepositoryConfig, optional
            The repository configuration. If not provided, a default configuration will be used.
        virtual_chunk_credentials : dict[str, AnyCredential], optional
            Credentials for virtual chunks.

        Returns
        -------
        Self
            An instance of the Repository class.
        """
        return cls(
            PyRepository.create(
                storage,
                config=config,
                virtual_chunk_credentials=virtual_chunk_credentials,
            )
        )

    @classmethod
    def open(
        cls,
        storage: Storage,
        config: RepositoryConfig | None = None,
        virtual_chunk_credentials: dict[str, AnyCredential] | None = None,
    ) -> Self:
        """
        Open an existing Icechunk repository.

        If no repository exists at the given storage location, an error will be raised.

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.
        config : RepositoryConfig, optional
            The repository settings. If not provided, a default configuration will be
            loaded from the repository.
        virtual_chunk_credentials : dict[str, AnyCredential], optional
            Credentials for virtual chunks.

        Returns
        -------
        Self
            An instance of the Repository class.
        """
        return cls(
            PyRepository.open(
                storage,
                config=config,
                virtual_chunk_credentials=virtual_chunk_credentials,
            )
        )

    @classmethod
    def open_or_create(
        cls,
        storage: Storage,
        config: RepositoryConfig | None = None,
        virtual_chunk_credentials: dict[str, AnyCredential] | None = None,
    ) -> Self:
        """
        Open an existing Icechunk repository or create a new one if it does not exist.

        Parameters
        ----------
        storage : Storage
            The storage configuration for the repository.
        config : RepositoryConfig, optional
            The repository settings. If not provided, a default configuration will be
            loaded from the repository.
        virtual_chunk_credentials : dict[str, AnyCredential], optional
            Credentials for virtual chunks.

        Returns
        -------
        Self
            An instance of the Repository class.
        """
        return cls(
            PyRepository.open_or_create(
                storage,
                config=config,
                virtual_chunk_credentials=virtual_chunk_credentials,
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

    def save_config(self) -> None:
        """
        Save the repository configuration to storage, this configuration will be used in future calls to Repository.open.

        Returns
        -------
        None
        """
        return self._repository.save_config()

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

    def ancestry(
        self,
        *,
        branch: str | None = None,
        tag: str | None = None,
        snapshot: str | None = None,
    ) -> list[SnapshotInfo]:
        """
        Get the ancestry of a snapshot.

        Parameters
        ----------
        branch : str, optional
            The branch to get the ancestry of.
        tag : str, optional
            The tag to get the ancestry of.
        snapshot : str, optional
            The snapshot ID to get the ancestry of.

        Returns
        -------
        list[SnapshotInfo]
            The ancestry of the snapshot, listing out the snapshots and their metadata.

        Notes
        -----
        Only one of the arguments can be specified.
        """
        return self._repository.ancestry(branch=branch, tag=tag, snapshot=snapshot)

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

    def list_branches(self) -> set[str]:
        """
        List the branches in the repository.

        Returns
        -------
        set[str]
            A set of branch names.
        """
        return self._repository.list_branches()

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

    def reset_branch(self, branch: str, snapshot_id: str) -> None:
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

        Returns
        -------
        None
        """
        self._repository.reset_branch(branch, snapshot_id)

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

    def delete_tag(self, branch: str) -> None:
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
        self._repository.delete_tag(branch)

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

    def list_tags(self) -> set[str]:
        """
        List the tags in the repository.

        Returns
        -------
        set[str]
            A set of tag names.
        """
        return self._repository.list_tags()

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

    def readonly_session(
        self,
        *,
        branch: str | None = None,
        tag: str | None = None,
        snapshot: str | None = None,
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
        snapshot : str, optional
            If provided, the snapshot ID to create the session on.

        Returns
        -------
        Session
            The read-only session, pointing to the specified snapshot, tag, or branch.

        Notes
        -----
        Only one of the arguments can be specified.
        """
        return Session(
            self._repository.readonly_session(branch=branch, tag=tag, snapshot=snapshot)
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
