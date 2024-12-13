from typing import Self

from icechunk._icechunk_python import (
    PyRepository,
    RepositoryConfig,
    SnapshotMetadata,
    StorageConfig,
)
from icechunk.session import Session


class Repository:
    _repository: PyRepository

    def __init__(self, repository: PyRepository):
        self._repository = repository

    @classmethod
    def create(
        cls, storage, config: RepositoryConfig | None = None, virtual_ref_config=None
    ) -> Self:
        return cls(
            PyRepository.create(
                storage, config=config, virtual_ref_config=virtual_ref_config
            )
        )

    @classmethod
    def open(
        cls, storage, *, config: RepositoryConfig | None = None, virtual_ref_config=None
    ) -> Self:
        return cls(
            PyRepository.open(
                storage, config=config, virtual_ref_config=virtual_ref_config
            )
        )

    @classmethod
    def open_or_create(
        cls,
        storage: StorageConfig,
        *,
        config: RepositoryConfig | None = None,
        virtual_ref_config=None,
    ) -> Self:
        return cls(
            PyRepository.open_or_create(
                storage, config=config, virtual_ref_config=virtual_ref_config
            )
        )

    @staticmethod
    def exists(storage: StorageConfig) -> bool:
        return PyRepository.exists(storage)

    def ancestry(self, snapshot_id: str) -> list[SnapshotMetadata]:
        return self._repository.ancestry(snapshot_id)

    def create_branch(self, branch: str, snapshot_id: str) -> None:
        self._repository.create_branch(branch, snapshot_id)

    def list_branches(self) -> list[str]:
        return self._repository.list_branches()

    def branch_tip(self, branch: str) -> str:
        return self._repository.branch_tip(branch)

    def reset_branch(self, branch: str, snapshot_id: str) -> None:
        self._repository.reset_branch(branch, snapshot_id)

    def create_tag(self, tag: str, snapshot_id: str) -> None:
        self._repository.create_tag(tag, snapshot_id)

    def list_tags(self) -> list[str]:
        return self._repository.list_tags()

    def tag(self, tag: str) -> str:
        return self._repository.tag(tag)

    def readonly_session(
        self,
        *,
        branch=None,
        tag=None,
        snapshot_id=None,
    ) -> Session:
        return Session(
            self._repository.readonly_session(
                branch=branch, tag=tag, snapshot_id=snapshot_id
            )
        )

    def writeable_session(self, branch: str) -> Session:
        return Session(self._repository.writeable_session(branch))
