"""The Icechunk exception tree.

All exceptions raised by Icechunk derive from
[`IcechunkError`][icechunk.IcechunkError], grouped into categories by what a
handler would do about them. Every instance carries:

- `message`: the chain of causes followed by the operation context (also what
  `str(e)` returns). The full diagnostic report is attached as a PEP 678
  note, visible in tracebacks.
- `kind`: a stable machine-readable code (see
  [`ErrorKind`][icechunk.ErrorKind]) identifying the precise failure without
  needing one class per failure.

The tree::

    IcechunkError
    ├── ConflictError
    │   └── RebaseFailedError
    ├── NotFoundError                 (also a KeyError)
    │   ├── NodeNotFoundError
    │   ├── SnapshotNotFoundError
    │   ├── RefNotFoundError
    │   └── RepositoryNotFoundError
    ├── AlreadyExistsError
    ├── ReadOnlyError
    ├── InvalidInputError             (also a ValueError)
    ├── SessionStateError
    ├── StorageError
    ├── FormatError
    └── InternalError
"""

from enum import StrEnum

from icechunk._icechunk_python import (
    ConflictError,
    IcechunkError,
    RebaseFailedError,
)


class ErrorKind(StrEnum):
    """Stable machine-readable codes for the `kind` attribute of [`IcechunkError`][icechunk.IcechunkError].

    Codes are append-only: new ones may appear in any release, but existing
    ones are not renamed or removed.
    """

    # conflicts
    COMMIT_CONFLICT = "commit-conflict"
    BRANCH_UPDATE_CONFLICT = "branch-update-conflict"
    REBASE_FAILED = "rebase-failed"
    CONFIG_UPDATED = "config-updated"
    REPO_INFO_UPDATED = "repo-info-updated"
    UPDATE_ATTEMPTS_EXHAUSTED = "update-attempts-exhausted"
    # not found
    NODE_NOT_FOUND = "node-not-found"
    ANCESTOR_NODE_NOT_FOUND = "ancestor-node-not-found"
    CHUNK_NOT_FOUND = "chunk-not-found"
    KEY_NOT_FOUND = "key-not-found"
    SNAPSHOT_NOT_FOUND = "snapshot-not-found"
    NO_SNAPSHOT_AT_TIME = "no-snapshot-at-time"
    REF_NOT_FOUND = "ref-not-found"
    BRANCH_NOT_FOUND = "branch-not-found"
    TAG_NOT_FOUND = "tag-not-found"
    REPOSITORY_NOT_FOUND = "repository-not-found"
    MANIFEST_INFO_NOT_FOUND = "manifest-info-not-found"
    # already exists
    NODE_ALREADY_EXISTS = "node-already-exists"
    BRANCH_ALREADY_EXISTS = "branch-already-exists"
    TAG_ALREADY_EXISTS = "tag-already-exists"
    TAG_PREVIOUSLY_DELETED = "tag-previously-deleted"
    MOVE_WONT_OVERWRITE = "move-wont-overwrite"
    # read only
    READ_ONLY_SESSION = "read-only-session"
    READ_ONLY_STORE = "read-only-store"
    READ_ONLY_STORAGE = "read-only-storage"
    READ_ONLY_REPOSITORY = "read-only-repository"
    # invalid input
    INVALID_KEY = "invalid-key"
    INVALID_CHUNK_INDEX = "invalid-chunk-index"
    INVALID_MANIFEST_SPLIT_INDEX = "invalid-manifest-split-index"
    INVALID_REF_TYPE = "invalid-ref-type"
    INVALID_REF_NAME = "invalid-ref-name"
    INVALID_SNAPSHOT_ID = "invalid-snapshot-id"
    INVALID_PATH = "invalid-path"
    BAD_KEY_PREFIX = "bad-key-prefix"
    BAD_METADATA = "bad-metadata"
    BAD_PREFIX = "bad-prefix"
    NOT_AN_ARRAY = "not-an-array"
    NOT_A_GROUP = "not-a-group"
    INVALID_BYTE_RANGE = "invalid-byte-range"
    INVALID_COMMIT_CONFIGURATION = "invalid-commit-configuration"
    INVALID_ARRAY_METADATA = "invalid-array-metadata"
    INVALID_FEATURE_FLAG = "invalid-feature-flag"
    INVALID_VIRTUAL_REF = "invalid-virtual-ref"
    INVALID_CREDENTIALS = "invalid-credentials"
    NO_CONTAINER_FOR_VIRTUAL_CHUNK = "no-container-for-virtual-chunk"
    UNAUTHORIZED_VIRTUAL_CHUNK_CONTAINER = "unauthorized-virtual-chunk-container"
    EMPTY_PREFIX_CREATION = "empty-prefix-creation"
    PARENT_DIRECTORY_NOT_CLEAN = "parent-directory-not-clean"
    BAD_SNAPSHOT_CHAIN_FOR_DIFF = "bad-snapshot-chain-for-diff"
    # session/repository state
    COMMIT_NOT_ALLOWED = "commit-not-allowed"
    MERGE_NOT_ALLOWED = "merge-not-allowed"
    CANNOT_FORK_READ_ONLY_SESSION = "cannot-fork-read-only-session"
    REARRANGE_SESSION_ONLY = "rearrange-session-only"
    NON_REARRANGE_SESSION = "non-rearrange-session"
    MOVE_INTO_SELF_OR_DESCENDANT = "move-into-self-or-descendant"
    MOVE_DESTINATION_PARENT_MISSING = "move-destination-parent-missing"
    MOVE_DESTINATION_PARENT_NOT_GROUP = "move-destination-parent-not-group"
    NO_CHANGES_TO_COMMIT = "no-changes-to-commit"
    NO_AMEND_FOR_INITIAL_COMMIT = "no-amend-for-initial-commit"
    NOT_ON_BRANCH = "not-on-branch"
    NO_SNAPSHOT = "no-snapshot"
    CANNOT_DELETE_MAIN = "cannot-delete-main"
    UNCOMMITTED_CHANGES = "uncommitted-changes"
    MERGE_FAILED = "merge-failed"
    NOT_ALLOWED = "not-allowed"
    UNIMPLEMENTED = "unimplemented"
    REBASE_TX_LOG_PRUNED = "rebase-tx-log-pruned"
    FEATURE_FLAG_DISABLED = "feature-flag-disabled"
    TAG_ERROR = "tag-error"
    INVALID_REPOSITORY_MIGRATION = "invalid-repository-migration"
    # storage / environment
    OBJECT_NOT_FOUND = "object-not-found"
    OBJECT_STORE = "object-store"
    IO = "io"
    STORAGE_CONFIG = "storage-config"
    CANNOT_PARSE_URL = "cannot-parse-url"
    BAD_REDIRECT = "bad-redirect"
    SEMAPHORE_ACQUIRE = "semaphore-acquire"
    STORAGE_OTHER = "storage-other"
    VIRTUAL_CHUNK_FETCH = "virtual-chunk-fetch"
    VIRTUAL_CHUNK_MODIFIED = "virtual-chunk-modified"
    INVALID_OBJECT_SIZE = "invalid-object-size"
    # format
    SERIALIZATION = "serialization"
    DESERIALIZATION = "deserialization"
    CONFIG_DESERIALIZATION = "config-deserialization"
    INVALID_FILE_FORMAT = "invalid-file-format"
    UNSUPPORTED_SPEC_VERSION = "unsupported-spec-version"
    UNSUPPORTED_OPERATION_FOR_VERSION = "unsupported-operation-for-version"
    SNAPSHOT_TIMESTAMP = "snapshot-timestamp"
    DUPLICATE_SNAPSHOT_ID = "duplicate-snapshot-id"
    # internal
    CONCURRENCY = "concurrency"
    FLUSH = "flush"
    PARTIAL_VALUES_PANIC = "partial-values-panic"
    CONFLICTING_PATH_NOT_FOUND = "conflicting-path-not-found"
    INCONSISTENT_MANIFESTS = "inconsistent-manifests"
    POISON_LOCK = "poison-lock"
    PICKLE = "pickle"
    UNKNOWN = "unknown"


class NotFoundError(IcechunkError, KeyError):
    """Something that was asked for does not exist.

    Also a `KeyError`, so existing `except KeyError` handlers keep working.
    """


class NodeNotFoundError(NotFoundError):
    """A group, array, or chunk does not exist at the given path."""


class SnapshotNotFoundError(NotFoundError):
    """No snapshot exists with the given id (or at the given time)."""


class RefNotFoundError(NotFoundError):
    """No branch or tag exists with the given name."""


class RepositoryNotFoundError(NotFoundError):
    """No Icechunk repository exists at the given storage location."""


class AlreadyExistsError(IcechunkError):
    """Something that was being created already exists (node, branch, tag...)."""


class ReadOnlyError(IcechunkError):
    """A write was attempted through a read-only session, store, or repository."""


class InvalidInputError(IcechunkError, ValueError):
    """An argument, key, index, or metadata value is invalid.

    Also a `ValueError`, so existing `except ValueError` handlers keep working.
    """


class SessionStateError(IcechunkError):
    """The operation is not allowed in the current session or repository state.

    Examples: committing a fork, moving nodes outside a rearrange session,
    committing without changes, deleting the main branch.
    """


class StorageError(IcechunkError):
    """The object store or local storage failed, or is misconfigured.

    These errors are environmental and often transient.
    """


class FormatError(IcechunkError):
    """On-disk data could not be read or written: corruption, (de)serialization
    failures, or an unsupported Icechunk spec version."""


class InternalError(IcechunkError):
    """An unexpected internal error; likely a bug worth reporting to
    https://github.com/earth-mover/icechunk/issues."""


__all__ = [
    "AlreadyExistsError",
    "ConflictError",
    "ErrorKind",
    "FormatError",
    "IcechunkError",
    "InternalError",
    "InvalidInputError",
    "NodeNotFoundError",
    "NotFoundError",
    "ReadOnlyError",
    "RebaseFailedError",
    "RefNotFoundError",
    "RepositoryNotFoundError",
    "SessionStateError",
    "SnapshotNotFoundError",
    "StorageError",
]
