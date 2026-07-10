use icechunk::{
    StorageError,
    format::{
        IcechunkFormatError, IcechunkFormatErrorKind,
        manifest::{VirtualReferenceError, VirtualReferenceErrorKind},
    },
    migrations::{MigrationError, MigrationErrorKind},
    ops::{gc::GCError, manifests::ManifestOpsError},
    refs::RefErrorKind,
    repository::{RepositoryError, RepositoryErrorKind},
    session::{SessionError, SessionErrorKind},
    storage::StorageErrorKind,
    store::{KeyNotFoundError, StoreError, StoreErrorKind},
};
use miette::{Diagnostic, GraphicalReportHandler};
use pyo3::{
    PyErr,
    conversion::IntoPyObjectExt as _,
    exceptions::{PyException, PyKeyError, PyValueError},
    prelude::*,
    sync::PyOnceLock,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{conflicts::PyConflict, impl_pickle};

/// A simple wrapper around the `StoreError` to make it easier to convert to a `PyErr`
///
/// When you use the ? operator, the error is coerced. But if you return the value it is not.
/// So for now we just use the extra operation to get the coercion instead of manually mapping
/// the errors where this is returned from a python class
#[expect(clippy::enum_variant_names)]
#[derive(Debug, Error, Diagnostic)]
#[expect(dead_code)]
pub(crate) enum PyIcechunkStoreError {
    #[error(transparent)]
    StorageError(StorageError),
    #[error(transparent)]
    StoreError(StoreError),
    #[error(transparent)]
    RepositoryError(#[from] RepositoryError),
    #[error(transparent)]
    MigrationError(#[from] MigrationError),
    #[error("session error: {0}")]
    SessionError(SessionError),
    #[error(transparent)]
    IcechunkFormatError(#[from] IcechunkFormatError),
    #[error(transparent)]
    GCError(#[from] GCError),
    #[error(transparent)]
    ManifestOpsError(#[from] ManifestOpsError),
    #[error(transparent)]
    VirtualReferenceError(#[from] VirtualReferenceError),
    #[error("{0}")]
    PyKeyError(String),
    #[error("{0}")]
    PyValueError(String),
    #[error(transparent)]
    PyError(#[from] PyErr),
    #[error("{0}")]
    PickleError(String),
    #[error("{0}")]
    UnkownError(String),
}

impl From<StoreError> for PyIcechunkStoreError {
    fn from(error: StoreError) -> Self {
        PyIcechunkStoreError::StoreError(error)
    }
}

impl From<SessionError> for PyIcechunkStoreError {
    fn from(error: SessionError) -> Self {
        PyIcechunkStoreError::SessionError(error)
    }
}

/// Stable machine-readable identifiers stamped on every raised exception as
/// its `kind` attribute. Mirrored by `icechunk.ErrorKind`; a test asserts both
/// sides stay in sync. Adding a code is non-breaking, renaming one is not.
pub(crate) mod codes {
    macro_rules! error_kinds {
        ($($name:ident => $s:literal),* $(,)?) => {
            $(pub(crate) const $name: &str = $s;)*
            pub(crate) const ALL: &[&str] = &[$($s),*];
        };
    }

    error_kinds! {
        // conflicts
        COMMIT_CONFLICT => "commit-conflict",
        BRANCH_UPDATE_CONFLICT => "branch-update-conflict",
        REBASE_FAILED => "rebase-failed",
        CONFIG_UPDATED => "config-updated",
        REPO_INFO_UPDATED => "repo-info-updated",
        UPDATE_ATTEMPTS_EXHAUSTED => "update-attempts-exhausted",
        // not found
        NODE_NOT_FOUND => "node-not-found",
        ANCESTOR_NODE_NOT_FOUND => "ancestor-node-not-found",
        CHUNK_NOT_FOUND => "chunk-not-found",
        KEY_NOT_FOUND => "key-not-found",
        SNAPSHOT_NOT_FOUND => "snapshot-not-found",
        NO_SNAPSHOT_AT_TIME => "no-snapshot-at-time",
        REF_NOT_FOUND => "ref-not-found",
        BRANCH_NOT_FOUND => "branch-not-found",
        TAG_NOT_FOUND => "tag-not-found",
        REPOSITORY_NOT_FOUND => "repository-not-found",
        MANIFEST_INFO_NOT_FOUND => "manifest-info-not-found",
        // already exists
        NODE_ALREADY_EXISTS => "node-already-exists",
        BRANCH_ALREADY_EXISTS => "branch-already-exists",
        TAG_ALREADY_EXISTS => "tag-already-exists",
        TAG_PREVIOUSLY_DELETED => "tag-previously-deleted",
        MOVE_WONT_OVERWRITE => "move-wont-overwrite",
        // read only
        READ_ONLY_SESSION => "read-only-session",
        READ_ONLY_STORE => "read-only-store",
        READ_ONLY_STORAGE => "read-only-storage",
        READ_ONLY_REPOSITORY => "read-only-repository",
        // invalid input
        INVALID_KEY => "invalid-key",
        INVALID_CHUNK_INDEX => "invalid-chunk-index",
        INVALID_MANIFEST_SPLIT_INDEX => "invalid-manifest-split-index",
        INVALID_REF_TYPE => "invalid-ref-type",
        INVALID_REF_NAME => "invalid-ref-name",
        INVALID_SNAPSHOT_ID => "invalid-snapshot-id",
        INVALID_PATH => "invalid-path",
        BAD_KEY_PREFIX => "bad-key-prefix",
        BAD_METADATA => "bad-metadata",
        BAD_PREFIX => "bad-prefix",
        NOT_AN_ARRAY => "not-an-array",
        NOT_A_GROUP => "not-a-group",
        INVALID_BYTE_RANGE => "invalid-byte-range",
        INVALID_COMMIT_CONFIGURATION => "invalid-commit-configuration",
        INVALID_ARRAY_METADATA => "invalid-array-metadata",
        INVALID_FEATURE_FLAG => "invalid-feature-flag",
        INVALID_VIRTUAL_REF => "invalid-virtual-ref",
        INVALID_CREDENTIALS => "invalid-credentials",
        NO_CONTAINER_FOR_VIRTUAL_CHUNK => "no-container-for-virtual-chunk",
        UNAUTHORIZED_VIRTUAL_CHUNK_CONTAINER => "unauthorized-virtual-chunk-container",
        EMPTY_PREFIX_CREATION => "empty-prefix-creation",
        PARENT_DIRECTORY_NOT_CLEAN => "parent-directory-not-clean",
        BAD_SNAPSHOT_CHAIN_FOR_DIFF => "bad-snapshot-chain-for-diff",
        // session/repository state
        COMMIT_NOT_ALLOWED => "commit-not-allowed",
        MERGE_NOT_ALLOWED => "merge-not-allowed",
        CANNOT_FORK_READ_ONLY_SESSION => "cannot-fork-read-only-session",
        REARRANGE_SESSION_ONLY => "rearrange-session-only",
        NON_REARRANGE_SESSION => "non-rearrange-session",
        MOVE_INTO_SELF_OR_DESCENDANT => "move-into-self-or-descendant",
        MOVE_DESTINATION_PARENT_MISSING => "move-destination-parent-missing",
        MOVE_DESTINATION_PARENT_NOT_GROUP => "move-destination-parent-not-group",
        NO_CHANGES_TO_COMMIT => "no-changes-to-commit",
        NO_AMEND_FOR_INITIAL_COMMIT => "no-amend-for-initial-commit",
        NOT_ON_BRANCH => "not-on-branch",
        NO_SNAPSHOT => "no-snapshot",
        CANNOT_DELETE_MAIN => "cannot-delete-main",
        UNCOMMITTED_CHANGES => "uncommitted-changes",
        MERGE_FAILED => "merge-failed",
        NOT_ALLOWED => "not-allowed",
        UNIMPLEMENTED => "unimplemented",
        REBASE_TX_LOG_PRUNED => "rebase-tx-log-pruned",
        FEATURE_FLAG_DISABLED => "feature-flag-disabled",
        TAG_ERROR => "tag-error",
        INVALID_REPOSITORY_MIGRATION => "invalid-repository-migration",
        // storage / environment
        OBJECT_NOT_FOUND => "object-not-found",
        OBJECT_STORE => "object-store",
        IO => "io",
        STORAGE_CONFIG => "storage-config",
        CANNOT_PARSE_URL => "cannot-parse-url",
        BAD_REDIRECT => "bad-redirect",
        SEMAPHORE_ACQUIRE => "semaphore-acquire",
        STORAGE_OTHER => "storage-other",
        VIRTUAL_CHUNK_FETCH => "virtual-chunk-fetch",
        VIRTUAL_CHUNK_MODIFIED => "virtual-chunk-modified",
        INVALID_OBJECT_SIZE => "invalid-object-size",
        // format
        SERIALIZATION => "serialization",
        DESERIALIZATION => "deserialization",
        CONFIG_DESERIALIZATION => "config-deserialization",
        INVALID_FILE_FORMAT => "invalid-file-format",
        UNSUPPORTED_SPEC_VERSION => "unsupported-spec-version",
        UNSUPPORTED_OPERATION_FOR_VERSION => "unsupported-operation-for-version",
        SNAPSHOT_TIMESTAMP => "snapshot-timestamp",
        DUPLICATE_SNAPSHOT_ID => "duplicate-snapshot-id",
        // internal
        CONCURRENCY => "concurrency",
        FLUSH => "flush",
        PARTIAL_VALUES_PANIC => "partial-values-panic",
        CONFLICTING_PATH_NOT_FOUND => "conflicting-path-not-found",
        INCONSISTENT_MANIFESTS => "inconsistent-manifests",
        POISON_LOCK => "poison-lock",
        PICKLE => "pickle",
        UNKNOWN => "unknown",
    }
}

enum Classified {
    Conflict {
        expected_parent: Option<String>,
        actual_parent: Option<String>,
        code: &'static str,
    },
    RebaseFailed {
        snapshot: String,
        conflicts: Vec<PyConflict>,
    },
    /// `class` names a class in `icechunk._exceptions`; `None` is the
    /// Rust-defined `IcechunkError` root.
    Class {
        class: Option<&'static str>,
        code: &'static str,
    },
}

fn class(class: &'static str, code: &'static str) -> Classified {
    Classified::Class { class: Some(class), code }
}

fn general(code: &'static str) -> Classified {
    Classified::Class { class: None, code }
}

// The `Kind` enums in the core crates are mostly #[non_exhaustive], which
// forces the `_ => unknown` arms below from this crate.
fn classify_session(kind: &SessionErrorKind) -> Classified {
    use SessionErrorKind as K;
    match kind {
        K::RepositoryError(k) => classify_repository(k),
        K::StorageError(k) => classify_storage(k),
        K::FormatError(k) => classify_format(k),
        K::VirtualReferenceError(k) => classify_virtual_ref(k),
        K::RefError(k) => classify_ref(k),
        K::ReadOnlySession => class("ReadOnlyError", codes::READ_ONLY_SESSION),
        K::CommitNotAllowed => class("SessionStateError", codes::COMMIT_NOT_ALLOWED),
        K::MergeNotAllowed => class("SessionStateError", codes::MERGE_NOT_ALLOWED),
        K::CannotForkReadOnlySession => {
            class("SessionStateError", codes::CANNOT_FORK_READ_ONLY_SESSION)
        }
        K::RearrangeSessionOnly => {
            class("SessionStateError", codes::REARRANGE_SESSION_ONLY)
        }
        K::NonRearrangeSession => {
            class("SessionStateError", codes::NON_REARRANGE_SESSION)
        }
        K::MoveWontOverwrite(_) => {
            class("AlreadyExistsError", codes::MOVE_WONT_OVERWRITE)
        }
        K::MoveIntoSelfOrDescendant { .. } => {
            class("SessionStateError", codes::MOVE_INTO_SELF_OR_DESCENDANT)
        }
        K::MoveDestinationParentMissing { .. } => {
            class("SessionStateError", codes::MOVE_DESTINATION_PARENT_MISSING)
        }
        K::MoveDestinationParentNotGroup { .. } => {
            class("SessionStateError", codes::MOVE_DESTINATION_PARENT_NOT_GROUP)
        }
        K::SnapshotNotFound { .. } => {
            class("SnapshotNotFoundError", codes::SNAPSHOT_NOT_FOUND)
        }
        K::AncestorNodeNotFound { .. } => {
            class("NodeNotFoundError", codes::ANCESTOR_NODE_NOT_FOUND)
        }
        K::NodeNotFound { .. } => class("NodeNotFoundError", codes::NODE_NOT_FOUND),
        K::NotAnArray { .. } => class("InvalidInputError", codes::NOT_AN_ARRAY),
        K::NotAGroup { .. } => class("InvalidInputError", codes::NOT_A_GROUP),
        K::AlreadyExists { .. } => {
            class("AlreadyExistsError", codes::NODE_ALREADY_EXISTS)
        }
        K::NoChangesToCommit => class("SessionStateError", codes::NO_CHANGES_TO_COMMIT),
        K::InvalidSnapshotTimestampOrdering { .. }
        | K::InvalidSnapshotTimestamp { .. } => {
            class("FormatError", codes::SNAPSHOT_TIMESTAMP)
        }
        K::OtherFlushError => class("InternalError", codes::FLUSH),
        K::ConcurrencyError(_) => class("InternalError", codes::CONCURRENCY),
        K::Conflict { expected_parent, actual_parent } => Classified::Conflict {
            expected_parent: expected_parent.as_ref().map(|s| s.to_string()),
            actual_parent: actual_parent.as_ref().map(|s| s.to_string()),
            code: codes::COMMIT_CONFLICT,
        },
        K::RebaseFailed { snapshot, conflicts } => Classified::RebaseFailed {
            snapshot: snapshot.to_string(),
            conflicts: conflicts.iter().map(PyConflict::from).collect(),
        },
        K::MissingPrunedAncestorTxLog { .. } => {
            class("SessionStateError", codes::REBASE_TX_LOG_PRUNED)
        }
        K::JsonSerializationError(_) | K::SerializationError(_) => {
            class("FormatError", codes::SERIALIZATION)
        }
        K::DeserializationError(_) => class("FormatError", codes::DESERIALIZATION),
        K::ConflictingPathNotFound(_) => {
            class("InternalError", codes::CONFLICTING_PATH_NOT_FOUND)
        }
        K::InvalidIndex { .. } => class("InvalidInputError", codes::INVALID_CHUNK_INDEX),
        K::InvalidIndexForSplitManifests { .. } => {
            class("InvalidInputError", codes::INVALID_MANIFEST_SPLIT_INDEX)
        }
        K::BadSnapshotChainForDiff => {
            class("InvalidInputError", codes::BAD_SNAPSHOT_CHAIN_FOR_DIFF)
        }
        K::NoAmendForInitialCommit => {
            class("SessionStateError", codes::NO_AMEND_FOR_INITIAL_COMMIT)
        }
        K::ManifestCreationError(inner) => classify_session(inner.kind()),
        K::ManifestsInconsistencyError { .. } => {
            class("InternalError", codes::INCONSISTENT_MANIFESTS)
        }
        K::SessionMerge(_) => class("SessionStateError", codes::MERGE_FAILED),
        K::InvalidByteRange { .. } => {
            class("InvalidInputError", codes::INVALID_BYTE_RANGE)
        }
        K::InvalidCommitConfiguration { .. } => {
            class("InvalidInputError", codes::INVALID_COMMIT_CONFIGURATION)
        }
        _ => general(codes::UNKNOWN),
    }
}

fn classify_repository(kind: &RepositoryErrorKind) -> Classified {
    use RepositoryErrorKind as K;
    match kind {
        K::StorageError(k) => classify_storage(k),
        K::FormatError(k) => classify_format(k),
        K::Ref(k) => classify_ref(k),
        K::SnapshotNotFound { .. } => {
            class("SnapshotNotFoundError", codes::SNAPSHOT_NOT_FOUND)
        }
        K::InvalidAsOfSpec { .. } => {
            class("SnapshotNotFoundError", codes::NO_SNAPSHOT_AT_TIME)
        }
        K::InvalidSnapshotId(_) => class("InvalidInputError", codes::INVALID_SNAPSHOT_ID),
        K::Tag(_) => class("SessionStateError", codes::TAG_ERROR),
        K::ParentDirectoryNotClean => {
            class("InvalidInputError", codes::PARENT_DIRECTORY_NOT_CLEAN)
        }
        K::EmptyPrefixCreation => {
            class("InvalidInputError", codes::EMPTY_PREFIX_CREATION)
        }
        K::RepositoryDoesntExist => {
            class("RepositoryNotFoundError", codes::REPOSITORY_NOT_FOUND)
        }
        K::SerializationError(_) => class("FormatError", codes::SERIALIZATION),
        K::DeserializationError(_) => class("FormatError", codes::DESERIALIZATION),
        K::ConflictingPathNotFound(_) => {
            class("InternalError", codes::CONFLICTING_PATH_NOT_FOUND)
        }
        K::ConfigDeserializationError(_) => {
            class("FormatError", codes::CONFIG_DESERIALIZATION)
        }
        K::ConfigWasUpdated => Classified::Conflict {
            expected_parent: None,
            actual_parent: None,
            code: codes::CONFIG_UPDATED,
        },
        K::Conflict { expected_parent, actual_parent } => Classified::Conflict {
            expected_parent: expected_parent.as_ref().map(|s| s.to_string()),
            actual_parent: actual_parent.as_ref().map(|s| s.to_string()),
            code: codes::BRANCH_UPDATE_CONFLICT,
        },
        K::RepoInfoUpdated => Classified::Conflict {
            expected_parent: None,
            actual_parent: None,
            code: codes::REPO_INFO_UPDATED,
        },
        K::IOError(_) => class("StorageError", codes::IO),
        K::ConcurrencyError(_) => class("InternalError", codes::CONCURRENCY),
        K::AcquireError(_) => class("StorageError", codes::SEMAPHORE_ACQUIRE),
        K::CannotDeleteMain => class("SessionStateError", codes::CANNOT_DELETE_MAIN),
        K::ReadonlyStorage(_) => class("ReadOnlyError", codes::READ_ONLY_STORAGE),
        K::ReadonlyRepository(_) => class("ReadOnlyError", codes::READ_ONLY_REPOSITORY),
        K::NoAmendForInitialCommit => {
            class("SessionStateError", codes::NO_AMEND_FOR_INITIAL_COMMIT)
        }
        K::RepoUpdateAttemptsLimit(_) => Classified::Conflict {
            expected_parent: None,
            actual_parent: None,
            code: codes::UPDATE_ATTEMPTS_EXHAUSTED,
        },
        K::BadRepoVersion { .. } => class("FormatError", codes::UNSUPPORTED_SPEC_VERSION),
        K::PoisonLock => class("InternalError", codes::POISON_LOCK),
        _ => general(codes::UNKNOWN),
    }
}

fn classify_storage(kind: &StorageErrorKind) -> Classified {
    use StorageErrorKind as K;
    // StorageErrorKind is exhaustive: a new variant here is a compile error.
    match kind {
        K::ObjectNotFound => class("StorageError", codes::OBJECT_NOT_FOUND),
        K::BadPrefix(_) => class("InvalidInputError", codes::BAD_PREFIX),
        K::ObjectStore(_) => class("StorageError", codes::OBJECT_STORE),
        K::IOError(_) => class("StorageError", codes::IO),
        K::R2ConfigurationError(_) => class("StorageError", codes::STORAGE_CONFIG),
        K::CannotParseUrl { .. } => class("StorageError", codes::CANNOT_PARSE_URL),
        K::BadRedirect(_) => class("StorageError", codes::BAD_REDIRECT),
        K::Other(_) => class("StorageError", codes::STORAGE_OTHER),
    }
}

fn classify_ref(kind: &RefErrorKind) -> Classified {
    use RefErrorKind as K;
    // RefErrorKind is exhaustive: a new variant here is a compile error.
    match kind {
        K::Storage(k) => classify_storage(k),
        K::RefNotFound(_) => class("RefNotFoundError", codes::REF_NOT_FOUND),
        K::InvalidRefType(_) => class("InvalidInputError", codes::INVALID_REF_TYPE),
        K::InvalidRefName(_) => class("InvalidInputError", codes::INVALID_REF_NAME),
        K::TagAlreadyExists(_) => class("AlreadyExistsError", codes::TAG_ALREADY_EXISTS),
        K::IOError(_) => class("StorageError", codes::IO),
        K::Serialization(_) => class("FormatError", codes::SERIALIZATION),
        K::Conflict { expected_parent, actual_parent } => Classified::Conflict {
            expected_parent: expected_parent.as_ref().map(|s| s.to_string()),
            actual_parent: actual_parent.as_ref().map(|s| s.to_string()),
            code: codes::BRANCH_UPDATE_CONFLICT,
        },
    }
}

fn classify_format(kind: &IcechunkFormatErrorKind) -> Classified {
    use IcechunkFormatErrorKind as K;
    match kind {
        K::VirtualReferenceError(k) => classify_virtual_ref(k),
        K::NodeNotFound { .. } => class("NodeNotFoundError", codes::NODE_NOT_FOUND),
        K::ChunkCoordinatesNotFound { .. } => {
            class("NodeNotFoundError", codes::CHUNK_NOT_FOUND)
        }
        K::SnapshotIdNotFound { .. } => {
            class("SnapshotNotFoundError", codes::SNAPSHOT_NOT_FOUND)
        }
        K::BranchAlreadyExists { .. } => {
            class("AlreadyExistsError", codes::BRANCH_ALREADY_EXISTS)
        }
        K::BranchNotFound { .. } => class("RefNotFoundError", codes::BRANCH_NOT_FOUND),
        K::TagAlreadyExists { .. } => {
            class("AlreadyExistsError", codes::TAG_ALREADY_EXISTS)
        }
        K::TagPreviouslyDeleted { .. } => {
            class("AlreadyExistsError", codes::TAG_PREVIOUSLY_DELETED)
        }
        K::TagNotFound { .. } => class("RefNotFoundError", codes::TAG_NOT_FOUND),
        K::DuplicateSnapshotId { .. } => {
            class("FormatError", codes::DUPLICATE_SNAPSHOT_ID)
        }
        K::ManifestInfoNotFound { .. } => {
            class("FormatError", codes::MANIFEST_INFO_NOT_FOUND)
        }
        K::InvalidMagicNumbers
        | K::InvalidIcechunkHeaderSize { .. }
        | K::InvalidNodeType { .. }
        | K::InvalidFileType { .. }
        | K::UnknownFileType { .. }
        | K::InvalidCompressionAlgorithm
        | K::InvalidFlatBuffer(_)
        | K::InvalidTimestamp
        | K::MissingLocationCompressionDictionary
        | K::MissingRequiredField(_) => class("FormatError", codes::INVALID_FILE_FORMAT),
        K::InvalidSpecVersion { .. } => {
            class("FormatError", codes::UNSUPPORTED_SPEC_VERSION)
        }
        K::UnsupportedOperationForVersion { .. } => {
            class("FormatError", codes::UNSUPPORTED_OPERATION_FOR_VERSION)
        }
        K::DeserializationError(_) | K::DeserializationErrorFlexBuffers(_) => {
            class("FormatError", codes::DESERIALIZATION)
        }
        K::SerializationError(_) | K::SerializationErrorFlexBuffers(_) => {
            class("FormatError", codes::SERIALIZATION)
        }
        K::IO(_) => class("StorageError", codes::IO),
        K::Path(_) => class("InvalidInputError", codes::INVALID_PATH),
        K::InvalidUpdateTimestamp { .. } => {
            class("FormatError", codes::SNAPSHOT_TIMESTAMP)
        }
        K::InvalidFeatureFlagName { .. } | K::InvalidFeatureFlagId { .. } => {
            class("InvalidInputError", codes::INVALID_FEATURE_FLAG)
        }
        K::FeatureFlagDisabled { .. } => {
            class("SessionStateError", codes::FEATURE_FLAG_DISABLED)
        }
        K::InvalidArrayMetadata(_) => {
            class("InvalidInputError", codes::INVALID_ARRAY_METADATA)
        }
        _ => general(codes::UNKNOWN),
    }
}

fn classify_virtual_ref(kind: &VirtualReferenceErrorKind) -> Classified {
    use VirtualReferenceErrorKind as K;
    match kind {
        K::NoContainerForUrl(_) | K::NoContainerForName(_) => {
            class("InvalidInputError", codes::NO_CONTAINER_FOR_VIRTUAL_CHUNK)
        }
        K::CannotParseUrl { .. } => {
            class("InvalidInputError", codes::INVALID_VIRTUAL_REF)
        }
        K::InvalidCredentials(_) => {
            class("InvalidInputError", codes::INVALID_CREDENTIALS)
        }
        K::UnauthorizedVirtualChunkContainer { .. } => {
            class("InvalidInputError", codes::UNAUTHORIZED_VIRTUAL_CHUNK_CONTAINER)
        }
        K::NoPathSegments(_)
        | K::UnsupportedScheme(_)
        | K::CannotParseBucketName(_)
        | K::UnsupportedObjectKeyForBackend(_)
        | K::Decoding(_) => class("InvalidInputError", codes::INVALID_VIRTUAL_REF),
        K::FetchError(_) => class("StorageError", codes::VIRTUAL_CHUNK_FETCH),
        K::ObjectModified(_) => class("StorageError", codes::VIRTUAL_CHUNK_MODIFIED),
        K::InvalidObjectSize { .. } => class("StorageError", codes::INVALID_OBJECT_SIZE),
        K::AzureConfigurationMustIncludeAccount => {
            class("StorageError", codes::STORAGE_CONFIG)
        }
        _ => general(codes::UNKNOWN),
    }
}

fn classify_store(kind: &StoreErrorKind) -> Classified {
    use StoreErrorKind as K;
    match kind {
        K::SessionError(k) => classify_session(k),
        K::RepositoryError(k) => classify_repository(k),
        K::RefError(k) => classify_ref(k),
        K::InvalidKey { .. } => class("InvalidInputError", codes::INVALID_KEY),
        K::InvalidIndex { .. } => class("InvalidInputError", codes::INVALID_CHUNK_INDEX),
        K::NotAllowed(_) => class("SessionStateError", codes::NOT_ALLOWED),
        K::NotFound(k) => classify_key_not_found(k),
        K::MergeError(_) => class("SessionStateError", codes::MERGE_FAILED),
        K::NoSnapshot => class("SessionStateError", codes::NO_SNAPSHOT),
        K::PathError(_) => class("InvalidInputError", codes::INVALID_PATH),
        K::NotOnBranch => class("SessionStateError", codes::NOT_ON_BRANCH),
        K::BadMetadata(_) => class("InvalidInputError", codes::BAD_METADATA),
        K::BadChunkGridMetadata(_) => class("InvalidInputError", codes::BAD_METADATA),
        K::DeserializationError(_) => class("FormatError", codes::DESERIALIZATION),
        K::SerializationError(_) => class("FormatError", codes::SERIALIZATION),
        K::Unimplemented(_) => class("SessionStateError", codes::UNIMPLEMENTED),
        K::BadKeyPrefix { .. } => class("InvalidInputError", codes::BAD_KEY_PREFIX),
        K::PartialValuesPanic => class("InternalError", codes::PARTIAL_VALUES_PANIC),
        K::ReadOnly => class("ReadOnlyError", codes::READ_ONLY_STORE),
        K::UncommittedChanges => class("SessionStateError", codes::UNCOMMITTED_CHANGES),
        K::InvalidVirtualChunkContainer { .. } => {
            class("InvalidInputError", codes::NO_CONTAINER_FOR_VIRTUAL_CHUNK)
        }
        _ => general(codes::UNKNOWN),
    }
}

fn classify_key_not_found(kind: &KeyNotFoundError) -> Classified {
    use KeyNotFoundError as K;
    match kind {
        K::ChunkNotFound { .. } => class("NodeNotFoundError", codes::CHUNK_NOT_FOUND),
        K::NodeNotFound { .. } => class("NodeNotFoundError", codes::NODE_NOT_FOUND),
        _ => class("NodeNotFoundError", codes::KEY_NOT_FOUND),
    }
}

fn classify_migration(kind: &MigrationErrorKind) -> Classified {
    use MigrationErrorKind as K;
    match kind {
        K::RepositoryError(k) => classify_repository(k),
        K::RefError(k) => classify_ref(k),
        K::FormatError(k) => classify_format(k),
        K::StorageError(k) => classify_storage(k),
        K::InvalidRepositoryMigration { .. } => {
            class("SessionStateError", codes::INVALID_REPOSITORY_MIGRATION)
        }
        K::ReadonlyRepo => class("ReadOnlyError", codes::READ_ONLY_STORAGE),
        K::Unknown => class("InternalError", codes::UNKNOWN),
        _ => general(codes::UNKNOWN),
    }
}

fn classify_gc(error: &GCError) -> Classified {
    // GCError is exhaustive: a new variant here is a compile error.
    match error {
        GCError::Ref(e) => classify_ref(e.kind()),
        GCError::Repository(e) => classify_repository(e.kind()),
        GCError::FormatError(e) => classify_format(e.kind()),
        GCError::StorageError(e) => classify_storage(e.kind()),
    }
}

fn classify_manifest_ops(error: &ManifestOpsError) -> Classified {
    // ManifestOpsError is exhaustive: a new variant here is a compile error.
    match error {
        ManifestOpsError::ManifestRewriteError(e) => classify_session(e.kind()),
        ManifestOpsError::AmendNotSupportedForV1 => {
            class("FormatError", codes::UNSUPPORTED_OPERATION_FOR_VERSION)
        }
    }
}

fn classify(error: &PyIcechunkStoreError) -> Classified {
    use PyIcechunkStoreError as E;
    match error {
        E::StorageError(e) => classify_storage(e.kind()),
        E::StoreError(e) => classify_store(e.kind()),
        E::RepositoryError(e) => classify_repository(e.kind()),
        E::MigrationError(e) => classify_migration(e.kind()),
        E::SessionError(e) => classify_session(e.kind()),
        E::IcechunkFormatError(e) => classify_format(e.kind()),
        E::GCError(e) => classify_gc(e),
        E::ManifestOpsError(e) => classify_manifest_ops(e),
        E::VirtualReferenceError(e) => classify_virtual_ref(e.kind()),
        E::PickleError(_) => class("InternalError", codes::PICKLE),
        E::UnkownError(_) => class("InternalError", codes::UNKNOWN),
        // handled before classification in From<PyIcechunkStoreError> for PyErr
        E::PyKeyError(_) | E::PyValueError(_) | E::PyError(_) => general(codes::UNKNOWN),
    }
}

/// `str(e)`: a one-line Display chain of causes, followed by the `ICError`
/// span-trace context blocks (tests and users rely on the spans being in the
/// message). The full miette report goes into a PEP 678 note as well.
fn chain_message(error: &(dyn std::error::Error + 'static)) -> String {
    const CTX_MARKER: &str = "\n\ncontext:\n";
    let mut parts: Vec<String> = Vec::new();
    let mut contexts: Vec<String> = Vec::new();
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(err) = current {
        let mut msg = err.to_string();
        if let Some(idx) = msg.find(CTX_MARKER) {
            let ctx = msg[idx + CTX_MARKER.len()..].trim_end().to_string();
            if !ctx.is_empty() && !contexts.contains(&ctx) {
                contexts.push(ctx);
            }
            msg.truncate(idx);
        }
        // #[from] variants often embed their source in their own message
        if parts.last().is_none_or(|prev| !prev.contains(&msg)) {
            parts.push(msg);
        }
        current = err.source();
    }
    let mut out = parts.join(": ");
    for ctx in contexts {
        out.push_str(CTX_MARKER);
        out.push_str(&ctx);
    }
    out
}

fn render_report(error: &PyIcechunkStoreError) -> String {
    let mut buf = String::new();
    match GraphicalReportHandler::new().render_report(&mut buf, error) {
        Ok(()) => buf,
        Err(_) => error.to_string(),
    }
}

/// The `icechunk._exceptions` module, imported once per interpreter.
static EXC_MODULE: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

fn exc_class<'py>(py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
    let m = EXC_MODULE
        .get_or_try_init(py, || py.import("icechunk._exceptions").map(|m| m.unbind()))?;
    m.bind(py).getattr(name)
}

fn build_pyerr(
    py: Python<'_>,
    classified: Classified,
    message: String,
    note: Option<String>,
) -> PyResult<PyErr> {
    let instance = match classified {
        Classified::Conflict { expected_parent, actual_parent, code } => py
            .get_type::<PyConflictError>()
            .call1((expected_parent, actual_parent, Some(message), Some(code)))?,
        Classified::RebaseFailed { snapshot, conflicts } => {
            py.get_type::<PyRebaseFailedError>().call1((snapshot, conflicts))?
        }
        Classified::Class { class: None, code } => {
            py.get_type::<IcechunkError>().call1((message, Some(code)))?
        }
        Classified::Class { class: Some(name), code } => {
            exc_class(py, name)?.call1((message, Some(code)))?
        }
    };
    if let Some(note) = note {
        instance.call_method1("add_note", (note,))?;
    }
    Ok(PyErr::from_value(instance))
}

impl From<PyIcechunkStoreError> for PyErr {
    fn from(error: PyIcechunkStoreError) -> Self {
        match error {
            // Session-level conflicts keep their historical payloads and messages
            PyIcechunkStoreError::SessionError(SessionError {
                kind: SessionErrorKind::Conflict { expected_parent, actual_parent },
                ..
            }) => PyConflictError::new_err(
                expected_parent.map(|s| s.to_string()),
                actual_parent.map(|s| s.to_string()),
            ),
            PyIcechunkStoreError::SessionError(SessionError {
                kind: SessionErrorKind::RebaseFailed { snapshot, conflicts },
                ..
            }) => PyRebaseFailedError::new_err(
                snapshot.to_string(),
                conflicts.iter().map(PyConflict::from).collect(),
            ),
            PyIcechunkStoreError::PyKeyError(e) => PyKeyError::new_err(e),
            PyIcechunkStoreError::PyValueError(e) => PyValueError::new_err(e),
            PyIcechunkStoreError::PyError(err) => err,
            error => {
                let classified = classify(&error);
                // skip the expensive report on the missing-chunk/node hot path
                let note = if matches!(
                    classified,
                    Classified::Class { class: Some("NodeNotFoundError"), .. }
                ) {
                    None
                } else {
                    Some(render_report(&error))
                };
                // the SessionError variant is not #[error(transparent)], so it
                // has no source(); chain from the inner error to keep causes
                let message = match &error {
                    PyIcechunkStoreError::SessionError(e) => {
                        format!("session error: {}", chain_message(e))
                    }
                    error => chain_message(error),
                };
                Python::attach(|py| {
                    build_pyerr(py, classified, message, note).unwrap_or_else(|e| e)
                })
            }
        }
    }
}

pub(crate) type PyIcechunkStoreResult<T> = Result<T, PyIcechunkStoreError>;

/// All `kind` codes the classifier can emit; `icechunk.ErrorKind` must match.
#[pyfunction]
pub(crate) fn _all_error_kinds() -> Vec<&'static str> {
    codes::ALL.to_vec()
}

#[pyclass(extends=PyException, subclass, module = "icechunk")]
#[derive(Serialize, Deserialize)]
pub(crate) struct IcechunkError {
    #[pyo3(get)]
    message: String,
    #[pyo3(get)]
    kind: String,
}

#[pymethods]
impl IcechunkError {
    #[new]
    #[pyo3(signature = (message, kind = None))]
    pub(crate) fn new(message: String, kind: Option<String>) -> Self {
        Self { message, kind: kind.unwrap_or_else(|| codes::UNKNOWN.to_string()) }
    }

    fn __repr__(&self) -> String {
        format!("icechunk.IcechunkError(message=\"{}\")", self.message)
    }

    fn __str__(&self) -> String {
        self.message.clone()
    }

    // Control pickling to work with tblib. Uses the runtime class so
    // Python-defined subclasses round-trip as themselves.
    fn __reduce__(slf: &Bound<'_, Self>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let py = slf.py();
        let cls = slf.get_type().into_py_any(py)?;
        let this = slf.borrow();
        let args = (this.message.clone(), this.kind.clone()).into_py_any(py)?;
        Ok((cls, args))
    }
}

impl_pickle!(IcechunkError);

#[pyclass(extends=IcechunkError, subclass, name = "ConflictError", module = "icechunk")]
#[derive(Serialize, Deserialize)]
pub(crate) struct PyConflictError {
    #[pyo3(get)]
    expected_parent: Option<String>,
    #[pyo3(get)]
    actual_parent: Option<String>,
}

impl PyConflictError {
    fn new_err(expected_parent: Option<String>, actual_parent: Option<String>) -> PyErr {
        PyErr::new::<PyConflictError, (Option<String>, Option<String>)>((
            expected_parent,
            actual_parent,
        ))
    }
}

#[pymethods]
impl PyConflictError {
    #[new]
    #[pyo3(signature = (expected_parent = None, actual_parent = None, message = None, kind = None))]
    pub(crate) fn new(
        expected_parent: Option<String>,
        actual_parent: Option<String>,
        message: Option<String>,
        kind: Option<String>,
    ) -> PyClassInitializer<Self> {
        let message = message.unwrap_or_else(|| {
            format!(
                "Failed to commit, expected parent: {expected_parent:?}, actual parent: {actual_parent:?}"
            )
        });
        let kind = kind.unwrap_or_else(|| codes::COMMIT_CONFLICT.to_string());
        PyClassInitializer::from(IcechunkError { message, kind })
            .add_subclass(Self { expected_parent, actual_parent })
    }

    fn __repr__(&self) -> String {
        format!(
            "icechunk.ConflictError(expected_parent={}, actual_parent={})",
            self.expected_parent.as_deref().unwrap_or("None"),
            self.actual_parent.as_deref().unwrap_or("None")
        )
    }

    // Control pickling to work with tblib
    fn __reduce__(slf: &Bound<'_, Self>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let py = slf.py();
        let cls = slf.get_type().into_py_any(py)?;
        let this = slf.borrow();
        let parent = this.as_super();
        let args = (
            this.expected_parent.clone(),
            this.actual_parent.clone(),
            Some(parent.message.clone()),
            Some(parent.kind.clone()),
        )
            .into_py_any(py)?;
        Ok((cls, args))
    }
}

impl_pickle!(PyConflictError);

#[pyclass(extends=PyConflictError, name = "RebaseFailedError", module = "icechunk")]
#[derive(Serialize, Deserialize)]
pub(crate) struct PyRebaseFailedError {
    #[pyo3(get)]
    snapshot: String,
    #[pyo3(get)]
    conflicts: Vec<PyConflict>,
}

impl PyRebaseFailedError {
    fn new_err(snapshot: String, conflicts: Vec<PyConflict>) -> PyErr {
        PyErr::new::<PyRebaseFailedError, (String, Vec<PyConflict>)>((
            snapshot, conflicts,
        ))
    }
}

#[pymethods]
impl PyRebaseFailedError {
    #[new]
    pub(crate) fn new(
        snapshot: String,
        conflicts: Vec<PyConflict>,
    ) -> PyClassInitializer<Self> {
        let message = format!(
            "Rebase failed on snapshot {}: {} conflicts found",
            snapshot,
            conflicts.len()
        );
        PyConflictError::new(
            None,
            None,
            Some(message),
            Some(codes::REBASE_FAILED.to_string()),
        )
        .add_subclass(Self { snapshot, conflicts })
    }

    fn __repr__(&self) -> String {
        format!(
            "icechunk.RebaseFailedError(snapshot=\"{}\", conflicts={:?})",
            self.snapshot, self.conflicts
        )
    }

    // Control pickling to work with tblib
    fn __reduce__(slf: &Bound<'_, Self>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let py = slf.py();
        let cls = slf.get_type().into_py_any(py)?;
        let this = slf.borrow();
        let args = (this.snapshot.clone(), this.conflicts.clone()).into_py_any(py)?;
        Ok((cls, args))
    }
}

impl_pickle!(PyRebaseFailedError);
