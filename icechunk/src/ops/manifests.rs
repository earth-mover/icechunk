//! Manifest optimization and rebuilding.

use crate::{
    Repository,
    format::{
        SnapshotId, format_constants::SpecVersionBin, snapshot::SnapshotProperties,
    },
    session::{CommitMethod, SessionError},
};

#[derive(Debug, thiserror::Error)]
pub enum ManifestOpsError {
    #[error("error rewriting manifests")]
    ManifestRewriteError(#[from] Box<SessionError>),
    #[error(
        "amend is not supported for spec version 1 repositories, use new commit instead"
    )]
    AmendNotSupportedForV1,
}

pub type ManifestOpsResult<A> = Result<A, ManifestOpsError>;

pub async fn rewrite_manifests(
    repository: &Repository,
    branch: &str,
    message: &str,
    max_concurrent_manifests: usize,
    properties: Option<SnapshotProperties>,
    commit_method: CommitMethod,
) -> ManifestOpsResult<SnapshotId> {
    if commit_method == CommitMethod::Amend
        && repository.spec_version() < SpecVersionBin::V2dot0
    {
        return Err(ManifestOpsError::AmendNotSupportedForV1);
    }

    let mut session = repository
        .writable_session(branch)
        .await
        .map_err(|e| ManifestOpsError::ManifestRewriteError(Box::new(e.into())))?;

    session
        .rewrite_manifests(message, max_concurrent_manifests, properties, commit_method)
        .await
        .map_err(|e| ManifestOpsError::ManifestRewriteError(Box::new(e)))
}
