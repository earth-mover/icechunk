use crate::{
    Repository,
    format::{SnapshotId, snapshot::SnapshotProperties},
    session::SessionError,
};

#[derive(Debug, thiserror::Error)]
pub enum ManifestOpsError {
    #[error("error rewriting manifests")]
    ManifestRewriteError(#[from] Box<SessionError>),
}

pub type ManifestOpsResult<A> = Result<A, ManifestOpsError>;

pub async fn rewrite_manifests(
    repository: &Repository,
    branch: &str,
    message: &str,
    properties: Option<SnapshotProperties>,
) -> ManifestOpsResult<SnapshotId> {
    let mut session = repository
        .writable_session(branch)
        .await
        .map_err(|e| ManifestOpsError::ManifestRewriteError(Box::new(e.into())))?;

    session
        .rewrite_manifests(message, properties)
        .await
        .map_err(|e| ManifestOpsError::ManifestRewriteError(Box::new(e)))
}
