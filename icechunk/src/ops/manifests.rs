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

/// Rewrites every manifest of a branch in one commit. Build with [`rewrite_manifests`].
///
/// ```no_run
/// # use icechunk::{Repository, ops::manifests::rewrite_manifests};
/// # async fn f(repo: Repository) -> Result<(), Box<dyn std::error::Error>> {
/// let snapshot_id = rewrite_manifests(&repo, "main", "rewrite manifests")
///     .max_concurrent_manifests(8)
///     .execute()
///     .await?;
/// # Ok(()) }
/// ```
pub struct RewriteManifestsBuilder<'a> {
    repository: &'a Repository,
    branch: &'a str,
    message: &'a str,
    max_concurrent_manifests: Option<usize>,
    properties: Option<SnapshotProperties>,
    commit_method: CommitMethod,
}

impl std::fmt::Debug for RewriteManifestsBuilder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RewriteManifestsBuilder")
            .field("branch", &self.branch)
            .field("message", &self.message)
            .field("max_concurrent_manifests", &self.max_concurrent_manifests)
            .field("properties", &self.properties)
            .field("commit_method", &self.commit_method)
            .finish_non_exhaustive()
    }
}

/// Rewrite every manifest of `branch` in a commit with `message`.
pub fn rewrite_manifests<'a>(
    repository: &'a Repository,
    branch: &'a str,
    message: &'a str,
) -> RewriteManifestsBuilder<'a> {
    RewriteManifestsBuilder {
        repository,
        branch,
        message,
        max_concurrent_manifests: None,
        properties: None,
        commit_method: CommitMethod::NewCommit,
    }
}

impl RewriteManifestsBuilder<'_> {
    /// Default: the session default.
    pub fn max_concurrent_manifests(mut self, value: usize) -> Self {
        self.max_concurrent_manifests = Some(value);
        self
    }

    /// Default: none.
    pub fn properties(mut self, value: SnapshotProperties) -> Self {
        self.properties = Some(value);
        self
    }

    /// Amend the branch tip, not a new commit. Fails on V1 repositories.
    /// Default: a new commit.
    pub fn amend(mut self) -> Self {
        self.commit_method = CommitMethod::Amend;
        self
    }

    pub async fn execute(self) -> ManifestOpsResult<SnapshotId> {
        if self.commit_method == CommitMethod::Amend
            && self.repository.spec_version() < SpecVersionBin::V2
        {
            return Err(ManifestOpsError::AmendNotSupportedForV1);
        }

        let mut session =
            self.repository.writable_session(self.branch).await.map_err(|e| {
                ManifestOpsError::ManifestRewriteError(Box::new(e.inject()))
            })?;

        let mut builder = session.commit(self.message).rewrite_manifests();
        if let Some(n) = self.max_concurrent_manifests {
            builder = builder.max_concurrent_nodes(n);
        }
        if self.commit_method == CommitMethod::Amend {
            builder = builder.amend();
        }
        if let Some(props) = self.properties {
            builder = builder.properties(props);
        }
        builder
            .execute()
            .await
            .map_err(|e| ManifestOpsError::ManifestRewriteError(Box::new(e)))
    }
}

#[cfg(test)]
mod tests {
    use icechunk_macros::tokio_test;

    // `tokio_test` expands to nothing under shuttle, so only the async tests use these.
    #[cfg(not(feature = "shuttle"))]
    use super::*;
    #[cfg(not(feature = "shuttle"))]
    use crate::storage::new_in_memory_storage;

    #[tokio_test]
    async fn rewrite_manifests_builder_defaults_and_methods()
    -> Result<(), Box<dyn std::error::Error>> {
        let repo = Repository::create(new_in_memory_storage().await?).execute().await?;
        let b = rewrite_manifests(&repo, "main", "msg");
        assert_eq!(b.max_concurrent_manifests, None);
        assert_eq!(b.properties, None);
        assert_eq!(b.commit_method, CommitMethod::NewCommit);
        let props = SnapshotProperties::from([("k".to_string(), "v".into())]);
        let b = b.max_concurrent_manifests(8).properties(props.clone()).amend();
        assert_eq!(b.max_concurrent_manifests, Some(8));
        assert_eq!(b.properties, Some(props));
        assert_eq!(b.commit_method, CommitMethod::Amend);
        Ok(())
    }
}
