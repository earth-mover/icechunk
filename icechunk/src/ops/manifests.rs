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

/// How a manifest rewrite commits: concurrency, snapshot properties, and commit method.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RewriteManifestsOptions {
    /// `None` uses the session default.
    pub max_concurrent_manifests: Option<usize>,
    pub properties: Option<SnapshotProperties>,
    pub commit_method: CommitMethod,
}

impl Default for RewriteManifestsOptions {
    fn default() -> Self {
        Self {
            max_concurrent_manifests: None,
            properties: None,
            commit_method: CommitMethod::NewCommit,
        }
    }
}

impl RewriteManifestsOptions {
    pub fn with_max_concurrent_manifests(mut self, value: usize) -> Self {
        self.max_concurrent_manifests = Some(value);
        self
    }

    pub fn with_properties(mut self, value: SnapshotProperties) -> Self {
        self.properties = Some(value);
        self
    }

    pub fn with_commit_method(mut self, value: CommitMethod) -> Self {
        self.commit_method = value;
        self
    }
}

pub async fn rewrite_manifests(
    repository: &Repository,
    branch: &str,
    message: &str,
    options: RewriteManifestsOptions,
) -> ManifestOpsResult<SnapshotId> {
    if options.commit_method == CommitMethod::Amend
        && repository.spec_version() < SpecVersionBin::V2
    {
        return Err(ManifestOpsError::AmendNotSupportedForV1);
    }

    let mut session = repository
        .writable_session(branch)
        .await
        .map_err(|e| ManifestOpsError::ManifestRewriteError(Box::new(e.inject())))?;

    let mut builder = session.commit(message).rewrite_manifests();
    if let Some(n) = options.max_concurrent_manifests {
        builder = builder.max_concurrent_nodes(n);
    }
    if options.commit_method == CommitMethod::Amend {
        builder = builder.amend();
    }
    if let Some(props) = options.properties {
        builder = builder.properties(props);
    }
    builder
        .execute()
        .await
        .map_err(|e| ManifestOpsError::ManifestRewriteError(Box::new(e)))
}

#[cfg(test)]
mod options_tests {
    use super::*;

    #[test]
    fn rewrite_manifests_options_default_makes_a_new_commit() {
        let o = RewriteManifestsOptions::default();
        assert_eq!(o.max_concurrent_manifests, None);
        assert_eq!(o.properties, None);
        assert_eq!(o.commit_method, CommitMethod::NewCommit);
    }

    #[test]
    fn rewrite_manifests_options_setters_set_fields() {
        let props = SnapshotProperties::from([("k".to_string(), "v".into())]);
        let o = RewriteManifestsOptions::default()
            .with_max_concurrent_manifests(8)
            .with_properties(props.clone())
            .with_commit_method(CommitMethod::Amend);
        assert_eq!(o.max_concurrent_manifests, Some(8));
        assert_eq!(o.properties, Some(props));
        assert_eq!(o.commit_method, CommitMethod::Amend);
    }
}
