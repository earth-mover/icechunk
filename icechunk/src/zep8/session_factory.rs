//! Session creation utilities for ZEP 8 icechunk URLs.
//!
//! Creates readonly Icechunk sessions from parsed path specifications.

use crate::repository::{Repository, VersionInfo};
use crate::session::Session;
use crate::zep8::path_spec::{IcechunkPathSpec, ReferenceType};
use std::fmt;

/// Error type for session creation.
#[derive(Debug)]
pub enum ZepSessionError {
    BranchNotFound(String),
    TagNotFound(String),
    SnapshotNotFound(String),
    SessionCreation(String),
}

impl fmt::Display for ZepSessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZepSessionError::BranchNotFound(branch) => {
                write!(f, "Branch '{branch}' not found")
            }
            ZepSessionError::TagNotFound(tag) => {
                write!(f, "Tag '{tag}' not found")
            }
            ZepSessionError::SnapshotNotFound(snapshot) => {
                write!(f, "Snapshot '{snapshot}' not found")
            }
            ZepSessionError::SessionCreation(msg) => {
                write!(f, "Session creation failed: {msg}")
            }
        }
    }
}

impl std::error::Error for ZepSessionError {}

/// Create readonly Icechunk session from parsed path specification.
///
/// # Arguments
/// * `repo` - Icechunk repository instance
/// * `path_spec` - Parsed path specification
///
/// # Returns
/// A Session instance for the specified version
///
/// # Errors
/// Returns `ZepSessionError` if the branch, tag, or snapshot doesn't exist,
/// or if session creation fails for any other reason.
///
/// # Examples
/// ```
/// use icechunk::zep8::{IcechunkPathSpec, create_readonly_session};
/// use icechunk::repository::Repository;
///
/// # async fn example(repo: Repository) -> Result<(), Box<dyn std::error::Error>> {
/// let path_spec = IcechunkPathSpec::parse("@branch.main/data")?;
/// let session = create_readonly_session(&repo, &path_spec).await?;
/// # Ok(())
/// # }
/// ```
pub async fn create_readonly_session(
    repo: &Repository,
    path_spec: &IcechunkPathSpec,
) -> Result<Session, ZepSessionError> {
    // Convert path spec to VersionInfo
    let version_info = match path_spec.reference_type {
        ReferenceType::Branch => {
            VersionInfo::BranchTipRef(path_spec.reference_value.clone())
        }
        ReferenceType::Tag => VersionInfo::TagRef(path_spec.reference_value.clone()),
        ReferenceType::Snapshot => {
            // TODO: Implement proper snapshot ID parsing
            // For now, return an error as a placeholder
            return Err(ZepSessionError::SnapshotNotFound(format!(
                "Snapshot parsing not implemented for '{}'",
                path_spec.reference_value
            )));
        }
    };

    // Create readonly session using the repository API
    repo.readonly_session(&version_info).await.map_err(|e| {
        let error_msg = e.to_string();
        if error_msg.contains("not found") || error_msg.contains("does not exist") {
            match path_spec.reference_type {
                ReferenceType::Branch => {
                    ZepSessionError::BranchNotFound(path_spec.reference_value.clone())
                }
                ReferenceType::Tag => {
                    ZepSessionError::TagNotFound(path_spec.reference_value.clone())
                }
                ReferenceType::Snapshot => {
                    ZepSessionError::SnapshotNotFound(path_spec.reference_value.clone())
                }
            }
        } else {
            ZepSessionError::SessionCreation(format!(
                "Failed to create session for {} '{}': {}",
                path_spec.reference_type_str(),
                path_spec.reference_value,
                e
            ))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_error_display() {
        let error = ZepSessionError::BranchNotFound("main".to_string());
        assert!(error.to_string().contains("Branch 'main' not found"));

        let error = ZepSessionError::TagNotFound("v1.0".to_string());
        assert!(error.to_string().contains("Tag 'v1.0' not found"));

        let error = ZepSessionError::SnapshotNotFound("abc123".to_string());
        assert!(error.to_string().contains("Snapshot 'abc123' not found"));

        let error = ZepSessionError::SessionCreation("test error".to_string());
        assert!(error.to_string().contains("Session creation failed"));
    }

    // Note: More comprehensive tests would require setting up actual repositories
    // Those would be better placed in integration tests
}
