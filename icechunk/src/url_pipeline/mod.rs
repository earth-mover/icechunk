//! Support for the `icechunk:` [URL pipeline](https://github.com/jbms/url-pipeline)
//! adapter scheme (ZEP 8).
//!
//! A URL pipeline such as
//! `s3://bucket/repo|icechunk://tag.v1/path/to/group|zarr3:` names a root
//! location (`s3://bucket/repo`), the Icechunk ref to read (`tag.v1`) and a
//! path inside the repository (`path/to/group`).
//!
//! - [`parse_icechunk_ref`] parses the body of an `icechunk:` segment.

mod reference;

pub use reference::{DEFAULT_BRANCH, parse_icechunk_ref};

use thiserror::Error;

use crate::error::ICError;

/// Errors in the text of a URL pipeline, or in how it was asked to be opened.
///
/// These are user-input errors. Language bindings map them to their
/// "invalid URL" error (in Python, `ValueError`, which the zarr adapter
/// turns into `zarr.errors.URLPipelineError`).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UrlPipelineErrorKind {
    #[error("invalid `icechunk:` URL segment `icechunk:{body}`: {reason}")]
    InvalidIcechunkSegment { body: String, reason: String },
}

pub type UrlPipelineError = ICError<UrlPipelineErrorKind>;
pub type UrlPipelineResult<T> = Result<T, UrlPipelineError>;
