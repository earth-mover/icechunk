//! Generic error wrapper with span tracing.
//!
//! [`ICError<E>`] wraps an error kind `E` with a [`SpanTrace`] for debugging.
//! Used as the base for [`SessionError`], [`RepositoryError`], [`StorageError`], etc.
//!
//! [`SpanTrace`]: tracing_error::SpanTrace
//! [`SessionError`]: crate::session::SessionError
//! [`RepositoryError`]: crate::repository::RepositoryError
//! [`StorageError`]: crate::storage::StorageError

pub use icechunk_types::error::ICError;
