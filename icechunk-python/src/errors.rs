use std::convert::Infallible;

use icechunk::{
    format::IcechunkFormatError, repository::RepositoryError, session::SessionError,
    store::StoreError, StorageError,
};
use pyo3::{
    create_exception,
    exceptions::{PyKeyError, PyValueError},
    prelude::*,
    PyErr,
};
use thiserror::Error;

use crate::conflicts::PyConflict;

/// A simple wrapper around the StoreError to make it easier to convert to a PyErr
///
/// When you use the ? operator, the error is coerced. But if you return the value it is not.
/// So for now we just use the extra operation to get the coercion instead of manually mapping
/// the errors where this is returned from a python class
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
#[allow(dead_code)]
pub(crate) enum PyIcechunkStoreError {
    #[error("storage error: {0}")]
    StorageError(StorageError),
    #[error("store error: {0}")]
    StoreError(StoreError),
    #[error("repository error: {0}")]
    RepositoryError(#[from] RepositoryError),
    #[error("session error: {0}")]
    SessionError(SessionError),
    #[error("icechunk format error: {0}")]
    IcechunkFormatError(#[from] IcechunkFormatError),
    #[error("{0}")]
    PyKeyError(String),
    #[error("{0}")]
    PyValueError(String),
    #[error("{0}")]
    PyError(#[from] PyErr),
    #[error("{0}")]
    UnkownError(String),
}

impl From<Infallible> for PyIcechunkStoreError {
    fn from(_: Infallible) -> Self {
        PyIcechunkStoreError::UnkownError("Infallible".to_string())
    }
}

impl From<StoreError> for PyIcechunkStoreError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound(e) => PyIcechunkStoreError::PyKeyError(e.to_string()),
            StoreError::SessionError(SessionError::NodeNotFound { path, message: _ }) => {
                PyIcechunkStoreError::PyKeyError(format!("{}", path))
            }
            _ => PyIcechunkStoreError::StoreError(error),
        }
    }
}

impl From<SessionError> for PyIcechunkStoreError {
    fn from(error: SessionError) -> Self {
        match error {
            SessionError::NodeNotFound { path, message: _ } => {
                PyIcechunkStoreError::PyKeyError(format!("{}", path))
            }
            _ => PyIcechunkStoreError::SessionError(error),
        }
    }
}

impl From<PyIcechunkStoreError> for PyErr {
    fn from(error: PyIcechunkStoreError) -> Self {
        match error {
            PyIcechunkStoreError::SessionError(SessionError::Conflict {
                expected_parent,
                actual_parent,
            }) => PyConflictError::new_err(PyConflictErrorData {
                expected_parent: expected_parent.map(|s| s.to_string()),
                actual_parent: actual_parent.map(|s| s.to_string()),
            }),
            PyIcechunkStoreError::SessionError(SessionError::RebaseFailed {
                snapshot,
                conflicts,
            }) => PyRebaseFailedError::new_err(PyRebaseFailedData {
                snapshot: snapshot.to_string(),
                conflicts: conflicts.iter().map(PyConflict::from).collect(),
            }),
            PyIcechunkStoreError::PyKeyError(e) => PyKeyError::new_err(e),
            PyIcechunkStoreError::PyValueError(e) => PyValueError::new_err(e),
            PyIcechunkStoreError::PyError(err) => err,
            _ => IcechunkError::new_err(error.to_string()),
        }
    }
}

pub(crate) type PyIcechunkStoreResult<T> = Result<T, PyIcechunkStoreError>;

create_exception!(icechunk, IcechunkError, PyValueError);

create_exception!(icechunk, PyConflictError, IcechunkError);

#[pyclass(name = "ConflictErrorData")]
pub struct PyConflictErrorData {
    #[pyo3(get)]
    expected_parent: Option<String>,
    #[pyo3(get)]
    actual_parent: Option<String>,
}

#[pymethods]
impl PyConflictErrorData {
    fn __repr__(&self) -> String {
        format!(
            "ConflictErrorData(expected_parent={}, actual_parent={})",
            self.expected_parent.as_deref().unwrap_or("None"),
            self.actual_parent.as_deref().unwrap_or("None")
        )
    }

    fn __str__(&self) -> String {
        format!(
            "Failed to commit, expected parent: {:?}, actual parent: {:?}",
            self.expected_parent, self.actual_parent
        )
    }
}

create_exception!(icechunk, PyRebaseFailedError, IcechunkError);

#[pyclass(name = "RebaseFailedData")]
#[derive(Debug, Clone)]
pub struct PyRebaseFailedData {
    #[pyo3(get)]
    snapshot: String,
    #[pyo3(get)]
    conflicts: Vec<PyConflict>,
}

#[pymethods]
impl PyRebaseFailedData {
    fn __repr__(&self) -> String {
        format!(
            "RebaseFailedData(snapshot={}, conflicts={:?})",
            self.snapshot, self.conflicts
        )
    }

    fn __str__(&self) -> String {
        format!(
            "Rebase failed on snapshot {}: {} conflicts found",
            self.snapshot,
            self.conflicts.len()
        )
    }
}
