use icechunk::{
    StorageError,
    format::IcechunkFormatError,
    ops::{gc::GCError, manifests::ManifestOpsError},
    repository::RepositoryError,
    session::{SessionError, SessionErrorKind},
    store::{StoreError, StoreErrorKind},
};
use miette::{Diagnostic, GraphicalReportHandler};
use pyo3::{
    PyErr,
    exceptions::{PyException, PyKeyError, PyValueError},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{conflicts::PyConflict, impl_pickle};

/// A simple wrapper around the StoreError to make it easier to convert to a PyErr
///
/// When you use the ? operator, the error is coerced. But if you return the value it is not.
/// So for now we just use the extra operation to get the coercion instead of manually mapping
/// the errors where this is returned from a python class
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error, Diagnostic)]
#[allow(dead_code)]
pub(crate) enum PyIcechunkStoreError {
    #[error(transparent)]
    StorageError(StorageError),
    #[error(transparent)]
    StoreError(StoreError),
    #[error(transparent)]
    RepositoryError(#[from] RepositoryError),
    #[error("session error: {0}")]
    SessionError(SessionError),
    #[error(transparent)]
    IcechunkFormatError(#[from] IcechunkFormatError),
    #[error(transparent)]
    GCError(#[from] GCError),
    #[error(transparent)]
    ManifestOpsError(#[from] ManifestOpsError),
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
        match error {
            StoreError { kind: StoreErrorKind::NotFound(e), .. } => {
                PyIcechunkStoreError::PyKeyError(e.to_string())
            }
            StoreError {
                kind:
                    StoreErrorKind::SessionError(SessionErrorKind::NodeNotFound {
                        path,
                        message: _,
                    }),
                ..
            } => PyIcechunkStoreError::PyKeyError(format!("{path}")),
            _ => PyIcechunkStoreError::StoreError(error),
        }
    }
}

impl From<SessionError> for PyIcechunkStoreError {
    fn from(error: SessionError) -> Self {
        match error {
            SessionError {
                kind: SessionErrorKind::NodeNotFound { path, message: _ },
                ..
            } => PyIcechunkStoreError::PyKeyError(format!("{path}")),
            _ => PyIcechunkStoreError::SessionError(error),
        }
    }
}

impl From<PyIcechunkStoreError> for PyErr {
    fn from(error: PyIcechunkStoreError) -> Self {
        match error {
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
                let mut buf = String::new();
                let message =
                    match GraphicalReportHandler::new().render_report(&mut buf, &error) {
                        Ok(_) => buf,
                        Err(_) => error.to_string(),
                    };
                IcechunkError::new_err(message)
            }
        }
    }
}

pub(crate) type PyIcechunkStoreResult<T> = Result<T, PyIcechunkStoreError>;

#[pyclass(extends=PyException, subclass, module = "icechunk")]
#[derive(Serialize, Deserialize)]
pub struct IcechunkError {
    #[pyo3(get)]
    message: String,
}

impl IcechunkError {
    fn new_err(message: String) -> PyErr {
        PyErr::new::<IcechunkError, String>(message)
    }
}

#[pymethods]
impl IcechunkError {
    #[new]
    pub fn new(message: String) -> Self {
        Self { message }
    }

    fn __repr__(&self) -> String {
        format!("IcechunkError(message={})", self.message)
    }

    fn __str__(&self) -> String {
        self.message.clone()
    }
}

impl_pickle!(IcechunkError);

#[pyclass(extends=PyException, name = "ConflictError", module = "icechunk")]
#[derive(Serialize, Deserialize)]
pub struct PyConflictError {
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
    pub fn new(expected_parent: Option<String>, actual_parent: Option<String>) -> Self {
        Self { expected_parent, actual_parent }
    }

    fn __repr__(&self) -> String {
        format!(
            "ConflictError(expected_parent={}, actual_parent={})",
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

impl_pickle!(PyConflictError);

#[pyclass(extends=PyException, name = "RebaseFailedError", module = "icechunk")]
#[derive(Serialize, Deserialize)]
pub struct PyRebaseFailedError {
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
    pub fn new(snapshot: String, conflicts: Vec<PyConflict>) -> Self {
        Self { snapshot, conflicts }
    }

    fn __repr__(&self) -> String {
        format!(
            "RebaseFailedError(snapshot={}, conflicts={:?})",
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

impl_pickle!(PyRebaseFailedError);
