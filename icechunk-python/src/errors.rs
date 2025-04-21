use icechunk::{
    StorageError,
    format::IcechunkFormatError,
    ops::gc::GCError,
    repository::RepositoryError,
    session::{SessionError, SessionErrorKind},
    store::{StoreError, StoreErrorKind},
};
use miette::{Diagnostic, GraphicalReportHandler};
use pyo3::{
    PyErr, create_exception,
    exceptions::{PyException, PyKeyError, PyValueError},
    prelude::*,
    types::PyBytes,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::conflicts::PyConflict;

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
    #[error("{0}")]
    PyKeyError(String),
    #[error("{0}")]
    PyValueError(String),
    #[error(transparent)]
    PyError(#[from] PyErr),
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
            } => PyIcechunkStoreError::PyKeyError(format!("{}", path)),
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
            } => PyIcechunkStoreError::PyKeyError(format!("{}", path)),
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
            }) => PyConflictError::new_err(PyConflictErrorData {
                expected_parent: expected_parent.map(|s| s.to_string()),
                actual_parent: actual_parent.map(|s| s.to_string()),
            }),
            PyIcechunkStoreError::SessionError(SessionError {
                kind: SessionErrorKind::RebaseFailed { snapshot, conflicts },
                ..
            }) => PyRebaseFailedError::new_err(PyRebaseFailedData {
                snapshot: snapshot.to_string(),
                conflicts: conflicts.iter().map(PyConflict::from).collect(),
            }),
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

    pub fn __setstate__<'py>(&mut self, state: &Bound<'py, PyBytes>) -> PyResult<()> {
        *self = serde_json::from_slice(state.as_bytes())
            .map_err(|e| PyIcechunkStoreError::UnkownError(e.to_string()))?;
        Ok(())
    }

    pub fn __getstate__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let state = serde_json::to_vec(&self)
            .map_err(|e| PyIcechunkStoreError::UnkownError(e.to_string()))?;
        let bytes = PyBytes::new(py, &state);
        Ok(bytes)
    }

    pub fn __getnewargs__(&self) -> PyResult<(String,)> {
        Ok((self.message.clone(),))
    }
}

#[pyclass(extends=IcechunkError)]
#[derive(Serialize, Deserialize)]
pub struct PyConflictError(pub PyConflictErrorData);

impl PyConflictError {
    fn new_err(data: PyConflictErrorData) -> PyErr {
        PyErr::new::<PyConflictError, PyConflictErrorData>(data)
    }
}

#[pyclass(name = "ConflictErrorData")]
#[derive(Serialize, Deserialize)]
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
