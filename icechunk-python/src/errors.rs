use icechunk::{dataset::DatasetError, format::IcechunkFormatError, zarr::StoreError};
use pyo3::{exceptions::PyValueError, PyErr};
use thiserror::Error;

/// A simple wrapper around the StoreError to make it easier to convert to a PyErr
///
/// When you use the ? operator, the error is coerced. But if you return the value it is not.
/// So for now we just use the extra operation to get the coersion instead of manually mapping
/// the errors where this is returned from a python class
#[derive(Debug, Error)]
pub enum PyIcechunkStoreError {
    #[error("store error: {0}")]
    StoreError(#[from] StoreError),
    #[error("dataset Error: {0}")]
    DatasetError(#[from] DatasetError),
    #[error("icechunk format error: {0}")]
    IcechunkFormatError(#[from] IcechunkFormatError),
    #[error("value error: {0}")]
    PyValueError(#[from] PyValueError),
    #[error("{0}")]
    UnkownError(String),
}

impl From<PyIcechunkStoreError> for PyErr {
    fn from(error: PyIcechunkStoreError) -> Self {
        PyValueError::new_err(error.to_string())
    }
}

pub type PyIcechunkStoreResult<T> = Result<T, PyIcechunkStoreError>;
