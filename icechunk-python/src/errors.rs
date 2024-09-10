use icechunk::{dataset::DatasetError, format::IcechunkFormatError, zarr::StoreError};
use pyo3::{exceptions::PyValueError, PyErr};

/// A simple wrapper around the StoreError to make it easier to convert to a PyErr
///
/// When you use the ? operator, the error is coerced. But if you return the value it is not.
/// So for now we just use the extra operation to get the coersion instead of manually mapping
/// the errors where this is returned from a python class
pub struct PyIcechunkStoreError(PyErr);

impl From<PyIcechunkStoreError> for PyErr {
    fn from(error: PyIcechunkStoreError) -> Self {
        error.0
    }
}

impl From<StoreError> for PyIcechunkStoreError {
    fn from(other: StoreError) -> Self {
        Self(PyValueError::new_err(other.to_string()))
    }
}

impl From<DatasetError> for PyIcechunkStoreError {
    fn from(other: DatasetError) -> Self {
        Self(PyValueError::new_err(other.to_string()))
    }
}

impl From<IcechunkFormatError> for PyIcechunkStoreError {
    fn from(other: IcechunkFormatError) -> Self {
        Self(PyValueError::new_err(other.to_string()))
    }
}

impl From<String> for PyIcechunkStoreError {
    fn from(other: String) -> Self {
        Self(PyValueError::new_err(other))
    }
}

pub type PyIcechunkStoreResult<T> = Result<T, PyIcechunkStoreError>;
