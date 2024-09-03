use icechunk::zarr::StoreError;
use pyo3::{exceptions::PyValueError, PyErr};

/// A simple wrapper around the StoreError to make it easier to convert to a PyErr
///
/// When you use the ? operator, the error is coerced. But if you return the value it is not.
/// So for now we just use the extra operation to get the coersion instead of manually mapping
/// the errors where this is returned from a python class
pub struct IcechunkStoreError(pub StoreError);

impl From<IcechunkStoreError> for PyErr {
    fn from(error: IcechunkStoreError) -> Self {
        PyValueError::new_err(error.0.to_string())
    }
}

impl From<StoreError> for IcechunkStoreError {
    fn from(other: StoreError) -> Self {
        Self(other)
    }
}

pub type IcechunkStoreResult<T> = Result<T, IcechunkStoreError>;
