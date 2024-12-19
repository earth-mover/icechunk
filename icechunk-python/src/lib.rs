mod conflicts;
mod errors;
mod repository;
mod session;
mod storage;
mod store;
mod streams;

use conflicts::{
    PyBasicConflictSolver, PyConflict, PyConflictDetector, PyConflictSolver,
    PyConflictType, PyVersionSelection,
};
use errors::{
    IcechunkError, PyConflictError, PyConflictErrorData, PyRebaseFailedData,
    PyRebaseFailedError,
};
use pyo3::prelude::*;
use repository::{PyRepository, PyRepositoryConfig, PySnapshotMetadata};
use session::PySession;
use storage::{
    PyS3ClientOptions, PyS3Config, PyS3Credentials, PyStorageConfig, PyVirtualRefConfig,
};
use store::{PyStore, PyStoreConfig};

/// The icechunk Python module implemented in Rust.
#[pymodule]
fn _icechunk_python(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<PyRepository>()?;
    m.add_class::<PyRepositoryConfig>()?;
    m.add_class::<PySession>()?;
    m.add_class::<PyStorageConfig>()?;
    m.add_class::<PyStore>()?;
    m.add_class::<PyStoreConfig>()?;
    m.add_class::<PyS3Config>()?;
    m.add_class::<PyS3ClientOptions>()?;
    m.add_class::<PyS3Credentials>()?;
    m.add_class::<PyStoreConfig>()?;
    m.add_class::<PySnapshotMetadata>()?;
    m.add_class::<PyVirtualRefConfig>()?;
    m.add_class::<PyConflictSolver>()?;
    m.add_class::<PyBasicConflictSolver>()?;
    m.add_class::<PyConflictDetector>()?;
    m.add_class::<PyVersionSelection>()?;

    // Exceptions
    m.add("IcechunkError", py.get_type::<IcechunkError>())?;
    m.add("PyConflictError", py.get_type::<PyConflictError>())?;
    m.add_class::<PyConflictErrorData>()?;
    m.add("PyRebaseFailedError", py.get_type::<PyRebaseFailedError>())?;
    m.add_class::<PyConflictType>()?;
    m.add_class::<PyConflict>()?;
    m.add_class::<PyRebaseFailedData>()?;
    Ok(())
}
