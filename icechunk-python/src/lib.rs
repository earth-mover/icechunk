mod config;
mod conflicts;
mod errors;
mod repository;
mod session;
mod store;
mod streams;

use config::{
    PyCredentials, PyObjectStoreConfig, PyRepositoryConfig, PyS3CompatibleOptions,
    PyStaticCredentials, PyStorage, PyVirtualChunkContainer,
};
use conflicts::{
    PyBasicConflictSolver, PyConflict, PyConflictDetector, PyConflictSolver,
    PyConflictType, PyVersionSelection,
};
use errors::{
    IcechunkError, PyConflictError, PyConflictErrorData, PyRebaseFailedData,
    PyRebaseFailedError,
};
use pyo3::prelude::*;
use repository::{PyRepository, PySnapshotMetadata};
use session::PySession;
use store::PyStore;

/// The icechunk Python module implemented in Rust.
#[pymodule]
fn _icechunk_python(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<PyRepository>()?;
    m.add_class::<PyRepositoryConfig>()?;
    m.add_class::<PySession>()?;
    m.add_class::<PyStore>()?;
    m.add_class::<PySnapshotMetadata>()?;
    m.add_class::<PyConflictSolver>()?;
    m.add_class::<PyBasicConflictSolver>()?;
    m.add_class::<PyConflictDetector>()?;
    m.add_class::<PyVersionSelection>()?;
    m.add_class::<PyStaticCredentials>()?;
    m.add_class::<PyCredentials>()?;
    m.add_class::<PyS3CompatibleOptions>()?;
    m.add_class::<PyObjectStoreConfig>()?;
    m.add_class::<PyStorage>()?;
    m.add_class::<PyVirtualChunkContainer>()?;

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
