mod conflicts;
mod errors;
mod repository;
mod session;
mod storage;
mod store;
mod streams;

use conflicts::{
    PyBasicConflictSolver, PyConflictDetector, PyConflictSolver, PyVersionSelection,
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
fn _icechunk_python(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
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
    Ok(())
}
