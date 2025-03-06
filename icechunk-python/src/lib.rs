mod config;
mod conflicts;
mod errors;
mod repository;
mod session;
mod store;
mod streams;

use std::env;

use config::{
    PyAzureCredentials, PyAzureStaticCredentials, PyCachingConfig,
    PyCompressionAlgorithm, PyCompressionConfig, PyCredentials, PyGcsBearerCredential,
    PyGcsCredentials, PyGcsStaticCredentials, PyManifestConfig,
    PyManifestPreloadCondition, PyManifestPreloadConfig, PyObjectStoreConfig,
    PyRepositoryConfig, PyS3Credentials, PyS3Options, PyS3StaticCredentials, PyStorage,
    PyStorageConcurrencySettings, PyStorageSettings, PyVirtualChunkContainer,
    PythonCredentialsFetcher,
};
use conflicts::{
    PyBasicConflictSolver, PyConflict, PyConflictDetector, PyConflictSolver,
    PyConflictType, PyVersionSelection,
};
use errors::{
    IcechunkError, PyConflictError, PyConflictErrorData, PyRebaseFailedData,
    PyRebaseFailedError,
};
use icechunk::{format::format_constants::SpecVersionBin, initialize_tracing};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use repository::{PyDiff, PyGCSummary, PyRepository, PySnapshotInfo};
use session::PySession;
use store::{PyStore, VirtualChunkSpec};

#[cfg(feature = "cli")]
use clap::Parser;
#[cfg(feature = "cli")]
use icechunk::cli::interface::{run_cli, IcechunkCLI};

#[cfg(feature = "cli")]
#[pyfunction]
fn cli_entrypoint(py: Python) -> PyResult<()> {
    let sys = py.import("sys")?;
    let args: Vec<String> = sys.getattr("argv")?.extract()?;
    match IcechunkCLI::try_parse_from(args.to_vec()) {
        Ok(cli_args) => pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            if let Err(e) = run_cli(cli_args).await {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
            Ok(())
        }),
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}

#[cfg(not(feature = "cli"))]
#[pyfunction]
fn cli_entrypoint(_py: Python) -> PyResult<()> {
    println!("Must install the optional `cli` feature to use the Icechunk CLI.");
    Ok(())
}

#[pyfunction]
fn initialize_logs() -> PyResult<()> {
    if env::var("ICECHUNK_NO_LOGS").is_err() {
        initialize_tracing()
    }
    Ok(())
}

#[pyfunction]
/// The spec version that this version of the Icechunk library
/// uses to write metadata files
fn spec_version() -> u8 {
    SpecVersionBin::current() as u8
}

/// The icechunk Python module implemented in Rust.
#[pymodule]
fn _icechunk_python(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<PyRepository>()?;
    m.add_class::<PyRepositoryConfig>()?;
    m.add_class::<PySession>()?;
    m.add_class::<PyStore>()?;
    m.add_class::<PySnapshotInfo>()?;
    m.add_class::<PyConflictSolver>()?;
    m.add_class::<PyBasicConflictSolver>()?;
    m.add_class::<PyConflictDetector>()?;
    m.add_class::<PyVersionSelection>()?;
    m.add_class::<PyS3StaticCredentials>()?;
    m.add_class::<PythonCredentialsFetcher>()?;
    m.add_class::<PyS3Credentials>()?;
    m.add_class::<PyGcsCredentials>()?;
    m.add_class::<PyGcsBearerCredential>()?;
    m.add_class::<PyGcsStaticCredentials>()?;
    m.add_class::<PyAzureCredentials>()?;
    m.add_class::<PyAzureStaticCredentials>()?;
    m.add_class::<PyCredentials>()?;
    m.add_class::<PyS3Options>()?;
    m.add_class::<PyObjectStoreConfig>()?;
    m.add_class::<PyStorage>()?;
    m.add_class::<PyVirtualChunkContainer>()?;
    m.add_class::<PyCompressionAlgorithm>()?;
    m.add_class::<PyCompressionConfig>()?;
    m.add_class::<PyCachingConfig>()?;
    m.add_class::<PyStorageConcurrencySettings>()?;
    m.add_class::<PyManifestPreloadConfig>()?;
    m.add_class::<PyManifestPreloadCondition>()?;
    m.add_class::<PyManifestConfig>()?;
    m.add_class::<PyStorageSettings>()?;
    m.add_class::<PyGCSummary>()?;
    m.add_class::<PyDiff>()?;
    m.add_class::<VirtualChunkSpec>()?;
    m.add_function(wrap_pyfunction!(initialize_logs, m)?)?;
    m.add_function(wrap_pyfunction!(spec_version, m)?)?;
    m.add_function(wrap_pyfunction!(cli_entrypoint, m)?)?;

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
