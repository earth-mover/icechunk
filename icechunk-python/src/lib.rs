mod config;
mod conflicts;
mod errors;
mod pickle;
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
    PyStorageConcurrencySettings, PyStorageRetriesSettings, PyStorageSettings,
    PyVirtualChunkContainer,
};
use config::{
    PyManifestSplitCondition, PyManifestSplitDimCondition, PyManifestSplittingConfig,
};
use conflicts::{
    PyBasicConflictSolver, PyConflict, PyConflictDetector, PyConflictSolver,
    PyConflictType, PyVersionSelection,
};
use errors::{IcechunkError, PyConflictError, PyRebaseFailedError};
use icechunk::{format::format_constants::SpecVersionBin, initialize_tracing};
use pyo3::prelude::*;
use pyo3::types::PyMapping;
use pyo3::wrap_pyfunction;
use repository::{PyDiff, PyGCSummary, PyManifestFileInfo, PyRepository, PySnapshotInfo};
use session::PySession;
use store::{PyStore, VirtualChunkSpec};

#[cfg(feature = "cli")]
use clap::Parser;
#[cfg(feature = "cli")]
use icechunk::cli::interface::{IcechunkCLI, run_cli};

#[cfg(feature = "cli")]
#[pyfunction]
fn cli_entrypoint(py: Python) -> PyResult<()> {
    let sys = py.import("sys")?;
    let args: Vec<String> = sys.getattr("argv")?.extract()?;
    match IcechunkCLI::try_parse_from(args.to_vec()) {
        Ok(cli_args) => pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            if let Err(e) = run_cli(cli_args).await {
                eprintln!("{e}");
                std::process::exit(1);
            }
            Ok(())
        }),
        Err(e) => {
            if e.use_stderr() {
                eprintln!("{e}");
                std::process::exit(e.exit_code());
            } else {
                println!("{e}");
                Ok(())
            }
        }
    }
}

#[cfg(not(feature = "cli"))]
#[pyfunction]
fn cli_entrypoint(_py: Python) -> PyResult<()> {
    println!("Must install the optional `cli` feature to use the Icechunk CLI.");
    Ok(())
}

fn log_filters_from_env(py: Python) -> PyResult<Option<String>> {
    let os = py.import("os")?;
    let environ = os.getattr("environ")?;
    let environ: &Bound<PyMapping> = environ.downcast()?;
    let value = environ.get_item("ICECHUNK_LOG").ok().and_then(|v| v.extract().ok());
    Ok(value)
}

#[pyfunction]
fn initialize_logs(py: Python) -> PyResult<()> {
    if env::var("ICECHUNK_NO_LOGS").is_err() {
        let log_filter_directive = log_filters_from_env(py)?;
        initialize_tracing(log_filter_directive.as_deref())
    }
    Ok(())
}

#[pyfunction]
fn set_logs_filter(py: Python, log_filter_directive: Option<String>) -> PyResult<()> {
    let log_filter_directive =
        log_filter_directive.or_else(|| log_filters_from_env(py).ok().flatten());
    initialize_tracing(log_filter_directive.as_deref());
    Ok(())
}

#[pyfunction]
/// The spec version that this version of the Icechunk library
/// uses to write metadata files
fn spec_version() -> u8 {
    SpecVersionBin::current() as u8
}

fn pep440_version() -> String {
    let cargo_version = env!("CARGO_PKG_VERSION");
    cargo_version.replace("-rc.", "rc").replace("-alpha.", "a").replace("-beta.", "b")
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
    m.add_class::<PyManifestFileInfo>()?;
    m.add_class::<PyConflictSolver>()?;
    m.add_class::<PyBasicConflictSolver>()?;
    m.add_class::<PyConflictDetector>()?;
    m.add_class::<PyVersionSelection>()?;
    m.add_class::<PyS3StaticCredentials>()?;
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
    m.add_class::<PyStorageRetriesSettings>()?;
    m.add_class::<PyManifestPreloadConfig>()?;
    m.add_class::<PyManifestPreloadCondition>()?;
    m.add_class::<PyManifestConfig>()?;
    m.add_class::<PyManifestSplitDimCondition>()?;
    m.add_class::<PyManifestSplitCondition>()?;
    m.add_class::<PyManifestSplittingConfig>()?;
    m.add_class::<PyStorageSettings>()?;
    m.add_class::<PyGCSummary>()?;
    m.add_class::<PyDiff>()?;
    m.add_class::<VirtualChunkSpec>()?;
    m.add_function(wrap_pyfunction!(initialize_logs, m)?)?;
    m.add_function(wrap_pyfunction!(set_logs_filter, m)?)?;
    m.add_function(wrap_pyfunction!(spec_version, m)?)?;
    m.add_function(wrap_pyfunction!(cli_entrypoint, m)?)?;
    m.add("__version__", pep440_version())?;

    // Exceptions
    m.add("IcechunkError", py.get_type::<IcechunkError>())?;
    m.add("ConflictError", py.get_type::<PyConflictError>())?;
    m.add("RebaseFailedError", py.get_type::<PyRebaseFailedError>())?;
    m.add_class::<PyConflictType>()?;
    m.add_class::<PyConflict>()?;

    Ok(())
}
