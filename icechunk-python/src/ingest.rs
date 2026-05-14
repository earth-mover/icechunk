//! Python binding for `icechunk::ingest::ingest_zarr`.
//!
//! Bridges a `pyo3-object_store`-extractable Python object (an obstore
//! `ObjectStore`) plus a `PyRepository` into the Rust ingest entry point.
//! The Rust core drives all sessions and commits itself; the Python
//! wrapper just unwraps the source and returns the final outcome.

use std::sync::Arc;

use icechunk::{
    format::SnapshotId,
    ingest::{
        DEFAULT_CHECKPOINT_EVERY, IngestError, IngestOptions, IngestOutcome, IngestStats,
        ingest_zarr,
    },
};
use object_store::{ObjectStore, path::Path as OsPath};
use pyo3::{exceptions::PyValueError, prelude::*};
use pyo3_object_store::AnyObjectStore;

use crate::{
    errors::{IcechunkError, PyIcechunkStoreError},
    repository::PyRepository,
};

/// Counters returned to Python after an ingest.
#[pyclass(name = "IngestStats", module = "icechunk", frozen)]
#[derive(Clone, Debug)]
pub(crate) struct PyIngestStats {
    #[pyo3(get)]
    pub keys: u64,
    #[pyo3(get)]
    pub bytes: u64,
}

impl From<IngestStats> for PyIngestStats {
    fn from(s: IngestStats) -> Self {
        Self { keys: s.keys, bytes: s.bytes }
    }
}

#[pymethods]
impl PyIngestStats {
    fn __repr__(&self) -> String {
        format!("IngestStats(keys={}, bytes={})", self.keys, self.bytes)
    }
}

/// Final outcome of a `py_ingest_zarr` call: the snapshot id of the
/// last commit plus running counters.
#[pyclass(name = "IngestOutcome", module = "icechunk", frozen)]
#[derive(Clone, Debug)]
pub(crate) struct PyIngestOutcome {
    #[pyo3(get)]
    pub snapshot_id: String,
    #[pyo3(get)]
    pub stats: PyIngestStats,
}

#[pymethods]
impl PyIngestOutcome {
    fn __repr__(&self) -> String {
        format!(
            "IngestOutcome(snapshot_id={:?}, stats={})",
            self.snapshot_id,
            self.stats.__repr__()
        )
    }
}

/// Translate ingest errors to Python exceptions. Programmer-error
/// variants surface as `ValueError`; storage/session errors flow
/// through the standard `IcechunkError` channel.
fn ingest_err_to_py(err: IngestError) -> PyErr {
    match err {
        IngestError::IncompatibleOptions => PyValueError::new_err(err.to_string()),
        IngestError::KeyCollision(_) => PyValueError::new_err(err.to_string()),
        IngestError::AlreadyComplete => PyValueError::new_err(err.to_string()),
        IngestError::SkeletonMismatch => PyValueError::new_err(err.to_string()),
        // Malformed ingest state in the repository — internal/unexpected error.
        IngestError::BadProperties(_) => {
            PyErr::new::<IcechunkError, String>(err.to_string())
        }
        IngestError::Store(e) => PyIcechunkStoreError::from(e).into(),
        IngestError::Session(e) => PyIcechunkStoreError::from(e).into(),
        IngestError::Repository(e) => PyIcechunkStoreError::from(e).into(),
        IngestError::ObjectStore(e) => {
            PyIcechunkStoreError::PyValueError(format!("source object_store error: {e}"))
                .into()
        }
        // Fallback for any future non-exhaustive variants.
        other => PyErr::new::<IcechunkError, String>(other.to_string()),
    }
}

/// Bridge a Python callable into the Rust progress-callback shape.
fn build_progress_cb(
    progress: Option<Py<PyAny>>,
) -> Option<Arc<dyn Fn(IngestStats) + Send + Sync>> {
    let cb = progress?;
    Some(Arc::new(move |stats: IngestStats| {
        Python::attach(|py| {
            // Best-effort: drop callback errors rather than panicking
            // mid-ingest. Users surface progress for observability, not
            // control flow.
            let _ = cb.call1(py, (stats.keys, stats.bytes));
        });
    }))
}

#[pyfunction]
#[pyo3(signature = (
    source,
    source_prefix,
    repo,
    paths,
    branch,
    concurrency,
    skip_existing,
    overwrite,
    checkpoint_every=None,
    message=None,
    progress=None,
))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn py_ingest_zarr(
    py: Python<'_>,
    source: AnyObjectStore,
    source_prefix: String,
    repo: &PyRepository,
    paths: Vec<String>,
    branch: String,
    concurrency: usize,
    skip_existing: bool,
    overwrite: bool,
    checkpoint_every: Option<usize>,
    message: Option<String>,
    progress: Option<Py<PyAny>>,
) -> PyResult<PyIngestOutcome> {
    let source: Arc<dyn ObjectStore> = source.into_dyn();
    let prefix = OsPath::from(source_prefix.as_str());

    let mut options = IngestOptions::new()
        .paths(paths)
        .concurrency(concurrency)
        .skip_existing(skip_existing)
        .branch(branch)
        .checkpoint_every(checkpoint_every.unwrap_or(DEFAULT_CHECKPOINT_EVERY));
    if overwrite {
        options = options.overwrite(true);
    }
    if let Some(m) = message {
        options = options.message(m);
    }
    if let Some(cb) = build_progress_cb(progress) {
        options = options.progress(cb);
    }

    let repo_arc = repo.inner();

    py.detach(move || {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let outcome: IngestOutcome = ingest_zarr(source, &prefix, repo_arc, options)
                .await
                .map_err(ingest_err_to_py)?;
            let snapshot_id: SnapshotId = outcome.final_snapshot_id;
            Ok(PyIngestOutcome {
                snapshot_id: snapshot_id.to_string(),
                stats: PyIngestStats::from(outcome.stats),
            })
        })
    })
}
