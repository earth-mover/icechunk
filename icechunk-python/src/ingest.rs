//! Python binding for `icechunk::ingest::ingest_zarr`.
//!
//! Bridges a `PyStorage` (the Python `icechunk.Storage`) plus a
//! `PyRepository` into the Rust ingest entry point. The `PyO3` layer
//! does one piece of marshalling — downcast the Python-provided
//! `Arc<dyn Storage>` to `&ObjectStorage` (the concrete type
//! `ingest_zarr` requires) — and then dispatches.

use std::sync::Arc;

use icechunk::{
    ObjectStorage,
    format::SnapshotId,
    ingest::{
        CollisionPolicy, DEFAULT_CHECKPOINT_EVERY, IngestError, IngestOptions,
        IngestOutcome, IngestStats, ingest_zarr,
    },
};
use pyo3::{exceptions::PyValueError, prelude::*};

use crate::{
    config::PyStorage,
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
    #[pyo3(get)]
    pub arrays_done: u64,
}

impl From<IngestStats> for PyIngestStats {
    fn from(s: IngestStats) -> Self {
        Self { keys: s.keys, bytes: s.bytes, arrays_done: s.arrays_done }
    }
}

#[pymethods]
impl PyIngestStats {
    fn __repr__(&self) -> String {
        format!(
            "IngestStats(keys={}, bytes={}, arrays_done={})",
            self.keys, self.bytes, self.arrays_done
        )
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

/// Python-side mirror of [`CollisionPolicy`].
#[pyclass(name = "CollisionPolicy", module = "icechunk", eq, eq_int)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PyCollisionPolicy {
    Fail,
    Skip,
    Overwrite,
}

impl From<CollisionPolicy> for PyCollisionPolicy {
    fn from(p: CollisionPolicy) -> Self {
        match p {
            CollisionPolicy::Fail => Self::Fail,
            CollisionPolicy::Skip => Self::Skip,
            CollisionPolicy::Overwrite => Self::Overwrite,
        }
    }
}

impl From<PyCollisionPolicy> for CollisionPolicy {
    fn from(p: PyCollisionPolicy) -> Self {
        match p {
            PyCollisionPolicy::Fail => Self::Fail,
            PyCollisionPolicy::Skip => Self::Skip,
            PyCollisionPolicy::Overwrite => Self::Overwrite,
        }
    }
}

/// Translate ingest errors to Python exceptions. Programmer-error
/// variants surface as `ValueError`; storage/session errors flow
/// through the standard `IcechunkError` channel.
fn ingest_err_to_py(err: IngestError) -> PyErr {
    match err {
        IngestError::KeyCollision(_)
        | IngestError::AlreadyComplete
        | IngestError::SkeletonMismatch { .. } => PyValueError::new_err(err.to_string()),
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
            let _ = cb.call1(py, (stats.keys, stats.bytes, stats.arrays_done));
        });
    }))
}

#[pyfunction]
#[pyo3(signature = (
    source,
    repo,
    paths,
    branch,
    concurrency,
    on_collision,
    verify_source_unchanged,
    checkpoint_every=None,
    message=None,
    progress=None,
))]
#[expect(clippy::too_many_arguments)]
pub(crate) fn py_ingest_zarr(
    py: Python<'_>,
    source: &PyStorage,
    repo: &PyRepository,
    paths: Vec<String>,
    branch: String,
    concurrency: usize,
    on_collision: PyCollisionPolicy,
    verify_source_unchanged: bool,
    checkpoint_every: Option<usize>,
    message: Option<String>,
    progress: Option<Py<PyAny>>,
) -> PyResult<PyIngestOutcome> {
    let mut options = IngestOptions::new()
        .paths(paths)
        .concurrency(concurrency)
        .on_collision(on_collision.into())
        .branch(branch)
        .verify_source_unchanged(verify_source_unchanged)
        .checkpoint_every(checkpoint_every.unwrap_or(DEFAULT_CHECKPOINT_EVERY));
    if let Some(m) = message {
        options = options.message(m);
    }
    if let Some(cb) = build_progress_cb(progress) {
        options = options.progress(cb);
    }

    // `ingest_zarr` requires an `&ObjectStorage` — the concrete `Storage`
    // impl that has an `ObjectStoreBackend`. The Python `icechunk.Storage`
    // exposed via `PyStorage` is constructed from `ObjectStorage` for
    // every supported backend (s3 / gcs / azure / local_filesystem /
    // http / in_memory), so the downcast is reliable in practice;
    // wrappers like `LatencyStorage` are intentionally rejected.
    let source_storage = Arc::clone(&source.0);
    let repo_arc = repo.inner();

    py.detach(move || {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let source_obj = source_storage
                .as_any()
                .downcast_ref::<ObjectStorage>()
                .ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "ingest source must be an object-store-backed icechunk.Storage; \
                         got Storage type `{}` which doesn't expose an ObjectStoreBackend",
                        source_storage.storage_info().backend_type,
                    ))
                })?;
            // Read lock held for the full ingest; only excludes admin
            // writes to the PyRepository wrapper.
            let repo_g = repo_arc.read().await;
            let outcome: IngestOutcome =
                ingest_zarr(source_obj, &repo_g, options)
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
