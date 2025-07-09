use std::{borrow::Cow, ops::Deref, sync::Arc};

use async_stream::try_stream;
use futures::{StreamExt, TryStreamExt};
use icechunk::{Store, session::Session};
use pyo3::{prelude::*, types::PyType};
use tokio::sync::{Mutex, RwLock};

use crate::{
    conflicts::PyConflictSolver,
    errors::{PyIcechunkStoreError, PyIcechunkStoreResult},
    repository::{PyDiff, PySnapshotProperties},
    store::PyStore,
    streams::PyAsyncGenerator,
};

#[pyclass]
#[derive(Clone)]
pub struct PySession(pub Arc<RwLock<Session>>);

#[pymethods]
/// Most functions in this class block, so they need to `allow_threads` so other
/// python threads can make progress
impl PySession {
    #[classmethod]
    fn from_bytes(
        _cls: Bound<'_, PyType>,
        py: Python<'_>,
        bytes: Vec<u8>,
    ) -> PyResult<Self> {
        // This is a compute intensive task, we need to release the Gil
        py.allow_threads(move || {
            let session =
                Session::from_bytes(bytes).map_err(PyIcechunkStoreError::SessionError)?;
            Ok(Self(Arc::new(RwLock::new(session))))
        })
    }

    fn __eq__(&self, other: &PySession) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn as_bytes(&self, py: Python<'_>) -> PyIcechunkStoreResult<Cow<[u8]>> {
        // This is a compute intensive task, we need to release the Gil
        py.allow_threads(move || {
            let bytes =
                self.0.blocking_read().as_bytes().map_err(PyIcechunkStoreError::from)?;
            Ok(Cow::Owned(bytes))
        })
    }

    #[getter]
    pub fn read_only(&self, py: Python<'_>) -> bool {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || self.0.blocking_read().read_only())
    }

    #[getter]
    pub fn snapshot_id(&self, py: Python<'_>) -> String {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || self.0.blocking_read().snapshot_id().to_string())
    }

    #[getter]
    pub fn branch(&self, py: Python<'_>) -> Option<String> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || self.0.blocking_read().branch().map(|b| b.to_string()))
    }

    #[getter]
    pub fn has_uncommitted_changes(&self, py: Python<'_>) -> bool {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || self.0.blocking_read().has_uncommitted_changes())
    }

    pub fn status(&self, py: Python<'_>) -> PyResult<PyDiff> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            let session = self.0.blocking_read();

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let res =
                    session.status().await.map_err(PyIcechunkStoreError::SessionError)?;
                Ok(res.into())
            })
        })
    }

    pub fn discard_changes(&self, py: Python<'_>) {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            self.0.blocking_write().discard_changes();
        })
    }

    #[getter]
    pub fn store(&self, py: Python<'_>) -> PyResult<PyStore> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            let session = self.0.blocking_read();
            let conc = session.config().get_partial_values_concurrency();
            let store = Store::from_session_and_config(self.0.clone(), conc);

            let store = Arc::new(store);
            Ok(PyStore(store))
        })
    }

    pub fn all_virtual_chunk_locations(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            let session = self.0.blocking_read();

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let res = session
                    .all_virtual_chunk_locations()
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?
                    .try_collect()
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;

                Ok(res)
            })
        })
    }

    /// Return vectors of coordinates, up to batch_size in length.
    ///
    /// We batch the results to make it faster.
    pub fn chunk_coordinates(
        &self,
        array_path: String,
        batch_size: u32,
    ) -> PyResult<PyAsyncGenerator> {
        // This is blocking function, we need to release the Gil
        let session = self.0.clone();
        let res = try_stream! {
            let session = session.read_owned().await;
            let array_path = array_path.try_into().map_err(|e| PyIcechunkStoreError::PyValueError(format!("Invalid path: {e}")))?;

            let stream = session
                .chunk_coordinates(&array_path)
                .await
                .map_err(PyIcechunkStoreError::SessionError)?
                .map_err(PyIcechunkStoreError::SessionError)
                .chunks(batch_size as usize);

            #[allow(unused_braces, deprecated)]
            { for await coords_vec in stream {
                let vec = coords_vec
                    .into_iter()
                    .map(|maybe_coord| maybe_coord.map(|coord| Python::with_gil(|py| coord.0.to_object(py))))
                    .collect::<Result<Vec<_>, _>>()?;

                let vec = Python::with_gil(|py| vec.to_object(py));
                yield vec
            } }
        };

        let prepared_list = Arc::new(Mutex::new(res.boxed()));
        Ok(PyAsyncGenerator::new(prepared_list))
    }

    pub fn merge(&self, other: &PySession, py: Python<'_>) -> PyResult<()> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            // TODO: bad clone
            let other = other.0.blocking_read().deref().clone();

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .write()
                    .await
                    .merge(other)
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(())
            })
        })
    }

    #[pyo3(signature = (message, metadata=None, rebase_with=None, rebase_tries=1_000))]
    pub fn commit(
        &self,
        py: Python<'_>,
        message: &str,
        metadata: Option<PySnapshotProperties>,
        rebase_with: Option<PyConflictSolver>,
        rebase_tries: Option<u16>,
    ) -> PyResult<String> {
        let metadata = metadata.map(|m| m.into());
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut session = self.0.write().await;
                let snapshot_id = if let Some(solver) = rebase_with {
                    session
                        .commit_rebasing(
                            solver.as_ref(),
                            rebase_tries.unwrap_or(1_000),
                            message,
                            metadata,
                            |_| async {},
                            |_| async {},
                        )
                        .await
                } else {
                    session.commit(message, metadata).await
                }
                .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(snapshot_id.to_string())
            })
        })
    }

    pub fn rebase(&self, solver: PyConflictSolver, py: Python<'_>) -> PyResult<()> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            let solver = solver.as_ref();
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                self.0
                    .write()
                    .await
                    .rebase(solver)
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(())
            })
        })
    }
}
