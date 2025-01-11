use std::{borrow::Cow, ops::Deref, sync::Arc};

use futures::TryStreamExt;
use icechunk::{session::Session, Store};
use pyo3::{prelude::*, types::PyType};
use tokio::sync::RwLock;

use crate::{
    conflicts::PyConflictSolver,
    errors::{PyIcechunkStoreError, PyIcechunkStoreResult},
    store::PyStore,
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
            let conc = session.config().get_partial_values_concurrency;
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

    pub fn merge(&self, other: &PySession, py: Python<'_>) -> PyResult<()> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            // TODO: Bad clone
            let changes = other.0.blocking_read().deref().changes().clone();

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .write()
                    .await
                    .merge(changes)
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(())
            })
        })
    }

    pub fn commit(&self, message: &str, py: Python<'_>) -> PyResult<String> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let snapshot_id = self
                    .0
                    .write()
                    .await
                    .commit(message, None)
                    .await
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
