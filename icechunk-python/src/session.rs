use std::{borrow::Cow, ops::Deref, sync::Arc};

use icechunk::{session::Session, Store};
use pyo3::{prelude::*, types::PyType};
use tokio::sync::RwLock;

use crate::{
    conflicts::PyConflictSolver,
    errors::{PyIcechunkStoreError, PyIcechunkStoreResult},
    store::{PyStore, PyStoreConfig},
};

#[pyclass]
#[derive(Clone)]
pub struct PySession(pub Arc<RwLock<Session>>);

#[pymethods]
impl PySession {
    #[classmethod]
    fn from_bytes(_cls: Bound<'_, PyType>, bytes: Vec<u8>) -> PyResult<Self> {
        let session =
            Session::from_bytes(bytes).map_err(PyIcechunkStoreError::SessionError)?;
        Ok(Self(Arc::new(RwLock::new(session))))
    }

    fn __eq__(&self, other: &PySession) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn as_bytes(&self) -> PyIcechunkStoreResult<Cow<[u8]>> {
        let bytes =
            self.0.blocking_read().as_bytes().map_err(PyIcechunkStoreError::from)?;
        Ok(Cow::Owned(bytes))
    }

    #[getter]
    pub fn read_only(&self) -> bool {
        self.0.blocking_read().read_only()
    }

    #[getter]
    pub fn snapshot_id(&self) -> String {
        self.0.blocking_read().snapshot_id().to_string()
    }

    #[getter]
    pub fn branch(&self) -> Option<String> {
        self.0.blocking_read().branch().map(|b| b.to_string())
    }

    #[getter]
    pub fn has_uncommitted_changes(&self) -> bool {
        self.0.blocking_read().has_uncommitted_changes()
    }

    pub fn discard_changes(&self) {
        self.0.blocking_write().discard_changes();
    }

    #[pyo3(signature = (config = None))]
    pub fn store(&self, config: Option<PyStoreConfig>) -> PyResult<PyStore> {
        let store =
            Store::from_session(self.0.clone(), config.map(|c| c.0).unwrap_or_default());

        let store = Arc::new(store);
        Ok(PyStore(store))
    }

    pub fn merge(&self, other: &PySession) -> PyResult<()> {
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
    }

    pub fn commit(&self, message: &str) -> PyResult<String> {
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
    }

    pub fn rebase(&self, solver: PyConflictSolver) -> PyResult<()> {
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
    }
}
