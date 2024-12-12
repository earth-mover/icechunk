use std::{borrow::Cow, ops::Deref, sync::Arc};

use icechunk::{session::Session, Store};
use pyo3::{exceptions::PyValueError, prelude::*};
use tokio::sync::RwLock;

use crate::{
    errors::PyIcechunkStoreError,
    store::{PyStore, PyStoreConfig},
};

#[pyclass(name = "Session")]
#[derive(Clone)]
pub struct PySession(Arc<RwLock<Session>>);

#[pymethods]
impl PySession {
    pub fn __eq__(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn as_bytes(&self) -> PyResult<Cow<[u8]>> {
        // FIXME: Use rmp_serde instead of serde_json to optimize performance
        let serialized = serde_json::to_vec(&self.0.blocking_read().deref())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Cow::Owned(serialized))
    }

    pub fn read_only(&self) -> bool {
        self.0.blocking_read().read_only()
    }

    pub fn snapshot_id(&self) -> String {
        self.0.blocking_read().snapshot_id().to_string()
    }

    pub fn branch(&self) -> Option<String> {
        self.0.blocking_read().branch().map(|b| b.to_string())
    }

    #[pyo3(signature = (config = None))]
    pub fn store(&self, config: Option<PyStoreConfig>) -> PyResult<PyStore> {
        let store = Store::from_session(
            self.0.clone(),
            config.map(|c| c.0).unwrap_or_default(),
            self.0.blocking_read().read_only(),
        );

        let store = Arc::new(RwLock::new(store));
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
}
