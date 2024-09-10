mod errors;
mod streams;

use std::{pin::Pin, sync::Arc};

use ::icechunk::{
    format::ChunkOffset,
    zarr::{ByteRange, StoreError},
    Store,
};
use bytes::Bytes;
use errors::IcechunkStoreResult;
use futures::Stream;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};
use streams::AsyncStringGenerator;
use tokio::sync::Mutex;

#[pyclass]
struct IcechunkStore {
    store: Store,
}

impl IcechunkStore {
    async fn from_json_config(json: &[u8]) -> Result<Self, String> {
        let store = Store::from_json_config(json).await?;
        Ok(Self { store })
    }
}

#[pyfunction]
async fn icechunk_store_from_json_config(json: String) -> PyResult<IcechunkStore> {
    let json = json.as_bytes();
    IcechunkStore::from_json_config(json).await.map_err(PyValueError::new_err)
}

#[pymethods]
impl IcechunkStore {
    pub async fn empty(&self) -> IcechunkStoreResult<bool> {
        let is_empty = self.store.empty().await?;
        Ok(is_empty)
    }

    pub async fn clear(&mut self) -> IcechunkStoreResult<()> {
        self.store.clear().await?;
        Ok(())
    }

    pub async fn get(
        &self,
        key: String,
        byte_range: Option<ByteRange>,
    ) -> IcechunkStoreResult<PyObject> {
        let byte_range = byte_range.unwrap_or((None, None));
        let data = self.store.get(&key, &byte_range).await?;
        let pybytes = Python::with_gil(|py| {
            let bound_bytes = PyBytes::new_bound(py, &data);
            bound_bytes.to_object(py)
        });
        Ok(pybytes)
    }

    pub async fn get_partial_values(
        &self,
        key_ranges: Vec<(String, ByteRange)>,
    ) -> IcechunkStoreResult<Vec<Option<PyObject>>> {
        let iter = key_ranges.into_iter();
        let result = self
            .store
            .get_partial_values(iter)
            .await?
            .into_iter()
            // If we want to error instead of returning None we can collect into
            // a Result<Vec<_>, _> and short circuit
            .map(|x| {
                x.map(|x| {
                    Python::with_gil(|py| {
                        let bound_bytes = PyBytes::new_bound(py, &x);
                        bound_bytes.to_object(py)
                    })
                })
                .ok()
            })
            .collect();

        Ok(result)
    }

    pub async fn exists<'a>(&self, key: String) -> IcechunkStoreResult<bool> {
        let exists = self.store.exists(&key).await?;
        Ok(exists)
    }

    pub fn supports_writes(&self) -> IcechunkStoreResult<bool> {
        let supports_writes = self.store.supports_writes()?;
        Ok(supports_writes)
    }

    pub async fn set(&mut self, key: String, value: Vec<u8>) -> IcechunkStoreResult<()> {
        self.store.set(&key, value.into()).await?;
        Ok(())
    }

    pub async fn delete(&mut self, key: String) -> IcechunkStoreResult<()> {
        self.store.delete(&key).await?;
        Ok(())
    }

    pub fn supports_partial_writes(&self) -> IcechunkStoreResult<bool> {
        let supports_partial_writes = self.store.supports_partial_writes()?;
        Ok(supports_partial_writes)
    }

    pub async fn set_partial_values(
        &mut self,
        key_start_values: Vec<(String, ChunkOffset, Vec<u8>)>,
    ) -> IcechunkStoreResult<()> {
        // We need to get our own copy of the keys to pass to the downstream store function because that
        // function requires a Vec<&str, which we cannot borrow from when we are borrowing from the tuple
        //
        // There is a choice made here, to clone the keys. This is because the keys are small and the
        // alternative is to have to clone the bytes instead of a copy which is usually more expensive
        // depending on the size of the chunk.
        let keys =
            key_start_values.iter().map(|(key, _, _)| key.clone()).collect::<Vec<_>>();

        let mapped_to_bytes =
            key_start_values.into_iter().enumerate().map(|(i, (_key, offset, value))| {
                (keys[i].as_str(), offset, Bytes::from(value))
            });

        let result = self.store.set_partial_values(mapped_to_bytes).await?;
        Ok(result)
    }

    pub fn supports_listing(&self) -> IcechunkStoreResult<bool> {
        let supports_listing = self.store.supports_listing()?;
        Ok(supports_listing)
    }

    pub async fn list(&self) -> IcechunkStoreResult<AsyncStringGenerator> {
        let list = self.store.list().await?;
        let prepared_list = pin_extend_stream(list);
        Ok(AsyncStringGenerator::new(prepared_list))
    }

    pub async fn list_prefix(
        &self,
        prefix: String,
    ) -> IcechunkStoreResult<AsyncStringGenerator> {
        let list = self.store.list_prefix(&prefix).await?;
        let prepared_list = pin_extend_stream(list);
        Ok(AsyncStringGenerator::new(prepared_list))
    }

    pub async fn list_dir(
        &self,
        prefix: String,
    ) -> IcechunkStoreResult<AsyncStringGenerator> {
        let list = self.store.list_dir(&prefix).await?;
        let prepared_list = pin_extend_stream(list);
        Ok(AsyncStringGenerator::new(prepared_list))
    }
}

// Prepares a stream for use in a Python AsyncGenerator impl.
// This is possibly unsafe. We have to do this because the stream is pinned
// and the lifetime of the stream is tied to the lifetime of the store. So the return
// value is valid as long as the store is valid. Rust has no way to express this to
// the Python runtime so we have to do it manually.
fn pin_extend_stream<'a>(
    stream: impl Stream<Item = Result<String, StoreError>> + Send,
) -> Arc<Mutex<Pin<Box<dyn Stream<Item = Result<String, StoreError>> + 'a + Send>>>> {
    let pinned = Box::pin(stream);
    let extended_stream = unsafe {
        core::mem::transmute::<
            Pin<Box<dyn Stream<Item = Result<String, StoreError>> + Send>>,
            Pin<Box<dyn Stream<Item = Result<String, StoreError>> + 'a + Send>>,
        >(pinned)
    };

    let mutexed_stream = Arc::new(Mutex::new(extended_stream));
    mutexed_stream
}

/// The icechunk Python module implemented in Rust.
#[pymodule]
fn _icechunk_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<IcechunkStore>()?;
    m.add_function(wrap_pyfunction!(icechunk_store_from_json_config, m)?)?;
    Ok(())
}
