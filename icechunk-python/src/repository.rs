use std::sync::Arc;

use icechunk::{storage::virtual_ref::{ObjectStoreVirtualChunkResolver, ObjectStoreVirtualChunkResolverConfig, VirtualChunkResolver}, Repository, RepositoryConfig};
use pyo3::{prelude::*, types::PyType};

use crate::{errors::PyIcechunkStoreError, storage::{PyStorageConfig, PyVirtualRefConfig}};

#[pyclass(name = "RepositoryConfig")]
#[derive(Clone, Debug)]
pub struct PyRepositoryConfig(RepositoryConfig);

#[pymethods]
impl PyRepositoryConfig {
    #[new]
    #[pyo3(signature = (*, inline_chunk_threshold_bytes = 512, unsafe_overwrite_refs = false))]
    fn new(inline_chunk_threshold_bytes: u16, unsafe_overwrite_refs: bool) -> Self {
        Self(RepositoryConfig { inline_chunk_threshold_bytes, unsafe_overwrite_refs })
    }
}

#[pyclass(name = "Repository")]
pub struct PyRepository(Repository);

#[pymethods]
impl PyRepository {
    #[classmethod]
    #[pyo3(signature = (config, storage, virtual_ref_config = None))]
    fn create(
        _cls: &Bound<'_, PyType>,
        config: PyRepositoryConfig,
        storage: PyStorageConfig,
        virtual_ref_config: Option<PyVirtualRefConfig>,
    ) -> PyResult<Self> {
        let repository = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async move {
                let storage = storage.create_storage().await?;
                let virtual_ref_config = virtual_ref_config.map(|v| {
                    let config = ObjectStoreVirtualChunkResolverConfig::from(&v);
                    let resolver = ObjectStoreVirtualChunkResolver::new(Some(config));
                    Arc::new(resolver) as Arc<dyn VirtualChunkResolver + Sync + Send>
                });
                Ok(Repository::create(config.0, storage, virtual_ref_config).await?)
            })?;

        Ok(Self(repository))
    }
}
