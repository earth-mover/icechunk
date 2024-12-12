use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use icechunk::{
    format::{snapshot::SnapshotMetadata, ChunkOffset, SnapshotId},
    repository::RepositoryError,
    storage::virtual_ref::{
        ObjectStoreVirtualChunkResolver, ObjectStoreVirtualChunkResolverConfig,
        VirtualChunkResolver,
    },
    Repository, RepositoryConfig,
};
use pyo3::{prelude::*, types::PyType};

use crate::{
    errors::PyIcechunkStoreError,
    storage::{PyStorageConfig, PyVirtualRefConfig},
};

#[pyclass(name = "SnapshotMetadata")]
#[derive(Clone, Debug)]
pub struct PySnapshotMetadata {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    written_at: DateTime<Utc>,
    #[pyo3(get)]
    message: String,
}

impl From<SnapshotMetadata> for PySnapshotMetadata {
    fn from(val: SnapshotMetadata) -> Self {
        PySnapshotMetadata {
            id: val.id.to_string(),
            written_at: val.written_at,
            message: val.message,
        }
    }
}

pub type KeyRanges = Vec<(String, (Option<ChunkOffset>, Option<ChunkOffset>))>;

#[pyclass(name = "RepositoryConfig")]
#[derive(Clone, Debug)]
pub struct PyRepositoryConfig(RepositoryConfig);

#[pymethods]
impl PyRepositoryConfig {
    #[new]
    #[pyo3(signature = (*, inline_chunk_threshold_bytes = 512, unsafe_overwrite_refs = false))]
    fn new(inline_chunk_threshold_bytes: u16, unsafe_overwrite_refs: bool) -> Self {
        Self(RepositoryConfig { inline_chunk_threshold_bytes, unsafe_overwrite_refs, ..Default::default() })
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
        let repository =
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = storage
                    .create_storage()
                    .await
                    .map_err(PyIcechunkStoreError::StorageError)?;
                if Repository::exists(storage.as_ref())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?
                {
                    return Err(PyIcechunkStoreError::RepositoryError(
                        RepositoryError::AlreadyInitialized,
                    ));
                }

                Ok(Repository::create(Some(config.0), None, storage, HashMap::new())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?)
            })?;

        Ok(Self(repository))
    }

    #[classmethod]
    #[pyo3(signature = (config, storage, virtual_ref_config = None))]
    fn open(
        _cls: &Bound<'_, PyType>,
        config: PyRepositoryConfig,
        storage: PyStorageConfig,
        virtual_ref_config: Option<PyVirtualRefConfig>,
    ) -> PyResult<Self> {
        let repository =
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = storage
                    .create_storage()
                    .await
                    .map_err(PyIcechunkStoreError::StorageError)?;
                if !Repository::exists(storage.as_ref())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?
                {
                    return Err(PyIcechunkStoreError::RepositoryError(
                        RepositoryError::RepositoryDoesntExist,
                    ));
                }

                Ok(Repository::open(Some(config.0), NOne, storage, HashMap::new())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?)
            })?;

        Ok(Self(repository))
    }

    #[classmethod]
    #[pyo3(signature = (config, storage, virtual_ref_config = None))]
    fn open_or_create(
        _cls: &Bound<'_, PyType>,
        config: PyRepositoryConfig,
        storage: PyStorageConfig,
        virtual_ref_config: Option<PyVirtualRefConfig>,
    ) -> PyResult<Self> {
        let repository =
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = storage
                    .create_storage()
                    .await
                    .map_err(PyIcechunkStoreError::StorageError)?;

                Ok::<_, PyErr>(
                    Repository::open_or_create(Some(config.0), None, storage, HashMap::new())
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)?,
                )
            })?;

        Ok(Self(repository))
    }

    #[staticmethod]
    fn exists(storage: PyStorageConfig) -> PyResult<bool> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let storage = storage
                .create_storage()
                .await
                .map_err(PyIcechunkStoreError::StorageError)?;
            let exists = Repository::exists(storage.as_ref())
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(exists)
        })
    }

    pub fn ancestry(&self, snapshot_id: &str) -> PyResult<Vec<PySnapshotMetadata>> {
        let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
            PyIcechunkStoreError::RepositoryError(RepositoryError::InvalidSnapshotId(
                snapshot_id.to_owned(),
            ))
        })?;

        // TODO: this holds everything in memory
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let ancestry = self
                .0
                .ancestry(&snapshot_id)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?
                .map_ok(Into::<PySnapshotMetadata>::into)
                .try_collect::<Vec<_>>()
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(ancestry)
        })
    }

    pub fn create_branch(&self, branch_name: &str, snapshot_id: &str) -> PyResult<()> {
        let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
            PyIcechunkStoreError::RepositoryError(RepositoryError::InvalidSnapshotId(
                snapshot_id.to_owned(),
            ))
        })?;

        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            self.0
                .create_branch(branch_name, &snapshot_id)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(())
        })
    }

    pub fn list_branches(&self) -> PyResult<Vec<String>> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let branches = self
                .0
                .list_branches()
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(branches)
        })
    }

    pub fn branch_tip(&self, branch_name: &str) -> PyResult<String> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let tip = self
                .0
                .branch_tip(branch_name)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(tip.to_string())
        })
    }

    pub fn reset_branch(&self, branch_name: &str, snapshot_id: &str) -> PyResult<()> {
        let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
            PyIcechunkStoreError::RepositoryError(RepositoryError::InvalidSnapshotId(
                snapshot_id.to_owned(),
            ))
        })?;

        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            self.0
                .reset_branch(branch_name, &snapshot_id)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(())
        })
    }

    pub fn create_tag(&self, tag_name: &str, snapshot_id: &str) -> PyResult<()> {
        let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
            PyIcechunkStoreError::RepositoryError(RepositoryError::InvalidSnapshotId(
                snapshot_id.to_owned(),
            ))
        })?;

        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            self.0
                .create_tag(tag_name, &snapshot_id)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(())
        })
    }

    pub fn list_tags(&self) -> PyResult<Vec<String>> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let tags = self
                .0
                .list_tags()
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(tags)
        })
    }
}
