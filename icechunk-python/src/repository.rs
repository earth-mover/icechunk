use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use itertools::Itertools;

use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use icechunk::{
    Repository,
    config::Credentials,
    format::{
        SnapshotId,
        snapshot::{ManifestFileInfo, SnapshotInfo, SnapshotProperties},
        transaction_log::Diff,
    },
    ops::{
        gc::{ExpiredRefAction, GCConfig, GCSummary, expire, garbage_collect},
        manifests::rewrite_manifests,
        stats::repo_chunks_storage,
    },
    repository::{RepositoryErrorKind, VersionInfo},
};
use pyo3::{
    IntoPyObjectExt,
    exceptions::PyValueError,
    prelude::*,
    types::{PyDict, PyNone, PyType},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::{
    config::{
        PyCredentials, PyRepositoryConfig, PyStorage, PyStorageSettings, datetime_repr,
        format_option_to_string,
    },
    errors::PyIcechunkStoreError,
    impl_pickle,
    session::PySession,
    streams::PyAsyncGenerator,
};

/// Wrapper needed to implement pyo3 conversion classes
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonValue(pub serde_json::Value);

/// Wrapper needed to implement pyo3 conversion classes
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PySnapshotProperties(pub HashMap<String, JsonValue>);

#[pyclass(name = "SnapshotInfo", eq)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PySnapshotInfo {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    parent_id: Option<String>,
    #[pyo3(get)]
    written_at: DateTime<Utc>,
    #[pyo3(get)]
    message: String,
    #[pyo3(get)]
    metadata: PySnapshotProperties,
    #[pyo3(get)]
    manifests: Vec<PyManifestFileInfo>,
}

impl_pickle!(PySnapshotInfo);

#[pyclass(name = "ManifestFileInfo", eq)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PyManifestFileInfo {
    pub id: String,
    pub size_bytes: u64,
    pub num_chunk_refs: u32,
}

impl_pickle!(PyManifestFileInfo);

impl<'py> FromPyObject<'py> for PySnapshotProperties {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let m: HashMap<String, JsonValue> = ob.extract()?;
        Ok(Self(m))
    }
}

impl<'py> FromPyObject<'py> for JsonValue {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let value = ob
            .extract()
            .map(serde_json::Value::String)
            .or(ob.extract().map(serde_json::Value::Bool))
            .or(ob.downcast().map(|_: &Bound<'py, PyNone>| serde_json::Value::Null))
            .or(ob.extract().map(|n: i64| serde_json::Value::from(n)))
            .or(ob.extract().map(|n: u64| serde_json::Value::from(n)))
            .or(ob.extract().map(|n: f64| serde_json::Value::from(n)))
            .or(ob.extract().map(|xs: Vec<JsonValue>| serde_json::Value::from(xs)))
            .or(ob.extract().map(|xs: HashMap<String, JsonValue>| {
                serde_json::Value::Object(xs.into_iter().map(|(k, v)| (k, v.0)).collect())
            }))?;
        Ok(JsonValue(value))
    }
}

impl<'py> IntoPyObject<'py> for JsonValue {
    type Target = PyAny;

    type Output = Bound<'py, Self::Target>;

    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            serde_json::Value::Null => Ok(PyNone::get(py).to_owned().into_any()),
            serde_json::Value::Bool(b) => b.into_bound_py_any(py),
            serde_json::Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    n.as_i128().into_bound_py_any(py)
                } else {
                    n.as_f64().into_bound_py_any(py)
                }
            }
            serde_json::Value::String(s) => s.clone().into_bound_py_any(py),
            serde_json::Value::Array(vec) => {
                let res: Vec<_> = vec
                    .into_iter()
                    .map(|v| JsonValue(v).into_bound_py_any(py))
                    .try_collect()?;
                res.into_bound_py_any(py)
            }
            serde_json::Value::Object(map) => {
                let res: HashMap<_, _> = map
                    .into_iter()
                    .map(|(k, v)| JsonValue(v).into_bound_py_any(py).map(|v| (k, v)))
                    .try_collect()?;
                res.into_bound_py_any(py)
            }
        }
    }
}

impl<'py> IntoPyObject<'py> for PySnapshotProperties {
    type Target = PyDict;

    type Output = Bound<'py, Self::Target>;

    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.into_pyobject(py)
    }
}

impl From<JsonValue> for serde_json::Value {
    fn from(value: JsonValue) -> Self {
        value.0
    }
}

impl From<serde_json::Value> for JsonValue {
    fn from(value: serde_json::Value) -> Self {
        JsonValue(value)
    }
}

impl From<SnapshotProperties> for PySnapshotProperties {
    fn from(value: SnapshotProperties) -> Self {
        PySnapshotProperties(value.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl From<PySnapshotProperties> for SnapshotProperties {
    fn from(value: PySnapshotProperties) -> Self {
        value.0.into_iter().map(|(k, v)| (k, v.into())).collect()
    }
}

impl From<ManifestFileInfo> for PyManifestFileInfo {
    fn from(val: ManifestFileInfo) -> Self {
        Self {
            id: val.id.to_string(),
            size_bytes: val.size_bytes,
            num_chunk_refs: val.num_chunk_refs,
        }
    }
}

impl From<SnapshotInfo> for PySnapshotInfo {
    fn from(val: SnapshotInfo) -> Self {
        Self {
            id: val.id.to_string(),
            parent_id: val.parent_id.map(|id| id.to_string()),
            written_at: val.flushed_at,
            message: val.message,
            metadata: val.metadata.into(),
            manifests: val.manifests.into_iter().map(|v| v.into()).collect(),
        }
    }
}

#[pymethods]
impl PyManifestFileInfo {
    pub fn __repr__(&self) -> String {
        format!(
            r#"ManifestFileInfo(id="{id}", size_bytes={size}, num_chunk_refs={chunks})"#,
            id = self.id,
            size = self.size_bytes,
            chunks = self.num_chunk_refs,
        )
    }
}

#[pymethods]
impl PySnapshotInfo {
    pub fn __repr__(&self) -> String {
        // TODO: escape
        format!(
            r#"SnapshotInfo(id="{id}", parent_id={parent}, written_at={at}, message="{message}")"#,
            id = self.id,
            parent = format_option_to_string(self.parent_id.as_ref()),
            at = datetime_repr(&self.written_at),
            message = self.message.chars().take(10).collect::<String>() + "...",
        )
    }
}

#[pyclass(name = "Diff", eq)]
#[derive(Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PyDiff {
    #[pyo3(get)]
    pub new_groups: BTreeSet<String>,
    #[pyo3(get)]
    pub new_arrays: BTreeSet<String>,
    #[pyo3(get)]
    pub deleted_groups: BTreeSet<String>,
    #[pyo3(get)]
    pub deleted_arrays: BTreeSet<String>,
    #[pyo3(get)]
    pub updated_groups: BTreeSet<String>,
    #[pyo3(get)]
    pub updated_arrays: BTreeSet<String>,
    #[pyo3(get)]
    // A Vec instead of a set to avoid issues with list not being hashable in python
    pub updated_chunks: BTreeMap<String, Vec<Vec<u32>>>,
}

impl From<Diff> for PyDiff {
    fn from(value: Diff) -> Self {
        let new_groups =
            value.new_groups.into_iter().map(|path| path.to_string()).collect();
        let new_arrays =
            value.new_arrays.into_iter().map(|path| path.to_string()).collect();
        let deleted_groups =
            value.deleted_groups.into_iter().map(|path| path.to_string()).collect();
        let deleted_arrays =
            value.deleted_arrays.into_iter().map(|path| path.to_string()).collect();
        let updated_groups =
            value.updated_groups.into_iter().map(|path| path.to_string()).collect();
        let updated_arrays =
            value.updated_arrays.into_iter().map(|path| path.to_string()).collect();
        let updated_chunks = value
            .updated_chunks
            .into_iter()
            .map(|(k, v)| {
                let path = k.to_string();
                let map = v.into_iter().map(|idx| idx.0).collect();
                (path, map)
            })
            .collect();

        PyDiff {
            new_groups,
            new_arrays,
            deleted_groups,
            deleted_arrays,
            updated_groups,
            updated_arrays,
            updated_chunks,
        }
    }
}

#[pymethods]
impl PyDiff {
    pub fn __repr__(&self) -> String {
        let mut res = String::new();
        use std::fmt::Write;

        if !self.new_groups.is_empty() {
            res.push_str("Groups created:\n");
            for g in self.new_groups.iter() {
                writeln!(res, "    {g}").unwrap();
            }
            res.push('\n');
        }
        if !self.new_arrays.is_empty() {
            res.push_str("Arrays created:\n");
            for g in self.new_arrays.iter() {
                writeln!(res, "    {g}").unwrap();
            }
            res.push('\n');
        }

        if !self.updated_groups.is_empty() {
            res.push_str("Group definitions updated:\n");
            for g in self.updated_groups.iter() {
                writeln!(res, "    {g}").unwrap();
            }
            res.push('\n');
        }

        if !self.updated_arrays.is_empty() {
            res.push_str("Array definitions updated:\n");
            for g in self.updated_arrays.iter() {
                writeln!(res, "    {g}").unwrap();
            }
            res.push('\n');
        }

        if !self.deleted_groups.is_empty() {
            res.push_str("Groups deleted:\n");
            for g in self.deleted_groups.iter() {
                writeln!(res, "    {g}").unwrap();
            }
            res.push('\n');
        }

        if !self.deleted_arrays.is_empty() {
            res.push_str("Arrays deleted:\n");
            for g in self.deleted_arrays.iter() {
                writeln!(res, "    {g}").unwrap();
            }
            res.push('\n');
        }

        if !self.updated_chunks.is_empty() {
            res.push_str("Chunks updated:\n");
            for (path, chunks) in self.updated_chunks.iter() {
                writeln!(res, "    {path}:").unwrap();
                let coords = chunks
                    .iter()
                    .map(|idx| format!("        [{}]", idx.iter().join(", ")))
                    .take(10)
                    .join("\n");
                res.push_str(coords.as_str());
                res.push('\n');
                if chunks.len() > 10 {
                    writeln!(res, "        ... {} more", chunks.len() - 10).unwrap();
                }
            }
        }
        res
    }
}

impl_pickle!(PyDiff);

#[pyclass(name = "GCSummary", eq)]
#[derive(Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PyGCSummary {
    #[pyo3(get)]
    pub bytes_deleted: u64,
    #[pyo3(get)]
    pub chunks_deleted: u64,
    #[pyo3(get)]
    pub manifests_deleted: u64,
    #[pyo3(get)]
    pub snapshots_deleted: u64,
    #[pyo3(get)]
    pub attributes_deleted: u64,
    #[pyo3(get)]
    pub transaction_logs_deleted: u64,
}

impl From<GCSummary> for PyGCSummary {
    fn from(value: GCSummary) -> Self {
        Self {
            bytes_deleted: value.bytes_deleted,
            chunks_deleted: value.chunks_deleted,
            manifests_deleted: value.manifests_deleted,
            snapshots_deleted: value.snapshots_deleted,
            attributes_deleted: value.attributes_deleted,
            transaction_logs_deleted: value.transaction_logs_deleted,
        }
    }
}

#[pymethods]
impl PyGCSummary {
    pub fn __repr__(&self) -> String {
        format!(
            r#"GCSummary(bytes_deleted={bytes}, chunks_deleted={chunks}, manifests_deleted={manifests}, snapshots_deleted={snapshots}, attributes_deleted={atts}, transaction_logs_deleted={txs})"#,
            bytes = self.bytes_deleted,
            chunks = self.chunks_deleted,
            manifests = self.manifests_deleted,
            snapshots = self.snapshots_deleted,
            atts = self.attributes_deleted,
            txs = self.transaction_logs_deleted,
        )
    }
}

impl_pickle!(PyGCSummary);

#[pyclass]
pub struct PyRepository(Arc<RwLock<Repository>>);

#[pymethods]
/// Most functions in this class call `Runtime.block_on` so they need to `allow_threads` so other
/// python threads can make progress in the case of an actual block
impl PyRepository {
    #[classmethod]
    #[pyo3(signature = (storage, *, config = None, authorize_virtual_chunk_access = None))]
    fn create(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        storage: PyStorage,
        config: Option<&PyRepositoryConfig>,
        authorize_virtual_chunk_access: Option<HashMap<String, Option<PyCredentials>>>,
    ) -> PyResult<Self> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let repository =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let config = config
                        .map(|c| c.try_into().map_err(PyValueError::new_err))
                        .transpose()?;
                    Repository::create(
                        config,
                        storage.0,
                        map_credentials(authorize_virtual_chunk_access),
                    )
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)
                })?;

            Ok(Self(Arc::new(RwLock::new(repository))))
        })
    }

    #[classmethod]
    #[pyo3(signature = (storage, *, config = None, authorize_virtual_chunk_access = None))]
    fn open(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        storage: PyStorage,
        config: Option<&PyRepositoryConfig>,
        authorize_virtual_chunk_access: Option<HashMap<String, Option<PyCredentials>>>,
    ) -> PyResult<Self> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let repository =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let config = config
                        .map(|c| c.try_into().map_err(PyValueError::new_err))
                        .transpose()?;
                    Repository::open(
                        config,
                        storage.0,
                        map_credentials(authorize_virtual_chunk_access),
                    )
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)
                })?;

            Ok(Self(Arc::new(RwLock::new(repository))))
        })
    }

    #[classmethod]
    #[pyo3(signature = (storage, *, config = None, authorize_virtual_chunk_access = None))]
    fn open_or_create(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        storage: PyStorage,
        config: Option<&PyRepositoryConfig>,
        authorize_virtual_chunk_access: Option<HashMap<String, Option<PyCredentials>>>,
    ) -> PyResult<Self> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let repository =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let config = config
                        .map(|c| c.try_into().map_err(PyValueError::new_err))
                        .transpose()?;
                    Ok::<_, PyErr>(
                        Repository::open_or_create(
                            config,
                            storage.0,
                            map_credentials(authorize_virtual_chunk_access),
                        )
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)?,
                    )
                })?;

            Ok(Self(Arc::new(RwLock::new(repository))))
        })
    }

    #[staticmethod]
    fn exists(py: Python<'_>, storage: PyStorage) -> PyResult<bool> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let exists = Repository::exists(storage.0.as_ref())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(exists)
            })
        })
    }

    /// Reopen the repository changing its config and or virtual chunk credentials
    ///
    /// If config is None, it will use the same value as self
    /// If authorize_virtual_chunk_access is None, it will use the same value as self
    /// If authorize_virtual_chunk_access is Some(x), it will override with x
    #[pyo3(signature = (*, config = None, authorize_virtual_chunk_access = None::<Option<HashMap<String, Option<PyCredentials>>>>))]
    pub fn reopen(
        &self,
        py: Python<'_>,
        config: Option<&PyRepositoryConfig>,
        authorize_virtual_chunk_access: Option<
            Option<HashMap<String, Option<PyCredentials>>>,
        >,
    ) -> PyResult<Self> {
        py.allow_threads(move || {
            let config = config
                .map(|c| c.try_into().map_err(PyValueError::new_err))
                .transpose()?;
            Ok(Self(Arc::new(RwLock::new(
                self.0
                    .blocking_read()
                    .reopen(config, authorize_virtual_chunk_access.map(map_credentials))
                    .map_err(PyIcechunkStoreError::RepositoryError)?,
            ))))
        })
    }

    #[classmethod]
    fn from_bytes(
        _cls: Bound<'_, PyType>,
        py: Python<'_>,
        bytes: Vec<u8>,
    ) -> PyResult<Self> {
        // This is a compute intensive task, we need to release the Gil
        py.allow_threads(move || {
            let repository = Repository::from_bytes(bytes)
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(Self(Arc::new(RwLock::new(repository))))
        })
    }

    fn as_bytes(&self, py: Python<'_>) -> PyResult<Cow<[u8]>> {
        // This is a compute intensive task, we need to release the Gil
        py.allow_threads(move || {
            let bytes = self
                .0
                .blocking_read()
                .as_bytes()
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(Cow::Owned(bytes))
        })
    }

    #[staticmethod]
    fn fetch_config(
        py: Python<'_>,
        storage: PyStorage,
    ) -> PyResult<Option<PyRepositoryConfig>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let res = Repository::fetch_config(storage.0.as_ref())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(res.map(|res| res.0.into()))
            })
        })
    }

    fn save_config(&self, py: Python<'_>) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let _etag = self
                    .0
                    .read()
                    .await
                    .save_config()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    pub fn config(&self) -> PyRepositoryConfig {
        self.0.blocking_read().config().clone().into()
    }

    pub fn storage_settings(&self) -> PyStorageSettings {
        self.0.blocking_read().storage_settings().clone().into()
    }

    pub fn storage(&self) -> PyStorage {
        PyStorage(Arc::clone(self.0.blocking_read().storage()))
    }

    pub fn set_default_commit_metadata(
        &self,
        py: Python<'_>,
        metadata: PySnapshotProperties,
    ) {
        py.allow_threads(move || {
            let metadata = metadata.into();
            self.0.blocking_write().set_default_commit_metadata(metadata);
        })
    }

    pub fn default_commit_metadata(&self, py: Python<'_>) -> PySnapshotProperties {
        py.allow_threads(move || {
            let metadata = self.0.blocking_read().default_commit_metadata().clone();
            metadata.into()
        })
    }

    /// Returns an object that is both a sync and an async iterator
    #[pyo3(signature = (*, branch = None, tag = None, snapshot_id = None))]
    pub fn async_ancestry(
        &self,
        py: Python<'_>,
        branch: Option<String>,
        tag: Option<String>,
        snapshot_id: Option<String>,
    ) -> PyResult<PyAsyncGenerator> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let version = args_to_version_info(branch, tag, snapshot_id, None)?;
            let ancestry = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move {
                    let (snapshot_id, asset_manager) = {
                        let lock = self.0.read().await;
                        (
                            lock.resolve_version(&version).await?,
                            Arc::clone(lock.asset_manager()),
                        )
                    };
                    asset_manager.snapshot_ancestry(&snapshot_id).await
                })
                .map_err(PyIcechunkStoreError::RepositoryError)?
                .map_err(PyIcechunkStoreError::RepositoryError);

            let parents = ancestry.and_then(|info| async move {
                Python::with_gil(|py| {
                    let info = PySnapshotInfo::from(info);
                    Ok(Bound::new(py, info)?.into_any().unbind())
                })
            });

            let prepared_list = Arc::new(Mutex::new(parents.err_into().boxed()));
            Ok(PyAsyncGenerator::new(prepared_list))
        })
    }

    pub fn create_branch(
        &self,
        py: Python<'_>,
        branch_name: &str,
        snapshot_id: &str,
    ) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
                PyIcechunkStoreError::RepositoryError(
                    RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()).into(),
                )
            })?;

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .read()
                    .await
                    .create_branch(branch_name, &snapshot_id)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    pub fn list_branches(&self, py: Python<'_>) -> PyResult<BTreeSet<String>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let branches = self
                    .0
                    .read()
                    .await
                    .list_branches()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(branches)
            })
        })
    }

    pub fn lookup_branch(&self, py: Python<'_>, branch_name: &str) -> PyResult<String> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let tip = self
                    .0
                    .read()
                    .await
                    .lookup_branch(branch_name)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(tip.to_string())
            })
        })
    }

    pub fn lookup_snapshot(
        &self,
        py: Python<'_>,
        snapshot_id: &str,
    ) -> PyResult<PySnapshotInfo> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
                PyIcechunkStoreError::RepositoryError(
                    RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()).into(),
                )
            })?;
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let res = self
                    .0
                    .read()
                    .await
                    .lookup_snapshot(&snapshot_id)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(res.into())
            })
        })
    }

    pub fn reset_branch(
        &self,
        py: Python<'_>,
        branch_name: &str,
        snapshot_id: &str,
    ) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
                PyIcechunkStoreError::RepositoryError(
                    RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()).into(),
                )
            })?;

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .read()
                    .await
                    .reset_branch(branch_name, &snapshot_id)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    pub fn delete_branch(&self, py: Python<'_>, branch: &str) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .read()
                    .await
                    .delete_branch(branch)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    pub fn delete_tag(&self, py: Python<'_>, tag: &str) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .read()
                    .await
                    .delete_tag(tag)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    pub fn create_tag(
        &self,
        py: Python<'_>,
        tag_name: &str,
        snapshot_id: &str,
    ) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
                PyIcechunkStoreError::RepositoryError(
                    RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()).into(),
                )
            })?;

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .read()
                    .await
                    .create_tag(tag_name, &snapshot_id)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    pub fn list_tags(&self, py: Python<'_>) -> PyResult<BTreeSet<String>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let tags = self
                    .0
                    .read()
                    .await
                    .list_tags()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(tags)
            })
        })
    }

    pub fn lookup_tag(&self, py: Python<'_>, tag: &str) -> PyResult<String> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let tag = self
                    .0
                    .read()
                    .await
                    .lookup_tag(tag)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(tag.to_string())
            })
        })
    }

    #[pyo3(signature = (*, from_branch=None, from_tag=None, from_snapshot_id=None, to_branch=None, to_tag=None, to_snapshot_id=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn diff(
        &self,
        py: Python<'_>,
        from_branch: Option<String>,
        from_tag: Option<String>,
        from_snapshot_id: Option<String>,
        to_branch: Option<String>,
        to_tag: Option<String>,
        to_snapshot_id: Option<String>,
    ) -> PyResult<PyDiff> {
        let from = args_to_version_info(from_branch, from_tag, from_snapshot_id, None)?;
        let to = args_to_version_info(to_branch, to_tag, to_snapshot_id, None)?;

        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let diff = self
                    .0
                    .read()
                    .await
                    .diff(&from, &to)
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(diff.into())
            })
        })
    }

    #[pyo3(signature = (*, branch = None, tag = None, snapshot_id = None, as_of = None))]
    pub fn readonly_session(
        &self,
        py: Python<'_>,
        branch: Option<String>,
        tag: Option<String>,
        snapshot_id: Option<String>,
        as_of: Option<DateTime<Utc>>,
    ) -> PyResult<PySession> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let version = args_to_version_info(branch, tag, snapshot_id, as_of)?;
            let session =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    self.0
                        .read()
                        .await
                        .readonly_session(&version)
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)
                })?;

            Ok(PySession(Arc::new(RwLock::new(session))))
        })
    }

    pub fn writable_session(&self, py: Python<'_>, branch: &str) -> PyResult<PySession> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let session =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    self.0
                        .read()
                        .await
                        .writable_session(branch)
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)
                })?;

            Ok(PySession(Arc::new(RwLock::new(session))))
        })
    }

    #[pyo3(signature = (message, branch, metadata=None))]
    pub fn rewrite_manifests(
        &self,
        py: Python<'_>,
        message: &str,
        branch: &str,
        metadata: Option<PySnapshotProperties>,
    ) -> PyResult<String> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let metadata = metadata.map(|m| m.into());
            let result =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let lock = self.0.read().await;
                    rewrite_manifests(&lock, branch, message, metadata)
                        .await
                        .map_err(PyIcechunkStoreError::ManifestOpsError)
                })?;
            Ok(result.to_string())
        })
    }

    #[pyo3(signature = (older_than, *, delete_expired_branches = false, delete_expired_tags = false))]
    pub fn expire_snapshots(
        &self,
        py: Python<'_>,
        older_than: DateTime<Utc>,
        delete_expired_branches: bool,
        delete_expired_tags: bool,
    ) -> PyResult<HashSet<String>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let result =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let (storage, storage_settings, asset_manager) = {
                        let lock = self.0.read().await;
                        (
                            Arc::clone(lock.storage()),
                            lock.storage_settings().clone(),
                            Arc::clone(lock.asset_manager()),
                        )
                    };

                    let result = expire(
                        storage.as_ref(),
                        &storage_settings,
                        asset_manager,
                        older_than,
                        if delete_expired_branches {
                            ExpiredRefAction::Delete
                        } else {
                            ExpiredRefAction::Ignore
                        },
                        if delete_expired_tags {
                            ExpiredRefAction::Delete
                        } else {
                            ExpiredRefAction::Ignore
                        },
                    )
                    .await
                    .map_err(PyIcechunkStoreError::GCError)?;
                    Ok::<_, PyIcechunkStoreError>(
                        result
                            .released_snapshots
                            .iter()
                            .map(|id| id.to_string())
                            .collect(),
                    )
                })?;

            Ok(result)
        })
    }

    pub fn garbage_collect(
        &self,
        py: Python<'_>,
        delete_object_older_than: DateTime<Utc>,
    ) -> PyResult<PyGCSummary> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let result =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let gc_config = GCConfig::clean_all(
                        delete_object_older_than,
                        delete_object_older_than,
                        Default::default(),
                    );
                    let (storage, storage_settings, asset_manager) = {
                        let lock = self.0.read().await;
                        (
                            Arc::clone(lock.storage()),
                            lock.storage_settings().clone(),
                            Arc::clone(lock.asset_manager()),
                        )
                    };
                    let result = garbage_collect(
                        storage.as_ref(),
                        &storage_settings,
                        asset_manager,
                        &gc_config,
                    )
                    .await
                    .map_err(PyIcechunkStoreError::GCError)?;
                    Ok::<_, PyIcechunkStoreError>(result.into())
                })?;

            Ok(result)
        })
    }

    pub fn total_chunks_storage(&self, py: Python<'_>) -> PyResult<u64> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            let result =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let (storage, storage_settings, asset_manager) = {
                        let lock = self.0.read().await;
                        (
                            Arc::clone(lock.storage()),
                            lock.storage_settings().clone(),
                            Arc::clone(lock.asset_manager()),
                        )
                    };
                    let result = repo_chunks_storage(
                        storage.as_ref(),
                        &storage_settings,
                        asset_manager,
                    )
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                    Ok::<_, PyIcechunkStoreError>(result)
                })?;

            Ok(result)
        })
    }
}

fn map_credentials(
    cred: Option<HashMap<String, Option<PyCredentials>>>,
) -> HashMap<String, Option<Credentials>> {
    cred.map(|cred| {
        cred.into_iter().map(|(name, cred)| (name, cred.map(|c| c.into()))).collect()
    })
    .unwrap_or_default()
}

fn args_to_version_info(
    branch: Option<String>,
    tag: Option<String>,
    snapshot: Option<String>,
    as_of: Option<DateTime<Utc>>,
) -> PyResult<VersionInfo> {
    let n = [&branch, &tag, &snapshot].iter().filter(|r| !r.is_none()).count();
    if n > 1 {
        return Err(PyValueError::new_err(
            "Must provide one of branch, tag, or snapshot_id",
        ));
    }

    if as_of.is_some() && branch.is_none() {
        return Err(PyValueError::new_err(
            "as_of argument must be provided together with a branch name",
        ));
    }

    if let Some(branch) = branch {
        if let Some(at) = as_of {
            Ok(VersionInfo::AsOf { branch, at })
        } else {
            Ok(VersionInfo::BranchTipRef(branch))
        }
    } else if let Some(tag_name) = tag {
        Ok(VersionInfo::TagRef(tag_name))
    } else if let Some(snapshot_id) = snapshot {
        let snapshot_id = SnapshotId::try_from(snapshot_id.as_str()).map_err(|_| {
            PyIcechunkStoreError::RepositoryError(
                RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()).into(),
            )
        })?;

        Ok(VersionInfo::SnapshotId(snapshot_id))
    } else {
        return Err(PyValueError::new_err(
            "Must provide one of branch, tag, or snapshot_id",
        ));
    }
}
