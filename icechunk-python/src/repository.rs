use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt::Display,
    num::{NonZeroU16, NonZeroUsize},
    sync::Arc,
};

use crate::display::{PyRepr, ReprMode, py_bool, py_option};
use itertools::Itertools as _;

use chrono::{DateTime, Utc};
use futures::{StreamExt as _, TryStreamExt as _};
use icechunk::{
    Repository,
    config::Credentials,
    diff::Diff,
    feature_flags::FeatureFlag,
    format::{
        ManifestId, SnapshotId,
        format_constants::SpecVersionBin,
        repo_info::{RepoAvailability, RepoStatus, UpdateType},
        snapshot::{ManifestFileInfo, SnapshotInfo, SnapshotProperties},
    },
    inspect::{manifest_json, repo_info_json, snapshot_json, transaction_log_json},
    migrations,
    ops::{
        gc::{ExpiredRefAction, GCConfig, GCSummary, expire, garbage_collect},
        manifests::rewrite_manifests,
        stats::repo_chunks_storage,
    },
    repository::{RepositoryError, RepositoryErrorKind, VersionInfo},
};
use pyo3::{
    Borrowed, IntoPyObjectExt as _,
    exceptions::PyValueError,
    prelude::*,
    types::{PyDict, PyNone, PySet, PyType},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::{
    config::{
        PyCredentials, PyRepositoryConfig, PyStorage, PyStorageSettings, datetime_repr,
    },
    errors::PyIcechunkStoreError,
    impl_pickle,
    session::PySession,
    stats::PyChunkStorageStats,
    streams::PyAsyncGenerator,
};

fn parse_commit_method(method: &str) -> PyResult<icechunk::session::CommitMethod> {
    match method {
        "new_commit" => Ok(icechunk::session::CommitMethod::NewCommit),
        "amend" => Ok(icechunk::session::CommitMethod::Amend),
        other => Err(PyValueError::new_err(format!(
            "Invalid commit method: '{other}'. Expected 'new_commit' or 'amend'."
        ))),
    }
}

/// Wrapper needed to implement pyo3 conversion classes
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonValue(pub serde_json::Value);

/// Wrapper needed to implement pyo3 conversion classes
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PySnapshotProperties(pub HashMap<String, JsonValue>);

#[pyclass(name = "SnapshotInfo", eq)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PySnapshotInfo {
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
    // FIXME: breaking api by removing manifests
}

impl_pickle!(PySnapshotInfo);

#[pyclass(name = "ManifestFileInfo", eq)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PyManifestFileInfo {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub size_bytes: u64,
    #[pyo3(get)]
    pub num_chunk_refs: u32,
}

impl_pickle!(PyManifestFileInfo);

impl<'py> FromPyObject<'_, 'py> for PySnapshotProperties {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let m: HashMap<String, JsonValue> = ob.extract()?;
        Ok(Self(m))
    }
}

impl<'py> FromPyObject<'_, 'py> for JsonValue {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let value = ob
            .extract()
            .map(serde_json::Value::String)
            .or(ob.extract().map(serde_json::Value::Bool))
            .or(ob.cast::<PyNone>().map(|_| serde_json::Value::Null))
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
        }
    }
}

impl PyRepr for PyManifestFileInfo {
    const EXECUTABLE: bool = false;

    fn cls_name() -> &'static str {
        "icechunk.ManifestFileInfo"
    }

    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("id", self.id.clone()),
            ("size_bytes", self.size_bytes.to_string()),
            ("num_chunk_refs", self.num_chunk_refs.to_string()),
        ]
    }
}

#[pymethods]
impl PyManifestFileInfo {
    pub(crate) fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    pub(crate) fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    pub(crate) fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

impl PyRepr for PySnapshotInfo {
    const EXECUTABLE: bool = false;
    fn cls_name() -> &'static str {
        "icechunk.SnapshotInfo"
    }
    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("id", self.id.clone()),
            ("parent_id", py_option(&self.parent_id)),
            ("written_at", datetime_repr(&self.written_at)),
            ("message", self.message.clone()),
            ("metadata", format!("{:?}", self.metadata)),
        ]
    }
}

#[pymethods]
impl PySnapshotInfo {
    pub(crate) fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    pub(crate) fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    pub(crate) fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
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
    #[pyo3(get)]
    pub moved_nodes: Vec<(String, String)>,
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
        let moved_nodes = value
            .moved_nodes
            .into_iter()
            .map(|m| (m.from.to_string(), m.to.to_string()))
            .collect();

        PyDiff {
            new_groups,
            new_arrays,
            deleted_groups,
            deleted_arrays,
            updated_groups,
            updated_arrays,
            updated_chunks,
            moved_nodes,
        }
    }
}

impl PyRepr for PyDiff {
    const EXECUTABLE: bool = false;
    fn cls_name() -> &'static str {
        "icechunk.Diff"
    }
    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("new_groups", format!("{:?}", self.new_groups)),
            ("new_arrays", format!("{:?}", self.new_arrays)),
            ("deleted_groups", format!("{:?}", self.deleted_groups)),
            ("deleted_arrays", format!("{:?}", self.deleted_arrays)),
            ("updated_groups", format!("{:?}", self.updated_groups)),
            ("updated_arrays", format!("{:?}", self.updated_arrays)),
            ("updated_chunks", format!("{:?}", self.updated_chunks)),
            ("moved_nodes", format!("{:?}", self.moved_nodes)),
        ]
    }
}

#[pymethods]
impl PyDiff {
    /// Returns true if the diff contains no changes.
    pub fn is_empty(&self) -> bool {
        self.new_groups.is_empty()
            && self.new_arrays.is_empty()
            && self.deleted_groups.is_empty()
            && self.deleted_arrays.is_empty()
            && self.updated_groups.is_empty()
            && self.updated_arrays.is_empty()
            && self.updated_chunks.is_empty()
            && self.moved_nodes.is_empty()
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }

    /// Custom human-readable format (not generic `PyRepr`) because diffs
    /// are much more readable with section headers than field: value.
    #[expect(clippy::unwrap_used)]
    pub fn __str__(&self) -> String {
        let mut res = String::new();
        use std::fmt::Write as _;

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
        if !self.moved_nodes.is_empty() {
            res.push_str("Nodes moved/renamed:\n");
            for (from, to) in self.moved_nodes.iter() {
                writeln!(res, "    {from} -> {to}").unwrap();
            }
            res.push('\n');
        }
        res
    }

    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

impl_pickle!(PyDiff);

#[pyclass(name = "GCSummary", eq)]
#[derive(Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct PyGCSummary {
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

impl PyRepr for PyGCSummary {
    const EXECUTABLE: bool = false;
    fn cls_name() -> &'static str {
        "icechunk.GCSummary"
    }
    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("bytes_deleted", self.bytes_deleted.to_string()),
            ("chunks_deleted", self.chunks_deleted.to_string()),
            ("manifests_deleted", self.manifests_deleted.to_string()),
            ("snapshots_deleted", self.snapshots_deleted.to_string()),
            ("attributes_deleted", self.attributes_deleted.to_string()),
            ("transaction_logs_deleted", self.transaction_logs_deleted.to_string()),
        ]
    }
}

#[pymethods]
impl PyGCSummary {
    pub(crate) fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    pub(crate) fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    pub(crate) fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

impl_pickle!(PyGCSummary);

#[pyclass(name = "RepoAvailability", eq, eq_int, rename_all = "snake_case")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PyRepoAvailability {
    Online,
    ReadOnly,
}

impl From<RepoAvailability> for PyRepoAvailability {
    fn from(value: RepoAvailability) -> Self {
        match value {
            RepoAvailability::Online => PyRepoAvailability::Online,
            RepoAvailability::ReadOnly => PyRepoAvailability::ReadOnly,
        }
    }
}

impl From<PyRepoAvailability> for RepoAvailability {
    fn from(value: PyRepoAvailability) -> Self {
        match value {
            PyRepoAvailability::Online => RepoAvailability::Online,
            PyRepoAvailability::ReadOnly => RepoAvailability::ReadOnly,
        }
    }
}

#[pyclass(name = "RepoStatus", get_all, eq)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PyRepoStatus {
    availability: PyRepoAvailability,
    set_at: DateTime<Utc>,
    limited_availability_reason: Option<String>,
}

impl From<RepoStatus> for PyRepoStatus {
    fn from(value: RepoStatus) -> Self {
        Self {
            availability: value.availability.into(),
            set_at: value.set_at,
            limited_availability_reason: value.limited_availability_reason,
        }
    }
}

impl From<PyRepoStatus> for RepoStatus {
    fn from(value: PyRepoStatus) -> Self {
        Self {
            availability: value.availability.into(),
            set_at: value.set_at,
            limited_availability_reason: value.limited_availability_reason,
        }
    }
}

impl Display for PyRepoStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RepoStatus(availability={:?}, set_at={:?}, limited_availability_reason={:?})",
            self.availability, self.set_at, self.limited_availability_reason
        )
    }
}

impl PyRepr for PyRepoStatus {
    const EXECUTABLE: bool = false;
    fn cls_name() -> &'static str {
        "icechunk.RepoStatus"
    }
    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("availability", format!("{:?}", self.availability)),
            ("set_at", datetime_repr(&self.set_at)),
            ("limited_availability_reason", py_option(&self.limited_availability_reason)),
        ]
    }
}

#[pymethods]
impl PyRepoStatus {
    #[new]
    #[pyo3(signature = (availability, set_at = None, limited_availability_reason = None))]
    fn new(
        availability: PyRepoAvailability,
        set_at: Option<DateTime<Utc>>,
        limited_availability_reason: Option<String>,
    ) -> Self {
        let set_at = set_at.unwrap_or_else(Utc::now);
        Self { availability, set_at, limited_availability_reason }
    }

    fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

#[pyclass(name = "UpdateType", eq)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PyUpdateType {
    BranchCreated { name: String },
    BranchDeleted { name: String, previous_snap_id: String },
    BranchReset { name: String, previous_snap_id: String },
    CommitAmended { branch: String, previous_snap_id: String, new_snap_id: String },
    ConfigChanged {},
    ExpirationRan {},
    FeatureFlagChanged { id: u16, new_value: Option<bool> },
    GCRan {},
    MetadataChanged {},
    NewCommit { branch: String, new_snap_id: String },
    NewDetachedSnapshot { new_snap_id: String },
    RepoInitialized {},
    RepoMigrated { from_version: u8, to_version: u8 },
    RepoStatusChanged { status: PyRepoStatus },
    TagCreated { name: String },
    TagDeleted { name: String, previous_snap_id: String },
}

#[pymethods]
impl PyUpdateType {
    fn __repr__(&self) -> String {
        match self {
            Self::RepoInitialized {} => "icechunk.UpdateType.RepoInitialized()".into(),
            Self::ConfigChanged {} => "icechunk.UpdateType.ConfigChanged()".into(),
            Self::MetadataChanged {} => "icechunk.UpdateType.MetadataChanged()".into(),
            Self::TagCreated { name } => {
                format!("icechunk.UpdateType.TagCreated(name=\"{name}\")")
            }
            Self::TagDeleted { name, previous_snap_id } => format!(
                "icechunk.UpdateType.TagDeleted(name=\"{name}\", previous_snap_id=\"{previous_snap_id}\")"
            ),
            Self::BranchCreated { name } => {
                format!("icechunk.UpdateType.BranchCreated(name=\"{name}\")")
            }
            Self::BranchDeleted { name, previous_snap_id } => format!(
                "icechunk.UpdateType.BranchDeleted(name=\"{name}\", previous_snap_id=\"{previous_snap_id}\")"
            ),
            Self::BranchReset { name, previous_snap_id } => format!(
                "icechunk.UpdateType.BranchReset(name=\"{name}\", previous_snap_id=\"{previous_snap_id}\")"
            ),
            Self::NewCommit { branch, new_snap_id } => {
                format!(
                    "icechunk.UpdateType.NewCommit(branch=\"{branch}\", new_snap_id=\"{new_snap_id}\")"
                )
            }
            Self::CommitAmended { branch, previous_snap_id, new_snap_id } => format!(
                "icechunk.UpdateType.CommitAmended(branch=\"{branch}\", previous_snap_id=\"{previous_snap_id}\", new_snap_id=\"{new_snap_id}\")",
            ),
            Self::RepoMigrated { from_version, to_version } => format!(
                "icechunk.UpdateType.RepoMigrated(from_version={from_version}, to_version={to_version})"
            ),
            Self::RepoStatusChanged { status } => {
                format!("icechunk.UpdateType.RepoStatusChanged(status={status})")
            }
            Self::GCRan {} => "icechunk.UpdateType.GCRan()".into(),
            Self::FeatureFlagChanged { id, new_value } => format!(
                "icechunk.UpdateType.FeatureFlagChanged(id={id}, new_value={})",
                new_value.map(py_bool).unwrap_or_else(|| "None".to_string()),
            ),
            Self::ExpirationRan {} => "icechunk.UpdateType.ExpirationRan()".into(),
            Self::NewDetachedSnapshot { new_snap_id } => {
                format!(
                    "icechunk.UpdateType.NewDetachedSnapshot(new_snap_id=\"{new_snap_id}\")"
                )
            }
        }
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl PyUpdateType {
    pub(crate) fn render(&self, mode: ReprMode) -> String {
        match mode {
            ReprMode::Str => self.__str__(),
            ReprMode::Repr | ReprMode::Html => self.__repr__(),
        }
    }
}

#[pyclass(name = "Update")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PyUpdate {
    #[pyo3(get)]
    kind: PyUpdateType,

    #[pyo3(get)]
    updated_at: DateTime<Utc>,

    #[pyo3(get)]
    backup_path: Option<String>,
}

impl PyRepr for PyUpdate {
    const EXECUTABLE: bool = false;
    fn cls_name() -> &'static str {
        "icechunk.Update"
    }
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("kind", self.kind.render(mode)),
            ("updated_at", datetime_repr(&self.updated_at)),
            ("backup_path", py_option(&self.backup_path)),
        ]
    }
}

#[pymethods]
impl PyUpdate {
    fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }
    fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

#[pyclass(name = "FeatureFlag", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PyFeatureFlag {
    #[pyo3(get)]
    id: u16,
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    default_enabled: bool,
    #[pyo3(get)]
    setting: Option<bool>,
    #[pyo3(get)]
    enabled: bool,
}

impl PyRepr for PyFeatureFlag {
    const EXECUTABLE: bool = false;
    fn cls_name() -> &'static str {
        "icechunk.FeatureFlag"
    }
    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("id", self.id.to_string()),
            ("name", self.name.clone()),
            ("default_enabled", py_bool(self.default_enabled)),
            ("setting", self.setting.map(py_bool).unwrap_or_else(|| "None".to_string())),
            ("enabled", py_bool(self.enabled)),
        ]
    }
}

#[pymethods]
impl PyFeatureFlag {
    fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }
    fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

impl From<FeatureFlag> for PyFeatureFlag {
    fn from(flag: FeatureFlag) -> Self {
        Self {
            id: flag.id(),
            name: flag.name().to_string(),
            default_enabled: flag.default_enabled(),
            setting: flag.setting(),
            enabled: flag.enabled(),
        }
    }
}

fn mk_update(
    update: &UpdateType,
    updated_at: DateTime<Utc>,
    backup_path: Option<String>,
) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let res = match update {
            UpdateType::RepoInitializedUpdate => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::RepoInitialized {},
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::RepoMigratedUpdate { from_version, to_version } => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::RepoMigrated {
                        from_version: *from_version as u8,
                        to_version: *to_version as u8,
                    },
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::RepoStatusChangedUpdate { status } => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::RepoStatusChanged {
                        status: status.clone().into(),
                    },
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::ConfigChangedUpdate => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::ConfigChanged {},
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::MetadataChangedUpdate => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::MetadataChanged {},
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::TagCreatedUpdate { name } => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::TagCreated { name: name.clone() },
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::TagDeletedUpdate { name, previous_snap_id } => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::TagDeleted {
                        name: name.clone(),
                        previous_snap_id: previous_snap_id.to_string(),
                    },
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::BranchCreatedUpdate { name } => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::BranchCreated { name: name.clone() },
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::BranchDeletedUpdate { name, previous_snap_id } => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::BranchDeleted {
                        name: name.clone(),
                        previous_snap_id: previous_snap_id.to_string(),
                    },
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::BranchResetUpdate { name, previous_snap_id } => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::BranchReset {
                        name: name.clone(),
                        previous_snap_id: previous_snap_id.to_string(),
                    },
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::NewCommitUpdate { branch, new_snap_id } => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::NewCommit {
                        branch: branch.clone(),
                        new_snap_id: new_snap_id.to_string(),
                    },
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::CommitAmendedUpdate { branch, previous_snap_id, new_snap_id } => {
                Bound::new(
                    py,
                    PyUpdate {
                        kind: PyUpdateType::CommitAmended {
                            branch: branch.clone(),
                            previous_snap_id: previous_snap_id.to_string(),
                            new_snap_id: new_snap_id.to_string(),
                        },
                        updated_at,
                        backup_path,
                    },
                )?
                .into_any()
                .unbind()
            }
            UpdateType::NewDetachedSnapshotUpdate { new_snap_id } => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::NewDetachedSnapshot {
                        new_snap_id: new_snap_id.to_string(),
                    },
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::GCRanUpdate => Bound::new(
                py,
                PyUpdate { kind: PyUpdateType::GCRan {}, updated_at, backup_path },
            )?
            .into_any()
            .unbind(),
            UpdateType::ExpirationRanUpdate => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::ExpirationRan {},
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
            UpdateType::FeatureFlagChanged { id, new_value } => Bound::new(
                py,
                PyUpdate {
                    kind: PyUpdateType::FeatureFlagChanged {
                        id: *id,
                        new_value: *new_value,
                    },
                    updated_at,
                    backup_path,
                },
            )?
            .into_any()
            .unbind(),
        };
        Ok(res)
    })
}

#[repr(u8)]
#[pyclass(
    eq,
    eq_int,
    ord,
    rename_all = "snake_case",
    skip_from_py_object,
    name = "SpecVersion",
    frozen
)]
#[derive(PartialEq, Default, Clone, PartialOrd, Debug)]
pub enum PySpecVersion {
    V1 = 1u8,
    #[default]
    V2 = 2u8,
}

impl<'py> FromPyObject<'_, 'py> for PySpecVersion {
    type Error = PyErr;

    /// Custom implementation that allows passing an integer or `PySpecVersion`
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        // cast is cheaper, try it first
        let value = if let Ok(spec) = ob.cast::<PySpecVersion>() {
            spec.get().clone()
        } else if let Ok(v) = ob.extract::<u8>() {
            match v {
                1 => PySpecVersion::V1,
                2 => PySpecVersion::V2,
                v => {
                    return Err(PyValueError::new_err(format!(
                        "Unsupported version {v}"
                    )));
                }
            }
        } else {
            return Err(PyValueError::new_err("Couldn't parse a valid version"));
        };

        Ok(value)
    }
}

impl From<PySpecVersion> for SpecVersionBin {
    fn from(value: PySpecVersion) -> Self {
        match value {
            PySpecVersion::V1 => Self::V1,
            PySpecVersion::V2 => Self::V2,
        }
    }
}

impl From<SpecVersionBin> for PySpecVersion {
    fn from(value: SpecVersionBin) -> Self {
        match value {
            SpecVersionBin::V1 => Self::V1,
            SpecVersionBin::V2 => Self::V2,
        }
    }
}

#[pymethods]
impl PySpecVersion {
    pub(crate) fn __repr__(&self) -> String {
        match self {
            Self::V2 => "SpecVersion.v2 (current)".into(),
            Self::V1 => "SpecVersion.v1".into(),
        }
    }

    #[staticmethod]
    pub(crate) fn current() -> Self {
        Default::default()
    }
}

#[pyclass]
pub(crate) struct PyRepository(Arc<RwLock<Repository>>);

impl PyRepository {
    pub(crate) fn migrate_1_to_2(
        &self,
        py: Python<'_>,
        dry_run: bool,
        delete_unused_v1_files: bool,
        prefetch_concurrency: Option<usize>,
    ) -> PyResult<Self> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let repo = self.0.read().await;
                let storage = Arc::clone(repo.storage());
                let config = Some(repo.config().clone());
                drop(repo);

                let fresh =
                    Repository::open(config, Arc::clone(&storage), Default::default())
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)?;
                migrations::migrate_1_to_2(
                    fresh,
                    dry_run,
                    delete_unused_v1_files,
                    prefetch_concurrency,
                )
                .await
                .map_err(PyIcechunkStoreError::MigrationError)?;

                // Reopen to get a fresh repo with the correct spec version
                let reopened = Repository::open(None, storage, Default::default())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(Self(Arc::new(RwLock::new(reopened))))
            })
        })
    }
}

impl PyRepr for PyRepository {
    const EXECUTABLE: bool = false;

    fn cls_name() -> &'static str {
        "icechunk.Repository"
    }

    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        let repo = self.0.blocking_read();
        let storage = format!("{}", repo.storage());
        let py_config: PyRepositoryConfig = repo.config().clone().into();
        vec![("storage", storage), ("config", py_config.render(mode))]
    }
}

#[pymethods]
/// Most functions in this class call `Runtime.block_on` so they need to `detach` so other
/// python threads can make progress in the case of an actual block
impl PyRepository {
    pub(crate) fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    pub(crate) fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    pub(crate) fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }

    #[classmethod]
    #[pyo3(signature = (storage, *, config = None, authorize_virtual_chunk_access = None, spec_version = None, check_clean_root = true))]
    fn create(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        storage: PyStorage,
        config: Option<&PyRepositoryConfig>,
        authorize_virtual_chunk_access: Option<HashMap<String, Option<PyCredentials>>>,
        spec_version: Option<PySpecVersion>,
        check_clean_root: bool,
    ) -> PyResult<Self> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            let repository =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let config = config
                        .map(|c| c.try_into().map_err(PyValueError::new_err))
                        .transpose()?;
                    let version = spec_version.map(|v| v.into());
                    Repository::create(
                        config,
                        storage.0,
                        map_credentials(authorize_virtual_chunk_access),
                        version,
                        check_clean_root,
                    )
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)
                })?;

            Ok(Self(Arc::new(RwLock::new(repository))))
        })
    }

    #[classmethod]
    #[pyo3(signature = (storage, *, config = None, authorize_virtual_chunk_access = None, spec_version = None, check_clean_root = true))]
    fn create_async<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        storage: PyStorage,
        config: Option<&PyRepositoryConfig>,
        authorize_virtual_chunk_access: Option<HashMap<String, Option<PyCredentials>>>,
        spec_version: Option<PySpecVersion>,
        check_clean_root: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let config =
            config.map(|c| c.try_into().map_err(PyValueError::new_err)).transpose()?;
        let authorize_virtual_chunk_access =
            map_credentials(authorize_virtual_chunk_access);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let version = spec_version.map(|v| v.into());
            let repository = Repository::create(
                config,
                storage.0,
                authorize_virtual_chunk_access,
                version,
                check_clean_root,
            )
            .await
            .map_err(PyIcechunkStoreError::RepositoryError)?;

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
        py.detach(move || {
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
    fn open_async<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        storage: PyStorage,
        config: Option<&PyRepositoryConfig>,
        authorize_virtual_chunk_access: Option<HashMap<String, Option<PyCredentials>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let config =
            config.map(|c| c.try_into().map_err(PyValueError::new_err)).transpose()?;
        let authorize_virtual_chunk_access =
            map_credentials(authorize_virtual_chunk_access);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository =
                Repository::open(config, storage.0, authorize_virtual_chunk_access)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(Self(Arc::new(RwLock::new(repository))))
        })
    }

    #[classmethod]
    #[pyo3(signature = (storage, *, config = None, authorize_virtual_chunk_access = None, create_version = None, check_clean_root = true))]
    fn open_or_create(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        storage: PyStorage,
        config: Option<&PyRepositoryConfig>,
        authorize_virtual_chunk_access: Option<HashMap<String, Option<PyCredentials>>>,
        create_version: Option<PySpecVersion>,
        check_clean_root: bool,
    ) -> PyResult<Self> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            let repository =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let config = config
                        .map(|c| c.try_into().map_err(PyValueError::new_err))
                        .transpose()?;
                    let version = create_version.map(|v| v.into());
                    Ok::<_, PyErr>(
                        Repository::open_or_create(
                            config,
                            storage.0,
                            map_credentials(authorize_virtual_chunk_access),
                            version,
                            check_clean_root,
                        )
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)?,
                    )
                })?;

            Ok(Self(Arc::new(RwLock::new(repository))))
        })
    }

    #[classmethod]
    #[pyo3(signature = (storage, *, config = None, authorize_virtual_chunk_access = None, create_version = None, check_clean_root = true))]
    fn open_or_create_async<'py>(
        _cls: &Bound<'py, PyType>,
        py: Python<'py>,
        storage: PyStorage,
        config: Option<&PyRepositoryConfig>,
        authorize_virtual_chunk_access: Option<HashMap<String, Option<PyCredentials>>>,
        create_version: Option<PySpecVersion>,
        check_clean_root: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let config =
            config.map(|c| c.try_into().map_err(PyValueError::new_err)).transpose()?;
        let authorize_virtual_chunk_access =
            map_credentials(authorize_virtual_chunk_access);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let version = create_version.map(|v| v.into());
            let repository = Repository::open_or_create(
                config,
                storage.0,
                authorize_virtual_chunk_access,
                version,
                check_clean_root,
            )
            .await
            .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(Self(Arc::new(RwLock::new(repository))))
        })
    }

    #[staticmethod]
    #[pyo3(signature = (storage, storage_settings=None))]
    fn exists(
        py: Python<'_>,
        storage: PyStorage,
        storage_settings: Option<Py<PyStorageSettings>>,
    ) -> PyResult<bool> {
        let settings = storage_settings.map(|s| (&*s.borrow(py)).into());
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let exists = Repository::exists(storage.0, settings)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(exists)
            })
        })
    }

    #[staticmethod]
    #[pyo3(signature = (storage, storage_settings=None))]
    fn exists_async(
        py: Python<'_>,
        storage: PyStorage,
        storage_settings: Option<Py<PyStorageSettings>>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let settings = storage_settings.map(|s| (&*s.borrow(py)).into());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let exists = Repository::exists(storage.0, settings)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(exists)
        })
    }

    #[staticmethod]
    #[pyo3(signature = (storage, storage_settings=None))]
    fn fetch_spec_version(
        py: Python<'_>,
        storage: PyStorage,
        storage_settings: Option<Py<PyStorageSettings>>,
    ) -> PyResult<Option<PySpecVersion>> {
        let settings = storage_settings.map(|s| (&*s.borrow(py)).into());
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let spec_version = Repository::fetch_spec_version(storage.0, settings)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(spec_version.map(|v| v.into()))
            })
        })
    }

    #[staticmethod]
    #[pyo3(signature = (storage, storage_settings=None))]
    fn fetch_spec_version_async(
        py: Python<'_>,
        storage: PyStorage,
        storage_settings: Option<Py<PyStorageSettings>>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let settings = storage_settings.map(|s| (&*s.borrow(py)).into());
        pyo3_async_runtimes::tokio::future_into_py::<_, Option<PySpecVersion>>(
            py,
            async move {
                let spec_version = Repository::fetch_spec_version(storage.0, settings)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(spec_version.map(|v| v.into()))
            },
        )
    }

    /// Reopen the repository changing its config and or virtual chunk credentials
    ///
    /// If config is None, it will use the same value as self
    /// If `authorize_virtual_chunk_access` is None, it will use the same value as self
    /// If `authorize_virtual_chunk_access` is Some(x), it will override with x
    #[pyo3(signature = (*, config = None, authorize_virtual_chunk_access = None::<Option<HashMap<String, Option<PyCredentials>>>>))]
    pub(crate) fn reopen(
        &self,
        py: Python<'_>,
        config: Option<&PyRepositoryConfig>,
        authorize_virtual_chunk_access: Option<
            Option<HashMap<String, Option<PyCredentials>>>,
        >,
    ) -> PyResult<Self> {
        py.detach(move || {
            let config = config
                .map(|c| c.try_into().map_err(PyValueError::new_err))
                .transpose()?;
            let repo =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    self.0
                        .read()
                        .await
                        .reopen(
                            config,
                            authorize_virtual_chunk_access.map(map_credentials),
                        )
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)
                })?;
            Ok(Self(Arc::new(RwLock::new(repo))))
        })
    }

    #[pyo3(signature = (*, config = None, authorize_virtual_chunk_access = None::<Option<HashMap<String, Option<PyCredentials>>>>))]
    fn reopen_async<'py>(
        &'py self,
        py: Python<'py>,
        config: Option<&PyRepositoryConfig>,
        authorize_virtual_chunk_access: Option<
            Option<HashMap<String, Option<PyCredentials>>>,
        >,
    ) -> PyResult<Bound<'py, PyAny>> {
        let existing_repository = Arc::clone(&self.0);
        let config =
            config.map(|c| c.try_into().map_err(PyValueError::new_err)).transpose()?;
        let authorize_virtual_chunk_access =
            authorize_virtual_chunk_access.map(map_credentials);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository = existing_repository
                .read()
                .await
                .reopen(config, authorize_virtual_chunk_access)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(Self(Arc::new(RwLock::new(repository))))
        })
    }

    #[classmethod]
    fn from_bytes(
        _cls: Bound<'_, PyType>,
        py: Python<'_>,
        bytes: Vec<u8>,
    ) -> PyResult<Self> {
        // This is a compute intensive task, we need to release the Gil
        py.detach(move || {
            let repository = Repository::from_bytes(&bytes)
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(Self(Arc::new(RwLock::new(repository))))
        })
    }

    fn as_bytes(&self, py: Python<'_>) -> PyResult<Cow<'_, [u8]>> {
        // This is a compute intensive task, we need to release the Gil
        py.detach(move || {
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
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let res = Repository::fetch_config(storage.0)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(res.map(|res| res.0.into()))
            })
        })
    }

    #[staticmethod]
    fn fetch_config_async(
        py: Python<'_>,
        storage: PyStorage,
    ) -> PyResult<Bound<'_, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py::<_, Option<PyRepositoryConfig>>(
            py,
            async move {
                let res = Repository::fetch_config(storage.0)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(res.map(|res| res.0.into()))
            },
        )
    }

    fn save_config(&self, py: Python<'_>) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
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

    fn save_config_async<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository = repository.read().await;
            repository
                .save_config()
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(())
        })
    }

    pub(crate) fn config(&self) -> PyRepositoryConfig {
        self.0.blocking_read().config().clone().into()
    }

    pub(crate) fn storage_settings(&self) -> PyStorageSettings {
        self.0.blocking_read().storage_settings().clone().into()
    }

    pub(crate) fn storage(&self) -> PyStorage {
        PyStorage(Arc::clone(self.0.blocking_read().storage()))
    }

    #[getter]
    fn authorized_virtual_container_prefixes<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PySet>> {
        let prefixes = self.0.blocking_read().authorized_virtual_container_prefixes();
        PySet::new(py, prefixes.iter().map(|s| s.as_str()))
    }

    pub(crate) fn set_default_commit_metadata(
        &self,
        py: Python<'_>,
        metadata: PySnapshotProperties,
    ) {
        py.detach(move || {
            let metadata = metadata.into();
            self.0.blocking_write().set_default_commit_metadata(metadata);
        });
    }

    pub(crate) fn default_commit_metadata(&self, py: Python<'_>) -> PySnapshotProperties {
        py.detach(move || {
            let metadata = self.0.blocking_read().default_commit_metadata().clone();
            metadata.into()
        })
    }

    pub(crate) fn get_metadata(&self, py: Python<'_>) -> PyResult<PySnapshotProperties> {
        py.detach(move || {
            let metadata =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    self.0
                        .read()
                        .await
                        .get_metadata()
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)
                })?;
            Ok(metadata.into())
        })
    }

    pub(crate) fn get_metadata_async<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository = repository.read().await;
            let res: PySnapshotProperties = repository
                .get_metadata()
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?
                .into();
            Ok(res)
        })
    }

    pub(crate) fn set_metadata(
        &self,
        py: Python<'_>,
        metadata: PySnapshotProperties,
    ) -> PyResult<()> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .read()
                    .await
                    .set_metadata(&metadata.into())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)
            })?;
            Ok(())
        })
    }

    pub(crate) fn set_metadata_async<'py>(
        &'py self,
        py: Python<'py>,
        metadata: PySnapshotProperties,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository = repository.read().await;
            repository
                .set_metadata(&metadata.into())
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(())
        })
    }

    pub(crate) fn update_metadata(
        &self,
        py: Python<'_>,
        metadata: PySnapshotProperties,
    ) -> PyResult<PySnapshotProperties> {
        py.detach(move || {
            let res = pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .read()
                    .await
                    .update_metadata(&metadata.into())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)
            })?;
            Ok(res.into())
        })
    }

    pub(crate) fn update_metadata_async<'py>(
        &'py self,
        py: Python<'py>,
        metadata: PySnapshotProperties,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository = repository.read().await;
            let res: PySnapshotProperties = repository
                .update_metadata(&metadata.into())
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?
                .into();
            Ok(res)
        })
    }

    pub(crate) fn get_status(&self, py: Python<'_>) -> PyResult<PyRepoStatus> {
        py.detach(move || {
            let status =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    self.0
                        .read()
                        .await
                        .get_status()
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)
                })?;
            Ok(status.into())
        })
    }

    pub(crate) fn get_status_async<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository = repository.read().await;
            let res: PyRepoStatus = repository
                .get_status()
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?
                .into();
            Ok(res)
        })
    }

    pub(crate) fn set_status(
        &self,
        py: Python<'_>,
        status: PyRepoStatus,
    ) -> PyResult<()> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .read()
                    .await
                    .set_status(&status.into())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)
            })?;
            Ok(())
        })
    }

    pub(crate) fn set_status_async<'py>(
        &'py self,
        py: Python<'py>,
        status: PyRepoStatus,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository = repository.read().await;
            repository
                .set_status(&status.into())
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(())
        })
    }

    pub(crate) fn feature_flags(&self, py: Python<'_>) -> PyResult<Vec<PyFeatureFlag>> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let flags: Vec<PyFeatureFlag> = self
                    .0
                    .read()
                    .await
                    .feature_flags()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?
                    .map(PyFeatureFlag::from)
                    .collect();
                Ok(flags)
            })
        })
    }

    pub(crate) fn feature_flags_async<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let flags: Vec<PyFeatureFlag> = repository
                .read()
                .await
                .feature_flags()
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?
                .map(PyFeatureFlag::from)
                .collect();
            Ok(flags)
        })
    }

    pub(crate) fn enabled_feature_flags(
        &self,
        py: Python<'_>,
    ) -> PyResult<Vec<PyFeatureFlag>> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let flags: Vec<PyFeatureFlag> = self
                    .0
                    .read()
                    .await
                    .enabled_feature_flags()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?
                    .map(PyFeatureFlag::from)
                    .collect();
                Ok(flags)
            })
        })
    }

    pub(crate) fn enabled_feature_flags_async<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let flags: Vec<PyFeatureFlag> = repository
                .read()
                .await
                .enabled_feature_flags()
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?
                .map(PyFeatureFlag::from)
                .collect();
            Ok(flags)
        })
    }

    pub(crate) fn disabled_feature_flags(
        &self,
        py: Python<'_>,
    ) -> PyResult<Vec<PyFeatureFlag>> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let flags: Vec<PyFeatureFlag> = self
                    .0
                    .read()
                    .await
                    .disabled_feature_flags()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?
                    .map(PyFeatureFlag::from)
                    .collect();
                Ok(flags)
            })
        })
    }

    pub(crate) fn disabled_feature_flags_async<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let flags: Vec<PyFeatureFlag> = repository
                .read()
                .await
                .disabled_feature_flags()
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?
                .map(PyFeatureFlag::from)
                .collect();
            Ok(flags)
        })
    }

    pub(crate) fn set_feature_flag(
        &self,
        py: Python<'_>,
        name: &str,
        setting: Option<bool>,
    ) -> PyResult<()> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .read()
                    .await
                    .set_feature_flag(name, setting)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    pub(crate) fn set_feature_flag_async<'py>(
        &'py self,
        py: Python<'py>,
        name: String,
        setting: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            repository
                .read()
                .await
                .set_feature_flag(&name, setting)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(())
        })
    }

    /// Returns an object that is both a sync and an async iterator
    #[pyo3(signature = (*, branch = None, tag = None, snapshot_id = None))]
    pub(crate) fn async_ancestry(
        &self,
        py: Python<'_>,
        branch: Option<String>,
        tag: Option<String>,
        snapshot_id: Option<String>,
    ) -> PyResult<PyAsyncGenerator> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            let version = args_to_version_info(branch, tag, snapshot_id, None)?;
            let ancestry = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move {
                    let repo = self.0.read().await;
                    repo.ancestry(&version).await
                })
                .map_err(PyIcechunkStoreError::RepositoryError)?
                .map_err(PyIcechunkStoreError::RepositoryError);

            let parents = ancestry.and_then(|info| async move {
                Python::attach(|py| {
                    let info = PySnapshotInfo::from(info);
                    Ok(Bound::new(py, info)?.into_any().unbind())
                })
            });

            let prepared_list = Arc::new(Mutex::new(parents.err_into().boxed()));
            Ok(PyAsyncGenerator::new(prepared_list))
        })
    }

    pub(crate) fn async_ops_log(&self, py: Python<'_>) -> PyResult<PyAsyncGenerator> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            let ops = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move {
                    let repo = self.0.read().await;
                    repo.ops_log().await.map(|(stream, _, _)| stream)
                })
                .map_err(PyIcechunkStoreError::RepositoryError)?
                .map_err(PyIcechunkStoreError::RepositoryError)
                .and_then(|(ts, update, repo_path)| async move {
                    mk_update(&update, ts, repo_path).map_err(PyIcechunkStoreError::from)
                });

            let prepared_list = Arc::new(Mutex::new(ops.err_into().boxed()));
            Ok(PyAsyncGenerator::new(prepared_list))
        })
    }

    pub(crate) fn create_branch(
        &self,
        py: Python<'_>,
        branch_name: &str,
        snapshot_id: &str,
    ) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
                PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                    RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()),
                ))
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

    fn create_branch_async<'py>(
        &'py self,
        py: Python<'py>,
        branch_name: &str,
        snapshot_id: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        let branch_name = branch_name.to_owned();
        let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
            PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()),
            ))
        })?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository = repository.read().await;
            repository
                .create_branch(&branch_name, &snapshot_id)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(())
        })
    }

    pub(crate) fn list_branches(&self, py: Python<'_>) -> PyResult<BTreeSet<String>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
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

    fn list_branches_async<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py::<_, BTreeSet<String>>(
            py,
            async move {
                let repository = repository.read().await;
                let branches = repository
                    .list_branches()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(branches)
            },
        )
    }

    pub(crate) fn lookup_branch(
        &self,
        py: Python<'_>,
        branch_name: &str,
    ) -> PyResult<String> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
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

    fn lookup_branch_async<'py>(
        &'py self,
        py: Python<'py>,
        branch_name: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        let branch_name = branch_name.to_owned();
        pyo3_async_runtimes::tokio::future_into_py::<_, String>(py, async move {
            let repository = repository.read().await;
            let tip = repository
                .lookup_branch(&branch_name)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(tip.to_string())
        })
    }

    pub(crate) fn lookup_snapshot(
        &self,
        py: Python<'_>,
        snapshot_id: &str,
    ) -> PyResult<PySnapshotInfo> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
                PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                    RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()),
                ))
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

    fn lookup_snapshot_async<'py>(
        &'py self,
        py: Python<'py>,
        snapshot_id: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
            PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()),
            ))
        })?;
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py::<_, PySnapshotInfo>(py, async move {
            let repository = repository.read().await;
            let res = repository
                .lookup_snapshot(&snapshot_id)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(res.into())
        })
    }

    pub(crate) fn list_manifest_files(
        &self,
        snapshot_id: &str,
    ) -> PyResult<Vec<PyManifestFileInfo>> {
        let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
            PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()),
            ))
        })?;
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let res = self
                .0
                .read()
                .await
                .lookup_manifest_files(&snapshot_id)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?
                .map_into()
                .collect();
            Ok(res)
        })
    }

    pub(crate) fn list_manifest_files_async<'py>(
        &'py self,
        py: Python<'py>,
        snapshot_id: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
            PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()),
            ))
        })?;
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py::<_, Vec<PyManifestFileInfo>>(
            py,
            async move {
                let repository = repository.read().await;
                let res = repository
                    .lookup_manifest_files(&snapshot_id)
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?
                    .map_into()
                    .collect();
                Ok(res)
            },
        )
    }

    pub(crate) fn reset_branch(
        &self,
        py: Python<'_>,
        branch_name: &str,
        to_snapshot_id: &str,
        from_snapshot_id: Option<&str>,
    ) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            let to_snapshot_id = SnapshotId::try_from(to_snapshot_id).map_err(|_| {
                PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                    RepositoryErrorKind::InvalidSnapshotId(to_snapshot_id.to_owned()),
                ))
            })?;

            let from_snapshot_id = from_snapshot_id
                .map(|sid| {
                    SnapshotId::try_from(sid).map_err(|_| {
                        PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                            RepositoryErrorKind::InvalidSnapshotId(sid.to_owned()),
                        ))
                    })
                })
                .transpose()?;

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .read()
                    .await
                    .reset_branch(branch_name, &to_snapshot_id, from_snapshot_id.as_ref())
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(())
            })
        })
    }

    fn reset_branch_async<'py>(
        &'py self,
        py: Python<'py>,
        branch_name: &str,
        to_snapshot_id: &str,
        from_snapshot_id: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        let branch_name = branch_name.to_owned();
        let to_snapshot_id = SnapshotId::try_from(to_snapshot_id).map_err(|_| {
            PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                RepositoryErrorKind::InvalidSnapshotId(to_snapshot_id.to_owned()),
            ))
        })?;

        let from_snapshot_id = from_snapshot_id
            .map(|sid| {
                SnapshotId::try_from(sid).map_err(|_| {
                    PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                        RepositoryErrorKind::InvalidSnapshotId(sid.to_owned()),
                    ))
                })
            })
            .transpose()?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository = repository.read().await;
            repository
                .reset_branch(&branch_name, &to_snapshot_id, from_snapshot_id.as_ref())
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(())
        })
    }

    pub(crate) fn delete_branch(&self, py: Python<'_>, branch: &str) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
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

    fn delete_branch_async<'py>(
        &'py self,
        py: Python<'py>,
        branch: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        let branch = branch.to_owned();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository = repository.read().await;
            repository
                .delete_branch(&branch)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(())
        })
    }

    pub(crate) fn delete_tag(&self, py: Python<'_>, tag: &str) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
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

    fn delete_tag_async<'py>(
        &'py self,
        py: Python<'py>,
        tag: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        let tag = tag.to_owned();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository = repository.read().await;
            repository
                .delete_tag(&tag)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(())
        })
    }

    pub(crate) fn create_tag(
        &self,
        py: Python<'_>,
        tag_name: &str,
        snapshot_id: &str,
    ) -> PyResult<()> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
                PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                    RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()),
                ))
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

    fn create_tag_async<'py>(
        &'py self,
        py: Python<'py>,
        tag_name: &str,
        snapshot_id: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        let tag_name = tag_name.to_owned();
        let snapshot_id = SnapshotId::try_from(snapshot_id).map_err(|_| {
            PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                RepositoryErrorKind::InvalidSnapshotId(snapshot_id.to_owned()),
            ))
        })?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let repository = repository.read().await;
            repository
                .create_tag(&tag_name, &snapshot_id)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(())
        })
    }

    pub(crate) fn list_tags(&self, py: Python<'_>) -> PyResult<BTreeSet<String>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
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

    fn list_tags_async<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py::<_, BTreeSet<String>>(
            py,
            async move {
                let repository = repository.read().await;
                let tags = repository
                    .list_tags()
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(tags)
            },
        )
    }

    pub(crate) fn lookup_tag(&self, py: Python<'_>, tag: &str) -> PyResult<String> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
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

    fn lookup_tag_async<'py>(
        &'py self,
        py: Python<'py>,
        tag: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        let tag_name = tag.to_owned();
        pyo3_async_runtimes::tokio::future_into_py::<_, String>(py, async move {
            let repository = repository.read().await;
            let tip = repository
                .lookup_tag(&tag_name)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(tip.to_string())
        })
    }

    #[pyo3(signature = (*, from_branch=None, from_tag=None, from_snapshot_id=None, to_branch=None, to_tag=None, to_snapshot_id=None))]
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn diff(
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
        py.detach(move || {
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

    #[pyo3(signature = (*, from_branch=None, from_tag=None, from_snapshot_id=None, to_branch=None, to_tag=None, to_snapshot_id=None))]
    #[expect(clippy::too_many_arguments)]
    fn diff_async<'py>(
        &'py self,
        py: Python<'py>,
        from_branch: Option<String>,
        from_tag: Option<String>,
        from_snapshot_id: Option<String>,
        to_branch: Option<String>,
        to_tag: Option<String>,
        to_snapshot_id: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let from = args_to_version_info(from_branch, from_tag, from_snapshot_id, None)?;
        let to = args_to_version_info(to_branch, to_tag, to_snapshot_id, None)?;
        let repository = Arc::clone(&self.0);

        pyo3_async_runtimes::tokio::future_into_py::<_, PyDiff>(py, async move {
            let repository = repository.read().await;
            let diff = repository
                .diff(&from, &to)
                .await
                .map_err(PyIcechunkStoreError::SessionError)?;
            Ok(diff.into())
        })
    }

    #[pyo3(signature = (*, branch = None, tag = None, snapshot_id = None, as_of = None))]
    pub(crate) fn readonly_session(
        &self,
        py: Python<'_>,
        branch: Option<String>,
        tag: Option<String>,
        snapshot_id: Option<String>,
        as_of: Option<DateTime<Utc>>,
    ) -> PyResult<PySession> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
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

    #[pyo3(signature = (*, branch = None, tag = None, snapshot_id = None, as_of = None))]
    fn readonly_session_async<'py>(
        &'py self,
        py: Python<'py>,
        branch: Option<String>,
        tag: Option<String>,
        snapshot_id: Option<String>,
        as_of: Option<DateTime<Utc>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let version = args_to_version_info(branch, tag, snapshot_id, as_of)?;
        let repository = Arc::clone(&self.0);

        pyo3_async_runtimes::tokio::future_into_py::<_, PySession>(py, async move {
            let repository = repository.read().await;
            let session = repository
                .readonly_session(&version)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(PySession(Arc::new(RwLock::new(session))))
        })
    }

    pub(crate) fn writable_session(
        &self,
        py: Python<'_>,
        branch: &str,
    ) -> PyResult<PySession> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
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

    fn writable_session_async<'py>(
        &'py self,
        py: Python<'py>,
        branch: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        let branch = branch.to_owned();
        pyo3_async_runtimes::tokio::future_into_py::<_, PySession>(py, async move {
            let repository = repository.read().await;
            let session = repository
                .writable_session(&branch)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(PySession(Arc::new(RwLock::new(session))))
        })
    }

    pub(crate) fn rearrange_session(
        &self,
        py: Python<'_>,
        branch: &str,
    ) -> PyResult<PySession> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            let session =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    self.0
                        .read()
                        .await
                        .rearrange_session(branch)
                        .await
                        .map_err(PyIcechunkStoreError::RepositoryError)
                })?;

            Ok(PySession(Arc::new(RwLock::new(session))))
        })
    }

    fn rearrange_session_async<'py>(
        &'py self,
        py: Python<'py>,
        branch: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        let branch = branch.to_owned();
        pyo3_async_runtimes::tokio::future_into_py::<_, PySession>(py, async move {
            let repository = repository.read().await;
            let session = repository
                .rearrange_session(&branch)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(PySession(Arc::new(RwLock::new(session))))
        })
    }

    #[pyo3(signature = (message, *, branch, metadata=None, commit_method="new_commit"))]
    pub(crate) fn rewrite_manifests(
        &self,
        py: Python<'_>,
        message: &str,
        branch: &str,
        metadata: Option<PySnapshotProperties>,
        commit_method: &str,
    ) -> PyResult<String> {
        // This function calls block_on, so we need to allow other thread python to make progress
        let commit_method = parse_commit_method(commit_method)?;
        py.detach(move || {
            let metadata = metadata.map(|m| m.into());
            let result =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let lock = self.0.read().await;
                    rewrite_manifests(&lock, branch, message, 1, metadata, commit_method)
                        .await
                        .map_err(PyIcechunkStoreError::ManifestOpsError)
                })?;
            Ok(result.to_string())
        })
    }

    #[pyo3(signature = (message, *, branch, metadata=None, commit_method="new_commit"))]
    fn rewrite_manifests_async<'py>(
        &'py self,
        py: Python<'py>,
        message: &str,
        branch: &str,
        metadata: Option<PySnapshotProperties>,
        commit_method: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        let message = message.to_owned();
        let branch = branch.to_owned();
        let metadata = metadata.map(|m| m.into());
        let commit_method = parse_commit_method(commit_method)?;

        pyo3_async_runtimes::tokio::future_into_py::<_, String>(py, async move {
            let repository = repository.read().await;
            let result = rewrite_manifests(
                &repository,
                &branch,
                &message,
                1,
                metadata,
                commit_method,
            )
            .await
            .map_err(PyIcechunkStoreError::ManifestOpsError)?;
            Ok(result.to_string())
        })
    }

    #[pyo3(signature = (older_than, *, delete_expired_branches = false, delete_expired_tags = false))]
    pub(crate) fn expire_snapshots(
        &self,
        py: Python<'_>,
        older_than: DateTime<Utc>,
        delete_expired_branches: bool,
        delete_expired_tags: bool,
    ) -> PyResult<HashSet<String>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            let result =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let (asset_manager, num_updates) = {
                        let lock = self.0.read().await;
                        let num_updates = lock.config().num_updates_per_repo_info_file();
                        (Arc::clone(lock.asset_manager()), num_updates)
                    };

                    let result = expire(
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
                        None,
                        num_updates,
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

    #[pyo3(signature = (older_than, *, delete_expired_branches = false, delete_expired_tags = false))]
    fn expire_snapshots_async<'py>(
        &'py self,
        py: Python<'py>,
        older_than: DateTime<Utc>,
        delete_expired_branches: bool,
        delete_expired_tags: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py::<_, HashSet<String>>(py, async move {
            let (asset_manager, num_updates) = {
                let lock = repository.read().await;
                let num_updates = lock.config().num_updates_per_repo_info_file();
                (Arc::clone(lock.asset_manager()), num_updates)
            };

            let result = expire(
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
                None,
                num_updates,
            )
            .await
            .map_err(PyIcechunkStoreError::GCError)?;
            Ok(result.released_snapshots.iter().map(|id| id.to_string()).collect())
        })
    }

    pub(crate) fn garbage_collect(
        &self,
        py: Python<'_>,
        delete_object_older_than: DateTime<Utc>,
        dry_run: bool,
        max_snapshots_in_memory: NonZeroU16,
        max_compressed_manifest_mem_bytes: NonZeroUsize,
        max_concurrent_manifest_fetches: NonZeroU16,
    ) -> PyResult<PyGCSummary> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            let result =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let gc_config = GCConfig::clean_all(
                        delete_object_older_than,
                        delete_object_older_than,
                        Default::default(),
                        max_snapshots_in_memory,
                        max_compressed_manifest_mem_bytes,
                        max_concurrent_manifest_fetches,
                        dry_run,
                    );
                    let (asset_manager, num_updates) = {
                        let lock = self.0.read().await;
                        let num_updates = lock.config().num_updates_per_repo_info_file();
                        (Arc::clone(lock.asset_manager()), num_updates)
                    };
                    let result =
                        garbage_collect(asset_manager, &gc_config, None, num_updates)
                            .await
                            .map_err(PyIcechunkStoreError::GCError)?;
                    Ok::<_, PyIcechunkStoreError>(result.into())
                })?;

            Ok(result)
        })
    }

    fn garbage_collect_async<'py>(
        &'py self,
        py: Python<'py>,
        delete_object_older_than: DateTime<Utc>,
        dry_run: bool,
        max_snapshots_in_memory: NonZeroU16,
        max_compressed_manifest_mem_bytes: NonZeroUsize,
        max_concurrent_manifest_fetches: NonZeroU16,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py::<_, PyGCSummary>(py, async move {
            let gc_config = GCConfig::clean_all(
                delete_object_older_than,
                delete_object_older_than,
                Default::default(),
                max_snapshots_in_memory,
                max_compressed_manifest_mem_bytes,
                max_concurrent_manifest_fetches,
                dry_run,
            );
            let (asset_manager, num_updates) = {
                let lock = repository.read().await;
                let num_updates = lock.config().num_updates_per_repo_info_file();
                (Arc::clone(lock.asset_manager()), num_updates)
            };
            let result = garbage_collect(asset_manager, &gc_config, None, num_updates)
                .await
                .map_err(PyIcechunkStoreError::GCError)?;
            Ok(result.into())
        })
    }

    pub(crate) fn chunk_storage_stats(
        &self,
        py: Python<'_>,
        max_snapshots_in_memory: NonZeroU16,
        max_compressed_manifest_mem_bytes: NonZeroUsize,
        max_concurrent_manifest_fetches: NonZeroU16,
    ) -> PyResult<PyChunkStorageStats> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.detach(move || {
            let stats =
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let asset_manager = {
                        let lock = self.0.read().await;
                        Arc::clone(lock.asset_manager())
                    };
                    let stats = repo_chunks_storage(
                        asset_manager,
                        max_snapshots_in_memory,
                        max_compressed_manifest_mem_bytes,
                        max_concurrent_manifest_fetches,
                    )
                    .await
                    .map_err(PyIcechunkStoreError::RepositoryError)?;
                    Ok::<_, PyIcechunkStoreError>(stats)
                })?;

            Ok(stats.into())
        })
    }

    pub(crate) fn chunk_storage_stats_async<'py>(
        &'py self,
        py: Python<'py>,
        max_snapshots_in_memory: NonZeroU16,
        max_compressed_manifest_mem_bytes: NonZeroUsize,
        max_concurrent_manifest_fetches: NonZeroU16,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py::<_, PyChunkStorageStats>(
            py,
            async move {
                let asset_manager = {
                    let lock = repository.read().await;
                    Arc::clone(lock.asset_manager())
                };
                let stats = repo_chunks_storage(
                    asset_manager,
                    max_snapshots_in_memory,
                    max_compressed_manifest_mem_bytes,
                    max_concurrent_manifest_fetches,
                )
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
                Ok(stats.into())
            },
        )
    }

    #[pyo3(signature = (snapshot_id, *, pretty = true))]
    fn inspect_snapshot(&self, snapshot_id: String, pretty: bool) -> PyResult<String> {
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async move {
                let lock = self.0.read().await;
                let snap = SnapshotId::try_from(snapshot_id.as_str()).map_err(|e| {
                    RepositoryError::capture(RepositoryErrorKind::Other(e.to_string()))
                })?;
                let res = snapshot_json(lock.asset_manager(), &snap, pretty).await?;
                Ok(res)
            })
            .map_err(PyIcechunkStoreError::RepositoryError)?;
        Ok(result)
    }

    #[pyo3(signature = (snapshot_id, *, pretty = true))]
    fn inspect_snapshot_async<'py>(
        &self,
        py: Python<'py>,
        snapshot_id: String,
        pretty: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let lock = repository.read().await;
            let snap = SnapshotId::try_from(snapshot_id.as_str())
                .map_err(|e| {
                    RepositoryError::capture(RepositoryErrorKind::Other(e.to_string()))
                })
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            let res = snapshot_json(lock.asset_manager(), &snap, pretty)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(res)
        })
    }

    #[pyo3(signature = (*, pretty = true))]
    fn inspect_repo_info(&self, pretty: bool) -> PyResult<String> {
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async move {
                let lock = self.0.read().await;
                let res = repo_info_json(lock.asset_manager(), pretty).await?;
                Ok(res)
            })
            .map_err(PyIcechunkStoreError::RepositoryError)?;
        Ok(result)
    }

    #[pyo3(signature = (*, pretty = true))]
    fn inspect_repo_info_async<'py>(
        &self,
        py: Python<'py>,
        pretty: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let lock = repository.read().await;
            let res = repo_info_json(lock.asset_manager(), pretty)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(res)
        })
    }

    #[pyo3(signature = (manifest_id, *, pretty = true))]
    fn inspect_manifest(&self, manifest_id: String, pretty: bool) -> PyResult<String> {
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async move {
                let lock = self.0.read().await;
                let id = ManifestId::try_from(manifest_id.as_str()).map_err(|e| {
                    RepositoryError::capture(RepositoryErrorKind::Other(e.to_string()))
                })?;
                let res = manifest_json(lock.asset_manager(), &id, pretty).await?;
                Ok(res)
            })
            .map_err(PyIcechunkStoreError::RepositoryError)?;
        Ok(result)
    }

    #[pyo3(signature = (manifest_id, *, pretty = true))]
    fn inspect_manifest_async<'py>(
        &self,
        py: Python<'py>,
        manifest_id: String,
        pretty: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let lock = repository.read().await;
            let id = ManifestId::try_from(manifest_id.as_str())
                .map_err(|e| {
                    RepositoryError::capture(RepositoryErrorKind::Other(e.to_string()))
                })
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            let res = manifest_json(lock.asset_manager(), &id, pretty)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(res)
        })
    }

    #[pyo3(signature = (snapshot_id, *, pretty = true))]
    fn inspect_transaction_log(
        &self,
        snapshot_id: String,
        pretty: bool,
    ) -> PyResult<String> {
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async move {
                let lock = self.0.read().await;
                let snap = SnapshotId::try_from(snapshot_id.as_str()).map_err(|e| {
                    RepositoryError::capture(RepositoryErrorKind::Other(e.to_string()))
                })?;
                let res =
                    transaction_log_json(lock.asset_manager(), &snap, pretty).await?;
                Ok(res)
            })
            .map_err(PyIcechunkStoreError::RepositoryError)?;
        Ok(result)
    }

    #[pyo3(signature = (snapshot_id, *, pretty = true))]
    fn inspect_transaction_log_async<'py>(
        &self,
        py: Python<'py>,
        snapshot_id: String,
        pretty: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let repository = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let lock = repository.read().await;
            let snap = SnapshotId::try_from(snapshot_id.as_str())
                .map_err(|e| {
                    RepositoryError::capture(RepositoryErrorKind::Other(e.to_string()))
                })
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            let res = transaction_log_json(lock.asset_manager(), &snap, pretty)
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            Ok(res)
        })
    }

    #[getter]
    fn spec_version(&self) -> PySpecVersion {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let repo = self.0.read().await;
            repo.spec_version().into()
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
            PyIcechunkStoreError::RepositoryError(RepositoryError::capture(
                RepositoryErrorKind::InvalidSnapshotId(snapshot_id.clone()),
            ))
        })?;

        Ok(VersionInfo::SnapshotId(snapshot_id))
    } else {
        Err(PyValueError::new_err("Must provide one of branch, tag, or snapshot_id"))
    }
}
