use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use icechunk::config::{Credentials, RepositoryConfig};
use icechunk::diff::Diff;
use icechunk::feature_flags::FeatureFlag;
use icechunk::format::SnapshotId;
use icechunk::format::format_constants::SpecVersionBin;
use icechunk::format::snapshot::SnapshotInfo;
use icechunk::repository::{Repository, VersionInfo};
use napi_derive::napi;
use tokio::sync::RwLock;

use crate::config::{JsRepositoryConfig, JsStorageSettings};
use crate::errors::IntoNapiResult;
use crate::session::JsSession;
use crate::storage::JsStorage;

#[cfg(not(target_family = "wasm"))]
use crate::storage::JsCredentials;

#[cfg(target_family = "wasm")]
#[allow(dead_code)]
type JsCredentials = ();

fn convert_config(
    config: Option<JsRepositoryConfig>,
) -> napi::Result<Option<RepositoryConfig>> {
    config
        .map(|c| RepositoryConfig::try_from(c).map_err(napi::Error::from_reason))
        .transpose()
}

fn convert_credentials(
    creds: Option<HashMap<String, Option<JsCredentials>>>,
) -> HashMap<String, Option<Credentials>> {
    #[cfg(not(target_family = "wasm"))]
    {
        creds
            .map(|c| c.into_iter().map(|(k, v)| (k, v.map(|c| c.into()))).collect())
            .unwrap_or_default()
    }
    #[cfg(target_family = "wasm")]
    {
        let _ = creds;
        HashMap::new()
    }
}

#[napi(object)]
pub struct ReadonlySessionOptions {
    pub branch: Option<String>,
    pub tag: Option<String>,
    pub snapshot_id: Option<String>,
}

#[napi(object, js_name = "VersionOptions")]
pub struct JsVersionOptions {
    pub branch: Option<String>,
    pub tag: Option<String>,
    pub snapshot_id: Option<String>,
}

fn version_options_to_version_info(opts: JsVersionOptions) -> napi::Result<VersionInfo> {
    if let Some(snap) = opts.snapshot_id {
        let id = SnapshotId::try_from(snap.as_str()).map_napi_err()?;
        Ok(VersionInfo::SnapshotId(id))
    } else if let Some(tag) = opts.tag {
        Ok(VersionInfo::TagRef(tag))
    } else if let Some(branch) = opts.branch {
        Ok(VersionInfo::BranchTipRef(branch))
    } else {
        Err(napi::Error::from_reason(
            "One of branch, tag, or snapshot_id must be provided".to_string(),
        ))
    }
}

#[napi(object, js_name = "SnapshotInfo")]
pub struct JsSnapshotInfo {
    pub id: String,
    pub parent_id: Option<String>,
    pub written_at: String,
    pub message: String,
    pub metadata: serde_json::Value,
}

impl From<SnapshotInfo> for JsSnapshotInfo {
    fn from(info: SnapshotInfo) -> Self {
        let metadata: serde_json::Map<String, serde_json::Value> =
            info.metadata.into_iter().collect();
        Self {
            id: info.id.to_string(),
            parent_id: info.parent_id.map(|id| id.to_string()),
            written_at: info.flushed_at.to_rfc3339(),
            message: info.message,
            metadata: serde_json::Value::Object(metadata),
        }
    }
}

#[napi(object, js_name = "DiffResult")]
pub struct JsDiff {
    pub new_groups: Vec<String>,
    pub new_arrays: Vec<String>,
    pub deleted_groups: Vec<String>,
    pub deleted_arrays: Vec<String>,
    pub updated_groups: Vec<String>,
    pub updated_arrays: Vec<String>,
    pub updated_chunks: serde_json::Value,
    pub moved_nodes: Vec<JsMovedNode>,
}

#[napi(object, js_name = "MovedNode")]
pub struct JsMovedNode {
    pub from: String,
    pub to: String,
}

impl From<Diff> for JsDiff {
    fn from(diff: Diff) -> Self {
        let updated_chunks: BTreeMap<String, Vec<Vec<u32>>> = diff
            .updated_chunks
            .into_iter()
            .map(|(k, v)| {
                let path = k.to_string();
                let indices = v.into_iter().map(|idx| idx.0).collect();
                (path, indices)
            })
            .collect();
        let moved_nodes = diff
            .moved_nodes
            .into_iter()
            .map(|m| JsMovedNode { from: m.from.to_string(), to: m.to.to_string() })
            .collect();
        Self {
            new_groups: diff.new_groups.into_iter().map(|p| p.to_string()).collect(),
            new_arrays: diff.new_arrays.into_iter().map(|p| p.to_string()).collect(),
            deleted_groups: diff
                .deleted_groups
                .into_iter()
                .map(|p| p.to_string())
                .collect(),
            deleted_arrays: diff
                .deleted_arrays
                .into_iter()
                .map(|p| p.to_string())
                .collect(),
            updated_groups: diff
                .updated_groups
                .into_iter()
                .map(|p| p.to_string())
                .collect(),
            updated_arrays: diff
                .updated_arrays
                .into_iter()
                .map(|p| p.to_string())
                .collect(),
            updated_chunks: serde_json::to_value(updated_chunks).unwrap_or_default(),
            moved_nodes,
        }
    }
}

#[napi(object, js_name = "FeatureFlag")]
pub struct JsFeatureFlag {
    pub id: u32,
    pub name: String,
    pub default_enabled: bool,
    pub setting: Option<bool>,
    pub enabled: bool,
}

impl From<FeatureFlag> for JsFeatureFlag {
    fn from(flag: FeatureFlag) -> Self {
        Self {
            id: flag.id() as u32,
            name: flag.name().to_string(),
            default_enabled: flag.default_enabled(),
            setting: flag.setting(),
            enabled: flag.enabled(),
        }
    }
}

#[napi(object, js_name = "DiffOptions")]
pub struct JsDiffOptions {
    pub from_branch: Option<String>,
    pub from_tag: Option<String>,
    pub from_snapshot_id: Option<String>,
    pub to_branch: Option<String>,
    pub to_tag: Option<String>,
    pub to_snapshot_id: Option<String>,
}

#[napi(js_name = "Repository")]
pub struct JsRepository(pub(crate) Arc<RwLock<Repository>>);

#[napi]
impl JsRepository {
    #[napi(factory)]
    pub async fn create(
        storage: &JsStorage,
        config: Option<JsRepositoryConfig>,
        spec_version: Option<u32>,
        authorize_virtual_chunk_access: Option<HashMap<String, Option<JsCredentials>>>,
        check_clean_root: Option<bool>,
    ) -> napi::Result<JsRepository> {
        let config = convert_config(config)?;
        let version = spec_version
            .map(|v| SpecVersionBin::try_from(v as u8))
            .transpose()
            .map_napi_err()?;
        let creds = convert_credentials(authorize_virtual_chunk_access);
        let repo = Repository::create(
            config,
            Arc::clone(&storage.0),
            creds,
            version,
            check_clean_root.unwrap_or(true),
        )
        .await
        .map_napi_err()?;
        Ok(JsRepository(Arc::new(RwLock::new(repo))))
    }

    #[napi(factory)]
    pub async fn open(
        storage: &JsStorage,
        config: Option<JsRepositoryConfig>,
        authorize_virtual_chunk_access: Option<HashMap<String, Option<JsCredentials>>>,
    ) -> napi::Result<JsRepository> {
        let config = convert_config(config)?;
        let creds = convert_credentials(authorize_virtual_chunk_access);
        let repo = Repository::open(config, Arc::clone(&storage.0), creds)
            .await
            .map_napi_err()?;
        Ok(JsRepository(Arc::new(RwLock::new(repo))))
    }

    #[napi(factory)]
    pub async fn open_or_create(
        storage: &JsStorage,
        config: Option<JsRepositoryConfig>,
        spec_version: Option<u32>,
        authorize_virtual_chunk_access: Option<HashMap<String, Option<JsCredentials>>>,
        check_clean_root: Option<bool>,
    ) -> napi::Result<JsRepository> {
        let config = convert_config(config)?;
        let version = spec_version
            .map(|v| SpecVersionBin::try_from(v as u8))
            .transpose()
            .map_napi_err()?;
        let creds = convert_credentials(authorize_virtual_chunk_access);
        let repo = Repository::open_or_create(
            config,
            Arc::clone(&storage.0),
            creds,
            version,
            check_clean_root.unwrap_or(true),
        )
        .await
        .map_napi_err()?;
        Ok(JsRepository(Arc::new(RwLock::new(repo))))
    }

    #[napi]
    pub async fn exists(
        storage: &JsStorage,
        storage_settings: Option<JsStorageSettings>,
    ) -> napi::Result<bool> {
        let settings = storage_settings.map(|s| s.into());
        Repository::exists(Arc::clone(&storage.0), settings).await.map_napi_err()
    }

    #[napi]
    pub async fn readonly_session(
        &self,
        options: Option<ReadonlySessionOptions>,
    ) -> napi::Result<JsSession> {
        let version = match options {
            Some(opts) => {
                if let Some(snap) = opts.snapshot_id {
                    let id = SnapshotId::try_from(snap.as_str()).map_napi_err()?;
                    VersionInfo::SnapshotId(id)
                } else if let Some(tag) = opts.tag {
                    VersionInfo::TagRef(tag)
                } else if let Some(branch) = opts.branch {
                    VersionInfo::BranchTipRef(branch)
                } else {
                    VersionInfo::BranchTipRef("main".to_string())
                }
            }
            None => VersionInfo::BranchTipRef("main".to_string()),
        };

        let repo = self.0.read().await;
        let session = repo.readonly_session(&version).await.map_napi_err()?;
        Ok(JsSession::new(session))
    }

    #[napi]
    pub async fn writable_session(&self, branch: String) -> napi::Result<JsSession> {
        let repo = self.0.read().await;
        let session = repo.writable_session(&branch).await.map_napi_err()?;
        Ok(JsSession::new(session))
    }

    #[napi]
    pub async fn list_branches(&self) -> napi::Result<Vec<String>> {
        let repo = self.0.read().await;
        let branches = repo.list_branches().await.map_napi_err()?;
        Ok(branches.into_iter().collect())
    }

    #[napi]
    pub async fn create_branch(
        &self,
        name: String,
        snapshot_id: String,
    ) -> napi::Result<()> {
        let id = SnapshotId::try_from(snapshot_id.as_str()).map_napi_err()?;
        let repo = self.0.read().await;
        repo.create_branch(&name, &id).await.map_napi_err()
    }

    #[napi]
    pub async fn list_tags(&self) -> napi::Result<Vec<String>> {
        let repo = self.0.read().await;
        let tags = repo.list_tags().await.map_napi_err()?;
        Ok(tags.into_iter().collect())
    }

    #[napi]
    pub async fn create_tag(
        &self,
        name: String,
        snapshot_id: String,
    ) -> napi::Result<()> {
        let id = SnapshotId::try_from(snapshot_id.as_str()).map_napi_err()?;
        let repo = self.0.read().await;
        repo.create_tag(&name, &id).await.map_napi_err()
    }

    #[napi]
    pub async fn lookup_manifest_files(
        &self,
        snapshot_id: String,
    ) -> napi::Result<Vec<JsManifestFileInfo>> {
        let id = SnapshotId::try_from(snapshot_id.as_str()).map_napi_err()?;
        let repo = self.0.read().await;
        let files = repo.lookup_manifest_files(&id).await.map_napi_err()?;
        Ok(files
            .map(|f| JsManifestFileInfo {
                id: f.id.to_string(),
                size_bytes: f.size_bytes as i64,
                num_chunk_refs: f.num_chunk_refs,
            })
            .collect())
    }

    // --- Branch operations ---

    #[napi]
    pub async fn lookup_branch(&self, branch: String) -> napi::Result<String> {
        let repo = self.0.read().await;
        let tip = repo.lookup_branch(&branch).await.map_napi_err()?;
        Ok(tip.to_string())
    }

    #[napi]
    pub async fn reset_branch(
        &self,
        branch: String,
        to_snapshot_id: String,
        from_snapshot_id: Option<String>,
    ) -> napi::Result<()> {
        let to_id = SnapshotId::try_from(to_snapshot_id.as_str()).map_napi_err()?;
        let from_id = from_snapshot_id
            .map(|s| SnapshotId::try_from(s.as_str()).map_napi_err())
            .transpose()?;
        let repo = self.0.read().await;
        repo.reset_branch(&branch, &to_id, from_id.as_ref()).await.map_napi_err()
    }

    #[napi]
    pub async fn delete_branch(&self, branch: String) -> napi::Result<()> {
        let repo = self.0.read().await;
        repo.delete_branch(&branch).await.map_napi_err()
    }

    // --- Tag operations ---

    #[napi]
    pub async fn lookup_tag(&self, tag: String) -> napi::Result<String> {
        let repo = self.0.read().await;
        let id = repo.lookup_tag(&tag).await.map_napi_err()?;
        Ok(id.to_string())
    }

    #[napi]
    pub async fn delete_tag(&self, tag: String) -> napi::Result<()> {
        let repo = self.0.read().await;
        repo.delete_tag(&tag).await.map_napi_err()
    }

    // --- Snapshot operations ---

    #[napi]
    pub async fn lookup_snapshot(
        &self,
        snapshot_id: String,
    ) -> napi::Result<JsSnapshotInfo> {
        let id = SnapshotId::try_from(snapshot_id.as_str()).map_napi_err()?;
        let repo = self.0.read().await;
        let info = repo.lookup_snapshot(&id).await.map_napi_err()?;
        Ok(info.into())
    }

    #[napi]
    pub async fn fetch_spec_version(
        storage: &JsStorage,
        storage_settings: Option<JsStorageSettings>,
    ) -> napi::Result<Option<u32>> {
        let settings = storage_settings.map(|s| s.into());
        let detected = Repository::fetch_spec_version(Arc::clone(&storage.0), settings)
            .await
            .map_napi_err()?;
        Ok(detected.map(|d| d.spec_version() as u32))
    }

    // --- Config operations ---

    #[napi]
    pub async fn fetch_config(
        storage: &JsStorage,
    ) -> napi::Result<Option<JsRepositoryConfig>> {
        let res =
            Repository::fetch_config(Arc::clone(&storage.0)).await.map_napi_err()?;
        match res {
            Some((config, _version_info)) => Ok(Some(config.into())),
            None => Ok(None),
        }
    }

    #[napi]
    pub async fn save_config(&self) -> napi::Result<()> {
        let repo = self.0.read().await;
        repo.save_config().await.map_napi_err()?;
        Ok(())
    }

    #[napi(getter)]
    pub fn config(&self) -> JsRepositoryConfig {
        self.0.blocking_read().config().clone().into()
    }

    #[napi(getter)]
    pub fn spec_version(&self) -> u32 {
        self.0.blocking_read().spec_version() as u32
    }

    // --- Diff ---

    #[napi]
    pub async fn diff(&self, options: JsDiffOptions) -> napi::Result<JsDiff> {
        let from = if let Some(snap) = options.from_snapshot_id {
            let id = SnapshotId::try_from(snap.as_str()).map_napi_err()?;
            VersionInfo::SnapshotId(id)
        } else if let Some(tag) = options.from_tag {
            VersionInfo::TagRef(tag)
        } else if let Some(branch) = options.from_branch {
            VersionInfo::BranchTipRef(branch)
        } else {
            return Err(napi::Error::from_reason(
                "One of from_branch, from_tag, or from_snapshot_id must be provided",
            ));
        };

        let to = if let Some(snap) = options.to_snapshot_id {
            let id = SnapshotId::try_from(snap.as_str()).map_napi_err()?;
            VersionInfo::SnapshotId(id)
        } else if let Some(tag) = options.to_tag {
            VersionInfo::TagRef(tag)
        } else if let Some(branch) = options.to_branch {
            VersionInfo::BranchTipRef(branch)
        } else {
            return Err(napi::Error::from_reason(
                "One of to_branch, to_tag, or to_snapshot_id must be provided",
            ));
        };

        let repo = self.0.read().await;
        let diff = repo.diff(&from, &to).await.map_napi_err()?;
        Ok(diff.into())
    }

    // --- Feature flags ---

    #[napi]
    pub async fn feature_flags(&self) -> napi::Result<Vec<JsFeatureFlag>> {
        let repo = self.0.read().await;
        let flags = repo.feature_flags().await.map_napi_err()?;
        Ok(flags.map(JsFeatureFlag::from).collect())
    }

    #[napi]
    pub async fn enabled_feature_flags(&self) -> napi::Result<Vec<JsFeatureFlag>> {
        let repo = self.0.read().await;
        let flags = repo.enabled_feature_flags().await.map_napi_err()?;
        Ok(flags.map(JsFeatureFlag::from).collect())
    }

    #[napi]
    pub async fn disabled_feature_flags(&self) -> napi::Result<Vec<JsFeatureFlag>> {
        let repo = self.0.read().await;
        let flags = repo.disabled_feature_flags().await.map_napi_err()?;
        Ok(flags.map(JsFeatureFlag::from).collect())
    }

    #[napi]
    pub async fn set_feature_flag(
        &self,
        name: String,
        setting: Option<bool>,
    ) -> napi::Result<()> {
        let repo = self.0.read().await;
        repo.set_feature_flag(&name, setting).await.map_napi_err()
    }

    // --- Session ---

    #[napi]
    pub async fn rearrange_session(&self, branch: String) -> napi::Result<JsSession> {
        let repo = self.0.read().await;
        let session = repo.rearrange_session(&branch).await.map_napi_err()?;
        Ok(JsSession::new(session))
    }

    // --- Ancestry ---

    #[napi]
    pub async fn ancestry(
        &self,
        options: JsVersionOptions,
    ) -> napi::Result<Vec<JsSnapshotInfo>> {
        use futures::TryStreamExt;
        let version = version_options_to_version_info(options)?;
        let repo = self.0.read().await;
        let stream = repo.ancestry(&version).await.map_napi_err()?;
        let snapshots: Vec<SnapshotInfo> = stream.try_collect().await.map_napi_err()?;
        Ok(snapshots.into_iter().map(JsSnapshotInfo::from).collect())
    }
}

#[napi(object, js_name = "ManifestFileInfo")]
pub struct JsManifestFileInfo {
    pub id: String,
    pub size_bytes: i64,
    pub num_chunk_refs: u32,
}
