use std::collections::HashMap;
use std::sync::Arc;

use icechunk::config::{Credentials, RepositoryConfig};
use icechunk::format::SnapshotId;
use icechunk::format::format_constants::SpecVersionBin;
use icechunk::repository::{Repository, VersionInfo};
use napi_derive::napi;
use tokio::sync::RwLock;

use crate::config::JsRepositoryConfig;
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
    ) -> napi::Result<JsRepository> {
        let config = convert_config(config)?;
        let version = spec_version
            .map(|v| SpecVersionBin::try_from(v as u8))
            .transpose()
            .map_napi_err()?;
        let creds = convert_credentials(authorize_virtual_chunk_access);
        let repo = Repository::create(config, Arc::clone(&storage.0), creds, version)
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
    ) -> napi::Result<JsRepository> {
        let config = convert_config(config)?;
        let version = spec_version
            .map(|v| SpecVersionBin::try_from(v as u8))
            .transpose()
            .map_napi_err()?;
        let creds = convert_credentials(authorize_virtual_chunk_access);
        let repo =
            Repository::open_or_create(config, Arc::clone(&storage.0), creds, version)
                .await
                .map_napi_err()?;
        Ok(JsRepository(Arc::new(RwLock::new(repo))))
    }

    #[napi]
    pub async fn exists(storage: &JsStorage) -> napi::Result<bool> {
        Repository::exists(Arc::clone(&storage.0)).await.map_napi_err()
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
}

#[napi(object, js_name = "ManifestFileInfo")]
pub struct JsManifestFileInfo {
    pub id: String,
    pub size_bytes: i64,
    pub num_chunk_refs: u32,
}
