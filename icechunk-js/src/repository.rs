use std::collections::HashMap;
use std::sync::Arc;

use icechunk::format::SnapshotId;
use icechunk::repository::{Repository, VersionInfo};
use napi_derive::napi;
use tokio::sync::RwLock;

use crate::errors::IntoNapiResult;
use crate::session::JsSession;
use crate::storage::JsStorage;

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
    pub async fn create(storage: &JsStorage) -> napi::Result<JsRepository> {
        let repo = Repository::create(None, Arc::clone(&storage.0), HashMap::new(), None)
            .await
            .map_napi_err()?;
        Ok(JsRepository(Arc::new(RwLock::new(repo))))
    }

    #[napi(factory)]
    pub async fn open(storage: &JsStorage) -> napi::Result<JsRepository> {
        let repo = Repository::open(None, Arc::clone(&storage.0), HashMap::new())
            .await
            .map_napi_err()?;
        Ok(JsRepository(Arc::new(RwLock::new(repo))))
    }

    #[napi(factory)]
    pub async fn open_or_create(storage: &JsStorage) -> napi::Result<JsRepository> {
        let repo = Repository::open_or_create(
            None,
            Arc::clone(&storage.0),
            HashMap::new(),
            None,
        )
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
}
