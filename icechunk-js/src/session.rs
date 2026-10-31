use std::ops::Deref as _;
use std::sync::Arc;

use icechunk::format::Path;
use icechunk::session::Session;
use napi_derive::napi;
use tokio::sync::RwLock;

use crate::config::JsRepositoryConfig;
use crate::errors::IntoNapiResult;
use crate::repository::JsDiff;
use crate::store::JsStore;

#[cfg(not(target_family = "wasm"))]
use futures::TryStreamExt;

#[napi(js_name = "Session")]
pub struct JsSession(pub(crate) Arc<RwLock<Session>>);

impl JsSession {
    pub fn new(session: Session) -> Self {
        JsSession(Arc::new(RwLock::new(session)))
    }

    pub fn new_from_arc(session: Arc<RwLock<Session>>) -> Self {
        JsSession(session)
    }
}

#[napi]
impl JsSession {
    #[napi(getter)]
    pub fn read_only(&self) -> bool {
        let session = self.0.blocking_read();
        session.read_only()
    }

    #[napi(getter)]
    pub fn snapshot_id(&self) -> String {
        let session = self.0.blocking_read();
        session.snapshot_id().to_string()
    }

    #[napi(getter)]
    pub fn branch(&self) -> Option<String> {
        let session = self.0.blocking_read();
        session.branch().map(|s| s.to_string())
    }

    #[napi(getter)]
    pub fn has_uncommitted_changes(&self) -> bool {
        let session = self.0.blocking_read();
        session.has_uncommitted_changes()
    }

    #[napi(getter)]
    pub fn store(&self) -> JsStore {
        let store =
            icechunk::store::Store::from_session_and_config(Arc::clone(&self.0), 10);
        JsStore(Arc::new(store))
    }

    #[napi]
    pub async fn commit(&self, message: String) -> napi::Result<String> {
        let mut session = self.0.write().await;
        let snapshot_id = session.commit(&message).execute().await.map_napi_err()?;
        Ok(snapshot_id.to_string())
    }

    #[napi]
    pub async fn discard_changes(&self) -> napi::Result<()> {
        let mut session = self.0.write().await;
        session.discard_changes().map_napi_err()
    }

    #[napi(getter)]
    pub fn is_fork(&self) -> bool {
        self.0.blocking_read().is_fork()
    }

    #[napi(getter)]
    pub fn mode(&self) -> String {
        match self.0.blocking_read().mode() {
            icechunk::session::SessionMode::Readonly => "readonly".to_string(),
            icechunk::session::SessionMode::Writable => "writable".to_string(),
            icechunk::session::SessionMode::Rearrange => "rearrange".to_string(),
        }
    }

    #[napi(getter)]
    pub fn config(&self) -> JsRepositoryConfig {
        self.0.blocking_read().config().clone().into()
    }

    #[napi]
    pub async fn status(&self) -> napi::Result<JsDiff> {
        let session = self.0.read().await;
        let diff = session.status().await.map_napi_err()?;
        Ok(diff.into())
    }

    #[napi]
    pub async fn move_node(
        &self,
        from_path: String,
        to_path: String,
    ) -> napi::Result<()> {
        let from = Path::try_from(from_path.as_str()).map_napi_err()?;
        let to = Path::try_from(to_path.as_str()).map_napi_err()?;
        let mut session = self.0.write().await;
        session.move_node(from, to).await.map_napi_err()
    }

    #[napi]
    pub async fn get_node_id(&self, path: String) -> napi::Result<String> {
        let path = Path::try_from(path.as_str()).map_napi_err()?;
        let session = self.0.read().await;
        let node = session.get_node(&path).await.map_napi_err()?;
        Ok(node.id.to_string())
    }

    #[napi]
    pub async fn merge(&self, other: &JsSession) -> napi::Result<()> {
        let other_session = other.0.read().await.deref().clone();
        let mut session = self.0.write().await;
        session.merge(other_session).await.map_napi_err()
    }

    #[napi]
    pub async fn amend(&self, message: String) -> napi::Result<String> {
        let mut session = self.0.write().await;
        let snapshot_id =
            session.commit(&message).amend().execute().await.map_napi_err()?;
        Ok(snapshot_id.to_string())
    }

    #[napi]
    pub async fn flush(&self, message: String) -> napi::Result<String> {
        let mut session = self.0.write().await;
        let snapshot_id =
            session.commit(&message).anonymous().execute().await.map_napi_err()?;
        Ok(snapshot_id.to_string())
    }

    #[napi]
    pub async fn rebase(&self) -> napi::Result<()> {
        let solver = icechunk::conflicts::detector::ConflictDetector;
        let mut session = self.0.write().await;
        session.rebase(&solver).await.map_napi_err()
    }
}

#[cfg(not(target_family = "wasm"))]
#[napi]
impl JsSession {
    /// Get all virtual chunk locations referenced by this session
    #[napi]
    pub async fn all_virtual_chunk_locations(&self) -> napi::Result<Vec<String>> {
        let session = self.0.read().await;
        let locations = session
            .all_virtual_chunk_locations()
            .await
            .map_napi_err()?
            .try_collect()
            .await
            .map_napi_err()?;
        Ok(locations)
    }
}
