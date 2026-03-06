use std::sync::Arc;

use icechunk::session::Session;
use napi_derive::napi;
use tokio::sync::RwLock;

use crate::errors::IntoNapiResult;
use crate::store::JsStore;

#[cfg(not(target_family = "wasm"))]
use futures::TryStreamExt;

#[napi(js_name = "Session")]
pub struct JsSession(pub(crate) Arc<RwLock<Session>>);

impl JsSession {
    pub fn new(session: Session) -> Self {
        JsSession(Arc::new(RwLock::new(session)))
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
        let snapshot_id = session.commit(&message, None).await.map_napi_err()?;
        Ok(snapshot_id.to_string())
    }

    #[napi]
    pub async fn discard_changes(&self) -> napi::Result<()> {
        let mut session = self.0.write().await;
        session.discard_changes().map_napi_err()
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
