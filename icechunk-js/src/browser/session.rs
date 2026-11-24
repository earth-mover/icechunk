use std::sync::Arc;

use icechunk::format::Path;
use js_sys::Promise;
use wasm_bindgen::prelude::*;

use super::{IntoJsResult as _, JsResult, read_now, spawn, to_js};
use crate::config::JsRepositoryConfig;
use crate::repository::JsDiff;
use crate::session::{JsSession, mode_name};
use crate::store::JsStore;

fn path(path: &str) -> JsResult<Path> {
    Path::try_from(path).js()
}

#[wasm_bindgen(js_class = Session)]
impl JsSession {
    #[wasm_bindgen(getter, js_name = readOnly)]
    pub fn read_only(&self) -> JsResult<bool> {
        Ok(read_now(&self.0)?.read_only())
    }

    #[wasm_bindgen(getter, js_name = snapshotId)]
    pub fn snapshot_id(&self) -> JsResult<String> {
        Ok(read_now(&self.0)?.snapshot_id().to_string())
    }

    /// The branch, or `null` for a session not on a branch.
    #[wasm_bindgen(getter)]
    pub fn branch(&self) -> JsResult<JsValue> {
        Ok(read_now(&self.0)?.branch().map_or(JsValue::NULL, JsValue::from_str))
    }

    #[wasm_bindgen(getter, js_name = hasUncommittedChanges)]
    pub fn has_uncommitted_changes(&self) -> JsResult<bool> {
        Ok(read_now(&self.0)?.has_uncommitted_changes())
    }

    #[wasm_bindgen(getter)]
    pub fn store(&self) -> JsStore {
        let store =
            icechunk::store::Store::from_session_and_config(Arc::clone(&self.0), 10);
        JsStore(Arc::new(store))
    }

    pub fn commit(&self, message: String) -> Promise {
        let session = Arc::clone(&self.0);
        spawn(async move {
            let id = session.write().await.commit(&message).execute().await.js()?;
            Ok(id.to_string().into())
        })
    }

    #[wasm_bindgen(js_name = discardChanges)]
    pub fn discard_changes(&self) -> Promise {
        let session = Arc::clone(&self.0);
        spawn(async move {
            session.write().await.discard_changes().js()?;
            Ok(JsValue::UNDEFINED)
        })
    }

    #[wasm_bindgen(getter, js_name = isFork)]
    pub fn is_fork(&self) -> JsResult<bool> {
        Ok(read_now(&self.0)?.is_fork())
    }

    #[wasm_bindgen(getter)]
    pub fn mode(&self) -> JsResult<String> {
        Ok(mode_name(read_now(&self.0)?.mode()))
    }

    #[wasm_bindgen(getter)]
    pub fn config(&self) -> JsResult<JsValue> {
        to_js(&JsRepositoryConfig::from(read_now(&self.0)?.config().clone()))
    }

    pub fn status(&self) -> Promise {
        let session = Arc::clone(&self.0);
        spawn(async move {
            let diff = session.read().await.status().await.js()?;
            to_js(&JsDiff::from(diff))
        })
    }

    #[wasm_bindgen(js_name = moveNode)]
    pub fn move_node(&self, from_path: String, to_path: String) -> Promise {
        let session = Arc::clone(&self.0);
        spawn(async move {
            let (from, to) = (path(&from_path)?, path(&to_path)?);
            session.write().await.move_node(from, to).await.js()?;
            Ok(JsValue::UNDEFINED)
        })
    }

    #[wasm_bindgen(js_name = getNodeId)]
    pub fn get_node_id(&self, node_path: String) -> Promise {
        let session = Arc::clone(&self.0);
        spawn(async move {
            let node_path = path(&node_path)?;
            let node = session.read().await.get_node(&node_path).await.js()?;
            Ok(node.id.to_string().into())
        })
    }

    pub fn merge(&self, other: &JsSession) -> Promise {
        let session = Arc::clone(&self.0);
        let other = Arc::clone(&other.0);
        spawn(async move {
            let other = other.read().await.clone();
            session.write().await.merge(other).await.js()?;
            Ok(JsValue::UNDEFINED)
        })
    }

    pub fn amend(&self, message: String) -> Promise {
        let session = Arc::clone(&self.0);
        spawn(async move {
            let id =
                session.write().await.commit(&message).amend().execute().await.js()?;
            Ok(id.to_string().into())
        })
    }

    pub fn flush(&self, message: String) -> Promise {
        let session = Arc::clone(&self.0);
        spawn(async move {
            let id = session
                .write()
                .await
                .commit(&message)
                .anonymous()
                .execute()
                .await
                .js()?;
            Ok(id.to_string().into())
        })
    }

    pub fn rebase(&self) -> Promise {
        let session = Arc::clone(&self.0);
        spawn(async move {
            let solver = icechunk::conflicts::detector::ConflictDetector;
            session.write().await.rebase(&solver).await.js()?;
            Ok(JsValue::UNDEFINED)
        })
    }
}
