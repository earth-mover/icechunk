use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt as _;
use icechunk::format::ByteRange;
use icechunk::store::{Store, StoreErrorKind};
use js_sys::{Promise, Uint8Array};
use wasm_bindgen::prelude::*;

use super::{IntoJsResult as _, JsResult, from_js, spawn, to_js};
use crate::session::JsSession;
use crate::store::{JsRangeQuery, JsStore, normalize_key};

/// The value at `key`, or `null` if there is none.
async fn get(store: &Store, key: &str, range: &ByteRange) -> JsResult<JsValue> {
    match store.get(normalize_key(key), range).await {
        Ok(bytes) => Ok(Uint8Array::from(bytes.as_ref()).into()),
        Err(e) if matches!(e.kind, StoreErrorKind::NotFound(_)) => Ok(JsValue::NULL),
        Err(e) => Err(e).js(),
    }
}

#[wasm_bindgen(js_class = Store)]
impl JsStore {
    pub fn get(&self, key: String) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move { get(&store, &key, &ByteRange::ALL).await })
    }

    /// Fetch a byte range from a key, given zarrita's RangeQuery:
    /// `{ offset, length }` or `{ suffixLength }`.
    #[wasm_bindgen(js_name = getRange)]
    pub fn get_range(&self, key: String, range: JsValue) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move {
            let range = ByteRange::from(from_js::<JsRangeQuery>(range)?);
            get(&store, &key, &range).await
        })
    }

    pub fn set(&self, key: String, value: Vec<u8>) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move {
            store.set(normalize_key(&key), Bytes::from(value)).await.js()?;
            Ok(JsValue::UNDEFINED)
        })
    }

    pub fn exists(&self, key: String) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move { Ok(store.exists(normalize_key(&key)).await.js()?.into()) })
    }

    pub fn delete(&self, key: String) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move {
            store.delete(normalize_key(&key)).await.js()?;
            Ok(JsValue::UNDEFINED)
        })
    }

    pub fn list(&self) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move {
            let keys: Vec<String> = store.list().await.js()?.try_collect().await.js()?;
            to_js(&keys)
        })
    }

    #[wasm_bindgen(js_name = listPrefix)]
    pub fn list_prefix(&self, prefix: String) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move {
            let keys: Vec<String> = store
                .list_prefix(normalize_key(&prefix))
                .await
                .js()?
                .try_collect()
                .await
                .js()?;
            to_js(&keys)
        })
    }

    #[wasm_bindgen(js_name = listDir)]
    pub fn list_dir(&self, prefix: String) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move {
            let keys: Vec<String> = store
                .list_dir(normalize_key(&prefix))
                .await
                .js()?
                .try_collect()
                .await
                .js()?;
            to_js(&keys)
        })
    }

    #[wasm_bindgen(getter, js_name = supportsWrites)]
    pub fn supports_writes(&self) -> bool {
        true
    }

    #[wasm_bindgen(getter, js_name = supportsDeletes)]
    pub fn supports_deletes(&self) -> bool {
        true
    }

    #[wasm_bindgen(getter, js_name = supportsListing)]
    pub fn supports_listing(&self) -> bool {
        true
    }

    /// A promise, like the napi build's async getter.
    #[wasm_bindgen(getter, js_name = readOnly)]
    pub fn read_only(&self) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move { Ok(store.read_only().await.into()) })
    }

    #[wasm_bindgen(getter)]
    pub fn session(&self) -> JsSession {
        JsSession::new_from_arc(self.0.session())
    }

    #[wasm_bindgen(js_name = isEmpty)]
    pub fn is_empty(&self, prefix: String) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(
            async move { Ok(store.is_empty(normalize_key(&prefix)).await.js()?.into()) },
        )
    }

    pub fn clear(&self) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move {
            store.clear().await.js()?;
            Ok(JsValue::UNDEFINED)
        })
    }

    #[wasm_bindgen(js_name = setIfNotExists)]
    pub fn set_if_not_exists(&self, key: String, value: Vec<u8>) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move {
            store
                .set_if_not_exists(normalize_key(&key), Bytes::from(value))
                .await
                .js()?;
            Ok(JsValue::UNDEFINED)
        })
    }

    #[wasm_bindgen(js_name = deleteDir)]
    pub fn delete_dir(&self, prefix: String) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move {
            store.delete_dir(normalize_key(&prefix)).await.js()?;
            Ok(JsValue::UNDEFINED)
        })
    }

    pub fn getsize(&self, key: String) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move {
            Ok((store.getsize(normalize_key(&key)).await.js()? as f64).into())
        })
    }

    #[wasm_bindgen(js_name = getsizePrefix)]
    pub fn getsize_prefix(&self, prefix: String) -> Promise {
        let store = Arc::clone(&self.0);
        spawn(async move {
            Ok((store.getsize_prefix(normalize_key(&prefix)).await.js()? as f64).into())
        })
    }
}
