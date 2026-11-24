use std::{fmt, ops::Range, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt as _, stream::BoxStream};
use icechunk::storage::{
    DeleteObjectsResult, GetModifiedResult, ListInfo, Storage, StorageContext,
    StorageInfo, StorageResult, VersionInfo, VersionedUpdateResult,
};
use js_sys::{Array, Function, Promise, Reflect};
use send_wrapper::SendWrapper;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use super::{IntoJsResult as _, JsResult, from_js, js_error, js_message, spawn, to_js};
use crate::storage::{
    JsStorage, JsStorageCopyObjectArgs, JsStorageDeleteBatchArgs,
    JsStorageDeleteObjectsResult, JsStorageGetModifiedResult,
    JsStorageGetObjectConditionalArgs, JsStorageGetObjectRangeArgs,
    JsStorageGetObjectResponse, JsStoragePutObjectArgs, JsStorageVersionedUpdateResult,
    ObjectStream, js_storage_error, other_storage_error,
};

#[wasm_bindgen(js_class = Storage)]
impl JsStorage {
    #[wasm_bindgen(js_name = newInMemory)]
    pub fn new_in_memory() -> Promise {
        spawn(async {
            let storage = icechunk::storage::new_in_memory_storage().await.js()?;
            Ok(JsStorage(storage).into())
        })
    }

    /// Create a Storage backed by a JS object implementing the StorageBackend
    /// interface. Its methods are called error-first, `(null, args)`, like the
    /// napi build's, so the same backend works in both.
    #[wasm_bindgen(js_name = newCustom)]
    pub fn new_custom(backend: JsValue) -> JsResult<JsStorage> {
        let method = |name: &str| -> JsResult<SendWrapper<Function>> {
            Reflect::get(&backend, &JsValue::from_str(name))?
                .dyn_into::<Function>()
                .map(SendWrapper::new)
                .map_err(|_| {
                    js_error(format!(
                        "Storage.newCustom: backend.{name} must be a function"
                    ))
                })
        };
        let storage = JsCallbackStorage {
            can_write: method("canWrite")?,
            get_object_range: method("getObjectRange")?,
            put_object: method("putObject")?,
            copy_object: method("copyObject")?,
            list_objects: method("listObjects")?,
            delete_batch: method("deleteBatch")?,
            get_object_last_modified: method("getObjectLastModified")?,
            get_object_conditional: method("getObjectConditional")?,
            backend: SendWrapper::new(backend),
        };
        Ok(JsStorage(Arc::new(storage)))
    }
}

/// Storage backend that delegates all operations to JS callbacks.
///
/// `Storage` must be `Send + Sync`, and JS values are neither. There are no
/// other threads on `wasm32-unknown-unknown`, so the callbacks are held in
/// `SendWrapper`, which panics instead of misbehaving if that ever changes.
/// The storage's futures need not be `Send` in the browser, so they can await
/// the JS promises directly.
pub(crate) struct JsCallbackStorage {
    /// `this` for the calls
    backend: SendWrapper<JsValue>,
    can_write: SendWrapper<Function>,
    get_object_range: SendWrapper<Function>,
    put_object: SendWrapper<Function>,
    copy_object: SendWrapper<Function>,
    list_objects: SendWrapper<Function>,
    delete_batch: SendWrapper<Function>,
    get_object_last_modified: SendWrapper<Function>,
    get_object_conditional: SendWrapper<Function>,
}

impl JsCallbackStorage {
    /// Call `f(null, arg)` and await the promise it returns.
    async fn call(
        &self,
        f: &Function,
        arg: &JsValue,
        name: &str,
    ) -> StorageResult<JsValue> {
        let returned = f.call2(&self.backend, &JsValue::NULL, arg).map_err(|e| {
            js_storage_error(&format!("JS {name} call failed"), js_message(&e))
        })?;
        JsFuture::from(Promise::resolve(&returned))
            .await
            .map_err(|e| js_storage_error(&format!("JS {name} failed"), js_message(&e)))
    }

    async fn call_with<A: Serialize, R: DeserializeOwned>(
        &self,
        f: &Function,
        args: &A,
        name: &str,
    ) -> StorageResult<R> {
        let args = to_js(args).map_err(|e| other_storage_error(js_message(&e)))?;
        let result = self.call(f, &args, name).await?;
        from_js(result).map_err(|e| {
            other_storage_error(format!(
                "JS {name} returned an invalid value: {}",
                js_message(&e)
            ))
        })
    }
}

/// A JS `Date`, an RFC 3339 string, or milliseconds since the epoch.
fn js_date(value: &JsValue) -> Option<DateTime<Utc>> {
    if let Some(date) = value.dyn_ref::<js_sys::Date>() {
        DateTime::from_timestamp_millis(date.get_time() as i64)
    } else if let Some(s) = value.as_string() {
        DateTime::parse_from_rfc3339(&s).ok().map(|d| d.to_utc())
    } else {
        value.as_f64().and_then(|ms| DateTime::from_timestamp_millis(ms as i64))
    }
}

fn list_info(item: &JsValue) -> StorageResult<ListInfo<String>> {
    let field =
        |name: &str| Reflect::get(item, &JsValue::from_str(name)).unwrap_or_default();
    let invalid = |what: &str| other_storage_error(format!("JS listObjects: {what}"));
    Ok(ListInfo {
        id: field("id").as_string().ok_or_else(|| invalid("id must be a string"))?,
        created_at: js_date(&field("createdAt"))
            .ok_or_else(|| invalid("createdAt must be a Date"))?,
        size_bytes: field("sizeBytes")
            .as_f64()
            .ok_or_else(|| invalid("sizeBytes must be a number"))?
            as u64,
    })
}

impl fmt::Debug for JsCallbackStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsCallbackStorage").finish()
    }
}

impl fmt::Display for JsCallbackStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JsCallbackStorage")
    }
}

impl Serialize for JsCallbackStorage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Helper {}
        Helper {}.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for JsCallbackStorage {
    fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "JsCallbackStorage cannot be deserialized: JS callbacks are not serializable",
        ))
    }
}

impl icechunk_storage::sealed::Sealed for JsCallbackStorage {}

#[icechunk_storage::maybe_send_async_trait]
#[typetag::serde(name = "js_callback_storage")]
impl Storage for JsCallbackStorage {
    fn storage_info(&self) -> StorageInfo {
        StorageInfo { backend_type: "JsCallback", fields: vec![] }
    }

    async fn can_write(&self) -> StorageResult<bool> {
        self.call(&self.can_write, &JsValue::UNDEFINED, "canWrite")
            .await?
            .as_bool()
            .ok_or_else(|| other_storage_error("JS canWrite must return a boolean"))
    }

    async fn get_object_range(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(ObjectStream, VersionInfo)> {
        let args = JsStorageGetObjectRangeArgs::new(path, range);
        let response: JsStorageGetObjectResponse =
            self.call_with(&self.get_object_range, &args, "getObjectRange").await?;
        Ok(response.into_rust())
    }

    async fn put_object(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult> {
        let args = JsStoragePutObjectArgs::new(
            path,
            bytes,
            content_type,
            metadata,
            previous_version,
        );
        let result: JsStorageVersionedUpdateResult =
            self.call_with(&self.put_object, &args, "putObject").await?;
        result.into_rust()
    }

    async fn copy_object(
        &self,
        _ctx: &StorageContext<'_>,
        from: &str,
        to: &str,
        content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        let args = JsStorageCopyObjectArgs::new(from, to, content_type, version);
        let result: JsStorageVersionedUpdateResult =
            self.call_with(&self.copy_object, &args, "copyObject").await?;
        result.into_rust()
    }

    async fn list_objects<'a>(
        &'a self,
        _ctx: &StorageContext<'_>,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let items = self
            .call(&self.list_objects, &JsValue::from_str(prefix), "listObjects")
            .await?
            .dyn_into::<Array>()
            .map_err(|_| other_storage_error("JS listObjects must return an array"))?;
        let items: Vec<_> = items.iter().map(|item| list_info(&item)).collect();
        Ok(futures::stream::iter(items).boxed())
    }

    async fn delete_batch(
        &self,
        _ctx: &StorageContext<'_>,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        let args = JsStorageDeleteBatchArgs::new(prefix, batch);
        let result: JsStorageDeleteObjectsResult =
            self.call_with(&self.delete_batch, &args, "deleteBatch").await?;
        Ok(DeleteObjectsResult {
            deleted_objects: result.deleted_objects as u64,
            deleted_bytes: result.deleted_bytes as u64,
        })
    }

    async fn get_object_last_modified(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
    ) -> StorageResult<DateTime<Utc>> {
        let value = self
            .call(
                &self.get_object_last_modified,
                &JsValue::from_str(path),
                "getObjectLastModified",
            )
            .await?;
        js_date(&value).ok_or_else(|| {
            other_storage_error("JS getObjectLastModified must return a Date")
        })
    }

    async fn get_object_conditional(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        let args = JsStorageGetObjectConditionalArgs::new(path, previous_version);
        let result: JsStorageGetModifiedResult = self
            .call_with(&self.get_object_conditional, &args, "getObjectConditional")
            .await?;
        result.into_rust()
    }
}
