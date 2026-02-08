use std::sync::Arc;

use icechunk::Storage;
use napi_derive::napi;

use crate::errors::IntoNapiResult;

#[napi(js_name = "Storage")]
pub struct JsStorage(pub(crate) Arc<dyn Storage + Send + Sync>);

#[napi]
impl JsStorage {
    #[napi(factory)]
    pub async fn new_in_memory() -> napi::Result<JsStorage> {
        let storage = icechunk::storage::new_in_memory_storage().await.map_napi_err()?;
        Ok(JsStorage(storage))
    }
}
