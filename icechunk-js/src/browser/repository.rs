use std::collections::HashMap;
use std::sync::Arc;

use futures::TryStreamExt as _;
use icechunk::repository::Repository;
use icechunk::storage::Storage;
use js_sys::{Function, Promise};
use send_wrapper::SendWrapper;
use tokio::sync::RwLock;
use wasm_bindgen::prelude::*;

use super::virtual_chunks::JsHttpVirtualChunkFetcher;
use super::{
    IntoJsResult as _, JsResult, from_js, from_js_optional, js_error, read_now, spawn,
    spawn_void, to_js, to_js_nullable,
};
use crate::config::{JsRepositoryConfig, JsStorageSettings};
use crate::repository::{
    JsCredentials, JsDiff, JsDiffOptions, JsFeatureFlag, JsManifestFileInfo,
    JsRepository, JsSnapshotInfo, JsVersionOptions, ReadonlySessionOptions,
    convert_config, convert_credentials, convert_spec_version, diff_versions,
    parse_snapshot_id, readonly_session_version, version_options_to_version_info,
};
use crate::session::JsSession;
use crate::storage::JsStorage;

fn repository(repo: Repository) -> JsValue {
    JsRepository(Arc::new(RwLock::new(repo))).into()
}

fn session(session: icechunk::session::Session) -> JsValue {
    JsSession::browser(Arc::new(RwLock::new(session))).into()
}

fn credentials(
    value: JsValue,
) -> JsResult<HashMap<String, Option<icechunk::config::Credentials>>> {
    let creds: Option<HashMap<String, Option<JsCredentials>>> = from_js_optional(value)?;
    convert_credentials(creds).map_err(js_error)
}

fn config(value: JsValue) -> JsResult<Option<icechunk::config::RepositoryConfig>> {
    convert_config(from_js_optional::<JsRepositoryConfig>(value)?).map_err(js_error)
}

#[wasm_bindgen(js_class = Repository)]
impl JsRepository {
    /// Register an HTTP virtual-chunk callback before creating read sessions.
    /// Callbacks use the error-first convention, like Storage.newCustom.
    #[wasm_bindgen(js_name = setHttpVirtualChunkFetcher)]
    pub fn set_http_virtual_chunk_fetcher(&self, callback: Function) -> Promise {
        let repo = Arc::clone(&self.0);
        let fetcher = Arc::new(JsHttpVirtualChunkFetcher(SendWrapper::new(callback)));
        spawn_void(async move {
            #[expect(
                deprecated,
                reason = "the JS API is not deprecated; it uses the core shim"
            )]
            repo.write().await.set_http_virtual_chunk_fetcher(fetcher);
            Ok(())
        })
    }

    /// Read the virtual chunks of the container with `urlPrefix` from `storage`.
    /// See `index.d.ts`.
    #[wasm_bindgen(js_name = setVirtualChunkStorage)]
    pub fn set_virtual_chunk_storage(
        &self,
        url_prefix: String,
        storage: &JsStorage,
    ) -> Promise {
        let repo = Arc::clone(&self.0);
        let storage = Arc::clone(&storage.0);
        spawn_void(async move {
            repo.write().await.set_virtual_chunk_storage(&url_prefix, storage).js()
        })
    }

    pub fn create(
        storage: &JsStorage,
        config: JsValue,
        spec_version: Option<u32>,
        authorize_virtual_chunk_access: JsValue,
        check_clean_root: Option<bool>,
    ) -> Promise {
        spawn(create_or_open(
            Arc::clone(&storage.0),
            config,
            spec_version,
            authorize_virtual_chunk_access,
            check_clean_root,
            false,
        ))
    }

    pub fn open(
        storage: &JsStorage,
        config: JsValue,
        authorize_virtual_chunk_access: JsValue,
    ) -> Promise {
        let storage = Arc::clone(&storage.0);
        spawn(async move {
            let builder = Repository::open(storage).authorize_virtual_chunk_access(
                credentials(authorize_virtual_chunk_access)?,
            );
            let builder = match self::config(config)? {
                Some(c) => builder.config(c),
                None => builder,
            };
            Ok(repository(builder.execute().await.js()?))
        })
    }

    #[wasm_bindgen(js_name = openOrCreate)]
    pub fn open_or_create(
        storage: &JsStorage,
        config: JsValue,
        spec_version: Option<u32>,
        authorize_virtual_chunk_access: JsValue,
        check_clean_root: Option<bool>,
    ) -> Promise {
        spawn(create_or_open(
            Arc::clone(&storage.0),
            config,
            spec_version,
            authorize_virtual_chunk_access,
            check_clean_root,
            true,
        ))
    }

    pub fn exists(storage: &JsStorage, storage_settings: JsValue) -> Promise {
        let storage = Arc::clone(&storage.0);
        spawn(async move {
            let settings = from_js_optional::<JsStorageSettings>(storage_settings)?;
            let exists =
                Repository::exists(storage, settings.map(Into::into)).await.js()?;
            Ok(exists.into())
        })
    }

    #[wasm_bindgen(js_name = readonlySession)]
    pub fn readonly_session(&self, options: JsValue) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            let options = from_js_optional::<ReadonlySessionOptions>(options)?;
            let version = readonly_session_version(options).map_err(js_error)?;
            Ok(session(repo.read().await.readonly_session(&version).await.js()?))
        })
    }

    #[wasm_bindgen(js_name = writableSession)]
    pub fn writable_session(&self, branch: String) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            Ok(session(repo.read().await.writable_session(&branch).await.js()?))
        })
    }

    #[wasm_bindgen(js_name = listBranches)]
    pub fn list_branches(&self) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            let branches: Vec<String> =
                repo.read().await.list_branches().await.js()?.into_iter().collect();
            to_js(&branches)
        })
    }

    #[wasm_bindgen(js_name = createBranch)]
    pub fn create_branch(&self, name: String, snapshot_id: String) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn_void(async move {
            let id = parse_snapshot_id(&snapshot_id).map_err(js_error)?;
            repo.read().await.create_branch(&name, &id).await.js()
        })
    }

    #[wasm_bindgen(js_name = listTags)]
    pub fn list_tags(&self) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            let tags: Vec<String> =
                repo.read().await.list_tags().await.js()?.into_iter().collect();
            to_js(&tags)
        })
    }

    #[wasm_bindgen(js_name = createTag)]
    pub fn create_tag(&self, name: String, snapshot_id: String) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn_void(async move {
            let id = parse_snapshot_id(&snapshot_id).map_err(js_error)?;
            repo.read().await.create_tag(&name, &id).await.js()
        })
    }

    #[wasm_bindgen(js_name = lookupManifestFiles)]
    pub fn lookup_manifest_files(&self, snapshot_id: String) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            let id = parse_snapshot_id(&snapshot_id).map_err(js_error)?;
            let files: Vec<JsManifestFileInfo> = repo
                .read()
                .await
                .lookup_manifest_files(&id)
                .await
                .js()?
                .map(Into::into)
                .collect();
            to_js(&files)
        })
    }

    #[wasm_bindgen(js_name = lookupBranch)]
    pub fn lookup_branch(&self, branch: String) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            Ok(repo.read().await.lookup_branch(&branch).await.js()?.to_string().into())
        })
    }

    #[wasm_bindgen(js_name = resetBranch)]
    pub fn reset_branch(
        &self,
        branch: String,
        to_snapshot_id: String,
        from_snapshot_id: Option<String>,
    ) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn_void(async move {
            let to_id = parse_snapshot_id(&to_snapshot_id).map_err(js_error)?;
            let from_id = from_snapshot_id
                .map(|s| parse_snapshot_id(&s))
                .transpose()
                .map_err(js_error)?;
            repo.read().await.reset_branch(&branch, &to_id, from_id.as_ref()).await.js()
        })
    }

    #[wasm_bindgen(js_name = deleteBranch)]
    pub fn delete_branch(&self, branch: String) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn_void(async move { repo.read().await.delete_branch(&branch).await.js() })
    }

    #[wasm_bindgen(js_name = lookupTag)]
    pub fn lookup_tag(&self, tag: String) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            Ok(repo.read().await.lookup_tag(&tag).await.js()?.to_string().into())
        })
    }

    #[wasm_bindgen(js_name = deleteTag)]
    pub fn delete_tag(&self, tag: String) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn_void(async move { repo.read().await.delete_tag(&tag).await.js() })
    }

    #[wasm_bindgen(js_name = lookupSnapshot)]
    pub fn lookup_snapshot(&self, snapshot_id: String) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            let id = parse_snapshot_id(&snapshot_id).map_err(js_error)?;
            let info = repo.read().await.lookup_snapshot(&id).await.js()?;
            to_js(&JsSnapshotInfo::from(info))
        })
    }

    #[wasm_bindgen(js_name = fetchSpecVersion)]
    pub fn fetch_spec_version(storage: &JsStorage, storage_settings: JsValue) -> Promise {
        let storage = Arc::clone(&storage.0);
        spawn(async move {
            let settings = from_js_optional::<JsStorageSettings>(storage_settings)?;
            let detected =
                Repository::fetch_spec_version(storage, settings.map(Into::into))
                    .await
                    .js()?;
            to_js_nullable(detected.map(|d| d.spec_version() as u32))
        })
    }

    #[wasm_bindgen(js_name = fetchConfig)]
    pub fn fetch_config(storage: &JsStorage) -> Promise {
        let storage = Arc::clone(&storage.0);
        spawn(async move {
            let config = Repository::fetch_config(storage).await.js()?;
            to_js_nullable(config.map(|(config, _)| JsRepositoryConfig::from(config)))
        })
    }

    #[wasm_bindgen(js_name = saveConfig)]
    pub fn save_config(&self) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn_void(async move {
            repo.read().await.save_config().await.js()?;
            Ok(())
        })
    }

    #[wasm_bindgen(getter)]
    pub fn config(&self) -> JsResult<JsValue> {
        to_js(&JsRepositoryConfig::from(read_now(&self.0)?.config().clone()))
    }

    #[wasm_bindgen(getter, js_name = specVersion)]
    pub fn spec_version(&self) -> JsResult<u32> {
        Ok(read_now(&self.0)?.spec_version() as u32)
    }

    pub fn diff(&self, options: JsValue) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            let (from, to) =
                diff_versions(from_js::<JsDiffOptions>(options)?).map_err(js_error)?;
            let diff = repo.read().await.diff(&from, &to).await.js()?;
            to_js(&JsDiff::from(diff))
        })
    }

    #[wasm_bindgen(js_name = featureFlags)]
    pub fn feature_flags(&self) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            let flags: Vec<JsFeatureFlag> =
                repo.read().await.feature_flags().await.js()?.map(Into::into).collect();
            to_js(&flags)
        })
    }

    #[wasm_bindgen(js_name = enabledFeatureFlags)]
    pub fn enabled_feature_flags(&self) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            let flags: Vec<JsFeatureFlag> = repo
                .read()
                .await
                .enabled_feature_flags()
                .await
                .js()?
                .map(Into::into)
                .collect();
            to_js(&flags)
        })
    }

    #[wasm_bindgen(js_name = disabledFeatureFlags)]
    pub fn disabled_feature_flags(&self) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            let flags: Vec<JsFeatureFlag> = repo
                .read()
                .await
                .disabled_feature_flags()
                .await
                .js()?
                .map(Into::into)
                .collect();
            to_js(&flags)
        })
    }

    #[wasm_bindgen(js_name = setFeatureFlag)]
    pub fn set_feature_flag(&self, name: String, setting: Option<bool>) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn_void(async move {
            repo.read().await.set_feature_flag(&name, setting).await.js()
        })
    }

    #[wasm_bindgen(js_name = rearrangeSession)]
    pub fn rearrange_session(&self, branch: String) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            Ok(session(repo.read().await.rearrange_session(&branch).await.js()?))
        })
    }

    pub fn ancestry(&self, options: JsValue) -> Promise {
        let repo = Arc::clone(&self.0);
        spawn(async move {
            let options = from_js::<JsVersionOptions>(options)?;
            let version = version_options_to_version_info(options).map_err(js_error)?;
            let repo = repo.read().await;
            let snapshots: Vec<JsSnapshotInfo> = repo
                .ancestry(&version)
                .await
                .js()?
                .map_ok(JsSnapshotInfo::from)
                .try_collect()
                .await
                .js()?;
            to_js(&snapshots)
        })
    }
}

/// `create`, or `openOrCreate` if `open_existing`.
async fn create_or_open(
    storage: Arc<dyn Storage + Send + Sync>,
    config: JsValue,
    spec_version: Option<u32>,
    authorize_virtual_chunk_access: JsValue,
    check_clean_root: Option<bool>,
    open_existing: bool,
) -> JsResult<JsValue> {
    let config = self::config(config)?;
    let version = convert_spec_version(spec_version).map_err(js_error)?;
    let creds = credentials(authorize_virtual_chunk_access)?;
    let check_clean_root = check_clean_root.unwrap_or(true);
    macro_rules! build {
        ($builder:expr) => {{
            let builder = $builder
                .authorize_virtual_chunk_access(creds)
                .check_clean_root(check_clean_root);
            let builder = match config {
                Some(c) => builder.config(c),
                None => builder,
            };
            let builder = match version {
                Some(v) => builder.spec_version(v),
                None => builder,
            };
            builder.execute().await.js()?
        }};
    }
    let repo = if open_existing {
        build!(Repository::open_or_create(storage))
    } else {
        build!(Repository::create(storage))
    };
    Ok(repository(repo))
}
