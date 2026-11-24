use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Weak};

use icechunk::config::RepositoryConfig;
use icechunk::format::Path;
use icechunk::session::Session;
use js_sys::Promise;
use tokio::sync::RwLock;
use wasm_bindgen::prelude::*;

use super::{IntoJsResult as _, JsResult, js_error, spawn, to_js};
use crate::config::JsRepositoryConfig;
use crate::repository::JsDiff;
use crate::session::{JsSession, mode_name};
use crate::store::JsStore;

fn path(path: &str) -> JsResult<Path> {
    Path::try_from(path).js()
}

/// What the synchronous getters report.
struct Summary {
    read_only: bool,
    snapshot_id: String,
    branch: Option<String>,
    has_uncommitted_changes: bool,
    is_fork: bool,
    mode: String,
    config: RepositoryConfig,
}

impl From<&Session> for Summary {
    fn from(session: &Session) -> Self {
        Summary {
            read_only: session.read_only(),
            snapshot_id: session.snapshot_id().to_string(),
            branch: session.branch().map(str::to_string),
            has_uncommitted_changes: session.has_uncommitted_changes(),
            is_fork: session.is_fork(),
            mode: mode_name(session.mode()),
            config: session.config().clone(),
        }
    }
}

type SharedSession = Arc<RwLock<Session>>;
type LastSeen = Vec<(Weak<RwLock<Session>>, Rc<Summary>)>;

thread_local! {
    /// The last state the getters saw for each live session. In napi a getter
    /// blocks until a commit releases the session; here it cannot, so while an
    /// operation holds the lock it reports this instead of throwing (UIs read
    /// getters while rendering).
    static LAST_SEEN: RefCell<LastSeen> =
        const { RefCell::new(Vec::new()) };
}

fn remember(lock: &SharedSession, summary: Rc<Summary>) {
    LAST_SEEN.with_borrow_mut(|seen| {
        seen.retain(|(session, _)| session.strong_count() > 0);
        match seen
            .iter_mut()
            .find(|(session, _)| std::ptr::eq(session.as_ptr(), Arc::as_ptr(lock)))
        {
            Some(entry) => entry.1 = summary,
            None => seen.push((Arc::downgrade(lock), summary)),
        }
    });
}

/// Record the session's state now, so getters have something to report if an
/// operation is holding it later. Call it whenever a session is handed to JS.
pub(crate) fn remember_now(lock: &SharedSession) {
    if let Ok(session) = lock.try_read() {
        remember(lock, Rc::new(Summary::from(&*session)));
    }
}

fn summary(lock: &SharedSession) -> JsResult<Rc<Summary>> {
    if let Ok(session) = lock.try_read() {
        let summary = Rc::new(Summary::from(&*session));
        remember(lock, Rc::clone(&summary));
        return Ok(summary);
    }
    LAST_SEEN
        .with_borrow(|seen| {
            seen.iter()
                .find(|(session, _)| std::ptr::eq(session.as_ptr(), Arc::as_ptr(lock)))
                .map(|(_, summary)| Rc::clone(summary))
        })
        .ok_or_else(|| {
            js_error("busy: another operation is using this session; await it first")
        })
}

impl JsSession {
    /// A JS handle for `session`, with its state recorded for the getters.
    pub(crate) fn browser(session: SharedSession) -> Self {
        remember_now(&session);
        JsSession(session)
    }
}

#[wasm_bindgen(js_class = Session)]
impl JsSession {
    #[wasm_bindgen(getter, js_name = readOnly)]
    pub fn read_only(&self) -> JsResult<bool> {
        Ok(summary(&self.0)?.read_only)
    }

    #[wasm_bindgen(getter, js_name = snapshotId)]
    pub fn snapshot_id(&self) -> JsResult<String> {
        Ok(summary(&self.0)?.snapshot_id.clone())
    }

    /// The branch, or `null` for a session not on a branch.
    #[wasm_bindgen(getter)]
    pub fn branch(&self) -> JsResult<JsValue> {
        Ok(summary(&self.0)?.branch.as_deref().map_or(JsValue::NULL, JsValue::from_str))
    }

    #[wasm_bindgen(getter, js_name = hasUncommittedChanges)]
    pub fn has_uncommitted_changes(&self) -> JsResult<bool> {
        Ok(summary(&self.0)?.has_uncommitted_changes)
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
        Ok(summary(&self.0)?.is_fork)
    }

    #[wasm_bindgen(getter)]
    pub fn mode(&self) -> JsResult<String> {
        Ok(summary(&self.0)?.mode.clone())
    }

    #[wasm_bindgen(getter)]
    pub fn config(&self) -> JsResult<JsValue> {
        to_js(&JsRepositoryConfig::from(summary(&self.0)?.config.clone()))
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
