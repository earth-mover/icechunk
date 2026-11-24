//! icechunk running on `wasm32-unknown-unknown`, with no Tokio runtime.
//!
//! wasm-bindgen-test runs these under node by default. Everything is driven
//! by the JS event loop, as it would be in a browser.
#![cfg(all(target_family = "wasm", target_os = "unknown"))]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{cell::Cell, rc::Rc, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::TryStreamExt as _;
use icechunk::{
    Repository, Storage, Store, format::ByteRange, new_in_memory_storage,
    repository::VersionInfo, rt,
};
use icechunk_wasm_tests::{JsYieldStorage, yield_to_js};
use tokio::sync::RwLock;
use wasm_bindgen_test::wasm_bindgen_test;

// ---- the runtime shim keeps Tokio's semantics ----

#[wasm_bindgen_test]
async fn rt_spawn_runs_non_send_futures() {
    // `Rc` makes the future !Send; tokio::spawn would reject it
    let hits = Rc::new(Cell::new(0));
    let hits_c = Rc::clone(&hits);
    let handle = rt::spawn(async move {
        yield_to_js().await;
        hits_c.set(hits_c.get() + 1);
        42
    });
    assert_eq!(handle.await.unwrap(), 42);
    assert_eq!(hits.get(), 1);
}

#[wasm_bindgen_test]
async fn rt_dropped_handle_detaches() {
    let done = Rc::new(Cell::new(false));
    let done_c = Rc::clone(&done);
    drop(rt::spawn(async move {
        rt::sleep(Duration::from_millis(5)).await;
        done_c.set(true);
    }));
    rt::sleep(Duration::from_millis(30)).await;
    assert!(done.get(), "a dropped JoinHandle must not cancel the task");
}

#[wasm_bindgen_test]
async fn rt_abort_cancels() {
    let done = Rc::new(Cell::new(false));
    let done_c = Rc::clone(&done);
    let handle = rt::spawn(async move {
        rt::sleep(Duration::from_millis(5)).await;
        done_c.set(true);
    });
    handle.abort();
    let err = handle.await.unwrap_err();
    assert!(err.is_cancelled());
    assert!(!err.is_panic());
    rt::sleep(Duration::from_millis(30)).await;
    assert!(!done.get());
}

#[wasm_bindgen_test]
async fn rt_join_set_progresses_without_being_polled() {
    // tasks must make progress on their own: the listing/walker workers feed a
    // channel the caller drains before it ever calls join_next
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(4);
    let mut set = rt::JoinSet::new();
    for i in 0..3 {
        let tx = tx.clone();
        set.spawn(async move {
            yield_to_js().await;
            tx.send(i).await.unwrap();
        });
    }
    drop(tx);
    let mut got = Vec::new();
    while let Some(i) = rx.recv().await {
        got.push(i);
    }
    got.sort();
    assert_eq!(got, vec![0, 1, 2]);
    let mut joined = 0;
    while let Some(res) = set.join_next().await {
        res.unwrap();
        joined += 1;
    }
    assert_eq!(joined, 3);
}

#[wasm_bindgen_test]
async fn rt_spawn_blocking_runs_inline() {
    assert_eq!(rt::spawn_blocking(|| 7).await.unwrap(), 7);
}

// ---- icechunk end to end, through a !Send storage ----

const GROUP: &[u8] = br#"{"zarr_format":3,"node_type":"group"}"#;
const ARRAY: &[u8] = br#"{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"uint8","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{}}]}"#;

async fn browser_storage() -> Arc<dyn Storage + Send + Sync> {
    Arc::new(JsYieldStorage::new(new_in_memory_storage().await.unwrap()))
}

#[wasm_bindgen_test]
async fn create_write_commit_reopen_read() {
    let storage = browser_storage().await;

    // create + write + commit
    let repo = Repository::create(Arc::clone(&storage)).execute().await.unwrap();
    let session = Arc::new(RwLock::new(repo.writable_session("main").await.unwrap()));
    let store = Store::from_session(Arc::clone(&session)).await;
    store.set("zarr.json", Bytes::from_static(GROUP)).await.unwrap();
    store.set("temps/zarr.json", Bytes::from_static(ARRAY)).await.unwrap();
    // above the 512 byte inline threshold, so this is a real chunk object
    let big = Bytes::from(vec![7u8; 4096]);
    store.set("temps/c/0", big.clone()).await.unwrap();
    store.set("temps/c/1", Bytes::from_static(&[1, 2])).await.unwrap();
    let snap = session.write().await.commit("from the browser").execute().await.unwrap();

    // reopen from scratch: open runs the spec-version fetch on rt::spawn and
    // decodes the snapshot/manifest via rt::spawn_blocking
    let repo = Repository::open(Arc::clone(&storage)).execute().await.unwrap();
    let session = repo
        .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
        .await
        .unwrap();
    assert_eq!(session.snapshot_id(), &snap);
    let store = Store::from_session(Arc::new(RwLock::new(session))).await;
    assert_eq!(store.get("temps/zarr.json", &ByteRange::ALL).await.unwrap(), ARRAY);
    assert_eq!(store.get("temps/c/0", &ByteRange::ALL).await.unwrap(), big);
    assert_eq!(store.get("temps/c/1", &ByteRange::ALL).await.unwrap(), &[1u8, 2][..]);

    // a public `impl Stream + MaybeSend` API works too
    let ancestry: Vec<_> = repo
        .ancestry(&VersionInfo::BranchTipRef("main".to_string()))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(ancestry.len(), 2);
    assert_eq!(ancestry[0].message, "from the browser");
}

// ---- typetag registration (inventory constructors) ----
//
// typetag finds `impl Storage` types through `inventory`, which registers them
// from static constructors. On wasm those only run if the linker or the host
// calls `__wasm_call_ctors`. If they did not run, every tag below would be an
// "unknown variant".

#[wasm_bindgen_test]
async fn typetag_storage_from_another_crate_roundtrips() {
    // ObjectStorage (icechunk-arrow-object-store) wrapping the in-memory
    // backend: two nested typetag registries
    let storage = new_in_memory_storage().await.unwrap();
    let json = serde_json::to_string(&storage).unwrap();
    assert!(json.contains(r#""type":"ObjectStorage""#), "{json}");
    assert!(json.contains("in_memory_object_store_provider"), "{json}");
    let back: Box<dyn Storage> = serde_json::from_str(&json).unwrap();
    assert_eq!(back.storage_info().backend_type, storage.storage_info().backend_type);
}

#[wasm_bindgen_test]
async fn typetag_storage_from_this_crate_roundtrips() {
    let storage: Box<dyn Storage> =
        Box::new(JsYieldStorage::new(new_in_memory_storage().await.unwrap()));
    let json = serde_json::to_string(&storage).unwrap();
    let back: Box<dyn Storage> = serde_json::from_str(&json).unwrap();
    assert_eq!(back.storage_info().backend_type, "JsYield");
}

#[wasm_bindgen_test]
fn typetag_unknown_tag_is_rejected() {
    // control: proves the success cases above actually went through the registry
    let err = serde_json::from_str::<Box<dyn Storage>>(r#"{"type":"NoSuchStorage"}"#)
        .err()
        .expect("unknown tag must fail");
    assert!(err.to_string().contains("unknown variant"), "{err}");
}

#[wasm_bindgen_test]
async fn repository_as_bytes_roundtrips() {
    // the path Python pickling uses: storage, config and fetchers via typetag
    let repo = Repository::create(new_in_memory_storage().await.unwrap())
        .execute()
        .await
        .unwrap();
    let bytes = repo.as_bytes().unwrap();
    let back = Repository::from_bytes(&bytes).unwrap();
    assert_eq!(
        back.storage().storage_info().backend_type,
        repo.storage().storage_info().backend_type
    );
}
