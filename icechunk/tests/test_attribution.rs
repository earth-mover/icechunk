use std::{collections::HashMap, sync::Arc};

use crate::common::capture::{CapturedRequest, FakeStore};
use bytes::Bytes;
use icechunk::{
    ObjectStoreConfig, Repository, Storage,
    config::{
        Credentials, HttpConfig, RepositoryConfig, S3Credentials, S3Options,
        S3StaticCredentials,
    },
    format::{
        ByteRange, ChunkIndices, Path,
        manifest::{ChunkPayload, VirtualChunkLocation, VirtualChunkRef},
        snapshot::ArrayShape,
    },
    new_s3_object_store_storage, new_s3_storage,
    repository::VersionInfo,
    session::get_chunk,
    storage::{Attribution, AttributionLabels, RequestAttribution, StorageContext},
    user_agent_product,
    virtual_chunks::VirtualChunkContainer,
};
use icechunk_macros::tokio_test;

fn s3_options(store: &FakeStore) -> S3Options {
    S3Options::default()
        .with_region("us-east-1")
        .with_endpoint_url(store.endpoint())
        .with_allow_http(true)
        .with_force_path_style(true)
}

fn static_creds() -> Option<S3Credentials> {
    Some(S3Credentials::Static(S3StaticCredentials {
        access_key_id: "k".into(),
        secret_access_key: "s".into(),
        session_token: None,
        expires_after: None,
    }))
}

fn native_s3(store: &FakeStore) -> Arc<dyn Storage + Send + Sync> {
    new_s3_storage(
        s3_options(store),
        "bucket".to_string(),
        Some("prefix".to_string()),
        static_creds(),
        Vec::new(),
        Vec::new(),
        Some(false),
    )
    .unwrap()
}

async fn object_store_s3(store: &FakeStore) -> Arc<dyn Storage + Send + Sync> {
    new_s3_object_store_storage(
        s3_options(store),
        "bucket".to_string(),
        Some("prefix".to_string()),
        static_creds(),
        Vec::new(),
        Vec::new(),
    )
    .await
    .unwrap()
}

fn attribution() -> Attribution {
    Attribution::new()
        .with_client("wrapper/1.0")
        .unwrap()
        .with_workload("wl")
        .unwrap()
        .with_principal("me")
        .unwrap()
}

fn only(requests: &[CapturedRequest], method: &str, path_part: &str) -> CapturedRequest {
    let matches: Vec<_> = requests
        .iter()
        .filter(|r| r.method == method && r.path.contains(path_part))
        .cloned()
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one {method} {path_part}: {requests:#?}"
    );
    matches.into_iter().next().unwrap()
}

fn fragment(comment: &str) -> String {
    format!("wrapper/1.0 {} ({comment})", user_agent_product())
}

const CHUNK: &[u8] = b"0123456789abcdef";

/// Drives one storage through a chunk PUT and GET, a manifest PUT, a
/// snapshot HEAD and a batch delete, then checks each request's `User-Agent`.
///
/// Deletes carry only the product token on `object_store`, whose delete takes
/// no request options; `deletes_attributed` says whether the backend attributes
/// them.
async fn exercise_storage(
    storage: Arc<dyn Storage + Send + Sync>,
    store: &FakeStore,
    deletes_attributed: bool,
) {
    let settings = storage.default_settings().await.unwrap();
    let labels = AttributionLabels::from(&attribution());
    let coords = [1u32, 2];
    let chunk_ctx = StorageContext::new(
        &settings,
        RequestAttribution {
            labels: &labels,
            array: Some("g/temp"),
            chunk: Some(&coords),
        },
    );
    let manifest_ctx = StorageContext::new(
        &settings,
        RequestAttribution { labels: &labels, array: Some("g/temp"), chunk: None },
    );
    let plain = StorageContext::without_node(&settings, &labels);

    storage
        .put_object(&chunk_ctx, "chunks/abc", CHUNK.into(), None, Vec::new(), None)
        .await
        .unwrap();
    storage
        .put_object(
            &manifest_ctx,
            "manifests/m1",
            b"m".as_ref().into(),
            None,
            Vec::new(),
            None,
        )
        .await
        .unwrap();
    storage
        .put_object(&plain, "snapshots/s1", b"s".as_ref().into(), None, Vec::new(), None)
        .await
        .unwrap();

    let (mut reader, _) =
        storage.get_object(&chunk_ctx, "chunks/abc", Some(&(0..4))).await.unwrap();
    let mut buf = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buf).await.unwrap();
    assert_eq!(buf, b"0123");
    storage.get_object_last_modified(&plain, "snapshots/s1").await.unwrap();
    storage.delete_batch(&plain, "snapshots", vec![("s1".to_string(), 1)]).await.unwrap();

    let requests = store.requests();
    let with_chunk = fragment("workload=wl; principal=me; array=g/temp; chunk=1/2");
    let with_array = fragment("workload=wl; principal=me; array=g/temp");
    let labels_only = fragment("workload=wl; principal=me");
    for (method, path, expected) in [
        ("PUT", "chunks/abc", &with_chunk),
        ("GET", "chunks/abc", &with_chunk),
        ("PUT", "manifests/m1", &with_array),
        ("PUT", "snapshots/s1", &labels_only),
        ("HEAD", "snapshots/s1", &labels_only),
    ] {
        let r = only(&requests, method, path);
        let ua =
            r.user_agent.unwrap_or_else(|| panic!("{method} {path} has no user agent"));
        assert!(
            ua.ends_with(expected),
            "{method} {path}: {ua:?} should end with {expected:?}"
        );
        assert!(!ua.contains("app/icechunk-rust"), "AppName must be gone: {ua}");
        assert_eq!(ua.matches("icechunk/").count(), 1, "product token once: {ua}");
    }

    let deletes: Vec<_> = requests.iter().filter(|r| r.method == "POST").collect();
    assert_eq!(deletes.len(), 1, "expected exactly one batch delete: {requests:#?}");
    let ua = deletes[0].user_agent.clone().expect("the delete has no user agent");
    if deletes_attributed {
        assert!(
            ua.ends_with(&labels_only),
            "delete: {ua:?} should end with {labels_only:?}"
        );
    } else {
        assert_eq!(ua, user_agent_product(), "delete");
    }
}

#[tokio_test]
async fn native_s3_requests_carry_attribution() {
    let store = FakeStore::start().await;
    exercise_storage(native_s3(&store), &store, true).await;
    // the sdk tokens come first, ours last
    let ua = store.requests()[0].user_agent.clone().unwrap();
    assert!(ua.starts_with("aws-sdk-rust/"), "{ua}");
}

#[tokio_test]
async fn object_store_s3_requests_carry_attribution() {
    let store = FakeStore::start().await;
    exercise_storage(object_store_s3(&store).await, &store, false).await;
    // object_store has no sdk prefix: the header is exactly the fragment
    let ua = store.requests()[0].user_agent.clone().unwrap();
    assert!(ua.starts_with("wrapper/1.0 icechunk/"), "{ua}");
}

/// Every request carries the labels; chunk and manifest requests also carry
/// the array, and chunk requests the coordinates.
fn assert_attributed(requests: &[CapturedRequest]) {
    assert!(!requests.is_empty());
    for r in requests {
        let ua =
            r.user_agent.as_deref().unwrap_or_else(|| panic!("no user agent on {r:?}"));
        assert!(ua.contains("wrapper/1.0 icechunk/"), "{r:?}");
        assert!(ua.contains("(workload=wl; principal=me"), "{r:?}");
        if r.path.contains("/chunks/") {
            assert!(ua.ends_with("array=g/temp; chunk=0/1)"), "{r:?}");
        } else if r.path.contains("/manifests/") {
            assert!(ua.ends_with("array=g/temp)"), "{r:?}");
        } else {
            assert!(
                !ua.contains("array="),
                "repo-level request must not name an array: {r:?}"
            );
        }
    }
}

async fn attributed_repo(store: &FakeStore) -> Repository {
    let config =
        RepositoryConfig { inline_chunk_threshold_bytes: Some(0), ..Default::default() };
    Repository::create(
        Some(config),
        native_s3(store),
        HashMap::new(),
        None,
        false,
        Some(attribution()),
    )
    .await
    .unwrap()
}

#[tokio_test]
async fn repository_keeps_attribution_across_serialization() {
    let store = FakeStore::start().await;
    let repo = attributed_repo(&store).await;
    assert_eq!(repo.attribution(), &attribution());
    let repo = Repository::from_bytes(&repo.as_bytes().unwrap()).unwrap();
    assert_eq!(repo.attribution(), &attribution());
    assert_eq!(
        repo.asset_manager().attribution_labels(),
        &AttributionLabels::from(&attribution())
    );

    // opening without attribution gives the default
    let reopened =
        Repository::open(None, native_s3(&store), HashMap::new(), None).await.unwrap();
    assert_eq!(reopened.attribution(), &Attribution::new());
}

#[tokio_test]
async fn repository_requests_carry_labels_array_and_chunk() {
    let store = FakeStore::start().await;
    let repo = attributed_repo(&store).await;
    assert_attributed(&store.requests());
    let array = Path::new("/g/temp").unwrap();
    let coords = ChunkIndices(vec![0, 1]);

    let mut session = repo.writable_session("main").await.unwrap();
    session.add_group(Path::root(), Bytes::new()).await.unwrap();
    session.add_group(Path::new("/g").unwrap(), Bytes::new()).await.unwrap();
    let shape = ArrayShape::new(vec![(4, 2), (4, 2)]).unwrap();
    session.add_array(array.clone(), shape, None, Bytes::new()).await.unwrap();

    store.clear();
    let writer = session.get_chunk_writer(&array, &coords).unwrap();
    let payload = writer(Bytes::from_static(CHUNK)).await.unwrap();
    session.set_chunk_ref(array.clone(), coords.clone(), Some(payload)).await.unwrap();
    let writes = store.requests();
    only(&writes, "PUT", "/chunks/");
    assert_attributed(&writes);

    store.clear();
    session.commit("c").execute().await.unwrap();
    let commit = store.requests();
    only(&commit, "PUT", "/manifests/");
    assert_attributed(&commit);

    // a freshly opened repository has cold caches, so the read fetches the
    // manifest too; the open's own spec-version probe is asserted as well
    store.clear();
    let repo =
        Repository::open(None, native_s3(&store), HashMap::new(), Some(attribution()))
            .await
            .unwrap();
    let session = repo
        .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
        .await
        .unwrap();
    let bytes = get_chunk(
        session.get_chunk_reader(&array, &coords, &ByteRange::ALL).await.unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(bytes.as_ref(), CHUNK);
    let reads = store.requests();
    only(&reads, "GET", "/chunks/");
    assert!(
        reads.iter().any(|r| r.method == "GET" && r.path.contains("/manifests/")),
        "{reads:#?}"
    );
    assert_attributed(&reads);
}

/// Reads chunk `0/1` of `/g/temp` through a virtual ref to `location`, served
/// by `container`, and returns the user agent of the external object's GET.
/// The object is `/virtual-bucket/some/file.nc` on the fake store.
async fn read_virtual_chunk(
    store: &FakeStore,
    container: VirtualChunkContainer,
    credentials: Option<Credentials>,
    location: &str,
) -> String {
    let prefix = container.url_prefix().to_string();
    let mut config =
        RepositoryConfig { inline_chunk_threshold_bytes: Some(0), ..Default::default() };
    config.set_virtual_chunk_container(container).unwrap();
    let repo = Repository::create(
        Some(config),
        native_s3(store),
        HashMap::from([(prefix, credentials)]),
        None,
        false,
        Some(attribution()),
    )
    .await
    .unwrap();

    // put the external object in place through a plain storage handle
    let external = new_s3_storage(
        s3_options(store),
        "virtual-bucket".to_string(),
        None,
        static_creds(),
        Vec::new(),
        Vec::new(),
        Some(false),
    )
    .unwrap();
    let settings = external.default_settings().await.unwrap();
    external
        .put_object(
            &StorageContext::unattributed(&settings),
            "some/file.nc",
            CHUNK.into(),
            None,
            Vec::new(),
            None,
        )
        .await
        .unwrap();

    let array = Path::new("/g/temp").unwrap();
    let coords = ChunkIndices(vec![0, 1]);
    let mut session = repo.writable_session("main").await.unwrap();
    session.add_group(Path::root(), Bytes::new()).await.unwrap();
    session.add_group(Path::new("/g").unwrap(), Bytes::new()).await.unwrap();
    let shape = ArrayShape::new(vec![(4, 2), (4, 2)]).unwrap();
    session.add_array(array.clone(), shape, None, Bytes::new()).await.unwrap();
    let vref = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(location).unwrap(),
        offset: 0,
        length: 4,
        checksum: None,
    };
    session
        .set_chunk_ref(array.clone(), coords.clone(), Some(ChunkPayload::Virtual(vref)))
        .await
        .unwrap();

    store.clear();
    let bytes = get_chunk(
        session.get_chunk_reader(&array, &coords, &ByteRange::ALL).await.unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(bytes.as_ref(), &CHUNK[..4]);
    only(&store.requests(), "GET", "/virtual-bucket/some/file.nc").user_agent.unwrap()
}

#[tokio_test]
async fn virtual_chunk_reads_carry_attribution() {
    let store = FakeStore::start().await;
    // the virtual bucket lives on the same fake store, under another bucket name
    let container = VirtualChunkContainer::new(
        "s3://virtual-bucket/".to_string(),
        ObjectStoreConfig::S3Compatible(s3_options(&store)),
    )
    .unwrap();
    let credentials = Some(Credentials::S3(static_creds().unwrap()));
    let ua = read_virtual_chunk(
        &store,
        container,
        credentials,
        "s3://virtual-bucket/some/file.nc",
    )
    .await;
    assert!(ua.starts_with("aws-sdk-rust/"), "{ua}");
    assert!(
        ua.ends_with(&fragment("workload=wl; principal=me; array=g/temp; chunk=0/1")),
        "{ua}"
    );
}

/// Reads the virtual chunk through an HTTP container, which `object_store`
/// serves, with `opts` added to the container's client options.
async fn read_http_virtual_chunk(store: &FakeStore, opts: &[(&str, &str)]) -> String {
    let prefix = format!("{}/virtual-bucket/", store.endpoint());
    let opts = [("allow_http", "true")]
        .iter()
        .chain(opts)
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let container = VirtualChunkContainer::new(
        prefix.clone(),
        ObjectStoreConfig::Http(HttpConfig { opts, headers: HashMap::new() }),
    )
    .unwrap();
    read_virtual_chunk(store, container, None, &format!("{prefix}some/file.nc")).await
}

/// HTTP containers are read through `object_store`, whose header is exactly
/// the fragment.
#[tokio_test]
async fn object_store_virtual_chunk_reads_carry_attribution() {
    let store = FakeStore::start().await;
    let ua = read_http_virtual_chunk(&store, &[]).await;
    assert_eq!(ua, fragment("workload=wl; principal=me; array=g/temp; chunk=0/1"));
}

/// A user agent configured on an `object_store` client prefixes the fragment.
#[tokio_test]
async fn object_store_user_agent_option_prefixes_attribution() {
    let store = FakeStore::start().await;
    let ua = read_http_virtual_chunk(&store, &[("user_agent", "myapp/2")]).await;
    assert_eq!(
        ua,
        format!(
            "myapp/2 {}",
            fragment("workload=wl; principal=me; array=g/temp; chunk=0/1")
        )
    );
}
