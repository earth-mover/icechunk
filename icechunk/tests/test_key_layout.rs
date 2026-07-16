//! Integration tests for the S3 key layout fix (#2239).
//!
//! Before the fix, an empty `prefix` made the native-S3 backend write every
//! object under a leading slash (`/chunks/...`); external tools 404'd and GC
//! silently orphaned objects (delete used a different join than write). These
//! tests verify the fix against local rustfs.

use std::{
    collections::HashMap,
    num::{NonZeroU16, NonZeroUsize},
    panic::AssertUnwindSafe,
    sync::Arc,
};

use bytes::Bytes;
use chrono::Utc;
use futures::FutureExt as _;
use icechunk::{
    Repository, RepositoryConfig, Storage,
    config::{S3Credentials, S3Options, S3StaticCredentials},
    format::{
        ByteRange, ChunkIndices, Path, format_constants::SpecVersionBin,
        snapshot::ArrayShape,
    },
    ops::gc::{GCConfig, garbage_collect},
    repository::{RepositoryError, RepositoryErrorKind, VersionInfo},
    session::get_chunk,
    storage::{Settings, mk_client, s3_storage},
};
use icechunk_macros::tokio_test;

use crate::common;

const ENDPOINT: &str = "http://localhost:4200";

fn rustfs_options() -> S3Options {
    S3Options::default()
        .with_region("us-east-1")
        .with_endpoint_url(ENDPOINT)
        .with_allow_http(true)
        .with_force_path_style(true)
}

fn static_credentials(access_key_id: &str, secret_access_key: &str) -> S3Credentials {
    S3Credentials::Static(S3StaticCredentials {
        access_key_id: access_key_id.to_string(),
        secret_access_key: secret_access_key.to_string(),
        session_token: None,
        expires_after: None,
    })
}

/// rustfs server admin credentials: can create buckets and access any bucket.
fn root_credentials() -> S3Credentials {
    static_credentials("test123", "test123")
}

fn root_storage(
    bucket: &str,
    prefix: Option<&str>,
    legacy_rooted_keys: bool,
) -> Arc<dyn Storage + Send + Sync> {
    // These tests deliberately create empty-prefix (bucket-root) repos, which is
    // normally refused, so apply the escape hatch before erasing the type.
    Arc::new(
        s3_storage(
            rustfs_options(),
            bucket.to_string(),
            prefix.map(str::to_string),
            Some(root_credentials()),
            Vec::new(),
            Vec::new(),
            legacy_rooted_keys.then_some(true),
        )
        .unwrap()
        .unsafe_allow_empty_prefix_creation(),
    )
}

const MINIO_ENDPOINT: &str = "http://localhost:4202";

fn minio_options() -> S3Options {
    S3Options::default()
        .with_region("us-east-1")
        .with_endpoint_url(MINIO_ENDPOINT)
        .with_allow_http(true)
        .with_force_path_style(true)
}

fn minio_credentials() -> S3Credentials {
    static_credentials("minioadmin", "minioadmin")
}

fn minio_storage(bucket: &str, prefix: Option<&str>) -> Arc<dyn Storage + Send + Sync> {
    // These tests deliberately create empty-prefix (bucket-root) repos, which is
    // normally refused, so apply the escape hatch before erasing the type.
    Arc::new(
        s3_storage(
            minio_options(),
            bucket.to_string(),
            prefix.map(str::to_string),
            Some(minio_credentials()),
            Vec::new(),
            Vec::new(),
            None,
        )
        .unwrap()
        .unsafe_allow_empty_prefix_creation(),
    )
}

/// Create a fresh, uniquely named bucket and return its name.
///
/// The name sorts lexicographically after `testbucket`, and the zero-padded
/// microsecond timestamp makes lexicographic order match creation order; the
/// random suffix keeps it unique under parallel test runs.
async fn fresh_bucket() -> String {
    create_fresh_bucket(&rustfs_options(), root_credentials()).await
}

async fn fresh_minio_bucket() -> String {
    create_fresh_bucket(&minio_options(), minio_credentials()).await
}

async fn create_fresh_bucket(options: &S3Options, credentials: S3Credentials) -> String {
    let bucket = format!(
        "testbucket-layout-{:016}-{:016x}",
        Utc::now().timestamp_micros(),
        rand::random::<u64>(),
    );
    let client =
        mk_client(options, credentials, vec![], vec![], &Settings::default()).await;
    client.create_bucket().bucket(&bucket).send().await.expect("create_bucket");
    bucket
}

/// List every object key in a bucket (raw, with the leading slash, if any, that
/// the object was actually stored under).
async fn raw_keys(bucket: &str) -> Vec<String> {
    let client = mk_client(
        &rustfs_options(),
        root_credentials(),
        vec![],
        vec![],
        &Settings::default(),
    )
    .await;
    // we have only a few objects, first page of results is enough
    let resp =
        client.list_objects_v2().bucket(bucket).send().await.expect("list_objects_v2");
    resp.contents().iter().filter_map(|o| o.key().map(str::to_string)).collect()
}

/// Minimal repo: one group, one array, one separate (non-inlined) chunk at [0].
async fn create_repo_with_one_chunk(
    storage: Arc<dyn Storage + Send + Sync>,
    spec_version: SpecVersionBin,
    value: i8,
) -> Result<Repository, Box<dyn std::error::Error>> {
    let repo = Repository::create(
        Some(RepositoryConfig {
            // force chunks to be written as separate objects (not inlined)
            inline_chunk_threshold_bytes: Some(0),
            ..Default::default()
        }),
        storage,
        HashMap::new(),
        Some(spec_version),
        true,
    )
    .await?;
    write_one_chunk(&repo, value).await?;
    Ok(repo)
}

fn version_anchor(spec_version: SpecVersionBin) -> &'static str {
    match spec_version {
        SpecVersionBin::V2 => "repo",
        SpecVersionBin::V1 => "refs/branch.main/ref.json",
    }
}

async fn write_one_chunk(
    repo: &Repository,
    value: i8,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut ds = repo.writable_session("main").await?;
    let array_path: Path = "/array".try_into().unwrap();
    if ds.get_node(&array_path).await.is_err() {
        ds.add_group(Path::root(), Bytes::new()).await?;
        let shape = ArrayShape::new(vec![(2, 1)]).unwrap();
        ds.add_array(array_path.clone(), shape, None, Bytes::new()).await?;
    }
    let payload =
        ds.get_chunk_writer()?(Bytes::copy_from_slice(&value.to_be_bytes())).await?;
    ds.set_chunk_ref(array_path.clone(), ChunkIndices(vec![0]), Some(payload)).await?;
    ds.commit(format!("write {value}")).execute().await?;
    Ok(())
}

async fn read_chunk0(repo: &Repository) -> Result<i8, Box<dyn std::error::Error>> {
    let array_path: Path = "/array".try_into().unwrap();
    let ds =
        repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;
    let bytes = get_chunk(
        ds.get_chunk_reader(&array_path, &ChunkIndices(vec![0]), &ByteRange::ALL).await?,
    )
    .await?
    .unwrap();
    Ok(i8::from_be_bytes([bytes[0]]))
}

/// Direct fix for #2239: an empty-prefix repository must write clean keys, never
/// a leading slash.
#[tokio_test]
async fn empty_prefix_writes_clean_keys() -> Result<(), Box<dyn std::error::Error>> {
    // Run against both spec versions: they write different anchor files (V2 `repo`,
    // V1 `refs/...`), which are the keys the layout probe detects.
    for spec_version in [SpecVersionBin::V1, SpecVersionBin::V2] {
        let bucket = fresh_bucket().await;
        let storage = root_storage(&bucket, Some(""), false);
        create_repo_with_one_chunk(Arc::clone(&storage), spec_version, 42).await?;

        let keys = raw_keys(&bucket).await;
        assert!(!keys.is_empty(), "repo wrote no objects");
        for key in &keys {
            assert!(
                !key.starts_with('/'),
                "key {key:?} starts with a slash (the #2239 bug)"
            );
        }
        let anchor = version_anchor(spec_version);
        assert!(
            keys.iter().any(|k| k == anchor),
            "expected a clean `{anchor}` key, got {keys:?}"
        );
        assert!(
            keys.iter().any(|k| k.starts_with("chunks/")),
            "expected a clean `chunks/...` key, got {keys:?}"
        );
    }
    Ok(())
}

/// Round-trip through icechunk on a clean empty-prefix repo. Both spec versions,
/// so reopen exercises layout detection via each version's anchor file.
#[tokio_test]
async fn empty_prefix_roundtrips() -> Result<(), Box<dyn std::error::Error>> {
    for spec_version in [SpecVersionBin::V1, SpecVersionBin::V2] {
        let bucket = fresh_bucket().await;
        create_repo_with_one_chunk(
            root_storage(&bucket, Some(""), false),
            spec_version,
            7,
        )
        .await?;

        // Re-open with a fresh storage (forces the detection probe to run again).
        let repo =
            Repository::open(None, root_storage(&bucket, None, false), HashMap::new())
                .await?;
        assert_eq!(read_chunk0(&repo).await?, 7);
    }
    Ok(())
}

/// Regression for #2239 on a *normalizing* store: `MinIO` maps `"/x"` to `"x"`, so
/// the layout probe sees the clean and rooted anchors as the same object. It must
/// resolve to the clean layout (by comparing `ETag`s), not raise a spurious
/// mixed-layout error. Without the fix, the reopen below fails.
#[tokio_test]
async fn empty_prefix_roundtrips_on_normalizing_store()
-> Result<(), Box<dyn std::error::Error>> {
    let bucket = fresh_minio_bucket().await;
    create_repo_with_one_chunk(minio_storage(&bucket, Some("")), SpecVersionBin::V2, 13)
        .await?;

    // A fresh storage forces the detection probe to run on reopen.
    let repo =
        Repository::open(None, minio_storage(&bucket, None), HashMap::new()).await?;
    assert_eq!(read_chunk0(&repo).await?, 13);
    Ok(())
}

/// `create` over a bucket that already holds an empty-prefix repo must refuse,
/// across every combination of the existing and new repo's spec versions.
#[tokio_test]
async fn empty_prefix_create_refuses_over_existing_repo()
-> Result<(), Box<dyn std::error::Error>> {
    use SpecVersionBin::{V1, V2};
    for (existing, new) in [(V1, V1), (V1, V2), (V2, V1), (V2, V2)] {
        let bucket = fresh_bucket().await;
        create_repo_with_one_chunk(root_storage(&bucket, Some(""), false), existing, 1)
            .await?;

        let err = Repository::create(
            None,
            root_storage(&bucket, Some(""), false),
            HashMap::new(),
            Some(new),
            true,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err,
                RepositoryError {
                    kind: RepositoryErrorKind::ParentDirectoryNotClean,
                    ..
                }
            ),
            "expected ParentDirectoryNotClean for {existing:?} -> {new:?}, got {err:?}"
        );
    }
    Ok(())
}

/// Opening an empty bucket with an empty prefix reports the repo as missing
#[tokio_test]
async fn empty_prefix_nonexistent_repo() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = fresh_bucket().await;
    let err =
        Repository::open(None, root_storage(&bucket, Some(""), false), HashMap::new())
            .await
            .unwrap_err();
    assert!(
        matches!(
            err,
            RepositoryError { kind: RepositoryErrorKind::RepositoryDoesntExist, .. }
        ),
        "expected RepositoryDoesntExist, got {err:?}"
    );
    Ok(())
}

/// Regression for the silent-orphan bug: on an empty-prefix repo, GC must
/// actually remove the chunk *objects* from the bucket (delete now builds the
/// same key as write, instead of a no-leading-slash key that hit nothing).
#[tokio_test]
async fn empty_prefix_gc_actually_deletes_chunks()
-> Result<(), Box<dyn std::error::Error>> {
    let bucket = fresh_bucket().await;
    let storage = root_storage(&bucket, Some(""), false);
    let repo =
        create_repo_with_one_chunk(Arc::clone(&storage), SpecVersionBin::V2, 42).await?;

    // second commit overwrites chunk [0]; capture the first snapshot to reset to.
    let first = repo.lookup_branch("main").await?;
    write_one_chunk(&repo, 7).await?;

    let chunks_before =
        raw_keys(&bucket).await.iter().filter(|k| k.starts_with("chunks/")).count();
    assert_eq!(chunks_before, 2, "expected two distinct chunk objects");

    // forget the second commit, making its chunk garbage
    repo.reset_branch("main", &first, None).await?;

    let now = Utc::now();
    let gc_config = GCConfig::clean_all(
        now,
        now,
        None,
        NonZeroU16::new(50).unwrap(),
        NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        NonZeroU16::new(500).unwrap(),
        false,
    );
    let summary =
        garbage_collect(Arc::clone(repo.asset_manager()), &gc_config, None, 100).await?;
    assert_eq!(summary.chunks_deleted, 1, "GC should report one deleted chunk");

    let chunks_after =
        raw_keys(&bucket).await.iter().filter(|k| k.starts_with("chunks/")).count();
    assert_eq!(
        chunks_after, 1,
        "the deleted chunk object must actually be gone from the bucket (orphan-bug regression)"
    );
    Ok(())
}

/// Create a rooted repo, then prove a plain auto-detecting client can read it,
/// append to it, and garbage-collect it — the end-to-end backward-compat
/// guarantee for pre-#2239 empty-prefix repositories.
async fn do_rooted_roundtrip(
    store: common::RealStore,
) -> Result<(), Box<dyn std::error::Error>> {
    // Remove any rooted objects left by a previous run, so `create` (which
    // conditionally creates `/repo`) starts from a clean root.
    store.cleanup_rooted_keys().await?;

    // Run the body under catch_unwind so we always clean up the rooted `/...` keys
    // afterwards, even when an assertion panics — otherwise a failed run would leave
    // them polluting the shared bucket.
    let result = AssertUnwindSafe(rooted_roundtrip_body(&store)).catch_unwind().await;

    // Leave the bucket as we found it, but don't let a cleanup error mask the body's
    // own panic or error.
    let cleanup = store.cleanup_rooted_keys().await;
    match result {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(Err(e)) => Err(e),
        Ok(Ok(())) => cleanup,
    }
}

/// The rooted round-trip work; see [`do_rooted_roundtrip`], which wraps this to
/// guarantee bucket cleanup even when an assertion here fails.
async fn rooted_roundtrip_body(
    store: &common::RealStore,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create a rooted repo (empty prefix + forced legacy layout). check_clean_root
    // is false because the bucket is shared with other integration tests' objects
    // (which live under non-slash prefixes, a disjoint key space).
    let repo = Repository::create(
        Some(RepositoryConfig {
            inline_chunk_threshold_bytes: Some(0),
            ..Default::default()
        }),
        store.rooted_storage(true)?,
        HashMap::new(),
        Some(SpecVersionBin::V2),
        false,
    )
    .await?;
    write_one_chunk(&repo, 42).await?;

    // Reopen with auto-detection (a fresh storage forces the probe). On a
    // leading-slash-preserving store this resolves to LegacyRoot and reads back.
    let repo =
        Repository::open(None, store.rooted_storage(false)?, HashMap::new()).await?;
    assert_eq!(read_chunk0(&repo).await?, 42);

    // Append through the reopened repo, then reopen + read again (write path).
    write_one_chunk(&repo, 7).await?;
    let repo =
        Repository::open(None, store.rooted_storage(false)?, HashMap::new()).await?;
    assert_eq!(read_chunk0(&repo).await?, 7);

    // GC must run cleanly under the detected (rooted) layout.
    let now = Utc::now();
    let gc_config = GCConfig::clean_all(
        now,
        now,
        None,
        NonZeroU16::new(50).unwrap(),
        NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        NonZeroU16::new(500).unwrap(),
        false,
    );
    garbage_collect(Arc::clone(repo.asset_manager()), &gc_config, None, 100).await?;
    Ok(())
}

#[tokio_test]
#[ignore = "needs credentials from env"]
async fn rooted_roundtrip_in_aws() -> Result<(), Box<dyn std::error::Error>> {
    let store = common::aws_real_store().expect("AWS_* env vars must be set");
    do_rooted_roundtrip(store).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
async fn rooted_roundtrip_in_r2() -> Result<(), Box<dyn std::error::Error>> {
    let store = common::r2_real_store().expect("R2_* env vars must be set");
    do_rooted_roundtrip(store).await
}

#[tokio_test]
#[ignore = "needs credentials from env"]
async fn rooted_roundtrip_in_tigris() -> Result<(), Box<dyn std::error::Error>> {
    let store = common::tigris_real_store().expect("TIGRIS_* env vars must be set");
    do_rooted_roundtrip(store).await
}

// These run only against real S3/R2/Tigris (hence `#[ignore]` + credentials), never
// the local stores: a rooted repo keeps every object under a leading slash
// (`/chunks/...`), and neither local store can hold those keys as written. rustfs
// (:4200) rejects leading-slash keys with a 400, and minio (:4202) silently
// normalizes them (`/x` -> `x`), which collapses the repo into the standard layout
// so there is no rooted repo left to round-trip. Only a store that preserves the
// leading slash, like real S3/R2/Tigris, can exercise the legacy layout end to end.
