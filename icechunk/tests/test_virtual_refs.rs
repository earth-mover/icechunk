use futures::TryStreamExt as _;
use icechunk::{
    ObjectStoreConfig, Repository, RepositoryConfig, Storage, Store,
    config::{
        AzureCredentials, Credentials, GcsCredentials, S3Credentials, S3Options,
        S3StaticCredentials,
    },
    format::{
        ByteRange, ChunkId, ChunkIndices, Path,
        format_constants::SpecVersionBin,
        manifest::{
            Checksum, ChunkPayload, SecondsSinceEpoch, VirtualChunkLocation,
            VirtualChunkRef, VirtualReferenceErrorKind,
        },
        snapshot::ArrayShape,
    },
    repository::VersionInfo,
    session::{SessionError, SessionErrorKind, get_chunk},
    storage::{
        self, ConcurrencySettings, ETag, ObjectStorage, mk_client, new_s3_storage,
    },
    store::{StoreError, StoreErrorKind},
    virtual_chunks::VirtualChunkContainer,
};
use icechunk_macros::tokio_test;
use rstest::rstest;
use rstest_reuse::{self, *};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    path::PathBuf,
    vec,
};
use std::{path::Path as StdPath, sync::Arc};
use tempfile::TempDir;
use tokio::sync::RwLock;

use bytes::Bytes;
use icechunk_arrow_object_store::object_store::{
    ObjectStore, PutMode, PutOptions, PutPayload, azure::AzureConfigKey,
    local::LocalFileSystem,
};
use pretty_assertions::assert_eq;

#[template]
#[rstest]
#[case::v1(SpecVersionBin::V1)]
#[case::v2(SpecVersionBin::V2)]
fn spec_version_cases(#[case] spec_version: SpecVersionBin) {}

fn minio_s3_config() -> (S3Options, S3Credentials) {
    let config = S3Options::default()
        .with_region("us-east-1")
        .with_endpoint_url("http://localhost:4200")
        .with_allow_http(true)
        .with_force_path_style(true);
    let credentials = S3Credentials::Static(S3StaticCredentials {
        access_key_id: "test123".into(),
        secret_access_key: "test123".into(),
        session_token: None,
        expires_after: None,
    });
    (config, credentials)
}

async fn create_repository(
    storage: Arc<dyn Storage + Send + Sync>,
    virtual_chunk_containers: Vec<VirtualChunkContainer>,
    credentials: HashMap<String, Option<Credentials>>,
    spec_version: SpecVersionBin,
) -> Repository {
    let virtual_chunk_containers = virtual_chunk_containers
        .into_iter()
        .map(|cont| (cont.url_prefix().to_string(), cont))
        .collect();

    Repository::create(
        Some(RepositoryConfig {
            virtual_chunk_containers: Some(virtual_chunk_containers),
            ..Default::default()
        }),
        storage,
        credentials,
        Some(spec_version),
        true,
    )
    .await
    .expect("Failed to initialize repository")
}

async fn write_chunks_to_store(
    store: impl ObjectStore,
    chunks: impl Iterator<Item = (String, Bytes)>,
) {
    // TODO: Switch to PutMode::Create when object_store supports that
    let opts = PutOptions { mode: PutMode::Overwrite, ..PutOptions::default() };

    for (path, bytes) in chunks {
        store
            .put_opts(
                &path.clone().into(),
                PutPayload::from_bytes(bytes.clone()),
                opts.clone(),
            )
            .await
            .expect(&format!("putting chunk to {} failed", &path));
    }
}
async fn create_local_repository(
    repo_path: &StdPath,
    chunks_path: Option<&StdPath>,
    spec_version: SpecVersionBin,
) -> Repository {
    let storage: Arc<dyn Storage + Send + Sync> = Arc::new(
        ObjectStorage::new_local_filesystem(repo_path)
            .await
            .expect("Creating local storage failed"),
    );

    let mut containers = vec![
        VirtualChunkContainer::new(
            "s3://testbucket/".to_string(),
            ObjectStoreConfig::S3(
                S3Options::default().with_region("us-east-1").with_anonymous(true),
            ),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "s3://earthmover-sample-data/".to_string(),
            ObjectStoreConfig::S3(
                S3Options::default().with_region("us-east-1").with_anonymous(true),
            ),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "gcs://testbucket/".to_string(),
            ObjectStoreConfig::Gcs(Default::default()),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "gcs://al-public-test-bucket/".to_string(),
            ObjectStoreConfig::Gcs(Default::default()),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "gcs://gcp-public-data-arco-era5/".to_string(),
            ObjectStoreConfig::Gcs(Default::default()),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "gs://testbucket/".to_string(),
            ObjectStoreConfig::Gcs(Default::default()),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "az://sea-surface-temp-whoi/".to_string(),
            ObjectStoreConfig::Azure(HashMap::from([(
                "account".to_string(),
                "noaacdr".to_string(),
            )])),
        )
        .unwrap(),
    ];

    let mut creds: HashMap<_, Option<Credentials>> = [
        ("s3://testbucket".to_string(), None),
        ("gcs://testbucket".to_string(), None),
        ("gs://testbucket".to_string(), None),
        (
            "s3://earthmover-sample-data".to_string(),
            Some(Credentials::S3(S3Credentials::Anonymous)),
        ),
        (
            "gcs://al-public-test-bucket".to_string(),
            Some(Credentials::Gcs(GcsCredentials::Anonymous)),
        ),
        (
            "gcs://gcp-public-data-arco-era5".to_string(),
            Some(Credentials::Gcs(GcsCredentials::Anonymous)),
        ),
        (
            "az://sea-surface-temp-whoi".to_string(),
            Some(Credentials::Azure(AzureCredentials::Anonymous)),
        ),
    ]
    .into();

    if let Some(chunks_path) = chunks_path {
        let prefix = format!("file://{}/", chunks_path.to_str().unwrap());
        containers.push(
            VirtualChunkContainer::new(
                prefix.clone(),
                ObjectStoreConfig::LocalFileSystem(PathBuf::new()),
            )
            .unwrap(),
        );
        creds.insert(prefix, None);
    }

    create_repository(storage, containers, creds, spec_version).await
}

async fn create_minio_repository(spec_version: SpecVersionBin) -> Repository {
    let prefix = format!("{:?}", ChunkId::random());
    let (config, credentials) = minio_s3_config();
    let storage: Arc<dyn Storage + Send + Sync> = new_s3_storage(
        config,
        "testbucket".to_string(),
        Some(prefix),
        Some(credentials),
        None,
    )
    .expect("Creating minio storage failed");

    let containers = vec![
        VirtualChunkContainer::new(
            "s3://testbucket/".to_string(),
            ObjectStoreConfig::S3Compatible(
                S3Options::default()
                    .with_region("us-east-1")
                    .with_endpoint_url("http://localhost:4200")
                    .with_allow_http(true)
                    .with_force_path_style(true),
            ),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "s3://testbucket/path with spaces/".to_string(),
            ObjectStoreConfig::S3Compatible(
                S3Options::default()
                    .with_region("us-east-1")
                    .with_endpoint_url("http://localhost:4200")
                    .with_allow_http(true)
                    .with_force_path_style(true),
            ),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "az://testcontainer/".to_string(),
            ObjectStoreConfig::Azure(HashMap::from([
                ("account".to_string(), "devstoreaccount1".to_string()),
                ("azure_storage_use_emulator".to_string(), "true".to_string()),
            ])),
        )
        .unwrap(),
    ];

    let credentials = [
        (
            "s3://testbucket/".to_string(),
            Some(Credentials::S3(S3Credentials::Static(S3StaticCredentials {
                access_key_id: "test123".to_string(),
                secret_access_key: "test123".to_string(),
                session_token: None,
                expires_after: None,
            }))),
        ),
        (
            "s3://testbucket/path with spaces/".to_string(),
            Some(Credentials::S3(S3Credentials::Static(S3StaticCredentials {
                access_key_id: "test123".to_string(),
                secret_access_key: "test123".to_string(),
                session_token: None,
                expires_after: None,
            }))),
        ),
        (
            "az://testcontainer/".to_string(),
            Some(Credentials::Azure(AzureCredentials::FromEnv)),
        ),
    ]
    .into();

    create_repository(storage, containers, credentials, spec_version).await
}

async fn write_chunks_to_local_fs(chunks: impl Iterator<Item = (String, Bytes)>) {
    let store =
        LocalFileSystem::new_with_prefix("/").expect("Failed to create local store");
    write_chunks_to_store(store, chunks).await;
}

async fn write_chunks_to_minio(chunks: impl Iterator<Item = (String, Bytes)>) {
    let (opts, creds) = minio_s3_config();
    let client =
        mk_client(&opts, creds, Vec::new(), Vec::new(), &storage::Settings::default())
            .await;

    let bucket_name = "testbucket".to_string();
    for (key, bytes) in chunks {
        client
            .put_object()
            .bucket(bucket_name.clone())
            .key(key.strip_prefix('/').unwrap())
            .body(bytes.into())
            .send()
            .await
            .unwrap();
    }
}

async fn write_chunks_to_azure(
    prefix: String,
    chunks: impl Iterator<Item = (ChunkId, Bytes)>,
) {
    let storage = Arc::new(
        ObjectStorage::new_azure(
            "devstoreaccount1".to_string(),
            "testcontainer".to_string(),
            Some(prefix),
            None,
            Some(HashMap::from([(AzureConfigKey::UseEmulator, "true".to_string())])),
        )
        .await
        .unwrap(),
    );

    for (chunk_id, bytes) in chunks {
        storage
            .put_object(
                &storage::Settings::default(),
                format!("chunks/{chunk_id}").as_str(),
                bytes,
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap();
    }
}

// Sets up a repository whose only authorized `file://` container is an
// `allowed/` directory. A legit chunk `allowed/real.bin` lives INSIDE the
// prefix, and a secret file lives OUTSIDE it (in the parent). Used to verify
// that path traversal cannot read the secret while in-prefix refs still work.
// The returned `TempDir`s must be kept alive for the duration of the test.
async fn setup_file_traversal_repo()
-> (Repository, PathBuf, Bytes, Bytes, TempDir, TempDir) {
    let parent = TempDir::new().unwrap();
    let allowed = parent.path().join("allowed");
    std::fs::create_dir_all(&allowed).unwrap();
    let legit = Bytes::copy_from_slice(b"legit-chunk-data");
    std::fs::write(allowed.join("real.bin"), &legit).unwrap();
    let secret = Bytes::copy_from_slice(b"TOP-SECRET-CONTENTS");
    std::fs::write(parent.path().join("secret.bin"), &secret).unwrap();

    let repo_dir = TempDir::new().unwrap();
    let repo =
        create_local_repository(repo_dir.path(), Some(&allowed), SpecVersionBin::V2)
            .await;
    (repo, allowed, legit, secret, parent, repo_dir)
}

// Proves the `file://` container at `allowed/` is live and authorized (an
// in-prefix ref reads back `legit`), then asserts that a `malicious_location`
// escaping the prefix is rejected with `NoContainerForUrl` — not silently read.
async fn assert_traversal_rejected_but_legit_works(
    repo: &Repository,
    allowed: &StdPath,
    legit: &Bytes,
    secret_len: u64,
    malicious_location: &str,
) -> Result<(), Box<dyn Error>> {
    let mut ds = repo.writable_session("main").await?;
    let shape = || ArrayShape::new(vec![(1, 1)]).unwrap();

    // Positive control: a ref INSIDE the authorized prefix reads back fine, so
    // a later rejection cannot be blamed on a missing/unauthorized container.
    let legit_path: Path = "/legit".try_into().unwrap();
    ds.add_array(legit_path.clone(), shape(), None, Bytes::new()).await?;
    let legit_location = format!("file://{}/real.bin", allowed.to_str().unwrap());
    ds.set_chunk_ref(
        legit_path.clone(),
        ChunkIndices(vec![0]),
        Some(ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation::from_url(&legit_location)?,
            offset: 0,
            length: legit.len() as u64,
            checksum: None,
        })),
    )
    .await?;
    let got = get_chunk(
        ds.get_chunk_reader(&legit_path, &ChunkIndices(vec![0]), &ByteRange::ALL).await?,
    )
    .await?;
    assert_eq!(
        got.as_ref(),
        Some(legit),
        "in-prefix file:// ref must read back; the container must be live"
    );

    // The traversal ref escaping the prefix must be rejected.
    let attack_path: Path = "/attack".try_into().unwrap();
    ds.add_array(attack_path.clone(), shape(), None, Bytes::new()).await?;
    ds.set_chunk_ref(
        attack_path.clone(),
        ChunkIndices(vec![0]),
        Some(ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation::from_url(malicious_location)?,
            offset: 0,
            length: secret_len,
            checksum: None,
        })),
    )
    .await?;
    let reader =
        ds.get_chunk_reader(&attack_path, &ChunkIndices(vec![0]), &ByteRange::ALL).await;
    let read = match reader {
        Ok(reader) => get_chunk(reader).await,
        Err(e) => Err(e),
    };
    assert!(
        matches!(
            &read,
            Err(SessionError {
                kind: SessionErrorKind::VirtualReferenceError(
                    VirtualReferenceErrorKind::NoContainerForUrl(_)
                ),
                ..
            })
        ),
        "file:// traversal must be rejected with NoContainerForUrl for \
         {malicious_location}, got: {read:?}"
    );
    Ok(())
}

// A plain `../` virtual ref cannot escape the authorized container prefix.
#[tokio_test]
async fn test_file_virtual_ref_rejects_dotdot_traversal() -> Result<(), Box<dyn Error>> {
    let (repo, allowed, legit, secret, _parent, _repo_dir) =
        setup_file_traversal_repo().await;
    let location = format!("file://{}/../secret.bin", allowed.to_str().unwrap());
    assert_traversal_rejected_but_legit_works(
        &repo,
        &allowed,
        &legit,
        secret.len() as u64,
        &location,
    )
    .await
}

// A percent-encoded `..` (`%2e%2e`) must not bypass the canonicalization.
#[tokio_test]
async fn test_file_virtual_ref_rejects_encoded_dotdot_traversal()
-> Result<(), Box<dyn Error>> {
    let (repo, allowed, legit, secret, _parent, _repo_dir) =
        setup_file_traversal_repo().await;
    let location = format!("file://{}/%2e%2e/secret.bin", allowed.to_str().unwrap());
    assert_traversal_rejected_but_legit_works(
        &repo,
        &allowed,
        &legit,
        secret.len() as u64,
        &location,
    )
    .await
}

// A `file://` ref with an empty (`//`) path segment matches its container, but
// the object_store local-FS backend cannot address such a key. The GET must
// fail with a clear `UnsupportedObjectKeyForBackend`, not a cryptic error.
#[tokio_test]
async fn test_file_virtual_ref_empty_segment_unsupported_on_read()
-> Result<(), Box<dyn Error>> {
    let (repo, allowed, _legit, _secret, _parent, _repo_dir) =
        setup_file_traversal_repo().await;
    let mut ds = repo.writable_session("main").await?;
    let path: Path = "/a".try_into().unwrap();
    ds.add_array(
        path.clone(),
        ArrayShape::new(vec![(1, 1)]).unwrap(),
        None,
        Bytes::new(),
    )
    .await?;
    let location = format!("file://{}/sub//chunk.bin", allowed.to_str().unwrap());
    ds.set_chunk_ref(
        path.clone(),
        ChunkIndices(vec![0]),
        Some(ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation::from_url(&location)?,
            offset: 0,
            length: 4,
            checksum: None,
        })),
    )
    .await?;
    let reader =
        ds.get_chunk_reader(&path, &ChunkIndices(vec![0]), &ByteRange::ALL).await;
    let read = match reader {
        Ok(reader) => get_chunk(reader).await,
        Err(e) => Err(e),
    };
    assert!(
        matches!(
            &read,
            Err(SessionError {
                kind: SessionErrorKind::VirtualReferenceError(
                    VirtualReferenceErrorKind::UnsupportedObjectKeyForBackend(_)
                ),
                ..
            })
        ),
        "a // key must fail with UnsupportedObjectKeyForBackend, got: {read:?}"
    );
    Ok(())
}

// Validating the container at write time rejects a `file://` key that the
// local-FS object_store backend cannot address, before it is ever stored.
#[tokio_test]
async fn test_file_virtual_ref_empty_segment_rejected_at_write()
-> Result<(), Box<dyn Error>> {
    let (repo, allowed, _legit, _secret, _parent, _repo_dir) =
        setup_file_traversal_repo().await;
    let session = repo.writable_session("main").await?;
    let store = Store::from_session(Arc::new(RwLock::new(session))).await;
    let location = format!("file://{}/sub//chunk.bin", allowed.to_str().unwrap());
    let reference = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&location)?,
        offset: 0,
        length: 4,
        checksum: None,
    };
    // Container validation runs before the ref is stored, so the array need not
    // exist: the unsupported key is what makes this fail.
    let res = store.set_virtual_ref("a/c/0", reference, true).await;
    assert!(
        matches!(
            &res,
            Err(StoreError {
                kind: StoreErrorKind::SessionError(
                    SessionErrorKind::VirtualReferenceError(
                        VirtualReferenceErrorKind::UnsupportedObjectKeyForBackend(_)
                    )
                ),
                ..
            })
        ),
        "a validated write of a // key must be rejected, got: {res:?}"
    );
    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_repository_with_local_virtual_refs(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let chunk_dir = TempDir::new()?;
    let chunk_1 = chunk_dir.path().join("chunk-1").to_str().unwrap().to_owned();
    let chunk_2 = chunk_dir.path().join("chunk-2").to_str().unwrap().to_owned();

    let bytes1 = Bytes::copy_from_slice(b"first");
    let bytes2 = Bytes::copy_from_slice(b"second0000");
    let chunks = [(chunk_1, bytes1.clone()), (chunk_2, bytes2.clone())];
    write_chunks_to_local_fs(chunks.iter().cloned()).await;

    let repo_dir = TempDir::new()?;
    let repo =
        create_local_repository(repo_dir.path(), Some(chunk_dir.path()), spec_version)
            .await;
    let mut ds = repo.writable_session("main").await.unwrap();

    let shape = ArrayShape::new(vec![(1, 1), (1, 1), (2, 2)]).unwrap();
    let user_data = Bytes::new();
    let payload1 = ChunkPayload::Virtual(VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            // intentional extra '/'
            "file://{}",
            chunks[0].0
        ))?,
        offset: 0,
        length: 5,
        checksum: None,
    });
    let payload2 = ChunkPayload::Virtual(VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!("file://{}", chunks[1].0,))?,
        offset: 1,
        length: 5,
        checksum: None,
    });

    let new_array_path: Path = "/array".try_into().unwrap();
    ds.add_array(new_array_path.clone(), shape, None, user_data).await.unwrap();

    ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![0, 0, 0]), Some(payload1))
        .await
        .unwrap();
    ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![0, 0, 1]), Some(payload2))
        .await
        .unwrap();

    assert_eq!(
        get_chunk(
            ds.get_chunk_reader(
                &new_array_path,
                &ChunkIndices(vec![0, 0, 0]),
                &ByteRange::ALL
            )
            .await
            .unwrap()
        )
        .await
        .unwrap(),
        Some(bytes1.clone()),
    );
    assert_eq!(
        get_chunk(
            ds.get_chunk_reader(
                &new_array_path,
                &ChunkIndices(vec![0, 0, 1]),
                &ByteRange::ALL
            )
            .await
            .unwrap()
        )
        .await
        .unwrap(),
        Some(Bytes::copy_from_slice(&bytes2[1..6])),
    );

    for range in [
        ByteRange::bounded(0u64, 3u64),
        ByteRange::from_offset(2u64),
        ByteRange::to_offset(4u64),
    ] {
        assert_eq!(
            get_chunk(
                ds.get_chunk_reader(
                    &new_array_path,
                    &ChunkIndices(vec![0, 0, 0]),
                    &range
                )
                .await
                .unwrap()
            )
            .await
            .unwrap(),
            Some(range.slice(&bytes1))
        );
    }
    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_repository_with_minio_virtual_refs(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let bytes1 = Bytes::copy_from_slice(b"first");
    let bytes2 = Bytes::copy_from_slice(b"second0000");
    let chunks = [
        ("/path/to/chunk-1".into(), bytes1.clone()),
        ("/path with spaces/to/chunk-2".into(), bytes2.clone()),
    ];
    write_chunks_to_minio(chunks.iter().cloned()).await;

    let repo = create_minio_repository(spec_version).await;
    let mut ds = repo.writable_session("main").await.unwrap();

    let shape = ArrayShape::new(vec![(1, 1), (1, 1), (2, 2)]).unwrap();
    let user_data = Bytes::new();
    let payload1 = ChunkPayload::Virtual(VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            "s3://testbucket{}",
            chunks[0].0
        ))?,
        offset: 0,
        length: 5,
        checksum: None,
    });
    let payload2 = ChunkPayload::Virtual(VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            "s3://testbucket{}",
            chunks[1].0,
        ))?,
        offset: 1,
        length: 5,
        checksum: None,
    });

    let new_array_path: Path = "/array".try_into().unwrap();
    ds.add_array(new_array_path.clone(), shape, None, user_data).await.unwrap();

    ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![0, 0, 0]), Some(payload1))
        .await
        .unwrap();
    ds.set_chunk_ref(new_array_path.clone(), ChunkIndices(vec![0, 0, 1]), Some(payload2))
        .await
        .unwrap();

    assert_eq!(
        get_chunk(
            ds.get_chunk_reader(
                &new_array_path,
                &ChunkIndices(vec![0, 0, 0]),
                &ByteRange::ALL
            )
            .await
            .unwrap()
        )
        .await
        .unwrap(),
        Some(bytes1.clone()),
    );
    assert_eq!(
        get_chunk(
            ds.get_chunk_reader(
                &new_array_path,
                &ChunkIndices(vec![0, 0, 1]),
                &ByteRange::ALL
            )
            .await
            .unwrap()
        )
        .await
        .unwrap(),
        Some(Bytes::copy_from_slice(&bytes2[1..6])),
    );

    for range in [
        ByteRange::bounded(0u64, 3u64),
        ByteRange::from_offset(2u64),
        ByteRange::to_offset(4u64),
    ] {
        assert_eq!(
            get_chunk(
                ds.get_chunk_reader(
                    &new_array_path,
                    &ChunkIndices(vec![0, 0, 0]),
                    &range
                )
                .await
                .unwrap()
            )
            .await
            .unwrap(),
            Some(range.slice(&bytes1))
        );
    }

    // check if we can fetch the virtual chunks in multiple small requests
    ds.commit("done").max_concurrent_nodes(8).execute().await?;

    let mut config = repo.config().clone();
    config.storage = Some(storage::Settings {
        concurrency: Some(ConcurrencySettings {
            max_concurrent_requests_for_object: Some(100.try_into()?),
            ideal_concurrent_request_size: Some(1.try_into()?),
        }),
        ..repo.storage().default_settings().await?
    });
    let repo = repo.reopen(Some(config), None).await?;
    assert_eq!(
        repo.config()
            .storage
            .as_ref()
            .unwrap()
            .concurrency
            .as_ref()
            .unwrap()
            .ideal_concurrent_request_size,
        Some(1.try_into()?)
    );
    let session = repo
        .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
        .await
        .unwrap();
    assert_eq!(
        get_chunk(
            session
                .get_chunk_reader(
                    &new_array_path,
                    &ChunkIndices(vec![0, 0, 0]),
                    &ByteRange::ALL
                )
                .await
                .unwrap()
        )
        .await
        .unwrap(),
        Some(bytes1.clone()),
    );
    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_zarr_store_virtual_refs_minio_set_and_get(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let bytes1 = Bytes::copy_from_slice(b"first");
    let bytes2 = Bytes::copy_from_slice(b"second0000");
    let chunks = [
        ("/path/to/chunk-1".into(), bytes1.clone()),
        ("/path with spaces/to/chunk-2".into(), bytes2.clone()),
    ];
    write_chunks_to_minio(chunks.iter().cloned()).await;

    let repo = create_minio_repository(spec_version).await;
    let ds = repo.writable_session("main").await.unwrap();
    let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

    store
        .set(
            "zarr.json",
            Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
        )
        .await?;
    let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
    store.set("array/zarr.json", zarr_meta.clone()).await?;
    assert_eq!(store.get("array/zarr.json", &ByteRange::ALL).await.unwrap(), zarr_meta);

    let ref1 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            "s3://testbucket{}",
            chunks[0].0
        ))?,
        offset: 0,
        length: 5,
        checksum: None,
    };
    let ref2 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            "s3://testbucket{}",
            chunks[1].0
        ))?,
        offset: 1,
        length: 5,
        checksum: None,
    };
    store.set_virtual_ref("array/c/0/0/0", ref1, false).await?;
    store.set_virtual_ref("array/c/0/0/1", ref2, false).await?;

    assert_eq!(store.get("array/c/0/0/0", &ByteRange::ALL).await?, bytes1,);
    assert_eq!(
        store.get("array/c/0/0/1", &ByteRange::ALL).await?,
        Bytes::copy_from_slice(&bytes2[1..6]),
    );

    // it shouldn't let us write to an non existing virtual chunk container
    let bad_location = VirtualChunkLocation::from_url(&format!(
        "bad-protocol://testbucket/{}",
        chunks[1].0
    ))?;

    let bad_ref = VirtualChunkRef {
        location: bad_location.clone(),
        offset: 1,
        length: 5,
        checksum: None,
    };
    assert!(matches!(
                store.set_virtual_ref("array/c/0/0/0", bad_ref, true).await,
                Err(StoreError{kind: StoreErrorKind::InvalidVirtualChunkContainer { chunk_location },..}) if chunk_location == bad_location.url()));
    Ok(())
}

/// A virtual chunk served by an HTTP store on a non-default port must be read
/// from that port. Serve a chunk from a local HTTP server bound to a random
/// (non-80) port and verify the read path honors the port stored in the ref.
/// Regression test for <https://github.com/earth-mover/icechunk/issues/2223>
#[cfg(feature = "object-store-http")]
#[tokio::test]
async fn test_zarr_store_virtual_refs_http_non_default_port() -> Result<(), Box<dyn Error>>
{
    use icechunk::config::HttpConfig;
    use tokio::sync::oneshot;

    let bytes = Bytes::copy_from_slice(b"first");

    // Serve a single chunk file over HTTP from a temp dir.
    let dir = TempDir::new()?;
    tokio::fs::write(dir.path().join("chunk-1"), &bytes).await?;

    let route = warp::fs::dir(dir.path().to_path_buf());
    let (stop, wait) = oneshot::channel();
    let port = port_check::free_local_ipv4_port_in_range(8000..65000).unwrap();
    let server = warp::serve(route).bind(([127, 0, 0, 1], port)).await.graceful(async {
        let _ = wait.await;
    });
    let join = tokio::task::spawn(server.run());

    let url_prefix = format!("http://127.0.0.1:{port}/");
    let container = VirtualChunkContainer::new(
        url_prefix.clone(),
        ObjectStoreConfig::Http(HttpConfig {
            opts: HashMap::from([("allow_http".to_string(), "true".to_string())]),
            headers: HashMap::new(),
        }),
    )?;
    let credentials = HashMap::from([(url_prefix.clone(), None)]);

    let storage = storage::new_in_memory_storage().await?;
    let repo =
        create_repository(storage, vec![container], credentials, SpecVersionBin::V2)
            .await;

    let ds = repo.writable_session("main").await.unwrap();
    let store = Store::from_session(Arc::new(RwLock::new(ds))).await;
    store
        .set(
            "zarr.json",
            Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
        )
        .await?;
    let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
    store.set("array/zarr.json", zarr_meta.clone()).await?;

    let reference = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            "http://127.0.0.1:{port}/chunk-1"
        ))?,
        offset: 0,
        length: 5,
        checksum: None,
    };
    store.set_virtual_ref("array/c/0/0/0", reference, false).await?;

    assert_eq!(store.get("array/c/0/0/0", &ByteRange::ALL).await?, bytes);

    // stop the server
    stop.send(()).unwrap();
    join.await?;
    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_zarr_store_virtual_refs_azure_set_and_get(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let bytes1 = Bytes::copy_from_slice(b"first");
    let bytes2 = Bytes::copy_from_slice(b"second0000");
    let chunks =
        [(ChunkId::random(), bytes1.clone()), (ChunkId::random(), bytes2.clone())];
    let prefix = ChunkId::random().to_string();
    write_chunks_to_azure(prefix.clone(), chunks.iter().cloned()).await;

    let repo = create_minio_repository(spec_version).await;
    let ds = repo.writable_session("main").await.unwrap();
    let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

    store
        .set(
            "zarr.json",
            Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
        )
        .await?;
    let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
    store.set("array/zarr.json", zarr_meta.clone()).await?;
    assert_eq!(store.get("array/zarr.json", &ByteRange::ALL).await.unwrap(), zarr_meta);

    let ref1 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            "az://testcontainer/{prefix}/chunks/{}",
            chunks[0].0
        ))?,
        offset: 0,
        length: 5,
        checksum: None,
    };
    let ref2 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            "az://testcontainer/{prefix}/chunks/{}",
            chunks[1].0
        ))?,
        offset: 1,
        length: 5,
        checksum: None,
    };
    store.set_virtual_ref("array/c/0/0/0", ref1, false).await?;
    store.set_virtual_ref("array/c/0/0/1", ref2, false).await?;

    assert_eq!(store.get("array/c/0/0/0", &ByteRange::ALL).await?, bytes1,);
    assert_eq!(
        store.get("array/c/0/0/1", &ByteRange::ALL).await?,
        Bytes::copy_from_slice(&bytes2[1..6]),
    );

    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_zarr_store_virtual_refs_from_public_s3(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let repo_dir = TempDir::new()?;
    let repo = create_local_repository(repo_dir.path(), None, spec_version).await;
    let ds = repo.writable_session("main").await.unwrap();

    let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

    store
        .set(
            "zarr.json",
            Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
        )
        .await
        .unwrap();

    let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[72],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[72]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value": 0.0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[],"dimension_names":["year"]}"#);
    store.set("year/zarr.json", zarr_meta.clone()).await.unwrap();

    let ref2 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "s3://earthmover-sample-data/netcdf/oscar_vel2018.nc",
        )?,
        offset: 22306,
        length: 288,
        checksum: None,
    };

    store.set_virtual_ref("year/c/0", ref2, false).await?;

    let chunk = store.get("year/c/0", &ByteRange::ALL).await.unwrap();
    assert_eq!(chunk.len(), 288);

    let second_year = f32::from_le_bytes(chunk[4..8].try_into().unwrap());
    assert!(second_year - 2018.0139 < 0.000001);

    let last_year = f32::from_le_bytes(chunk[(288 - 4)..].try_into().unwrap());
    assert!(last_year - 2018.9861 < 0.000001);
    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_zarr_store_virtual_refs_from_public_gcs(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let repo_dir = TempDir::new()?;
    let repo = create_local_repository(repo_dir.path(), None, spec_version).await;
    let ds = repo.writable_session("main").await.unwrap();

    let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

    store
        .set(
            "zarr.json",
            Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
        )
        .await
        .unwrap();

    let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[72],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value": 0.0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[],"dimension_names":["year"]}"#);
    store.set("year/zarr.json", zarr_meta.clone()).await.unwrap();

    let ref1 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "gcs://al-public-test-bucket/netcdf_test_echam_spectral.nc",
        )?,
        offset: 22306,
        length: 288,
        checksum: None,
    };

    let ref2 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "gcs://gcp-public-data-arco-era5/ar/1959-2022-1h-240x121_equiangular_with_poles_conservative.zarr/2m_temperature/0.0.0",
        )?,
        offset: 223,
        length: 400,
        checksum: None,
    };

    let ref3 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "gcs://gcp-public-data-arco-era5/ar/1959-2022-1h-240x121_equiangular_with_poles_conservative.zarr/2m_temperature/1.0.0",
        )?,
        offset: 0,
        length: 100,
        checksum: None,
    };

    let ref_expired = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "gcs://al-public-test-bucket/netcdf_test_echam_spectral.nc",
        )?,
        offset: 22306,
        length: 288,
        checksum: Some(Checksum::LastModified(SecondsSinceEpoch(3600))),
    };
    let ref_bad_tag = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "gcs://al-public-test-bucket/netcdf_test_echam_spectral.nc",
        )?,
        offset: 22306,
        length: 288,
        checksum: Some(Checksum::ETag(ETag("bad".to_string()))),
    };

    store.set_virtual_ref("year/c/0", ref1, false).await?;
    store.set_virtual_ref("year/c/1", ref2, false).await?;
    store.set_virtual_ref("year/c/2", ref3, false).await?;
    store.set_virtual_ref("year/c/3", ref_expired, false).await?;
    store.set_virtual_ref("year/c/4", ref_bad_tag, false).await?;

    let chunk = store.get("year/c/0", &ByteRange::ALL).await.unwrap();
    assert_eq!(chunk.len(), 288);
    let chunk = store.get("year/c/1", &ByteRange::ALL).await.unwrap();
    assert_eq!(chunk.len(), 400);
    let chunk = store.get("year/c/2", &ByteRange::ALL).await.unwrap();
    assert_eq!(chunk.len(), 100);

    assert!(store.get("year/c/3", &ByteRange::ALL).await.is_err());
    assert!(store.get("year/c/4", &ByteRange::ALL).await.is_err());
    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_zarr_store_virtual_refs_from_public_azure(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let repo_dir = TempDir::new()?;
    let repo = create_local_repository(repo_dir.path(), None, spec_version).await;
    let ds = repo.writable_session("main").await.unwrap();

    let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

    store
        .set(
            "zarr.json",
            Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
        )
        .await
        .unwrap();

    let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{},"shape":[8],"data_type":"uint8","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[8]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value": 0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[],"dimension_names":["x"]}"#);
    store.set("magic/zarr.json", zarr_meta.clone()).await.unwrap();

    // This is a permanent public dataset: the NOAA Sea Surface Temperature WHOI
    // Climate Data Record, hosted anonymously on Azure Blob Storage. See
    // https://planetarycomputer.microsoft.com/dataset/noaa-cdr-sea-surface-temperature-whoi
    let public_ref = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "az://sea-surface-temp-whoi/data/1988/SEAFLUX-OSB-CDR_V02R00_SST_D19880101_C20160820.nc",
        )?,
        offset: 0,
        length: 8,
        checksum: None,
    };

    store.set_virtual_ref("magic/c/0", public_ref, false).await?;

    let chunk = store.get("magic/c/0", &ByteRange::ALL).await.unwrap();
    // The first 8 bytes of these netCDF4 files are the HDF5 magic signature.
    assert_eq!(chunk.as_ref(), b"\x89HDF\r\n\x1a\n");
    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_zarr_store_with_multiple_virtual_chunk_containers(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    // we create a repository with 3 virtual chunk containers: one for minio chunks, one for
    // local filesystem chunks and one for chunks in a public S3 bucket.

    let prefix = format!("{:?}", ChunkId::random());
    let (config, credentials) = minio_s3_config();
    let storage: Arc<dyn Storage + Send + Sync> = new_s3_storage(
        config,
        "testbucket".to_string(),
        Some(prefix),
        Some(credentials),
        None,
    )
    .expect("Creating minio storage failed");

    let chunk_dir = TempDir::new()?;

    let containers = vec![
        VirtualChunkContainer::new(
            "s3://testbucket/".to_string(),
            ObjectStoreConfig::S3Compatible(
                S3Options::default()
                    .with_region("us-east-1")
                    .with_endpoint_url("http://localhost:4200")
                    .with_allow_http(true)
                    .with_force_path_style(true),
            ),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            format!("file://{}/", chunk_dir.path().to_str().unwrap()),
            ObjectStoreConfig::LocalFileSystem(PathBuf::new()),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "s3://earthmover-sample-data/".to_string(),
            ObjectStoreConfig::S3(
                S3Options::default().with_region("us-east-1").with_anonymous(true),
            ),
        )
        .unwrap(),
    ];

    let virtual_creds = HashMap::from([
        (
            "s3://testbucket".to_string(),
            Some(Credentials::S3(S3Credentials::Static(S3StaticCredentials {
                access_key_id: "test123".to_string(),
                secret_access_key: "test123".to_string(),
                session_token: None,
                expires_after: None,
            }))),
        ),
        (format!("file://{}", chunk_dir.path().to_str().unwrap()), None),
        (
            "s3://earthmover-sample-data".to_string(),
            Some(Credentials::S3(S3Credentials::Anonymous)),
        ),
    ]);

    let mut config = RepositoryConfig::default();
    for container in containers {
        config.set_virtual_chunk_container(container).unwrap();
    }

    let repo = Repository::create(
        Some(config),
        storage,
        virtual_creds,
        Some(spec_version),
        true,
    )
    .await?;

    let old_timestamp = SecondsSinceEpoch(chrono::Utc::now().timestamp() as u32 - 5);

    let minio_bytes1 = Bytes::copy_from_slice(b"first");
    let minio_bytes2 = Bytes::copy_from_slice(b"second0000");
    let minio_bytes3 = Bytes::copy_from_slice(b"modified");
    let chunks = [
        ("/path/to/chunk-1".into(), minio_bytes1.clone()),
        ("/path with spaces/to/chunk-2".into(), minio_bytes2.clone()),
        ("/path/to/chunk-3".into(), minio_bytes3.clone()),
    ];
    write_chunks_to_minio(chunks.iter().cloned()).await;

    let session = repo.writable_session("main").await?;
    let store = Store::from_session(Arc::new(RwLock::new(session))).await;

    store
        .set(
            "zarr.json",
            Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
        )
        .await?;
    let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[4,4,4],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
    store.set("array/zarr.json", zarr_meta.clone()).await?;

    // set virtual refs in minio
    let ref1 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            "s3://testbucket{}",
            chunks[0].0
        ))?,
        offset: 0,
        length: 5,
        checksum: None,
    };
    let ref2 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            "s3://testbucket{}",
            chunks[1].0
        ))?,
        offset: 1,
        length: 5,
        checksum: None,
    };
    let ref3 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            "s3://testbucket{}",
            chunks[2].0
        ))?,
        offset: 1,
        length: 5,
        checksum: Some(Checksum::LastModified(old_timestamp)),
    };
    store.set_virtual_ref("array/c/0/0/0", ref1, false).await?;
    store.set_virtual_ref("array/c/0/0/1", ref2, false).await?;
    store.set_virtual_ref("array/c/1/0/0", ref3, false).await?;

    // set virtual refs in local filesystem
    let chunk_1 = chunk_dir.path().join("chunk-1").to_str().unwrap().to_owned();
    let chunk_2 = chunk_dir.path().join("chunk-2").to_str().unwrap().to_owned();
    let chunk_3 = chunk_dir.path().join("chunk-3").to_str().unwrap().to_owned();

    let local_bytes1 = Bytes::copy_from_slice(b"first");
    let local_bytes2 = Bytes::copy_from_slice(b"second0000");
    let local_bytes3 = Bytes::copy_from_slice(b"modified");
    let local_chunks = [
        (chunk_1, local_bytes1.clone()),
        (chunk_2, local_bytes2.clone()),
        (chunk_3, local_bytes3.clone()),
    ];
    write_chunks_to_local_fs(local_chunks.iter().cloned()).await;

    let ref1 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            // intentional extra '/'
            "file://{}",
            local_chunks[0].0
        ))?,
        offset: 0,
        length: 5,
        checksum: None,
    };
    let ref2 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            "file://{}",
            local_chunks[1].0,
        ))?,
        offset: 1,
        length: 5,
        checksum: None,
    };
    let ref3 = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(&format!(
            "file://{}",
            local_chunks[2].0,
        ))?,
        offset: 1,
        length: 5,
        checksum: Some(Checksum::ETag(ETag(String::from("invalid etag")))),
    };

    store.set_virtual_ref("array/c/0/0/2", ref1, false).await?;
    store.set_virtual_ref("array/c/0/0/3", ref2, false).await?;
    store.set_virtual_ref("array/c/1/0/1", ref3, false).await?;

    // set a virtual ref in a public bucket
    let public_ref = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "s3://earthmover-sample-data/netcdf/oscar_vel2018.nc",
        )?,
        offset: 22306,
        length: 288,
        checksum: None,
    };
    let public_modified_ref = VirtualChunkRef {
        location: VirtualChunkLocation::from_url(
            "s3://earthmover-sample-data/netcdf/oscar_vel2018.nc",
        )?,
        offset: 22306,
        length: 288,
        checksum: Some(Checksum::ETag(ETag(String::from("invalid etag")))),
    };

    store.set_virtual_ref("array/c/1/1/1", public_ref, false).await?;
    store.set_virtual_ref("array/c/1/1/2", public_modified_ref, false).await?;

    // assert we can find all the virtual chunks

    // these are the minio chunks
    assert_eq!(store.get("array/c/0/0/0", &ByteRange::ALL).await?, minio_bytes1,);
    assert_eq!(
        store.get("array/c/0/0/1", &ByteRange::ALL).await?,
        Bytes::copy_from_slice(&minio_bytes2[1..6]),
    );
    // try to fetch the modified chunk
    assert!(matches!(
        store.get("array/c/1/0/0", &ByteRange::ALL).await,
        Err(StoreError{kind: StoreErrorKind::SessionError(SessionErrorKind::VirtualReferenceError(
            VirtualReferenceErrorKind::ObjectModified(location)
        )),..}) if location == "s3://testbucket/path/to/chunk-3"
    ));

    // these are the local file chunks
    assert_eq!(store.get("array/c/0/0/2", &ByteRange::ALL).await?, local_bytes1,);
    assert_eq!(
        store.get("array/c/0/0/3", &ByteRange::ALL).await?,
        Bytes::copy_from_slice(&local_bytes2[1..6]),
    );
    // try to fetch the modified chunk
    assert!(matches!(
        store.get("array/c/1/0/1", &ByteRange::ALL).await,
        Err(StoreError{kind: StoreErrorKind::SessionError(SessionErrorKind::VirtualReferenceError(
            VirtualReferenceErrorKind::ObjectModified(location)
        )),..}) if location.contains("chunk-3")
    ));

    // these are the public bucket chunks
    let chunk = store.get("array/c/1/1/1", &ByteRange::ALL).await.unwrap();
    assert_eq!(chunk.len(), 288);

    let second_year = f32::from_le_bytes(chunk[4..8].try_into().unwrap());
    assert!(second_year - 2018.0139 < 0.000001);

    let last_year = f32::from_le_bytes(chunk[(288 - 4)..].try_into().unwrap());
    assert!(last_year - 2018.9861 < 0.000001);

    // try to fetch the modified chunk
    assert!(matches!(
        store.get("array/c/1/1/2", &ByteRange::ALL).await,
        Err(StoreError{kind: StoreErrorKind::SessionError(SessionErrorKind::VirtualReferenceError(
            VirtualReferenceErrorKind::ObjectModified(location)
        )),..}) if location == "s3://earthmover-sample-data/netcdf/oscar_vel2018.nc"
    ));

    let session = store.session();
    let locations = session
        .read()
        .await
        .all_virtual_chunk_locations()
        .await?
        .try_collect::<HashSet<_>>()
        .await?;
    assert_eq!(
        locations,
        [
            "s3://earthmover-sample-data/netcdf/oscar_vel2018.nc".to_string(),
            "s3://testbucket/path/to/chunk-1".to_string(),
            // Stored verbatim now: the literal space is no longer re-encoded to
            // `%20` by `url::Url::parse` (object keys are opaque).
            "s3://testbucket/path with spaces/to/chunk-2".to_string(),
            "s3://testbucket/path/to/chunk-3".to_string(),
            format!("file://{}", local_chunks[0].0),
            format!("file://{}", local_chunks[1].0),
            format!("file://{}", local_chunks[2].0),
        ]
        .into()
    );

    Ok(())
}

#[tokio_test]
#[apply(spec_version_cases)]
async fn test_virtual_refs_with_vcc_relative_urls(
    #[case] spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    // Create local chunks on disk
    let chunk_dir = TempDir::new()?;
    let chunk_1 = chunk_dir.path().join("chunk-1").to_str().unwrap().to_owned();
    let chunk_2 = chunk_dir.path().join("chunk-2").to_str().unwrap().to_owned();

    let bytes1 = Bytes::copy_from_slice(b"first");
    let bytes2 = Bytes::copy_from_slice(b"second0000");
    let chunks = [(chunk_1.clone(), bytes1.clone()), (chunk_2.clone(), bytes2.clone())];
    write_chunks_to_local_fs(chunks.iter().cloned()).await;

    // Create a repo with a named virtual chunk container
    let repo_dir = TempDir::new()?;
    let prefix = format!("file://{}/", chunk_dir.path().to_str().unwrap());

    let storage: Arc<dyn Storage + Send + Sync> = Arc::new(
        ObjectStorage::new_local_filesystem(repo_dir.path())
            .await
            .expect("Creating local storage failed"),
    );

    let container = VirtualChunkContainer::new_named(
        "local-data".to_string(),
        prefix.clone(),
        ObjectStoreConfig::LocalFileSystem(PathBuf::new()),
    )
    .unwrap();

    let mut config = RepositoryConfig::default();
    config.set_virtual_chunk_container(container).unwrap();

    let creds: HashMap<String, Option<Credentials>> =
        [(format!("file://{}", chunk_dir.path().to_str().unwrap()), None)].into();

    let repo = Repository::create(Some(config), storage, creds, Some(spec_version), true)
        .await?;

    let session = repo.writable_session("main").await?;
    let store = Store::from_session(Arc::new(RwLock::new(session))).await;

    store
        .set(
            "zarr.json",
            Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
        )
        .await?;
    let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
    store.set("array/zarr.json", zarr_meta.clone()).await?;

    // Write virtual refs using vcc:// relative URLs
    let ref1 = VirtualChunkRef {
        location: VirtualChunkLocation::from_vcc_path("local-data", "chunk-1")?,
        offset: 0,
        length: 5,
        checksum: None,
    };
    let ref2 = VirtualChunkRef {
        location: VirtualChunkLocation::from_vcc_path("local-data", "chunk-2")?,
        offset: 1,
        length: 5,
        checksum: None,
    };

    // Verify the locations are relative
    assert!(ref1.location.is_relative());
    assert!(ref2.location.is_relative());
    assert_eq!(ref1.location.parse_vcc(), Some(("local-data", "chunk-1")));
    assert_eq!(ref2.location.parse_vcc(), Some(("local-data", "chunk-2")));

    store.set_virtual_ref("array/c/0/0/0", ref1, false).await?;
    store.set_virtual_ref("array/c/0/0/1", ref2, false).await?;

    // Read back and verify correct bytes
    assert_eq!(store.get("array/c/0/0/0", &ByteRange::ALL).await?, bytes1);
    assert_eq!(
        store.get("array/c/0/0/1", &ByteRange::ALL).await?,
        Bytes::copy_from_slice(&bytes2[1..6]),
    );

    // Verify byte range reads work with vcc:// refs
    for range in [
        ByteRange::bounded(0u64, 3u64),
        ByteRange::from_offset(2u64),
        ByteRange::to_offset(4u64),
    ] {
        assert_eq!(
            get_chunk(
                store
                    .session()
                    .read()
                    .await
                    .get_chunk_reader(
                        &"/array".try_into().unwrap(),
                        &ChunkIndices(vec![0, 0, 0]),
                        &range,
                    )
                    .await
                    .unwrap()
            )
            .await
            .unwrap(),
            Some(range.slice(&bytes1))
        );
    }

    // Commit and verify all_virtual_chunk_locations returns expanded absolute URLs
    store
        .session()
        .write()
        .await
        .commit("vcc test")
        .max_concurrent_nodes(8)
        .execute()
        .await?;

    let session =
        repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;

    let locations =
        session.all_virtual_chunk_locations().await?.try_collect::<HashSet<_>>().await?;

    // all_virtual_chunk_locations should expand vcc:// to absolute URLs
    assert_eq!(
        locations,
        [
            format!("file://{}/chunk-1", chunk_dir.path().to_str().unwrap()),
            format!("file://{}/chunk-2", chunk_dir.path().to_str().unwrap()),
        ]
        .into()
    );

    Ok(())
}

// A `vcc://` ref resolving to a `file://` container must not escape it via `..`.
// The relative path is stored verbatim (it is an opaque key suffix), so the only
// place that can enforce confinement is READ: once the location is expanded and
// WHATWG-normalized, the escaped path no longer matches the container prefix.
#[tokio_test]
async fn test_vcc_relative_ref_rejects_file_traversal() -> Result<(), Box<dyn Error>> {
    let parent = TempDir::new()?;
    let allowed = parent.path().join("allowed");
    std::fs::create_dir_all(&allowed)?;
    let secret = Bytes::copy_from_slice(b"TOP-SECRET-CONTENTS");
    std::fs::write(parent.path().join("secret.bin"), &secret)?;

    let repo_dir = TempDir::new()?;
    let storage: Arc<dyn Storage + Send + Sync> = Arc::new(
        ObjectStorage::new_local_filesystem(repo_dir.path())
            .await
            .expect("Creating local storage failed"),
    );
    let prefix = format!("file://{}/", allowed.to_str().unwrap());
    let container = VirtualChunkContainer::new_named(
        "local-data".to_string(),
        prefix.clone(),
        ObjectStoreConfig::LocalFileSystem(PathBuf::new()),
    )?;
    let mut config = RepositoryConfig::default();
    config.set_virtual_chunk_container(container)?;
    let creds: HashMap<String, Option<Credentials>> = [(prefix, None)].into();
    let repo =
        Repository::create(Some(config), storage, creds, Some(SpecVersionBin::V2), true)
            .await?;

    let mut ds = repo.writable_session("main").await?;
    let path: Path = "/a".try_into().unwrap();
    ds.add_array(
        path.clone(),
        ArrayShape::new(vec![(1, 1)]).unwrap(),
        None,
        Bytes::new(),
    )
    .await?;

    // The vcc relative path escapes the container via `..`; it is stored verbatim.
    let location = VirtualChunkLocation::from_url("vcc://local-data/../secret.bin")?;
    assert!(location.is_relative());
    ds.set_chunk_ref(
        path.clone(),
        ChunkIndices(vec![0]),
        Some(ChunkPayload::Virtual(VirtualChunkRef {
            location,
            offset: 0,
            length: secret.len() as u64,
            checksum: None,
        })),
    )
    .await?;

    let reader =
        ds.get_chunk_reader(&path, &ChunkIndices(vec![0]), &ByteRange::ALL).await;
    let read = match reader {
        Ok(reader) => get_chunk(reader).await,
        Err(e) => Err(e),
    };
    assert!(
        matches!(
            &read,
            Err(SessionError {
                kind: SessionErrorKind::VirtualReferenceError(
                    VirtualReferenceErrorKind::NoContainerForUrl(_)
                ),
                ..
            })
        ),
        "vcc://->file traversal must be rejected with NoContainerForUrl, got: {read:?}"
    );
    Ok(())
}
