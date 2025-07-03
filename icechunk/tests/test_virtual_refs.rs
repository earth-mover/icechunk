#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::expect_fun_call
)]
use futures::TryStreamExt;
use icechunk::{
    ObjectStoreConfig, Repository, RepositoryConfig, Storage, Store,
    config::{
        Credentials, GcsCredentials, S3Credentials, S3Options, S3StaticCredentials,
    },
    format::{
        ByteRange, ChunkId, ChunkIndices, Path,
        manifest::{
            Checksum, ChunkPayload, SecondsSinceEpoch, VirtualChunkLocation,
            VirtualChunkRef, VirtualReferenceErrorKind,
        },
        snapshot::ArrayShape,
    },
    repository::VersionInfo,
    session::{SessionErrorKind, get_chunk},
    storage::{
        self, ConcurrencySettings, ETag, ObjectStorage, new_s3_storage, s3::mk_client,
    },
    store::{StoreError, StoreErrorKind},
    virtual_chunks::VirtualChunkContainer,
};
use icechunk_macros::tokio_test;
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
use object_store::{
    ObjectStore, PutMode, PutOptions, PutPayload, local::LocalFileSystem,
};
use pretty_assertions::assert_eq;

fn minio_s3_config() -> (S3Options, S3Credentials) {
    let config = S3Options {
        region: Some("us-east-1".to_string()),
        endpoint_url: Some("http://localhost:9000".to_string()),
        allow_http: true,
        anonymous: false,
        force_path_style: true,
    };
    let credentials = S3Credentials::Static(S3StaticCredentials {
        access_key_id: "minio123".into(),
        secret_access_key: "minio123".into(),
        session_token: None,
        expires_after: None,
    });
    (config, credentials)
}

async fn create_repository(
    storage: Arc<dyn Storage + Send + Sync>,
    virtual_chunk_containers: Vec<VirtualChunkContainer>,
    credentials: HashMap<String, Option<Credentials>>,
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
) -> Repository {
    let storage: Arc<dyn Storage + Send + Sync> = Arc::new(
        ObjectStorage::new_local_filesystem(repo_path)
            .await
            .expect("Creating local storage failed"),
    );

    let mut containers = vec![
        VirtualChunkContainer::new(
            "s3://testbucket".to_string(),
            ObjectStoreConfig::S3(S3Options {
                region: Some("us-east-1".to_string()),
                endpoint_url: None,
                anonymous: true,
                allow_http: false,
                force_path_style: false,
            }),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "s3://earthmover-sample-data".to_string(),
            ObjectStoreConfig::S3(S3Options {
                region: Some("us-east-1".to_string()),
                endpoint_url: None,
                anonymous: true,
                allow_http: false,
                force_path_style: false,
            }),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "gcs://testbucket".to_string(),
            ObjectStoreConfig::Gcs(Default::default()),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "gcs://earthmover-sample-data".to_string(),
            ObjectStoreConfig::Gcs(Default::default()),
        )
        .unwrap(),
    ];

    let mut creds: HashMap<_, Option<Credentials>> = [
        ("s3://testbucket".to_string(), None),
        ("gcs://testbucket".to_string(), None),
        (
            "s3://earthmover-sample-data".to_string(),
            Some(Credentials::S3(S3Credentials::Anonymous)),
        ),
        (
            "gcs://earthmover-sample-data".to_string(),
            Some(Credentials::Gcs(GcsCredentials::Anonymous)),
        ),
    ]
    .into();

    if let Some(chunks_path) = chunks_path {
        let prefix = format!("file://{}", chunks_path.to_str().unwrap());
        containers.push(
            VirtualChunkContainer::new(
                prefix.clone(),
                ObjectStoreConfig::LocalFileSystem(PathBuf::new()),
            )
            .unwrap(),
        );
        creds.insert(prefix, None);
    }

    create_repository(storage, containers, creds).await
}

async fn create_minio_repository() -> Repository {
    let prefix = format!("{:?}", ChunkId::random());
    let (config, credentials) = minio_s3_config();
    let storage: Arc<dyn Storage + Send + Sync> =
        new_s3_storage(config, "testbucket".to_string(), Some(prefix), Some(credentials))
            .expect("Creating minio storage failed");

    let containers = vec![
        VirtualChunkContainer::new(
            "s3://testbucket".to_string(),
            ObjectStoreConfig::S3Compatible(S3Options {
                region: Some(String::from("us-east-1")),
                endpoint_url: Some("http://localhost:9000".to_string()),
                anonymous: false,
                allow_http: true,
                force_path_style: true,
            }),
        )
        .unwrap(),
    ];

    let credentials = [(
        "s3://testbucket".to_string(),
        Some(Credentials::S3(S3Credentials::Static(S3StaticCredentials {
            access_key_id: "minio123".to_string(),
            secret_access_key: "minio123".to_string(),
            session_token: None,
            expires_after: None,
        }))),
    )]
    .into();

    create_repository(storage, containers, credentials).await
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
            .key(key)
            .body(bytes.into())
            .send()
            .await
            .unwrap();
    }
}

#[tokio_test]
async fn test_repository_with_local_virtual_refs() -> Result<(), Box<dyn Error>> {
    let chunk_dir = TempDir::new()?;
    let chunk_1 = chunk_dir.path().join("chunk-1").to_str().unwrap().to_owned();
    let chunk_2 = chunk_dir.path().join("chunk-2").to_str().unwrap().to_owned();

    let bytes1 = Bytes::copy_from_slice(b"first");
    let bytes2 = Bytes::copy_from_slice(b"second0000");
    let chunks = [(chunk_1, bytes1.clone()), (chunk_2, bytes2.clone())];
    write_chunks_to_local_fs(chunks.iter().cloned()).await;

    let repo_dir = TempDir::new()?;
    let repo = create_local_repository(repo_dir.path(), Some(chunk_dir.path())).await;
    let mut ds = repo.writable_session("main").await.unwrap();

    let shape = ArrayShape::new(vec![(1, 1), (1, 1), (2, 1)]).unwrap();
    let user_data = Bytes::new();
    let payload1 = ChunkPayload::Virtual(VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(&format!(
            // intentional extra '/'
            "file://{}",
            chunks[0].0
        ))?,
        offset: 0,
        length: 5,
        checksum: None,
    });
    let payload2 = ChunkPayload::Virtual(VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(&format!(
            "file://{}",
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
            Some(range.slice(bytes1.clone()))
        );
    }
    Ok(())
}

#[tokio_test]
async fn test_repository_with_minio_virtual_refs() -> Result<(), Box<dyn Error>> {
    let bytes1 = Bytes::copy_from_slice(b"first");
    let bytes2 = Bytes::copy_from_slice(b"second0000");
    let chunks = [
        ("/path/to/chunk-1".into(), bytes1.clone()),
        ("/path/to/chunk-2".into(), bytes2.clone()),
    ];
    write_chunks_to_minio(chunks.iter().cloned()).await;

    let repo = create_minio_repository().await;
    let mut ds = repo.writable_session("main").await.unwrap();

    let shape = ArrayShape::new(vec![(1, 1), (1, 1), (2, 1)]).unwrap();
    let user_data = Bytes::new();
    let payload1 = ChunkPayload::Virtual(VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(&format!(
            // intentional extra '/'
            "s3://testbucket///{}",
            chunks[0].0
        ))?,
        offset: 0,
        length: 5,
        checksum: None,
    });
    let payload2 = ChunkPayload::Virtual(VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(&format!(
            "s3://testbucket/{}",
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
            Some(range.slice(bytes1.clone()))
        );
    }

    // check if we can fetch the virtual chunks in multiple small requests
    ds.commit("done", None).await?;

    let mut config = repo.config().clone();
    config.storage = Some(storage::Settings {
        concurrency: Some(ConcurrencySettings {
            max_concurrent_requests_for_object: Some(100.try_into()?),
            ideal_concurrent_request_size: Some(1.try_into()?),
        }),
        ..repo.storage().default_settings()
    });
    let repo = repo.reopen(Some(config), None)?;
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
async fn test_zarr_store_virtual_refs_minio_set_and_get()
-> Result<(), Box<dyn std::error::Error>> {
    let bytes1 = Bytes::copy_from_slice(b"first");
    let bytes2 = Bytes::copy_from_slice(b"second0000");
    let chunks = [
        ("/path/to/chunk-1".into(), bytes1.clone()),
        ("/path/to/chunk-2".into(), bytes2.clone()),
    ];
    write_chunks_to_minio(chunks.iter().cloned()).await;

    let repo = create_minio_repository().await;
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
        location: VirtualChunkLocation::from_absolute_path(&format!(
            // intentional extra '/'
            "s3://testbucket///{}",
            chunks[0].0
        ))?,
        offset: 0,
        length: 5,
        checksum: None,
    };
    let ref2 = VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(&format!(
            "s3://testbucket/{}",
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
    let bad_location = VirtualChunkLocation::from_absolute_path(&format!(
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
                Err(StoreError{kind: StoreErrorKind::InvalidVirtualChunkContainer { chunk_location },..}) if chunk_location == bad_location.0));
    Ok(())
}

#[tokio_test]
async fn test_zarr_store_virtual_refs_from_public_s3()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_dir = TempDir::new()?;
    let repo = create_local_repository(repo_dir.path(), None).await;
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
        location: VirtualChunkLocation::from_absolute_path(
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
async fn test_zarr_store_virtual_refs_from_public_gcs()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_dir = TempDir::new()?;
    let repo = create_local_repository(repo_dir.path(), None).await;
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
        location: VirtualChunkLocation::from_absolute_path(
            "gcs://earthmover-sample-data/netcdf/test_echam_spectral.nc",
        )?,
        offset: 22306,
        length: 288,
        checksum: None,
    };

    let ref2 = VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(
            "gcs://gcp-public-data-arco-era5/ar/1959-2022-1h-240x121_equiangular_with_poles_conservative.zarr/2m_temperature/0.0.0",
        )?,
        offset: 223,
        length: 400,
        checksum: None,
    };

    let ref3 = VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(
            "gcs://gcp-public-data-arco-era5/ar/1959-2022-1h-240x121_equiangular_with_poles_conservative.zarr/2m_temperature/1.0.0",
        )?,
        offset: 0,
        length: 100,
        checksum: None,
    };

    let ref_expired = VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(
            "gcs://earthmover-sample-data/netcdf/test_echam_spectral.nc",
        )?,
        offset: 22306,
        length: 288,
        checksum: Some(Checksum::LastModified(SecondsSinceEpoch(3600))),
    };
    let ref_bad_tag = VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(
            "gcs://earthmover-sample-data/netcdf/test_echam_spectral.nc",
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

    // FIXME: enable this once object_store can access public buckets without credentials
    // otherwise we get an error in GHA
    if false {
        let chunk = store.get("year/c/0", &ByteRange::ALL).await.unwrap();
        assert_eq!(chunk.len(), 288);
        let chunk = store.get("year/c/1", &ByteRange::ALL).await.unwrap();
        assert_eq!(chunk.len(), 400);
        let chunk = store.get("year/c/2", &ByteRange::ALL).await.unwrap();
        assert_eq!(chunk.len(), 100);

        assert!(store.get("year/c/3", &ByteRange::ALL).await.is_err());
        assert!(store.get("year/c/4", &ByteRange::ALL).await.is_err());
    }
    Ok(())
}

#[tokio_test]
async fn test_zarr_store_with_multiple_virtual_chunk_containers()
-> Result<(), Box<dyn std::error::Error>> {
    // we create a repository with 3 virtual chunk containers: one for minio chunks, one for
    // local filesystem chunks and one for chunks in a public S3 bucket.

    let prefix = format!("{:?}", ChunkId::random());
    let (config, credentials) = minio_s3_config();
    let storage: Arc<dyn Storage + Send + Sync> =
        new_s3_storage(config, "testbucket".to_string(), Some(prefix), Some(credentials))
            .expect("Creating minio storage failed");

    let chunk_dir = TempDir::new()?;

    let containers = vec![
        VirtualChunkContainer::new(
            "s3://testbucket".to_string(),
            ObjectStoreConfig::S3Compatible(S3Options {
                region: Some(String::from("us-east-1")),
                endpoint_url: Some("http://localhost:9000".to_string()),
                anonymous: false,
                allow_http: true,
                force_path_style: true,
            }),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            format!("file://{}", chunk_dir.path().to_str().unwrap()),
            ObjectStoreConfig::LocalFileSystem(PathBuf::new()),
        )
        .unwrap(),
        VirtualChunkContainer::new(
            "s3://earthmover-sample-data".to_string(),
            ObjectStoreConfig::S3(S3Options {
                region: Some(String::from("us-east-1")),
                endpoint_url: None,
                anonymous: true,
                allow_http: false,
                force_path_style: false,
            }),
        )
        .unwrap(),
    ];

    let virtual_creds = HashMap::from([
        (
            "s3://testbucket".to_string(),
            Some(Credentials::S3(S3Credentials::Static(S3StaticCredentials {
                access_key_id: "minio123".to_string(),
                secret_access_key: "minio123".to_string(),
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
        config.set_virtual_chunk_container(container);
    }

    let repo = Repository::create(Some(config), storage, virtual_creds).await?;

    let old_timestamp = SecondsSinceEpoch(chrono::Utc::now().timestamp() as u32 - 5);

    let minio_bytes1 = Bytes::copy_from_slice(b"first");
    let minio_bytes2 = Bytes::copy_from_slice(b"second0000");
    let minio_bytes3 = Bytes::copy_from_slice(b"modified");
    let chunks = [
        ("/path/to/chunk-1".into(), minio_bytes1.clone()),
        ("/path/to/chunk-2".into(), minio_bytes2.clone()),
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
        location: VirtualChunkLocation::from_absolute_path(&format!(
            // intentional extra '/'
            "s3://testbucket///{}",
            chunks[0].0
        ))?,
        offset: 0,
        length: 5,
        checksum: None,
    };
    let ref2 = VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(&format!(
            "s3://testbucket/{}",
            chunks[1].0
        ))?,
        offset: 1,
        length: 5,
        checksum: None,
    };
    let ref3 = VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(&format!(
            "s3://testbucket/{}",
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
        location: VirtualChunkLocation::from_absolute_path(&format!(
            // intentional extra '/'
            "file://{}",
            local_chunks[0].0
        ))?,
        offset: 0,
        length: 5,
        checksum: None,
    };
    let ref2 = VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(&format!(
            "file://{}",
            local_chunks[1].0,
        ))?,
        offset: 1,
        length: 5,
        checksum: None,
    };
    let ref3 = VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(&format!(
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
        location: VirtualChunkLocation::from_absolute_path(
            "s3://earthmover-sample-data/netcdf/oscar_vel2018.nc",
        )?,
        offset: 22306,
        length: 288,
        checksum: None,
    };
    let public_modified_ref = VirtualChunkRef {
        location: VirtualChunkLocation::from_absolute_path(
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
            "s3://testbucket/path/to/chunk-2".to_string(),
            "s3://testbucket/path/to/chunk-3".to_string(),
            format!("file://{}", local_chunks[0].0),
            format!("file://{}", local_chunks[1].0),
            format!("file://{}", local_chunks[2].0),
        ]
        .into()
    );

    Ok(())
}
