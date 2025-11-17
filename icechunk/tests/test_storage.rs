use std::{collections::HashMap, env, future::Future, pin::Pin, sync::Arc};

use bytes::Bytes;
use chrono::Utc;
use futures::{StreamExt, TryStreamExt, stream};
use icechunk::{
    ObjectStorage, Repository, RepositoryConfig, Storage,
    asset_manager::AssetManager,
    config::{
        DEFAULT_MAX_CONCURRENT_REQUESTS, S3Credentials, S3Options, S3StaticCredentials,
    },
    format::{
        CHUNKS_FILE_PATH, ChunkId, MANIFESTS_FILE_PATH, Path, SNAPSHOTS_FILE_PATH,
        SnapshotId, TRANSACTION_LOGS_FILE_PATH, format_constants::SpecVersionBin,
        snapshot::Snapshot,
    },
    new_local_filesystem_storage,
    refs::{RefData, RefErrorKind},
    repository::{RepositoryError, RepositoryErrorKind},
    storage::{
        self, ConcurrencySettings, ETag, Generation, StorageErrorKind, StorageResult,
        VersionInfo, new_http_storage, new_in_memory_storage, new_redirect_storage,
        new_s3_storage, s3::mk_client,
    },
};
use icechunk_macros::tokio_test;
use object_store::azure::AzureConfigKey;
use pretty_assertions::{assert_eq, assert_ne};
use tempfile::tempdir;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _},
    sync::oneshot,
};
use warp::Filter;
use zstd::zstd_safe::WriteBuf;

mod common;

#[allow(clippy::expect_used)]
async fn mk_s3_storage(prefix: &str) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_s3_storage(
        S3Options {
            region: Some("us-east-1".to_string()),
            endpoint_url: Some("http://localhost:9000".to_string()),
            allow_http: true,
            anonymous: false,
            force_path_style: true,
            network_stream_timeout_seconds: None,
        },
        "testbucket".to_string(),
        Some(prefix.to_string()),
        Some(S3Credentials::Static(S3StaticCredentials {
            access_key_id: "minio123".into(),
            secret_access_key: "minio123".into(),
            session_token: None,
            expires_after: None,
        })),
    )
    .expect("Creating minio storage failed");

    Ok(storage)
}

#[allow(clippy::expect_used)]
async fn mk_s3_object_store_storage(
    prefix: &str,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let storage = Arc::new(
        ObjectStorage::new_s3(
            "testbucket".to_string(),
            Some(prefix.to_string()),
            Some(S3Credentials::Static(S3StaticCredentials {
                access_key_id: "minio123".into(),
                secret_access_key: "minio123".into(),
                session_token: None,
                expires_after: None,
            })),
            Some(S3Options {
                region: Some("us-east-1".to_string()),
                endpoint_url: Some("http://localhost:9000".to_string()),
                allow_http: true,
                anonymous: false,
                force_path_style: true,
                network_stream_timeout_seconds: None,
            }),
        )
        .await?,
    );

    Ok(storage)
}

async fn mk_azure_blob_storage(
    prefix: &str,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let storage = Arc::new(
        ObjectStorage::new_azure(
            "devstoreaccount1".to_string(),
            "testcontainer".to_string(),
            Some(prefix.to_string()),
            None,
            Some(HashMap::from([(AzureConfigKey::UseEmulator, "true".to_string())])),
        )
        .await?,
    );

    Ok(storage)
}

#[allow(clippy::expect_used)]
async fn with_storage<F, Fut>(f: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn(&'static str, Arc<dyn Storage + Send + Sync>) -> Fut,
    Fut: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let s1 = mk_s3_storage(common::get_random_prefix("with_storage").as_str()).await?;
    let s1slash =
        mk_s3_storage(format!("{}/", common::get_random_prefix("with_storage")).as_str())
            .await?;
    #[allow(clippy::unwrap_used)]
    let s2 = new_in_memory_storage().await.unwrap();
    let s3 = mk_s3_object_store_storage(
        format!("{}_object_store", common::get_random_prefix("with_storage")).as_str(),
    )
    .await?;
    let s3slash = mk_s3_object_store_storage(
        format!("{}_object_store/", common::get_random_prefix("with_storage")).as_str(),
    )
    .await?;
    let s4 =
        mk_azure_blob_storage(common::get_random_prefix("with_storage").as_str()).await?;
    let s4slash = mk_azure_blob_storage(
        format!("{}/", common::get_random_prefix("with_storage")).as_str(),
    )
    .await?;
    let dir = tempdir().expect("cannot create temp dir");
    let s5 = new_local_filesystem_storage(dir.path())
        .await
        .expect("Cannot create local Storage");

    println!("Using in memory storage");
    f("in_memory", s2).await?;
    println!("Using local filesystem storage");
    f("local_filesystem", s5).await?;
    println!("Using s3 native storage on MinIO");
    f("s3_native", s1).await?;
    println!("Using s3 native storage on MinIO, slash prefix");
    f("s3_native", s1slash).await?;
    println!("Using s3 object_store storage on MinIO");
    f("s3_object_store", s3).await?;
    println!("Using s3 object_store storage on MinIO, slash prefix");
    f("s3_object_store", s3slash).await?;
    println!("Using azure_blob storage");
    f("azure_blob", s4).await?;
    println!("Using azure_blob storage, slash prefix");
    f("azure_blob", s4slash).await?;

    if env::var("AWS_BUCKET").is_ok() {
        let prefix = common::get_random_prefix("with_storage");
        let s = common::make_aws_integration_storage(prefix.clone())?;
        println!("Using AWS storage");
        f("AWS", s).await?;

        let prefix = format!("{}/", common::get_random_prefix("with_storage"));
        let s = common::make_aws_integration_storage(prefix.clone())?;
        println!("Using AWS storage, slashh prefix");
        f("AWS", s).await?;
    }
    if env::var("R2_BUCKET").is_ok() {
        let prefix = common::get_random_prefix("with_storage");
        let s = common::make_r2_integration_storage(prefix.clone())?;
        println!("Using R2 storage");
        f("R2", s).await?;

        let prefix = format!("{}/", common::get_random_prefix("with_storage"));
        let s = common::make_r2_integration_storage(prefix.clone())?;
        println!("Using R2 storage, slash prefix");
        f("R2", s).await?;
    }
    if env::var("TIGRIS_BUCKET").is_ok() {
        let prefix = common::get_random_prefix("with_storage");
        let s = common::make_tigris_integration_storage(prefix.clone())?;
        println!("Using Tigris storage");
        f("Tigris", s).await?;

        let prefix = format!("{}/", common::get_random_prefix("with_storage"));
        let s = common::make_tigris_integration_storage(prefix.clone())?;
        println!("Using Tigris storage, slash prefix");
        f("Tigris", s).await?;
    }

    Ok(())
}

async fn async_read_to_bytes(
    mut read: Pin<Box<dyn AsyncRead + Send>>,
) -> Result<Bytes, std::io::Error> {
    let mut data = Vec::with_capacity(1_024);
    read.read_to_end(&mut data).await?;
    Ok(Bytes::from_owner(data))
}

#[tokio_test]
pub async fn test_object_write_read() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings().await?;
        let id = SnapshotId::random();
        let mut bytes: [u8; 1024] = core::array::from_fn(|_| rand::random());
        bytes[42] = 42;
        bytes[43] = 99;

        for dir in [
            SNAPSHOTS_FILE_PATH,
            MANIFESTS_FILE_PATH,
            TRANSACTION_LOGS_FILE_PATH,
            CHUNKS_FILE_PATH,
        ] {
            let path = format!("{dir}/{id}");

            storage
                .put_object(
                    &storage_settings,
                    path.as_str(),
                    Bytes::copy_from_slice(&bytes[..]),
                    Some("application/foo"),
                    vec![("foo".to_string(), "bar".to_string())],
                    None,
                )
                .await?
                .must_write()?;

            // check with unknown size
            let (read, _) =
                storage.get_object(&storage_settings, path.as_str(), None).await?;
            assert_eq!(async_read_to_bytes(read).await?.as_slice(), bytes);

            // check with known size
            let (read, _) = storage
                .get_object(&storage_settings, path.as_str(), Some(&(0..1024)))
                .await?;
            assert_eq!(async_read_to_bytes(read).await?.as_slice(), bytes);

            // check with small range
            let (read, _) = storage
                .get_object(&storage_settings, path.as_str(), Some(&(42..44)))
                .await?;
            assert_eq!(
                async_read_to_bytes(read).await?.as_slice(),
                Bytes::copy_from_slice(&[42, 99])
            );
        }
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_tag_write_get() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let repo = Repository::create(None, storage, Default::default(), None).await?;
        repo.create_tag("mytag", &Snapshot::INITIAL_SNAPSHOT_ID).await?;
        let back = repo.lookup_tag("mytag").await?;
        assert_eq!(Snapshot::INITIAL_SNAPSHOT_ID, back);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_fetch_non_existing_tag() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let repo = Repository::create(None, storage, Default::default(), None).await?;
        repo.create_tag("mytag", &Snapshot::INITIAL_SNAPSHOT_ID).await?;
        let back = repo.lookup_tag("non-existing-tag").await;
        assert!(
            matches!(
                back,
                Err(RepositoryError{kind: RepositoryErrorKind::Ref(RefErrorKind::RefNotFound(r)), ..}) if r == "non-existing-tag"
            )
            );

        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_create_existing_tag() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let repo = Repository::create(None, storage, Default::default(), None).await?;
        repo.create_tag("mytag", &Snapshot::INITIAL_SNAPSHOT_ID).await?;
        let res  = repo.create_tag("mytag", &Snapshot::INITIAL_SNAPSHOT_ID).await;
        assert!(
            matches!(
                res,
                Err(RepositoryError{kind: RepositoryErrorKind::Ref(RefErrorKind::TagAlreadyExists(r)), ..}) if r == "mytag"
            )
            );
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_list_objects() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let settings = storage.default_settings().await?;
        storage
            .put_object(
                &settings,
                "foo/bar/1",
                Bytes::new(),
                None,
                Default::default(),
                None,
            )
            .await?
            .must_write()?;
        storage
            .put_object(
                &settings,
                "foo/bar/2",
                Bytes::new(),
                None,
                Default::default(),
                None,
            )
            .await?
            .must_write()?;
        storage
            .put_object(&settings, "foo/3", Bytes::new(), None, Default::default(), None)
            .await?
            .must_write()?;
        storage
            .put_object(&settings, "foo/4", Bytes::new(), None, Default::default(), None)
            .await?
            .must_write()?;
        storage
            .put_object(&settings, "5", Bytes::new(), None, Default::default(), None)
            .await?
            .must_write()?;
        storage
            .put_object(&settings, "6", Bytes::new(), None, Default::default(), None)
            .await?
            .must_write()?;

        for prefix in ["foo/bar", "foo/bar/"] {
            let mut obs: Vec<_> = storage
                .list_objects(&settings, prefix)
                .await?
                .map_ok(|li| li.id)
                .try_collect()
                .await?;
            obs.sort();
            assert_eq!(obs, vec!["1".to_string(), "2".to_string()]);
        }

        for prefix in ["foo", "foo/"] {
            let mut obs: Vec<_> = storage
                .list_objects(&settings, prefix)
                .await?
                .map_ok(|li| li.id)
                .try_collect()
                .await?;
            obs.sort();
            assert_eq!(
                obs,
                vec![
                    "3".to_string(),
                    "4".to_string(),
                    "bar/1".to_string(),
                    "bar/2".to_string()
                ]
            );
        }

        for prefix in ["", "/"] {
            let mut obs: Vec<_> = storage
                .list_objects(&settings, prefix)
                .await?
                .map_ok(|li| li.id)
                .try_collect()
                .await?;
            obs.sort();
            assert_eq!(
                obs,
                vec![
                    "5".to_string(),
                    "6".to_string(),
                    "foo/3".to_string(),
                    "foo/4".to_string(),
                    "foo/bar/1".to_string(),
                    "foo/bar/2".to_string()
                ]
            );
        }

        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_delete_objects() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let settings = storage.default_settings().await?;
        storage
            .put_object(
                &settings,
                "foo/bar/1",
                Bytes::new(),
                None,
                Default::default(),
                None,
            )
            .await?
            .must_write()?;
        storage
            .put_object(
                &settings,
                "foo/bar/2",
                Bytes::new(),
                None,
                Default::default(),
                None,
            )
            .await?
            .must_write()?;
        storage
            .put_object(&settings, "foo/3", Bytes::new(), None, Default::default(), None)
            .await?
            .must_write()?;
        storage
            .put_object(&settings, "foo/4", Bytes::new(), None, Default::default(), None)
            .await?
            .must_write()?;
        storage
            .put_object(&settings, "5", Bytes::new(), None, Default::default(), None)
            .await?
            .must_write()?;
        storage
            .put_object(&settings, "6", Bytes::new(), None, Default::default(), None)
            .await?
            .must_write()?;

        // passing a prefix without slash
        let res = storage
            .delete_objects(
                &settings,
                "foo/bar",
                stream::iter([("1".to_string(), 1)]).boxed(),
            )
            .await?;
        assert_eq!(res.deleted_objects, 1);
        assert_eq!(res.deleted_bytes, 1);

        // passing a prefix with slash
        let res = storage
            .delete_objects(
                &settings,
                "foo/bar/",
                stream::iter([("2".to_string(), 2)]).boxed(),
            )
            .await?;
        assert_eq!(res.deleted_objects, 1);
        assert_eq!(res.deleted_bytes, 2);

        let mut obs: Vec<_> = storage
            .list_objects(&settings, "foo/bar")
            .await?
            .map_ok(|li| li.id)
            .try_collect()
            .await?;
        obs.sort();
        assert!(obs.is_empty());

        // passing a prefix without slash
        let res = storage
            .delete_objects(
                &settings,
                "",
                stream::iter([("foo/3".to_string(), 5), ("foo/4".to_string(), 6)])
                    .boxed(),
            )
            .await?;
        assert_eq!(res.deleted_objects, 2);
        assert_eq!(res.deleted_bytes, 11);

        let mut obs: Vec<_> = storage
            .list_objects(&settings, "foo")
            .await?
            .map_ok(|li| li.id)
            .try_collect()
            .await?;
        obs.sort();
        assert!(obs.is_empty());

        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_fetch_non_existing_branch() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let repo = Repository::create(None, storage, Default::default(), None).await?;
        let back = repo.lookup_branch("non-existing-branch").await;
        assert!(
            matches!(
                back,
                Err(RepositoryError{kind: RepositoryErrorKind::Ref(RefErrorKind::RefNotFound(r)), ..}) if r == "non-existing-branch"
            )
        );
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
#[allow(clippy::panic)]
pub async fn test_write_config_on_empty() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings().await?;

        let am = Arc::new(AssetManager::new_no_cache(
            storage,
            storage_settings,
            SpecVersionBin::current(),
            1, // we are only reading, compression doesn't matter
            DEFAULT_MAX_CONCURRENT_REQUESTS,
        ));
        let config = RepositoryConfig::default();
        let version = match am
            .try_update_config(&config, &VersionInfo::for_creation(), None)
            .await?
        {
            Some(new_version) => new_version,
            None => panic!(),
        };
        assert_ne!(version, VersionInfo::for_creation());
        let res = am.fetch_config().await?;
        match res {
            Some((result, actual_version)) => {
                assert_eq!(result, config);
                assert_eq!(actual_version, version);
            }
            None => panic!("Couldn't get file"),
        }
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
#[allow(clippy::panic, clippy::unwrap_used)]
pub async fn test_write_config_on_existing() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let am = Arc::new(AssetManager::new_no_cache(
            Arc::clone(&storage),
            storage.default_settings().await?,
            SpecVersionBin::current(),
            1, // we are only reading, compression doesn't matter
            DEFAULT_MAX_CONCURRENT_REQUESTS,
        ));
        let config1 = RepositoryConfig::default();
        let first_version = match am
            .try_update_config(&config1, &VersionInfo::for_creation(), None)
            .await?
        {
            Some(new_version) => new_version,
            None => panic!(),
        };
        let config2 =
            RepositoryConfig { inline_chunk_threshold_bytes: Some(42), ..config1 };
        let second_version = match am
            .try_update_config(&config2, &first_version, Some("foo/baz"))
            .await?
        {
            Some(new_version) => new_version,
            None => panic!(),
        };
        assert_ne!(second_version, first_version);
        let (fetched_config, fetched_version) = am.fetch_config().await?.unwrap();
        assert_eq!(second_version, fetched_version);
        assert_eq!(config2, fetched_config);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_write_config_fails_on_bad_version_when_non_existing()
-> Result<(), Box<dyn std::error::Error>> {
    // FIXME: this test fails in MinIO but seems to work on S3
    #[allow(clippy::unwrap_used)]
    let storage = new_in_memory_storage().await.unwrap();
    let storage_settings = storage.default_settings().await?;
    let am = Arc::new(AssetManager::new_no_cache(
        storage,
        storage_settings,
        SpecVersionBin::current(),
        1, // we are only reading, compression doesn't matter
        DEFAULT_MAX_CONCURRENT_REQUESTS,
    ));
    let config = RepositoryConfig::default();
    let err = am
        .try_update_config(
            &config,
            &VersionInfo::from_etag_only("00000000000000000000000000000000".to_string()),
            Some("foo/bar"),
        )
        .await;
    assert!(matches!(
        err,
        Err(RepositoryError {
            kind: RepositoryErrorKind::StorageError(StorageErrorKind::ObjectNotFound,),
            ..
        })
    ));
    Ok(())
}

#[tokio_test]
#[allow(clippy::panic, clippy::unwrap_used)]
pub async fn test_write_config_fails_on_bad_version_when_existing()
-> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage_type, storage| async move {
        let storage_settings = storage.default_settings().await?;
        let am = Arc::new(AssetManager::new_no_cache(
            storage,
            storage_settings,
            SpecVersionBin::current(),
            1, // we are only reading, compression doesn't matter
            DEFAULT_MAX_CONCURRENT_REQUESTS,
        ));

        let config1 = RepositoryConfig::default();
        let version = match am
            .try_update_config(&config1, &VersionInfo::for_creation(), None)
            .await?
        {
            Some(new_version) => new_version,
            None => panic!(),
        };
        let update_res = am
            .try_update_config(
                &config1,
                &VersionInfo {
                    etag: Some(ETag("00000000000000000000000000000000".to_string())),
                    generation: Some(Generation("0".to_string())),
                },
                Some("foo/bar"),
            )
            .await?;

        if storage_type == "local_filesystem" {
            // FIXME: local file system doesn't have conditional updates yet
            assert!(update_res.is_some());
        } else {
            assert!(update_res.is_none());
        }

        let (fetched_config, fetched_version) = am.fetch_config().await?.unwrap();
        if storage_type == "local_filesystem" {
            // FIXME: local file system doesn't have conditional updates yet
            assert_ne!(fetched_version, version);
            assert_eq!(fetched_config, config1);
        } else {
            assert_eq!(fetched_version, version);
            assert_eq!(fetched_config, config1);
        }
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
#[allow(clippy::panic, clippy::unwrap_used)]
pub async fn test_write_config_can_overwrite_with_unsafe_config()
-> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage::Settings {
            unsafe_use_conditional_update: Some(false),
            unsafe_use_conditional_create: Some(false),
            ..storage.default_settings().await?
        };
        let am = Arc::new(AssetManager::new_no_cache(
            storage,
            storage_settings,
            SpecVersionBin::current(),
            1, // we are only reading, compression doesn't matter
            DEFAULT_MAX_CONCURRENT_REQUESTS,
        ));

        let config1 = RepositoryConfig::default();
        match am.try_update_config(&config1, &VersionInfo::for_creation(), None).await? {
            Some(new_version) => new_version,
            None => panic!(),
        };

        // attempt a bad change that should succeed in this config
        let update_res = am
            .try_update_config(
                &config1,
                &VersionInfo {
                    etag: Some(ETag("other-bad-etag".to_string())),
                    generation: Some(Generation("55".to_string())),
                },
                Some("foo/bar"),
            )
            .await?;
        assert!(update_res.is_some());
        assert_eq!(am.fetch_config().await?.unwrap().0, config1);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
#[allow(clippy::unwrap_used)]
pub async fn test_storage_classes() -> Result<(), Box<dyn std::error::Error>> {
    if env::var("AWS_BUCKET").is_err() {
        return Ok(());
    }
    let prefix = common::get_random_prefix("test_storage_classes");
    let st = common::make_aws_integration_storage(prefix.clone())?;
    let client = mk_client(
        &common::get_aws_integration_options()?,
        common::get_aws_integration_credentials()?,
        Vec::new(),
        Vec::new(),
        &storage::Settings::default(),
    )
    .await;

    // we write 2 chunks in IA and one in standard, in ascending order of id
    st.put_object(
        &storage::Settings {
            storage_class: Some("STANDARD_IA".to_string()),
            ..storage::Settings::default()
        },
        "chunks/000000000000",
        Bytes::new(),
        None,
        Default::default(),
        None,
    )
    .await?
    .must_write()?;
    st.put_object(
        &storage::Settings {
            storage_class: Some("STANDARD_IA".to_string()),
            ..storage::Settings::default()
        },
        "chunks/000000000001",
        Bytes::new(),
        None,
        Default::default(),
        None,
    )
    .await?
    .must_write()?;
    st.put_object(
        &storage::Settings::default(),
        "chunks/000000000002",
        Bytes::new(),
        None,
        Default::default(),
        None,
    )
    .await?
    .must_write()?;
    let out = client
        .list_objects_v2()
        .bucket(common::get_aws_integration_bucket()?)
        .prefix(format!("{prefix}/chunks"))
        .into_paginator()
        .send()
        .collect::<Vec<_>>()
        .await
        .pop()
        .unwrap()
        .unwrap();
    assert_eq!(
        out.contents()
            .iter()
            .map(|o| o.storage_class().unwrap().to_string())
            .collect::<Vec<_>>(),
        vec!["STANDARD_IA", "STANDARD_IA", "STANDARD"]
    );

    Ok(())
}

#[tokio::test]
pub async fn test_write_object_larger_than_multipart_threshold()
-> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let custom_settings = storage::Settings {
            minimum_size_for_multipart_upload: Some(100),
            ..storage.default_settings().await?
        };

        let id = ChunkId::random();
        let path = format!("{MANIFESTS_FILE_PATH}/{id}");
        let bytes = Bytes::copy_from_slice(&[0; 1024]);

        storage
            .put_object(
                &custom_settings,
                path.as_str(),
                bytes.clone(),
                None,
                Default::default(),
                None,
            )
            .await?
            .must_write()?;
        let fetched = async_read_to_bytes(
            storage
                .get_object(&custom_settings, path.as_str(), Some(&(0..1024)))
                .await?
                .0,
        )
        .await?;
        assert_eq!(fetched, bytes);

        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
/// Start an HTTP server serving static files from icechunk-python/tests/data/test-repo-v2
/// Create an HTTP storage that hits that server, and call some methods on it
pub async fn test_http_storage() -> Result<(), Box<dyn std::error::Error>> {
    let repo_path =
        std::env::current_dir()?.join("../icechunk-python/tests/data/test-repo-v2");
    let route = warp::fs::dir(repo_path.clone());
    let (stop, wait) = oneshot::channel();
    let port = port_check::free_local_ipv4_port_in_range(8000..65000).unwrap();
    let server = warp::serve(route).bind(([127, 0, 0, 1], port)).await.graceful(async {
        wait.await.ok();
    });

    let join = tokio::task::spawn(server.run());

    let url = format!("http://localhost:{port}");
    let storage1 = new_http_storage(url.as_str(), None)?;
    let storage2 = new_http_storage(url.as_str(), None)?;
    for storage in [storage1, storage2] {
        assert!(!storage.can_write().await?);

        let settings = storage.default_settings().await?;
        let mut data = Vec::with_capacity(1_024);

        let mut read = storage.get_object(&settings, "repo", None).await?.0;
        read.read_to_end(&mut data).await?;
        let expected = std::fs::metadata(repo_path.join("repo"))?.len();
        assert_eq!(expected, data.len() as u64);

        let mut data = Vec::with_capacity(1_024);
        let conc_settings = storage::Settings {
            concurrency: Some(ConcurrencySettings {
                max_concurrent_requests_for_object: Some(100.try_into()?),
                ideal_concurrent_request_size: Some(10.try_into()?),
            }),
            ..settings.clone()
        };
        let mut read =
            storage.get_object(&conc_settings, "repo", Some(&(0..100))).await?.0;
        read.read_to_end(&mut data).await?;
        assert_eq!(100, data.len() as u64);

        let mut data = Vec::with_capacity(1_024);
        let mut read = storage
            .get_object(&conc_settings, "snapshots/1CECHNKREP0F1RSTCMT0", None)
            .await?
            .0;
        read.read_to_end(&mut data).await?;
        let expected =
            std::fs::metadata(repo_path.join("snapshots/1CECHNKREP0F1RSTCMT0"))?.len();
        assert_eq!(expected, data.len() as u64);

        let lm = storage.get_object_last_modified("config.yaml", &settings).await?;
        assert!(lm < Utc::now());
    }

    // stop the server
    stop.send(()).unwrap();
    join.await?;

    Ok(())
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
/// Start a server that after a few redirects returns a redirect to a S3 public repo
/// Build a redirect storage and verify the S3 repo is accessible
pub async fn test_redirect_storage() -> Result<(), Box<dyn std::error::Error>> {
    let port = port_check::free_local_ipv4_port_in_range(9000..65000).unwrap();
    let route = warp::path::end()
        .map(|| warp::redirect::found(warp::http::Uri::from_static("1")))
        .or(warp::path("1")
            .map(|| warp::redirect::permanent(warp::http::Uri::from_static("/2"))))
        .or(warp::path("2")
            .map(|| warp::redirect::redirect(warp::http::Uri::from_static("/3"))))
        .or(warp::path("3").map(|| {
            warp::redirect::temporary(warp::http::Uri::from_static(
                "s3://icechunk-public-data/v1/era5_weatherbench2?region=us-east-1",
            ))
        }));

    let (stop, wait) = oneshot::channel();
    let server = warp::serve(route).bind(([127, 0, 0, 1], port)).await.graceful(async {
        wait.await.ok();
    });

    let join = tokio::task::spawn(server.run());
    let url = format!("http://localhost:{port}");
    let storage = new_redirect_storage(url.as_str())?;
    let mut data = Vec::with_capacity(1_024);
    let settings = storage.default_settings().await?;
    let mut read =
        storage.get_object(&settings, "refs/branch.main/ref.json", None).await?.0;

    // stop the server, because it shouldn't be needed after the first interaction
    stop.send(()).unwrap();
    join.await?;

    read.read_to_end(&mut data).await?;
    let _: RefData = serde_json::from_slice(&data)?;

    let repo = Repository::open(None, storage, Default::default()).await?;
    let session = repo
        .readonly_session(&icechunk::repository::VersionInfo::BranchTipRef(
            "main".to_string(),
        ))
        .await?;
    assert!(session.list_nodes(&Path::root()).await?.count() >= 8);

    Ok(())
}
