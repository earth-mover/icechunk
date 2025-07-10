use std::{
    collections::{HashMap, HashSet},
    env,
    future::Future,
    sync::Arc,
};

use bytes::Bytes;
use icechunk::{
    ObjectStorage, Storage,
    config::{S3Credentials, S3Options, S3StaticCredentials},
    format::{ChunkId, ManifestId, SnapshotId},
    new_local_filesystem_storage,
    refs::{
        Ref, RefError, RefErrorKind, create_tag, fetch_branch_tip, fetch_tag, list_refs,
        update_branch,
    },
    storage::{
        self, ETag, FetchConfigResult, Generation, StorageResult, UpdateConfigResult,
        VersionInfo, new_in_memory_storage, new_s3_storage, s3::mk_client,
    },
};
use icechunk_macros::tokio_test;
use object_store::azure::AzureConfigKey;
use pretty_assertions::{assert_eq, assert_ne};
use tempfile::tempdir;
use tokio::io::AsyncReadExt;

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
    let prefix = common::get_random_prefix("with_storage");
    let s1 = mk_s3_storage(prefix.as_str()).await?;
    #[allow(clippy::unwrap_used)]
    let s2 = new_in_memory_storage().await.unwrap();
    let s3 =
        mk_s3_object_store_storage(format!("{prefix}_object_store").as_str()).await?;
    let s4 = mk_azure_blob_storage(prefix.as_str()).await?;
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
    println!("Using s3 object_store storage on MinIO");
    f("s3_object_store", s3).await?;
    println!("Using azure_blob storage");
    f("azure_blob", s4).await?;

    if env::var("AWS_BUCKET").is_ok() {
        let s6 = common::make_aws_integration_storage(prefix.clone())?;
        println!("Using AWS storage");
        f("AWS", s6).await?;
    }
    if env::var("R2_BUCKET").is_ok() {
        let s7 = common::make_r2_integration_storage(prefix.clone())?;
        println!("Using R2 storage");
        f("R2", s7).await?;
    }
    if env::var("TIGRIS_BUCKET").is_ok() {
        let s8 = common::make_tigris_integration_storage(prefix.clone())?;
        println!("Using Tigris storage");
        f("Tigris", s8).await?;
    }

    Ok(())
}

#[tokio_test]
pub async fn test_snapshot_write_read() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings();
        let id = SnapshotId::random();
        let bytes: [u8; 1024] = core::array::from_fn(|_| rand::random());
        storage
            .write_snapshot(
                &storage_settings,
                id.clone(),
                vec![("foo".to_string(), "bar".to_string())],
                Bytes::copy_from_slice(&bytes[..]),
            )
            .await?;
        let mut read = storage.fetch_snapshot(&storage_settings, &id).await?;
        let mut bytes_back = [0; 1024];
        read.read_exact(&mut bytes_back).await?;
        assert_eq!(bytes_back, bytes);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_manifest_write_read() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings();
        let id = ManifestId::random();
        let bytes: [u8; 1024] = core::array::from_fn(|_| rand::random());
        storage
            .write_manifest(
                &storage_settings,
                id.clone(),
                vec![("foo".to_string(), "bar".to_string())],
                Bytes::copy_from_slice(&bytes[..]),
            )
            .await?;
        let mut read =
            storage.fetch_manifest_unknown_size(&storage_settings, &id).await?;
        let mut bytes_back = [0; 1024];
        read.read_exact(&mut bytes_back).await?;
        assert_eq!(bytes_back, bytes);

        let bytes_back = storage
            .fetch_manifest_known_size(&storage_settings, &id, 1024)
            .await?
            .to_bytes(1024)
            .await?;
        assert_eq!(bytes_back, Bytes::copy_from_slice(&bytes[..]));
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_chunk_write_read() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings();
        let id = ChunkId::random();
        let bytes = Bytes::from_static(b"hello");
        storage.write_chunk(&storage_settings, id.clone(), bytes.clone()).await?;

        let back = storage.fetch_chunk(&storage_settings, &id, &(1..4)).await?;
        assert_eq!(Bytes::from_static(b"ell"), back);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_tag_write_get() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings();
        let id = SnapshotId::random();
        create_tag(storage.as_ref(), &storage_settings, "mytag", id.clone()).await?;
        let back = fetch_tag(storage.as_ref(), &storage_settings, "mytag").await?;
        assert_eq!(id, back.snapshot);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_fetch_non_existing_tag() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings();
        let id = SnapshotId::random();
        create_tag(storage.as_ref(), &storage_settings, "mytag", id.clone())
            .await?;

        let back =
            fetch_tag(storage.as_ref(), &storage_settings, "non-existing-tag").await;
        assert!(matches!(back, Err(RefError{kind: RefErrorKind::RefNotFound(r), ..}) if r == "non-existing-tag"));
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_create_existing_tag() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings();
        let id = SnapshotId::random();
        create_tag(storage.as_ref(), &storage_settings, "mytag", id.clone())
            .await?;

        let res =
            create_tag(storage.as_ref(), &storage_settings, "mytag", id.clone())
                .await;
        assert!(matches!(res, Err(RefError{kind: RefErrorKind::TagAlreadyExists(r), ..}) if r == "mytag"));
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_branch_initialization() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings();
        let id = SnapshotId::random();

        update_branch(
            storage.as_ref(),
            &storage_settings,
            "some-branch",
            id.clone(),
            None,
        )
        .await?;

        let res =
            fetch_branch_tip(storage.as_ref(), &storage_settings, "some-branch").await?;
        assert_eq!(res.snapshot, id);

        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_fetch_non_existing_branch() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings();
        let id = SnapshotId::random();
        update_branch(
            storage.as_ref(),
            &storage_settings,
            "some-branch",
            id.clone(),
            None,
        )
        .await?;

        let back =
            fetch_branch_tip(storage.as_ref(), &storage_settings, "non-existing-branch")
                .await;
        assert!(
            matches!(back, Err(RefError{kind: RefErrorKind::RefNotFound(r),..}) if r == "non-existing-branch")
        );
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_branch_update() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings();
        let id1 = SnapshotId::random();
        let id2 = SnapshotId::random();
        let id3 = SnapshotId::random();

        update_branch(
            storage.as_ref(),
            &storage_settings,
            "some-branch",
            id1.clone(),
            None,
        )
        .await?;

        update_branch(
            storage.as_ref(),
            &storage_settings,
            "some-branch",
            id2.clone(),
            Some(&id1),
        )
        .await?;

        update_branch(
            storage.as_ref(),
            &storage_settings,
            "some-branch",
            id3.clone(),
            Some(&id2),
        )
        .await?;

        let res =
            fetch_branch_tip(storage.as_ref(), &storage_settings, "some-branch").await?;
        assert_eq!(res.snapshot, id3);

        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio_test]
pub async fn test_ref_names() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings();
        let id1 = SnapshotId::random();
        let id2 = SnapshotId::random();
        update_branch(storage.as_ref(), &storage_settings, "main", id1.clone(), None)
            .await?;
        update_branch(
            storage.as_ref(),
            &storage_settings,
            "main",
            id2.clone(),
            Some(&id1),
        )
        .await?;
        update_branch(storage.as_ref(), &storage_settings, "foo", id1.clone(), None)
            .await?;
        update_branch(storage.as_ref(), &storage_settings, "bar", id1.clone(), None)
            .await?;
        create_tag(storage.as_ref(), &storage_settings, "my-tag", id1.clone()).await?;
        create_tag(storage.as_ref(), &storage_settings, "my-other-tag", id1.clone())
            .await?;

        let res: HashSet<_> =
            HashSet::from_iter(list_refs(storage.as_ref(), &storage_settings).await?);
        assert_eq!(
            res,
            HashSet::from_iter([
                Ref::Tag("my-tag".to_string()),
                Ref::Tag("my-other-tag".to_string()),
                Ref::Branch("main".to_string()),
                Ref::Branch("foo".to_string()),
                Ref::Branch("bar".to_string()),
            ])
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
        let storage_settings = storage.default_settings();
        let config = Bytes::copy_from_slice(b"hello");
        let version = match storage.update_config(&storage_settings, config.clone(), &VersionInfo::for_creation()).await? {
    UpdateConfigResult::Updated { new_version } => new_version,
    UpdateConfigResult::NotOnLatestVersion => panic!(),
};
        assert_ne!(version, VersionInfo::for_creation());
        let res = storage.fetch_config(&storage_settings, ).await?;
        assert!(
            matches!(res, FetchConfigResult::Found{bytes, version: actual_version} if actual_version == version && bytes == config )
        );
        Ok(())
    }).await?;
    Ok(())
}

#[tokio_test]
#[allow(clippy::panic)]
pub async fn test_write_config_on_existing() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage.default_settings();
        let first_version = match storage.update_config(&storage_settings, Bytes::copy_from_slice(b"hello"), &VersionInfo::for_creation()).await? {
            UpdateConfigResult::Updated { new_version } => new_version,
            _ => panic!(),
        };
        let config = Bytes::copy_from_slice(b"bye");
        let second_version = match storage.update_config(&storage_settings, config.clone(), &first_version).await? {
            UpdateConfigResult::Updated { new_version } => new_version,
            _ => panic!(),
        };
        assert_ne!(second_version, first_version);
        let res = storage.fetch_config(&storage_settings, ).await?;
        assert!(
            matches!(res, FetchConfigResult::Found{bytes, version: actual_version} if actual_version == second_version && bytes == config )
        );
        Ok(())
    }).await?;
    Ok(())
}

#[tokio_test]
pub async fn test_write_config_fails_on_bad_version_when_non_existing()
-> Result<(), Box<dyn std::error::Error>> {
    // FIXME: this test fails in MinIO but seems to work on S3
    #[allow(clippy::unwrap_used)]
    let storage = new_in_memory_storage().await.unwrap();
    let storage_settings = storage.default_settings();
    let version = storage
        .update_config(
            &storage_settings,
            Bytes::copy_from_slice(b"hello"),
            &VersionInfo::from_etag_only("00000000000000000000000000000000".to_string()),
        )
        .await;

    assert!(matches!(version, Ok(UpdateConfigResult::NotOnLatestVersion)));
    Ok(())
}

#[tokio_test]
#[allow(clippy::panic)]
pub async fn test_write_config_fails_on_bad_version_when_existing()
-> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage_type, storage| async move {
        let storage_settings = storage.default_settings();
        let config = Bytes::copy_from_slice(b"hello");
        let version = match storage.update_config(&storage_settings, config.clone(), &VersionInfo::for_creation()).await? {
            UpdateConfigResult::Updated { new_version } => new_version,
            _ => panic!(),
        };
        let update_res = storage
            .update_config(&storage_settings,
                Bytes::copy_from_slice(b"bye"),
            &VersionInfo{
                etag: Some(ETag("00000000000000000000000000000000".to_string())),
                generation: Some(Generation("0".to_string())),
            },
            )
            .await?;
        if storage_type == "local_filesystem" {
            // FIXME: local file system doesn't have conditional updates yet
            assert!(matches!(update_res, UpdateConfigResult::Updated{..}));

        } else {
            assert!(matches!(update_res, UpdateConfigResult::NotOnLatestVersion));
        }

        let fetch_res = storage.fetch_config(&storage_settings, ).await?;
        if storage_type == "local_filesystem" {
            // FIXME: local file system doesn't have conditional updates yet
            assert!(
                matches!(fetch_res, FetchConfigResult::Found{bytes, version: actual_version}
                    if actual_version != version && bytes == Bytes::copy_from_slice(b"bye"))
            );
        } else {
            assert!(
                matches!(fetch_res, FetchConfigResult::Found{bytes, version: actual_version}
                    if actual_version == version && bytes == config )
            );
        }
        Ok(())
    }).await?;
    Ok(())
}

#[tokio_test]
#[allow(clippy::panic)]
pub async fn test_write_config_can_overwrite_with_unsafe_config()
-> Result<(), Box<dyn std::error::Error>> {
    with_storage(|_, storage| async move {
        let storage_settings = storage::Settings {
            unsafe_use_conditional_update: Some(false),
            unsafe_use_conditional_create: Some(false),
            ..storage.default_settings()
        };

        // create the initial version
        let config = Bytes::copy_from_slice(b"hello");
        match storage
            .update_config(
                &storage_settings,
                config.clone(),
                &VersionInfo {
                    etag: Some(ETag("some-bad-etag".to_string())),
                    generation: Some(Generation("42".to_string())),
                }
            )
            .await?
        {
            UpdateConfigResult::Updated { new_version } => new_version,
            _ => panic!(),
        };

        // attempt a bad change that should succeed in this config
        let update_res = storage
            .update_config(
                &storage_settings,
                Bytes::copy_from_slice(b"bye"),
                &VersionInfo {
                    etag: Some(ETag("other-bad-etag".to_string())),
                    generation: Some(Generation("55".to_string())),
                },
            )
            .await?;

        assert!(matches!(update_res, UpdateConfigResult::Updated { .. }));

        let fetch_res = storage.fetch_config(&storage_settings).await?;
        assert!(
            matches!(fetch_res, FetchConfigResult::Found{bytes, ..} if bytes.as_ref() == b"bye")
        );
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
    st.write_chunk(
        &storage::Settings {
            chunks_storage_class: Some("STANDARD_IA".to_string()),
            ..storage::Settings::default()
        },
        ChunkId::new([0; 12]),
        Bytes::new(),
    )
    .await?;
    st.write_chunk(
        &storage::Settings {
            storage_class: Some("STANDARD_IA".to_string()),
            ..storage::Settings::default()
        },
        ChunkId::new([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
        Bytes::new(),
    )
    .await?;
    st.write_chunk(
        &storage::Settings::default(),
        ChunkId::new([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]),
        Bytes::new(),
    )
    .await?;
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

    st.write_manifest(
        &storage::Settings {
            metadata_storage_class: Some("STANDARD_IA".to_string()),
            ..storage::Settings::default()
        },
        ManifestId::new([0; 12]),
        Vec::new(),
        Bytes::new(),
    )
    .await?;
    st.write_manifest(
        &storage::Settings {
            storage_class: Some("STANDARD_IA".to_string()),
            ..storage::Settings::default()
        },
        ManifestId::new([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
        Vec::new(),
        Bytes::new(),
    )
    .await?;
    st.write_manifest(
        &storage::Settings::default(),
        ManifestId::new([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]),
        Vec::new(),
        Bytes::new(),
    )
    .await?;
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
            ..storage.default_settings()
        };

        let id = ChunkId::random();
        let bytes = Bytes::copy_from_slice(&[0; 1024]);

        storage.write_chunk(&custom_settings, id.clone(), bytes.clone()).await?;
        let fetched = storage.fetch_chunk(&custom_settings, &id, &(0..1024)).await?;
        assert_eq!(fetched, bytes);

        Ok(())
    })
    .await?;
    Ok(())
}
