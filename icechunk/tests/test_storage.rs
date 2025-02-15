use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
};

use bytes::Bytes;
use icechunk::{
    config::{S3Credentials, S3Options, S3StaticCredentials},
    format::{ChunkId, ManifestId, SnapshotId},
    refs::{
        create_tag, fetch_branch_tip, fetch_tag, list_refs, update_branch, Ref, RefError,
        RefErrorKind,
    },
    storage::{
        new_in_memory_storage, new_s3_storage, FetchConfigResult, StorageResult,
        UpdateConfigResult,
    },
    ObjectStorage, Storage,
};
use object_store::azure::AzureConfigKey;
use pretty_assertions::{assert_eq, assert_ne};
use tokio::io::AsyncReadExt;

#[allow(clippy::expect_used)]
async fn mk_s3_storage(prefix: &str) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_s3_storage(
        S3Options {
            region: Some("us-east-1".to_string()),
            endpoint_url: Some("http://localhost:9000".to_string()),
            allow_http: true,
            anonymous: false,
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

async fn with_storage<F, Fut>(f: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn(Arc<dyn Storage + Send + Sync>) -> Fut,
    Fut: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let prefix = format!("{:?}", ChunkId::random());
    let s1 = mk_s3_storage(prefix.as_str()).await?;
    #[allow(clippy::unwrap_used)]
    let s2 = new_in_memory_storage().await.unwrap();
    let s3 = mk_s3_object_store_storage(format!("{prefix}2").as_str()).await?;
    let s4 = mk_azure_blob_storage(prefix.as_str()).await?;
    f(s1).await?;
    f(s2).await?;
    f(s3).await?;
    f(s4).await?;
    Ok(())
}

#[tokio::test]
pub async fn test_snapshot_write_read() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
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

#[tokio::test]
pub async fn test_manifest_write_read() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
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

#[tokio::test]
pub async fn test_chunk_write_read() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
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

#[tokio::test]
pub async fn test_tag_write_get() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let storage_settings = storage.default_settings();
        let id = SnapshotId::random();
        create_tag(storage.as_ref(), &storage_settings, "mytag", id.clone(), false)
            .await?;
        let back = fetch_tag(storage.as_ref(), &storage_settings, "mytag").await?;
        assert_eq!(id, back.snapshot);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_fetch_non_existing_tag() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let storage_settings = storage.default_settings();
        let id = SnapshotId::random();
        create_tag(storage.as_ref(), &storage_settings, "mytag", id.clone(), false)
            .await?;

        let back =
            fetch_tag(storage.as_ref(), &storage_settings, "non-existing-tag").await;
        assert!(matches!(back, Err(RefError{kind: RefErrorKind::RefNotFound(r), ..}) if r == "non-existing-tag"));
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_create_existing_tag() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let storage_settings = storage.default_settings();
        let id = SnapshotId::random();
        create_tag(storage.as_ref(), &storage_settings, "mytag", id.clone(), false)
            .await?;

        let res =
            create_tag(storage.as_ref(), &storage_settings, "mytag", id.clone(), false)
                .await;
        assert!(matches!(res, Err(RefError{kind: RefErrorKind::TagAlreadyExists(r), ..}) if r == "mytag"));
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_branch_initialization() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let storage_settings = storage.default_settings();
        let id = SnapshotId::random();

        let res = update_branch(
            storage.as_ref(),
            &storage_settings,
            "some-branch",
            id.clone(),
            None,
            false,
        )
        .await?;
        assert_eq!(res.0, 0);

        let res =
            fetch_branch_tip(storage.as_ref(), &storage_settings, "some-branch").await?;
        assert_eq!(res.snapshot, id);

        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_fetch_non_existing_branch() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let storage_settings = storage.default_settings();
        let id = SnapshotId::random();
        update_branch(
            storage.as_ref(),
            &storage_settings,
            "some-branch",
            id.clone(),
            None,
            false,
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

#[tokio::test]
pub async fn test_branch_update() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let storage_settings = storage.default_settings();
        let id1 = SnapshotId::random();
        let id2 = SnapshotId::random();
        let id3 = SnapshotId::random();

        let res = update_branch(
            storage.as_ref(),
            &storage_settings,
            "some-branch",
            id1.clone(),
            None,
            false,
        )
        .await?;
        assert_eq!(res.0, 0);

        let res = update_branch(
            storage.as_ref(),
            &storage_settings,
            "some-branch",
            id2.clone(),
            Some(&id1),
            false,
        )
        .await?;
        assert_eq!(res.0, 1);

        let res = update_branch(
            storage.as_ref(),
            &storage_settings,
            "some-branch",
            id3.clone(),
            Some(&id2),
            false,
        )
        .await?;
        assert_eq!(res.0, 2);

        let res =
            fetch_branch_tip(storage.as_ref(), &storage_settings, "some-branch").await?;
        assert_eq!(res.snapshot, id3);

        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_ref_names() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let storage_settings = storage.default_settings();
        let id1 = SnapshotId::random();
        let id2 = SnapshotId::random();
        update_branch(
            storage.as_ref(),
            &storage_settings,
            "main",
            id1.clone(),
            None,
            false,
        )
        .await?;
        update_branch(
            storage.as_ref(),
            &storage_settings,
            "main",
            id2.clone(),
            Some(&id1),
            false,
        )
        .await?;
        update_branch(
            storage.as_ref(),
            &storage_settings,
            "foo",
            id1.clone(),
            None,
            false,
        )
        .await?;
        update_branch(
            storage.as_ref(),
            &storage_settings,
            "bar",
            id1.clone(),
            None,
            false,
        )
        .await?;
        create_tag(storage.as_ref(), &storage_settings, "my-tag", id1.clone(), false)
            .await?;
        create_tag(
            storage.as_ref(),
            &storage_settings,
            "my-other-tag",
            id1.clone(),
            false,
        )
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

#[tokio::test]
#[allow(clippy::panic)]
pub async fn test_write_config_on_empty() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let storage_settings = storage.default_settings();
        let config = Bytes::copy_from_slice(b"hello");
        let etag = match storage.update_config(&storage_settings, config.clone(), None).await? {
    UpdateConfigResult::Updated { new_etag } => new_etag,
    UpdateConfigResult::NotOnLatestVersion => panic!(),
};
        assert_ne!(etag, "");
        let res = storage.fetch_config(&storage_settings, ).await?;
        assert!(
            matches!(res, FetchConfigResult::Found{bytes, etag: actual_etag} if actual_etag == etag && bytes == config )
        );
        Ok(())
    }).await?;
    Ok(())
}

#[tokio::test]
#[allow(clippy::panic)]
pub async fn test_write_config_on_existing() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let storage_settings = storage.default_settings();
        let first_etag = match storage.update_config(&storage_settings, Bytes::copy_from_slice(b"hello"), None).await? {
            UpdateConfigResult::Updated { new_etag } => new_etag,
            _ => panic!(),
        };
        let config = Bytes::copy_from_slice(b"bye");
        let second_etag = match storage.update_config(&storage_settings, config.clone(), Some(first_etag.as_str())).await? {
            UpdateConfigResult::Updated { new_etag } => new_etag,
            _ => panic!(),
        };
        assert_ne!(second_etag, first_etag);
        let res = storage.fetch_config(&storage_settings, ).await?;
        assert!(
            matches!(res, FetchConfigResult::Found{bytes, etag: actual_etag} if actual_etag == second_etag && bytes == config )
        );
        Ok(())
    }).await?;
    Ok(())
}

#[tokio::test]
pub async fn test_write_config_fails_on_bad_etag_when_non_existing(
) -> Result<(), Box<dyn std::error::Error>> {
    // FIXME: this test fails in MiniIO but seems to work on S3
    #[allow(clippy::unwrap_used)]
    let storage = new_in_memory_storage().await.unwrap();
    let storage_settings = storage.default_settings();
    let etag = storage
        .update_config(
            &storage_settings,
            Bytes::copy_from_slice(b"hello"),
            Some("00000000000000000000000000000000"),
        )
        .await;

    assert!(matches!(etag, Ok(UpdateConfigResult::NotOnLatestVersion)));
    Ok(())
}

#[tokio::test]
#[allow(clippy::panic)]
pub async fn test_write_config_fails_on_bad_etag_when_existing(
) -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let storage_settings = storage.default_settings();
        let config = Bytes::copy_from_slice(b"hello");
        let etag = match storage.update_config(&storage_settings, config.clone(), None).await? {
            UpdateConfigResult::Updated { new_etag } => new_etag,
            _ => panic!(),
        };
        let res = storage
            .update_config(&storage_settings,
                Bytes::copy_from_slice(b"bye"),
                Some("00000000000000000000000000000000"),
            )
            .await?;
        assert!(matches!(res, UpdateConfigResult::NotOnLatestVersion));

        let res = storage.fetch_config(&storage_settings, ).await?;
        assert!(
            matches!(res, FetchConfigResult::Found{bytes, etag: actual_etag} if actual_etag == etag && bytes == config )
        );
            Ok(())
    }).await?;
    Ok(())
}
