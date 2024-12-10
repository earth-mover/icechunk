use std::{collections::HashSet, future::Future, sync::Arc};

use bytes::Bytes;
use chrono::Utc;
use icechunk::{
    format::{
        manifest::Manifest, snapshot::Snapshot, ByteRange, ChunkId, ManifestId,
        SnapshotId,
    },
    refs::{
        create_tag, fetch_branch_tip, fetch_tag, list_refs, update_branch, Ref, RefError,
    },
    storage::{
        s3::{S3Config, S3Credentials, S3Storage, StaticS3Credentials},
        StorageResult,
    },
    ObjectStorage, Storage, StorageError,
};
use pretty_assertions::{assert_eq, assert_ne};

async fn mk_storage() -> StorageResult<S3Storage> {
    S3Storage::new_s3_store(
        "testbucket",
        "test_s3_storage__".to_string() + Utc::now().to_rfc3339().as_str(),
        Some(&S3Config {
            region: Some("us-east-1".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            credentials: S3Credentials::Static(StaticS3Credentials {
                access_key_id: "minio123".into(),
                secret_access_key: "minio123".into(),
                session_token: None,
            }),
            allow_http: true,
        }),
    )
    .await
}

fn mk_in_memory_storage() -> ObjectStorage {
    ObjectStorage::new_in_memory_store(Some("prefix".to_string()))
}

async fn with_storage<F, Fut>(f: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn(Arc<dyn Storage + Send + Sync>) -> Fut,
    Fut: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let s1 = Arc::new(mk_storage().await?);
    let s2 = Arc::new(mk_in_memory_storage());
    f(s1).await?;
    f(s2).await?;
    Ok(())
}

#[tokio::test]
pub async fn test_snapshot_write_read() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let id = SnapshotId::random();
        let snapshot = Arc::new(Snapshot::empty());
        storage.write_snapshot(id.clone(), snapshot.clone()).await?;
        let back = storage.fetch_snapshot(&id).await?;
        assert_eq!(snapshot, back);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_manifest_write_read() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let id = ManifestId::random();
        let manifest = Arc::new(Manifest::default());
        storage.write_manifests(id.clone(), manifest.clone()).await?;
        let back = storage.fetch_manifests(&id).await?;
        assert_eq!(manifest, back);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_chunk_write_read() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let id = ChunkId::random();
        let bytes = Bytes::from_static(b"hello");
        storage.write_chunk(id.clone(), bytes.clone()).await?;
        let back = storage.fetch_chunk(&id, &ByteRange::ALL).await?;
        assert_eq!(bytes, back);

        let back =
            storage.fetch_chunk(&id, &ByteRange::from_offset_with_length(1, 2)).await?;
        assert_eq!(Bytes::from_static(b"el"), back);

        let back = storage.fetch_chunk(&id, &ByteRange::from_offset(1)).await?;
        assert_eq!(Bytes::from_static(b"ello"), back);

        let back = storage.fetch_chunk(&id, &ByteRange::to_offset(3)).await?;
        assert_eq!(Bytes::from_static(b"hel"), back); // codespell:ignore

        let back = storage.fetch_chunk(&id, &ByteRange::bounded(1, 4)).await?;
        assert_eq!(Bytes::from_static(b"ell"), back);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_tag_write_get() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let id = SnapshotId::random();
        create_tag(storage.as_ref(), "mytag", id.clone(), false).await?;
        let back = fetch_tag(storage.as_ref(), "mytag").await?;
        assert_eq!(id, back.snapshot);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_fetch_non_existing_tag() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let id = SnapshotId::random();
        create_tag(storage.as_ref(), "mytag", id.clone(), false).await?;

        let back = fetch_tag(storage.as_ref(), "non-existing-tag").await;
        assert!(matches!(back, Err(RefError::RefNotFound(r)) if r == "non-existing-tag"));
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_create_existing_tag() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let id = SnapshotId::random();
        create_tag(storage.as_ref(), "mytag", id.clone(), false).await?;

        let res = create_tag(storage.as_ref(), "mytag", id.clone(), false).await;
        assert!(matches!(res, Err(RefError::TagAlreadyExists(r)) if r == "mytag"));
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_branch_initialization() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let id = SnapshotId::random();

        let res = update_branch(storage.as_ref(), "some-branch", id.clone(), None, false)
            .await?;
        assert_eq!(res.0, 0);

        let res = fetch_branch_tip(storage.as_ref(), "some-branch").await?;
        assert_eq!(res.snapshot, id);

        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_fetch_non_existing_branch() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let id = SnapshotId::random();
        update_branch(storage.as_ref(), "some-branch", id.clone(), None, false).await?;

        let back = fetch_branch_tip(storage.as_ref(), "non-existing-branch").await;
        assert!(
            matches!(back, Err(RefError::RefNotFound(r)) if r == "non-existing-branch")
        );
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_branch_update() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let id1 = SnapshotId::random();
        let id2 = SnapshotId::random();
        let id3 = SnapshotId::random();

        let res =
            update_branch(storage.as_ref(), "some-branch", id1.clone(), None, false)
                .await?;
        assert_eq!(res.0, 0);

        let res = update_branch(
            storage.as_ref(),
            "some-branch",
            id2.clone(),
            Some(&id1),
            false,
        )
        .await?;
        assert_eq!(res.0, 1);

        let res = update_branch(
            storage.as_ref(),
            "some-branch",
            id3.clone(),
            Some(&id2),
            false,
        )
        .await?;
        assert_eq!(res.0, 2);

        let res = fetch_branch_tip(storage.as_ref(), "some-branch").await?;
        assert_eq!(res.snapshot, id3);

        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
pub async fn test_ref_names() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let id1 = SnapshotId::random();
        let id2 = SnapshotId::random();
        update_branch(storage.as_ref(), "main", id1.clone(), None, false).await?;
        update_branch(storage.as_ref(), "main", id2.clone(), Some(&id1), false).await?;
        update_branch(storage.as_ref(), "foo", id1.clone(), None, false).await?;
        update_branch(storage.as_ref(), "bar", id1.clone(), None, false).await?;
        create_tag(storage.as_ref(), "my-tag", id1.clone(), false).await?;
        create_tag(storage.as_ref(), "my-other-tag", id1.clone(), false).await?;

        let res: HashSet<_> = HashSet::from_iter(list_refs(storage.as_ref()).await?);
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
pub async fn test_write_config_on_empty() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let config = Bytes::copy_from_slice(b"hello");
        let etag = storage.update_config(config.clone(), None).await?;
        assert_ne!(etag, "");
        let res = storage.fetch_config().await?;
        assert!(
            matches!(res, Some((bytes, actual_etag)) if actual_etag == etag && bytes == config )
        );
        Ok(())
    }).await?;
    Ok(())
}

#[tokio::test]
pub async fn test_write_config_on_existing() -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let first_etag = storage.update_config(Bytes::copy_from_slice(b"hello"), None).await?;
        let config = Bytes::copy_from_slice(b"bye");
        let second_etag = storage.update_config(config.clone(), Some(first_etag.as_str())).await?;
        assert_ne!(second_etag, first_etag);
        let res = storage.fetch_config().await?;
        assert!(
            matches!(res, Some((bytes, actual_etag)) if actual_etag == second_etag && bytes == config )
        );
        Ok(())
    }).await?;
    Ok(())
}

#[tokio::test]
pub async fn test_write_config_fails_on_bad_etag_when_non_existing(
) -> Result<(), Box<dyn std::error::Error>> {
    // FIXME: this test fails in MiniIO but seems to work on S3
    let storage = mk_in_memory_storage();
    let etag = storage
        .update_config(
            Bytes::copy_from_slice(b"hello"),
            Some("00000000000000000000000000000000"),
        )
        .await;

    assert!(etag.is_err());
    Ok(())
}

#[tokio::test]
pub async fn test_write_config_fails_on_bad_etag_when_existing(
) -> Result<(), Box<dyn std::error::Error>> {
    with_storage(|storage| async move {
        let config = Bytes::copy_from_slice(b"hello");
        let etag = storage.update_config(config.clone(), None).await?;
        let res = storage
            .update_config(
                Bytes::copy_from_slice(b"bye"),
                Some("00000000000000000000000000000000"),
            )
            .await;
        assert!(matches!(res, Err(StorageError::ConfigUpdateConflict)));

        let res = storage.fetch_config().await?;
        assert!(
            matches!(res, Some((bytes, actual_etag)) if actual_etag == etag && bytes == config )
        );
            Ok(())
    }).await?;
    Ok(())
}
