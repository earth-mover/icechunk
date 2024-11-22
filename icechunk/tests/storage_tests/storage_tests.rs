pub mod storage_tests {
    use std::{collections::HashSet, sync::Arc};

    use bytes::Bytes;
    use icechunk::{
        format::{
            manifest::Manifest, snapshot::Snapshot, ByteRange, ChunkId, ManifestId,
            SnapshotId,
        },
        refs::{
            create_tag, fetch_branch_tip, fetch_tag, list_refs, update_branch, Ref, RefError,
        }, storage,
    };
    use pretty_assertions::assert_eq;

    pub async fn test_snapshot_write_read<T>(storage : T) -> Result<(), Box<dyn std::error::Error>> where T: storage::Storage {        
        let id = SnapshotId::random();
        let snapshot = Arc::new(Snapshot::empty());
        storage.write_snapshot(id.clone(), snapshot.clone()).await?;
        let back = storage.fetch_snapshot(&id).await?;
        assert_eq!(snapshot, back);
        Ok(())
    }

    pub async fn test_manifest_write_read<T>(storage : T) -> Result<(), Box<dyn std::error::Error>> where T: storage::Storage {        
        let id = ManifestId::random();
        let manifest = Arc::new(Manifest::default());
        storage.write_manifests(id.clone(), manifest.clone()).await?;
        let back = storage.fetch_manifests(&id).await?;
        assert_eq!(manifest, back);
        Ok(())
    }

    pub async fn test_chunk_write_read<T>(storage : T) -> Result<(), Box<dyn std::error::Error>> where T: storage::Storage {        
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
    }

    pub async fn test_tag_write_get<T>(storage : T) -> Result<(), Box<dyn std::error::Error>> where T: storage::Storage + Send + Sync {        
        let id = SnapshotId::random();
        create_tag(&storage, "mytag", id.clone(), false).await?;
        let back = fetch_tag(&storage, "mytag").await?;
        assert_eq!(id, back.snapshot);
        Ok(())
    }

    pub async fn test_fetch_non_existing_tag<T>(storage : T) -> Result<(), Box<dyn std::error::Error>> where T: storage::Storage + Send + Sync {        
        let id = SnapshotId::random();
        create_tag(&storage, "mytag", id.clone(), false).await?;

        let back = fetch_tag(&storage, "non-existing-tag").await;
        assert!(matches!(back, Err(RefError::RefNotFound(r)) if r == "non-existing-tag"));
        Ok(())
    }

    pub async fn test_create_existing_tag<T>(storage : T) -> Result<(), Box<dyn std::error::Error>> where T: storage::Storage + Send + Sync {        
        let id = SnapshotId::random();
        create_tag(&storage, "mytag", id.clone(), false).await?;

        let res = create_tag(&storage, "mytag", id.clone(), false).await;
        assert!(matches!(res, Err(RefError::TagAlreadyExists(r)) if r == "mytag"));
        Ok(())
    }

    pub async fn test_branch_initialization<T>(storage : T) -> Result<(), Box<dyn std::error::Error>> where T: storage::Storage + Send + Sync {        
        let id = SnapshotId::random();

        let res = update_branch(&storage, "some-branch", id.clone(), None, false).await?;
        assert_eq!(res.0, 0);

        let res = fetch_branch_tip(&storage, "some-branch").await?;
        assert_eq!(res.snapshot, id);

        Ok(())
    }

    pub async fn test_fetch_non_existing_branch<T>(storage : T) -> Result<(), Box<dyn std::error::Error>> where T: storage::Storage + Send + Sync {   
        let id = SnapshotId::random();
        update_branch(&storage, "some-branch", id.clone(), None, false).await?;

        let back = fetch_branch_tip(&storage, "non-existing-branch").await;
        assert!(matches!(back, Err(RefError::RefNotFound(r)) if r == "non-existing-branch"));
        Ok(())
    }

    pub async fn test_branch_update<T>(storage : T) -> Result<(), Box<dyn std::error::Error>> where T: storage::Storage + Send + Sync {   
        let id1 = SnapshotId::random();
        let id2 = SnapshotId::random();
        let id3 = SnapshotId::random();

        let res = update_branch(&storage, "some-branch", id1.clone(), None, false).await?;
        assert_eq!(res.0, 0);

        let res =
            update_branch(&storage, "some-branch", id2.clone(), Some(&id1), false).await?;
        assert_eq!(res.0, 1);

        let res =
            update_branch(&storage, "some-branch", id3.clone(), Some(&id2), false).await?;
        assert_eq!(res.0, 2);

        let res = fetch_branch_tip(&storage, "some-branch").await?;
        assert_eq!(res.snapshot, id3);

        Ok(())
    }

    pub async fn test_ref_names<T>(storage : T) -> Result<(), Box<dyn std::error::Error>> where T: storage::Storage + Send + Sync {   
        let id1 = SnapshotId::random();
        let id2 = SnapshotId::random();
        update_branch(&storage, "main", id1.clone(), None, false).await?;
        update_branch(&storage, "main", id2.clone(), Some(&id1), false).await?;
        update_branch(&storage, "foo", id1.clone(), None, false).await?;
        update_branch(&storage, "bar", id1.clone(), None, false).await?;
        create_tag(&storage, "my-tag", id1.clone(), false).await?;
        create_tag(&storage, "my-other-tag", id1.clone(), false).await?;

        let res: HashSet<_> = HashSet::from_iter(list_refs(&storage).await?);
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
    }
}