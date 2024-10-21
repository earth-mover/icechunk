#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used, clippy::expect_fun_call)]
mod tests {
    use icechunk::{
        format::{
            manifest::{VirtualChunkLocation, VirtualChunkRef},
            ByteRange, ChunkId, ChunkIndices, Path,
        },
        metadata::{ChunkKeyEncoding, ChunkShape, DataType, FillValue},
        repository::{get_chunk, ChunkPayload, ZarrArrayMetadata},
        storage::{
            s3::{mk_client, S3Config, S3Credentials, S3Storage, StaticS3Credentials},
            virtual_ref::ObjectStoreVirtualChunkResolverConfig,
            ObjectStorage,
        },
        zarr::AccessMode,
        Repository, Storage, Store,
    };
    use std::{error::Error, num::NonZeroU64};
    use std::{path::Path as StdPath, sync::Arc};
    use tempfile::TempDir;

    use bytes::Bytes;
    use object_store::{
        local::LocalFileSystem, ObjectStore, PutMode, PutOptions, PutPayload,
    };
    use pretty_assertions::assert_eq;

    fn minino_s3_config() -> S3Config {
        S3Config {
            region: Some("us-east-1".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            credentials: S3Credentials::Static(StaticS3Credentials {
                access_key_id: "minio123".into(),
                secret_access_key: "minio123".into(),
                session_token: None,
            }),
            allow_http: true,
        }
    }

    fn anon_s3_config() -> S3Config {
        S3Config {
            region: Some("us-east-1".to_string()),
            endpoint: None,
            credentials: S3Credentials::Anonymous,
            allow_http: false,
        }
    }

    async fn create_repository(
        storage: Arc<dyn Storage + Send + Sync>,
        virtual_s3_config: S3Config,
    ) -> Repository {
        Repository::init(storage, true)
            .await
            .expect("building repository failed")
            .with_virtual_ref_config(ObjectStoreVirtualChunkResolverConfig::S3(
                virtual_s3_config,
            ))
            .build()
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
        path: &StdPath,
        virtual_s3_config: S3Config,
    ) -> Repository {
        let storage: Arc<dyn Storage + Send + Sync> = Arc::new(
            ObjectStorage::new_local_store(path).expect("Creating local storage failed"),
        );

        create_repository(storage, virtual_s3_config).await
    }

    async fn create_minio_repository() -> Repository {
        let storage: Arc<dyn Storage + Send + Sync> = Arc::new(
            S3Storage::new_s3_store(
                "testbucket".to_string(),
                format!("{:?}", ChunkId::random()),
                Some(&minino_s3_config()),
            )
            .await
            .expect("Creating minio storage failed"),
        );

        create_repository(storage, minino_s3_config()).await
    }

    async fn write_chunks_to_local_fs(chunks: impl Iterator<Item = (String, Bytes)>) {
        let store =
            LocalFileSystem::new_with_prefix("/").expect("Failed to create local store");
        write_chunks_to_store(store, chunks).await;
    }

    async fn write_chunks_to_minio(chunks: impl Iterator<Item = (String, Bytes)>) {
        let client = mk_client(Some(&minino_s3_config())).await;

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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repository_with_local_virtual_refs() -> Result<(), Box<dyn Error>> {
        let chunk_dir = TempDir::new()?;
        let chunk_1 = chunk_dir.path().join("chunk-1").to_str().unwrap().to_owned();
        let chunk_2 = chunk_dir.path().join("chunk-2").to_str().unwrap().to_owned();

        let bytes1 = Bytes::copy_from_slice(b"first");
        let bytes2 = Bytes::copy_from_slice(b"second0000");
        let chunks = [(chunk_1, bytes1.clone()), (chunk_2, bytes2.clone())];
        write_chunks_to_local_fs(chunks.iter().cloned()).await;

        let repo_dir = TempDir::new()?;
        let mut ds = create_local_repository(repo_dir.path(), anon_s3_config()).await;

        let zarr_meta = ZarrArrayMetadata {
            shape: vec![1, 1, 2],
            data_type: DataType::Int32,
            chunk_shape: ChunkShape(vec![NonZeroU64::new(2).unwrap()]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Int32(0),
            codecs: vec![],
            storage_transformers: None,
            dimension_names: None,
        };
        let payload1 = ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation::from_absolute_path(&format!(
                // intentional extra '/'
                "file://{}",
                chunks[0].0
            ))?,
            offset: 0,
            length: 5,
        });
        let payload2 = ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation::from_absolute_path(&format!(
                "file://{}",
                chunks[1].0,
            ))?,
            offset: 1,
            length: 5,
        });

        let new_array_path: Path = "/array".try_into().unwrap();
        ds.add_array(new_array_path.clone(), zarr_meta.clone()).await.unwrap();

        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 0]),
            Some(payload1),
        )
        .await
        .unwrap();
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 1]),
            Some(payload2),
        )
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_repository_with_minio_virtual_refs() -> Result<(), Box<dyn Error>> {
        let bytes1 = Bytes::copy_from_slice(b"first");
        let bytes2 = Bytes::copy_from_slice(b"second0000");
        let chunks = [
            ("/path/to/chunk-1".into(), bytes1.clone()),
            ("/path/to/chunk-2".into(), bytes2.clone()),
        ];
        write_chunks_to_minio(chunks.iter().cloned()).await;

        let mut ds = create_minio_repository().await;

        let zarr_meta = ZarrArrayMetadata {
            shape: vec![1, 1, 2],
            data_type: DataType::Int32,
            chunk_shape: ChunkShape(vec![NonZeroU64::new(2).unwrap()]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Int32(0),
            codecs: vec![],
            storage_transformers: None,
            dimension_names: None,
        };
        let payload1 = ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation::from_absolute_path(&format!(
                // intentional extra '/'
                "s3://testbucket///{}",
                chunks[0].0
            ))?,
            offset: 0,
            length: 5,
        });
        let payload2 = ChunkPayload::Virtual(VirtualChunkRef {
            location: VirtualChunkLocation::from_absolute_path(&format!(
                "s3://testbucket/{}",
                chunks[1].0,
            ))?,
            offset: 1,
            length: 5,
        });

        let new_array_path: Path = "/array".try_into().unwrap();
        ds.add_array(new_array_path.clone(), zarr_meta.clone()).await.unwrap();

        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 0]),
            Some(payload1),
        )
        .await
        .unwrap();
        ds.set_chunk_ref(
            new_array_path.clone(),
            ChunkIndices(vec![0, 0, 1]),
            Some(payload2),
        )
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

    #[tokio::test]
    async fn test_zarr_store_virtual_refs_minio_set_and_get(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bytes1 = Bytes::copy_from_slice(b"first");
        let bytes2 = Bytes::copy_from_slice(b"second0000");
        let chunks = [
            ("/path/to/chunk-1".into(), bytes1.clone()),
            ("/path/to/chunk-2".into(), bytes2.clone()),
        ];
        write_chunks_to_minio(chunks.iter().cloned()).await;

        let ds = create_minio_repository().await;
        let mut store = Store::from_repository(
            ds,
            AccessMode::ReadWrite,
            Some("main".to_string()),
            None,
        );

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(
            store.get("array/zarr.json", &ByteRange::ALL).await.unwrap(),
            zarr_meta
        );

        let ref1 = VirtualChunkRef {
            location: VirtualChunkLocation::from_absolute_path(&format!(
                // intentional extra '/'
                "s3://testbucket///{}",
                chunks[0].0
            ))?,
            offset: 0,
            length: 5,
        };
        let ref2 = VirtualChunkRef {
            location: VirtualChunkLocation::from_absolute_path(&format!(
                "s3://testbucket/{}",
                chunks[1].0
            ))?,
            offset: 1,
            length: 5,
        };
        store.set_virtual_ref("array/c/0/0/0", ref1).await?;
        store.set_virtual_ref("array/c/0/0/1", ref2).await?;

        assert_eq!(store.get("array/c/0/0/0", &ByteRange::ALL).await?, bytes1,);
        assert_eq!(
            store.get("array/c/0/0/1", &ByteRange::ALL).await?,
            Bytes::copy_from_slice(&bytes2[1..6]),
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_zarr_store_virtual_refs_from_public_s3(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let repo_dir = TempDir::new()?;
        let ds = create_local_repository(repo_dir.path(), anon_s3_config()).await;

        let mut store = Store::from_repository(
            ds,
            AccessMode::ReadWrite,
            Some("main".to_string()),
            None,
        );

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[10],"data_type":"float64","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[10]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value": 0.0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[],"dimension_names":["depth"]}"#);
        store.set("depth/zarr.json", zarr_meta.clone()).await.unwrap();

        let ref2 = VirtualChunkRef {
            location: VirtualChunkLocation::from_absolute_path(
                "s3://noaa-nos-ofs-pds/dbofs/netcdf/202410/dbofs.t00z.20241009.fields.f030.nc",
            )?,
            offset: 119339,
            length: 80,
        };

        store.set_virtual_ref("depth/c/0", ref2).await?;

        let chunk = store.get("depth/c/0", &ByteRange::ALL).await.unwrap();
        assert_eq!(chunk.len(), 80);

        let second_depth = f64::from_le_bytes(chunk[8..16].try_into().unwrap());
        assert!(second_depth - -0.85 < 0.000001);

        let last_depth = f64::from_le_bytes(chunk[(80 - 8)..].try_into().unwrap());
        assert!(last_depth - -0.05 < 0.000001);

        Ok(())
    }
}
