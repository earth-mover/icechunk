#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used, clippy::expect_fun_call)]
mod tests {
    use icechunk::{
        dataset::{ChunkPayload, ZarrArrayMetadata},
        format::{
            manifest::{VirtualChunkLocation, VirtualChunkRef},
            ByteRange, ChunkIndices,
        },
        metadata::{ChunkKeyEncoding, ChunkShape, DataType, FillValue},
        storage::ObjectStorage,
        zarr::{AccessMode, ObjectId},
        Dataset, Storage, Store,
    };
    use std::sync::Arc;
    use std::{error::Error, num::NonZeroU64, path::PathBuf};

    use bytes::Bytes;
    use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
    use pretty_assertions::assert_eq;

    async fn create_minio_dataset() -> Dataset {
        let storage: Arc<dyn Storage + Send + Sync> = Arc::new(
            ObjectStorage::new_s3_store(
                "testbucket".to_string(),
                format!("{:?}", ObjectId::random()),
                Some("minio123"),
                Some("minio123"),
                None::<String>,
                Some("http://localhost:9000"),
            )
            .expect("Creating minio storage failed"),
        );
        Dataset::init(Arc::clone(&storage), true)
            .await
            .expect("building dataset failed")
            .build()
    }

    async fn write_chunks_to_minio(chunks: impl Iterator<Item = (String, Bytes)>) {
        use object_store::aws::AmazonS3Builder;
        let bucket_name = "testbucket".to_string();
        // TODO: Switch to PutMode::Create when object_store supports that
        let opts = PutOptions { mode: PutMode::Overwrite, ..PutOptions::default() };

        let store = AmazonS3Builder::new()
            .with_access_key_id("minio123")
            .with_secret_access_key("minio123")
            .with_endpoint("http://localhost:9000")
            .with_allow_http(true)
            .with_bucket_name(bucket_name)
            .build()
            .expect("building S3 store failed");

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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dataset_with_virtual_refs() -> Result<(), Box<dyn Error>> {
        let bytes1 = Bytes::copy_from_slice(b"first");
        let bytes2 = Bytes::copy_from_slice(b"second0000");
        let chunks = [
            ("/path/to/chunk-1".into(), bytes1.clone()),
            ("/path/to/chunk-2".into(), bytes2.clone()),
        ];
        write_chunks_to_minio(chunks.iter().cloned()).await;

        let mut ds = create_minio_dataset().await;

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

        let new_array_path: PathBuf = "/array".to_string().into();
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
            ds.get_chunk(&new_array_path, &ChunkIndices(vec![0, 0, 0]), &ByteRange::ALL)
                .await
                .unwrap(),
            Some(bytes1.clone()),
        );
        assert_eq!(
            ds.get_chunk(&new_array_path, &ChunkIndices(vec![0, 0, 1]), &ByteRange::ALL)
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
                ds.get_chunk(&new_array_path, &ChunkIndices(vec![0, 0, 0]), &range)
                    .await
                    .unwrap(),
                Some(range.slice(bytes1.clone()))
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_zarr_store_virtual_refs_set_and_get(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bytes1 = Bytes::copy_from_slice(b"first");
        let bytes2 = Bytes::copy_from_slice(b"second0000");
        let chunks = [
            ("/path/to/chunk-1".into(), bytes1.clone()),
            ("/path/to/chunk-2".into(), bytes2.clone()),
        ];
        write_chunks_to_minio(chunks.iter().cloned()).await;

        let ds = create_minio_dataset().await;
        let mut store = Store::from_dataset(
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
                chunks[1].0
            ))?,
            offset: 1,
            length: 5,
        });
        store.set_virtual_ref("array/c/0/0/0", payload1).await?;
        store.set_virtual_ref("array/c/0/0/1", payload2).await?;

        assert_eq!(store.get("array/c/0/0/0", &ByteRange::ALL).await?, bytes1,);
        assert_eq!(
            store.get("array/c/0/0/1", &ByteRange::ALL).await?,
            Bytes::copy_from_slice(&bytes2[1..6]),
        );
        Ok(())
    }
}
