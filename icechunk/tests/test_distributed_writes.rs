#![allow(clippy::unwrap_used)]
use pretty_assertions::assert_eq;
use std::{collections::HashMap, num::NonZeroU64, ops::Range, sync::Arc};

use bytes::Bytes;
use icechunk::{
    config::{S3Credentials, S3Options, S3StaticCredentials},
    format::{snapshot::ZarrArrayMetadata, ByteRange, ChunkIndices, Path, SnapshotId},
    metadata::{ChunkKeyEncoding, ChunkShape, DataType, FillValue},
    repository::VersionInfo,
    session::{get_chunk, Session},
    storage::new_s3_storage,
    Repository, Storage,
};
use tokio::task::JoinSet;

const SIZE: usize = 10;

#[allow(clippy::expect_used)]
async fn mk_storage(
    prefix: &str,
) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error + Send + Sync>> {
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

async fn mk_repo(
    storage: Arc<dyn Storage + Send + Sync>,
    init: bool,
) -> Result<Repository, Box<dyn std::error::Error + Send + Sync>> {
    if init {
        Ok(Repository::create(None, storage, HashMap::new()).await?)
    } else {
        Ok(Repository::open(None, storage, HashMap::new()).await?)
    }
}

async fn write_chunks(
    repo: Repository,
    xs: Range<u32>,
    ys: Range<u32>,
) -> Result<Session, Box<dyn std::error::Error + Send + Sync>> {
    let mut ds = repo.writable_session("main").await?;
    for x in xs {
        for y in ys.clone() {
            let fx = x as f64;
            let fy = y as f64;
            let bytes: Vec<u8> = fx
                .to_le_bytes()
                .into_iter()
                .chain(fy.to_le_bytes().into_iter())
                .collect();
            let payload =
                ds.get_chunk_writer()(Bytes::copy_from_slice(bytes.as_slice())).await?;
            ds.set_chunk_ref(
                "/array".try_into().unwrap(),
                ChunkIndices(vec![x, y]),
                Some(payload),
            )
            .await?;
        }
    }
    Ok(ds)
}

async fn verify(ds: Session) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for x in 0..(SIZE / 2) as u32 {
        for y in 0..(SIZE / 2) as u32 {
            let bytes = get_chunk(
                ds.get_chunk_reader(
                    &"/array".try_into().unwrap(),
                    &ChunkIndices(vec![x, y]),
                    &ByteRange::ALL,
                )
                .await?,
            )
            .await?;
            assert!(bytes.is_some());
            let bytes = bytes.unwrap();
            let written_x = f64::from_le_bytes(bytes[0..8].try_into().unwrap());
            let written_y = f64::from_le_bytes(bytes[8..16].try_into().unwrap());
            assert_eq!(x as f64, written_x);
            assert_eq!(y as f64, written_y);
        }
    }
    Ok(())
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
/// This test does a distributed write from 4 different [`Repository`] instances, and then commits.
///
/// - We create a repo, and write an empty array to it.
/// - We commit to it
/// - We initialize 3 other Repos pointing to the same place
/// - We do concurrent writes from the 4 repo instances
/// - When done, we do a distributed commit using a random repo
/// - The changes from the other repos are serialized via [`ChangeSet::export_to_bytes`]
async fn test_distributed_writes() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let prefix = format!("test_distributed_writes_{}", SnapshotId::random());
    let storage1 = mk_storage(prefix.as_str()).await?;
    let storage2 = mk_storage(prefix.as_str()).await?;
    let storage3 = mk_storage(prefix.as_str()).await?;
    let storage4 = mk_storage(prefix.as_str()).await?;
    let repo1 = mk_repo(storage1, true).await?;

    let mut ds1 = repo1.writable_session("main").await?;

    let zarr_meta = ZarrArrayMetadata {
        shape: vec![SIZE as u64, SIZE as u64],
        data_type: DataType::Float64,
        chunk_shape: ChunkShape(vec![
            NonZeroU64::new(1).unwrap(),
            NonZeroU64::new(1).unwrap(),
        ]),
        chunk_key_encoding: ChunkKeyEncoding::Slash,
        fill_value: FillValue::Float64(f64::NAN),
        codecs: vec![],
        storage_transformers: None,
        dimension_names: None,
    };

    let new_array_path: Path = "/array".try_into().unwrap();
    ds1.add_array(new_array_path.clone(), zarr_meta.clone()).await?;
    ds1.commit("create array", None).await?;

    let repo2 = mk_repo(storage2, false).await?;
    let repo3 = mk_repo(storage3, false).await?;
    let repo4 = mk_repo(storage4, false).await?;

    let mut set = JoinSet::new();
    #[allow(clippy::erasing_op, clippy::identity_op)]
    {
        let size2 = SIZE as u32;
        let size24 = size2 / 4;
        let xrange1 = size24 * 0..size24 * 1;
        let xrange2 = size24 * 1..size24 * 2;
        let xrange3 = size24 * 2..size24 * 3;
        let xrange4 = size24 * 3..size24 * 4;
        set.spawn(async move { write_chunks(repo1, xrange1, 0..size2).await });
        set.spawn(async move { write_chunks(repo2, xrange2, 0..size2).await });
        set.spawn(async move { write_chunks(repo3, xrange3, 0..size2).await });
        set.spawn(async move { write_chunks(repo4, xrange4, 0..size2).await });
    }

    let mut write_results = set.join_all().await;

    // We have completed all the chunk writes
    assert!(write_results.len() == 4);
    assert!(write_results.iter().all(|r| r.is_ok()));

    // We recover our repo instances (the may be numbered in a different order, doesn't matter)
    let mut ds1 = write_results.pop().unwrap().unwrap();
    let ds2 = write_results.pop().unwrap().unwrap();
    let ds3 = write_results.pop().unwrap().unwrap();
    let ds4 = write_results.pop().unwrap().unwrap();

    // We get the ChangeSet from repos 2, 3 and 4, by converting them into bytes.
    // This simulates a marshalling  operation from a remote writer.
    let raw_sessions: Vec<Vec<u8>> =
        vec![ds2.as_bytes().unwrap(), ds3.as_bytes().unwrap(), ds4.as_bytes().unwrap()];
    let sessions =
        raw_sessions.into_iter().map(|bytes| Session::from_bytes(bytes).unwrap());

    // Merge the changesets into the first repo
    for session in sessions {
        ds1.merge(session.into()).await?;
    }

    // Distributed commit now, using arbitrarily one of the repos as base and the others as extra
    // changesets
    let _new_snapshot = ds1.commit("distributed commit", None).await?;

    // We check we can read all chunks correctly
    verify(ds1).await?;

    // To be safe, we create a new instance of the storage and repo, and verify again
    let storage = mk_storage(prefix.as_str()).await?;
    let repo = mk_repo(storage, false).await?;
    let ds =
        repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;
    verify(ds).await?;

    Ok(())
}
