#![allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
use std::{collections::HashMap, iter, num::NonZeroU64, sync::Arc};

use icechunk::{
    repository::{
        ChunkIndices, ChunkKeyEncoding, ChunkPayload, ChunkShape, Codec, DataType,
        FillValue, Path, StorageTransformer, UserAttributes, ZarrArrayMetadata,
    },
    storage::ObjectStorage,
    zarr::StoreError,
    Repository, Storage,
};
use itertools::Itertools;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("# The `Repository` abstraction in Icechunk 2");
    println!("## First create the `Repository`");
    println!(
        r#"
```
let storage: Arc<dyn Storage + Send + Sync> = Arc::new(InMemoryStorage::new());
let storage: Arc<dyn Storage + Send + Sync> =
    Arc::new(MemCachingStorage::new(storage, 100_000_000));
let mut ds = Repository::create(Arc::clone(&storage));
```
"#,
    );

    let storage: Arc<dyn Storage + Send + Sync> =
        Arc::new(ObjectStorage::new_in_memory_store(None));
    let mut ds = Repository::init(
        Repository::add_in_mem_asset_caching(Arc::clone(&storage)),
        false,
    )
    .await?
    .build();

    println!();
    println!();
    println!(
        r#"## Adding 3 groups

```
ds.add_group("/".into()).await?;
ds.add_group("/group1".into()).await?;
ds.add_group("/group2".into()).await?;
```
"#,
    );

    ds.add_group(Path::root()).await?;
    ds.add_group("/group1".try_into().unwrap()).await?;
    ds.add_group("/group2".try_into().unwrap()).await?;

    println!();
    print_nodes(&ds).await?;

    println!(
        r#"
```
let rows = ds
    .list_nodes()
    .await
    .sorted_by_key(|n| n.path.clone())
```
"#,
    );

    println!();
    println!("## Adding an array");

    println!(
        r#"
```
let zarr_meta1 = ZarrArrayMetadata {{
    shape: vec![3],
    data_type: DataType::Int32,
    chunk_shape: ChunkShape(vec![
        NonZeroU64::new(1).unwrap(),
        NonZeroU64::new(1).unwrap(),
        NonZeroU64::new(1).unwrap(),
    ]),
    chunk_key_encoding: ChunkKeyEncoding::Slash,
    fill_value: FillValue::Int32(0),
    codecs: Codecs("codec".to_string()),
    storage_transformers: Some(StorageTransformers("transformers".to_string())),
    dimension_names: Some(vec![
        Some("x".to_string()),
        Some("y".to_string()),
        Some("t".to_string()),
    ]),
}};

let array1_path: Path = "/group1/array1".into();

ds.add_array(array1_path.clone(), zarr_meta1).await?;
```
"#,
    );

    let zarr_meta1 = ZarrArrayMetadata {
        shape: vec![3],
        data_type: DataType::Int32,
        chunk_shape: ChunkShape(vec![
            NonZeroU64::new(1).unwrap(),
            NonZeroU64::new(1).unwrap(),
            NonZeroU64::new(1).unwrap(),
        ]),
        chunk_key_encoding: ChunkKeyEncoding::Slash,
        fill_value: FillValue::Int32(0),
        codecs: vec![Codec {
            name: "mycodec".to_string(),
            configuration: Some(HashMap::from_iter(iter::once((
                "foo".to_string(),
                serde_json::Value::from(42),
            )))),
        }],
        storage_transformers: Some(vec![StorageTransformer {
            name: "mytransformer".to_string(),
            configuration: Some(HashMap::from_iter(iter::once((
                "foo".to_string(),
                serde_json::Value::from(42),
            )))),
        }]),
        dimension_names: Some(vec![
            Some("x".to_string()),
            Some("y".to_string()),
            Some("t".to_string()),
        ]),
    };
    let array1_path: Path = "/group1/array1".try_into().unwrap();
    ds.add_array(array1_path.clone(), zarr_meta1).await?;
    println!();
    print_nodes(&ds).await?;

    println!();
    println!();
    println!("## Setting array user attributes");
    println!(
        r#"
```
ds.set_user_attributes(array1_path.clone(), Some("{{n:42}}".to_string())).await?;
```
 "#,
    );
    ds.set_user_attributes(
        array1_path.clone(),
        Some(UserAttributes::try_new(br#"{"n":42}"#).unwrap()),
    )
    .await?;
    print_nodes(&ds).await?;

    println!("## Committing");
    let v1_id = ds.flush("some message", Default::default()).await?;
    println!(
        r#"
```
ds.flush("some message", Default::default()).await?;
=> {v1_id:?}
```
 "#
    );

    println!("\nNow we continue to use the same repository instance");
    println!();
    println!();
    println!("## Adding an inline chunk");
    ds.set_chunk_ref(
        array1_path.clone(),
        ChunkIndices(vec![0]),
        Some(ChunkPayload::Inline("hello".into())),
    )
    .await?;
    println!(
        r#"
```
ds.set_chunk(
    array1_path.clone(),
    ChunkIndices(vec![0]),
    Some(ChunkPayload::Inline(b"hello".into())),
)
.await?;
```
 "#,
    );
    let chunk = ds.get_chunk_ref(&array1_path, &ChunkIndices(vec![0])).await.unwrap();
    println!("## Reading the chunk");
    println!(
        r#"
```
let chunk = ds.get_chunk_ref(&array1_path, &ChunkIndices(vec![0])).await.unwrap();
=> {chunk:?}
```
 "#
    );

    println!();
    println!("## Committing");
    let v2_id = ds.flush("a message", Default::default()).await?;
    println!(
        r#"
```
ds.flush("a message", Default::default()).await?;
=> {v2_id:?}
```
 "#
    );

    println!("## Creating a new Repository instance @ latest version");

    let mut ds = Repository::update(Arc::clone(&storage), v2_id.clone()).build();

    println!(
        r#"
```
let mut ds = Repository::update(Arc::clone(&storage), ObjectId.from("{v2_id:?}"));
```
 "#
    );

    print_nodes(&ds).await?;

    println!("## Adding a new inline chunk");
    ds.set_chunk_ref(
        array1_path.clone(),
        ChunkIndices(vec![1]),
        Some(ChunkPayload::Inline("bye".into())),
    )
    .await?;

    println!(
        r#"
```
ds.set_chunk(
    array1_path.clone(),
    ChunkIndices(vec![1]),
    Some(icechunk::ChunkPayload::Inline(b"bye".into())),
)
.await?;
```
 "#,
    );

    let chunk = ds.get_chunk_ref(&array1_path, &ChunkIndices(vec![1])).await.unwrap();
    println!("Reading the new chunk => `{chunk:?}`");

    let chunk = ds.get_chunk_ref(&array1_path, &ChunkIndices(vec![0])).await.unwrap();
    println!("Reading the old chunk => `{chunk:?}`");

    println!();
    println!("## Committing");
    let v3_id = ds.flush("commit", Default::default()).await?;
    println!(
        r#"
```
ds.flush("commit", Default::default()).await?;
=> {v3_id:?}
```
 "#
    );

    println!("Creating a new Repository instance, on the previous version");
    let ds = Repository::update(Arc::clone(&storage), v2_id.clone()).build();

    println!(
        r#"
```
let ds = Repository::update(Arc::clone(&storage), ObjectId::from("{v2_id:?}"));
```
 "#
    );

    print_nodes(&ds).await?;

    let chunk = ds.get_chunk_ref(&array1_path, &ChunkIndices(vec![0])).await.unwrap();
    println!("Reading the old chunk: {chunk:?}");

    let chunk = ds.get_chunk_ref(&array1_path, &ChunkIndices(vec![1])).await;
    println!("Reading the new chunk: {chunk:?}");

    Ok(())
}

async fn print_nodes(ds: &Repository) -> Result<(), StoreError> {
    println!("### List of nodes");
    let rows = ds
        .list_nodes()
        .await?
        .sorted_by_key(|n| n.path.clone())
        .map(|node| {
            format!(
                "|{:10?}|{:15}|{:10?}\n",
                node.node_type(),
                node.path.to_string(),
                node.user_attributes,
            )
        })
        .format("");

    println!("{}", rows);
    Ok(())
}
