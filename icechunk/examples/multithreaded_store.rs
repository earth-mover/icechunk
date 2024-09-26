use std::{ops::Range, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::StreamExt;
use icechunk::{format::ByteRange, Store};
use tokio::{sync::RwLock, task::JoinSet, time::sleep};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store_config_json = br#"
        {"storage":
            {"type": "in_memory"},
            "repository": {
                "previous_version": null,
                "inline_chunk_threshold_bytes":128
            }}
    "#;

    let store =
        Store::from_json(store_config_json, icechunk::zarr::AccessMode::ReadWrite)
            .await?;
    let store = Arc::new(RwLock::new(store));

    store
        .write()
        .await
        .set(
            "zarr.json",
            Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
        )
        .await?;

    let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[10000],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0.0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x"]}"#);
    store.write().await.set("array/zarr.json", zarr_meta.clone()).await?;

    // We start 100 async tasks that loop until they successfully read a specific chunk.
    // Initially this will loop, because no chunks are written.
    let mut set = JoinSet::new();
    for i in 500..600 {
        let store = Arc::clone(&store);
        set.spawn(async move {
            let mut attempts = 0u64;
            loop {
                if let Ok(value) = store
                    .read()
                    .await
                    .get(format!("array/c/{i}").as_str(), &ByteRange::ALL)
                    .await
                {
                    println!("Got {value:?} in {attempts} attempts");
                    return value;
                } else {
                    attempts += 1;
                    // in normal code we wouldn't poll constantly
                    sleep(Duration::from_millis(0)).await;
                }
            }
        });
    }

    async fn writer(name: &str, range: Range<u64>, store: Arc<RwLock<Store>>) {
        println!("Starting writer {name}.");
        for i in range {
            if let Err(err) = store
                .write()
                .await
                .set(
                    format!("array/c/{i}").as_str(),
                    Bytes::from(format!("Writer {name} wrote {i}")),
                )
                .await
            {
                dbg!(err);
            }

            sleep(Duration::from_millis(20)).await;
        }
        println!("Writer {name} is done.");
    }

    // We start two tasks that write all the needed chunks, sleeping between writes.
    let store1 = Arc::clone(&store);
    let store2 = Arc::clone(&store);

    let writer1 = tokio::spawn(async move { writer("1", 500..550, store1).await });
    let writer2 = tokio::spawn(async move { writer("2", 550..600, store2).await });

    // we join all tasks, program will stop once all chunks are successfully read
    writer1.await?;
    writer2.await?;
    while (set.join_next().await).is_some() {}

    let all_keys = store.read().await.list().await?.count().await;
    println!("Found {all_keys} keys in the store: 100 chunks + 1 root group metadata + 1 array metadata");

    Ok(())
}
