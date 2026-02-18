/// Integration test for AWS SDK stalled stream protection with MinIO + toxiproxy
///
/// This test verifies that we retry on "stalled stream errors"
/// (ThroughputBelowMinimum) that occur while streaming data, not during
/// connection establishment. The AWS SDK's built-in retry loop doesn't catch
/// these, so we add our own retry in `AssetManager::fetch_chunk`.
///
/// This test requires MinIO and Toxiproxy running via docker compose:
/// ```bash
/// docker compose up -d
/// cargo test test_stalled_stream --test test_stalled_stream -- --nocapture
/// ```
use std::{num::NonZeroU16, sync::Arc, time::Duration};

use bytes::Bytes;
use icechunk::{
    Storage,
    asset_manager::AssetManager,
    config::{S3Credentials, S3Options, S3StaticCredentials},
    format::{ChunkId, format_constants::SpecVersionBin},
    storage::{RetriesSettings, s3::S3Storage},
};
use noxious_client::{Client, StreamDirection, Toxic, ToxicKind};

/// Create S3 storage pointing to toxiproxy (which proxies to MinIO)
fn create_proxied_storage(
    proxy_port: u16,
    timeout_seconds: u32,
) -> Result<Arc<S3Storage>, Box<dyn std::error::Error>> {
    let storage = S3Storage::new(
        S3Options {
            region: Some("us-east-1".to_string()),
            endpoint_url: Some(format!("http://localhost:{}", proxy_port)),
            allow_http: true,
            anonymous: false,
            force_path_style: true,
            network_stream_timeout_seconds: Some(timeout_seconds),
            requester_pays: false,
        },
        "testbucket".to_string(),
        Some(format!("stalled-stream-test-{}", uuid::Uuid::new_v4())),
        S3Credentials::Static(S3StaticCredentials {
            access_key_id: "minio123".into(),
            secret_access_key: "minio123".into(),
            session_token: None,
            expires_after: None,
        }),
        true,
        Vec::new(),
        Vec::new(),
    )?;

    Ok(Arc::new(storage))
}

#[icechunk_macros::tokio_test]
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn test_stalled_stream_with_toxiproxy() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to toxiproxy
    let client = Client::new("http://localhost:8474");

    // Clean up any leftover proxies from previous runs
    println!("Cleaning up old proxies...");
    let proxies = client.proxies().await.unwrap_or_default();
    for (name, proxy) in proxies {
        println!("   Deleting old proxy: {}", name);
        proxy.delete().await.ok();
    }

    // Create a proxy: toxiproxy listens on 9002, forwards to MinIO on 9000
    let name = format!("minio_test_{}", uuid::Uuid::new_v4());
    let proxy = client.create_proxy(&name, "0.0.0.0:9002", "rustfs:9000").await?;
    println!("Created proxy: {} -> rustfs:9000", name);

    // Create storage with 2-second grace period for stalled stream detection
    let storage = create_proxied_storage(9002, 2)?;

    // Use fewer, faster retries to reduce test time
    let settings = icechunk::storage::Settings {
        retries: Some(RetriesSettings {
            max_tries: Some(NonZeroU16::new(3).unwrap()),
            initial_backoff_ms: Some(100),
            max_backoff_ms: Some(1000),
        }),
        ..Default::default()
    };

    let manager = AssetManager::new_no_cache(
        storage.clone() as Arc<dyn Storage + Send + Sync>,
        settings.clone(),
        SpecVersionBin::default(),
        1,
        100,
    );

    // Write a chunk
    let chunk_id = ChunkId::random();
    let test_data = Bytes::from(vec![42u8; 50 * 1024]); // 50KB of data
    manager.write_chunk(chunk_id.clone(), test_data.clone()).await?;
    println!("Wrote {} bytes", test_data.len());

    // Read without toxics should succeed
    let range = 0..test_data.len() as u64;
    let read_data = manager.fetch_chunk(&chunk_id, &range).await?;
    assert_eq!(test_data.len(), read_data.len());
    println!("Read without toxics completed successfully");

    // Now poison the connection
    let toxic1 = Toxic {
        kind: ToxicKind::LimitData { bytes: 1000 },
        name: "limit_data".into(),
        toxicity: 1.0,
        direction: StreamDirection::Downstream,
    };
    client.proxy(&name).await?.add_toxic(&toxic1).await?;

    let toxic2 = Toxic {
        kind: ToxicKind::SlowClose { delay: 30000 },
        name: "slow_close".into(),
        toxicity: 1.0,
        direction: StreamDirection::Downstream,
    };
    client.proxy(&name).await?.add_toxic(&toxic2).await?;

    println!("Reading with slowdown (expecting minimum throughput error)...");
    let result = manager.fetch_chunk(&chunk_id, &range).await;

    let err = result.expect_err("Should have failed with minimum throughput error");
    assert!(format!("{err:?}").contains("ThroughputBelowMinimum"));

    // Now remove the toxics *while* we are fetching a chunk.
    // The retry logic should eventually succeed once the toxics are removed.
    let grab_data = {
        println!("Fetching chunk again");
        manager.fetch_chunk(&chunk_id, &range)
    };
    let remove_toxics = async {
        // Wait for a couple of retries before removing toxics
        tokio::time::sleep(Duration::from_secs(7)).await;
        println!("Removing toxics");
        client.proxy(&name).await.unwrap().remove_toxic("slow_close").await.unwrap();
        client.proxy(&name).await.unwrap().remove_toxic("limit_data").await.unwrap();
    };

    // Remove toxics while chunk is fetched
    let (fetch_result, _) = tokio::join!(grab_data, remove_toxics);

    assert_eq!(fetch_result.unwrap().len(), test_data.len());
    println!("Successfully read data!");

    // Cleanup
    proxy.delete().await?;

    Ok(())
}
