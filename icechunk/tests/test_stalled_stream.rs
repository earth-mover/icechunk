use bytes::Bytes;
/// Integration test for AWS SDK stalled stream protection with MinIO + toxiproxy
///
/// This test verifies that we retry on "stalled stream errors"
/// These errors manifest as
// E           icechunk.IcechunkError:   x I/O error: streaming error
// E             |
// E             | context:
// E             |    0: icechunk::storage::s3::fetch_chunk
// E             |            with id=R1QAWQYJEJ335Y6EFHE0 range=0..493214
// E             |              at icechunk/src/storage/s3.rs:681
// E             |    1: icechunk::asset_manager::fetch_chunk
// E             |            with chunk_id=R1QAWQYJEJ335Y6EFHE0 range=0..493214
// E             |              at icechunk/src/asset_manager.rs:361
// E             |    2: icechunk::store::get
// E             |            with key="rasm/Tair/c/0/3/2" byte_range=From(0)
// E             |              at icechunk/src/store.rs:198
// E             |
// E             |-> I/O error: streaming error
// E             |-> streaming error
// E             `-> minimum throughput was specified at 1 B/s, but throughput of 0 B/s was observed
///
/// Importantly such errors are raised while streaming out data, not when the connection is established
/// so they are not caught by the AWS SDK's retry loop.
///
/// I had to use Claude to figure out this strategy trigger stalled stream protection:
/// - LimitData toxic: Sends only initial bytes (1000 bytes), then stops sending
/// - SlowClose toxic: Keeps TCP connection open (prevents hyper's IncompleteBody error)
/// - Result: Data flow stops (0 B/s) while connection stays open
/// - SDK sees: data stopped flowing, but connection not closed
/// - After grace period, SDK measures 0 B/s < 1 B/s → ThroughputBelowMinimum error
///
/// This test requires MinIO and Toxiproxy running via docker compose:
/// ```bash
/// docker compose up -d
/// cargo test test_stalled_stream --test test_stalled_stream -- --ignored --nocapture
/// ```
use std::time::Duration;

use icechunk::{
    Storage,
    config::{S3Credentials, S3Options, S3StaticCredentials},
    format::ChunkId,
    storage::{RetriesSettings, s3::S3Storage},
};
use noxious_client::{Client, StreamDirection, Toxic, ToxicKind};
use std::{num::NonZeroU16, sync::Arc};

/// Create S3 storage pointing to toxiproxy (which proxies to MinIO)
async fn create_proxied_storage(
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
async fn test_stalled_stream_with_toxiproxy() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to toxiproxy
    let client = Client::new("http://localhost:8474");

    // Clean up any leftover proxies from previous runs
    println!("Cleaning up old proxies...");
    let proxies = client.proxies().await.unwrap_or_default();
    for (name, proxy) in proxies {
        println!("   Deleting old proxy: {}", name);
        proxy.delete().await.ok(); // Ignore errors
    }

    // Create a proxy: toxiproxy listens on 9002, forwards to MinIO on 9000
    let name = format!("minio_test_{}", uuid::Uuid::new_v4());
    let proxy = client.create_proxy(&name, "0.0.0.0:9002", "minio:9000").await?;
    println!("Created proxy: {} -> minio:9000", name);

    // Create storage with 2-second grace period for stalled stream detection
    let storage = create_proxied_storage(9002, 2).await?;

    // Use fewer, faster retries to reduce test time
    let mut settings = storage.default_settings();
    settings.retries = Some(RetriesSettings {
        max_tries: Some(NonZeroU16::new(3).unwrap()),
        initial_backoff_ms: Some(100),
        max_backoff_ms: Some(1000),
    });

    // Let's write a chunk
    let chunk_id = ChunkId::random();
    let test_data = Bytes::from(vec![42u8; 50 * 1024]); // 50KB of data
    storage.write_chunk(&settings, chunk_id.clone(), test_data.clone()).await?;
    println!("Wrote {} bytes", test_data.len());

    // Read without toxics should succeed
    let read_data =
        storage.fetch_chunk(&settings, &chunk_id, &(0..test_data.len() as u64)).await?;
    assert_eq!(test_data.len(), read_data.len());
    println!("Read without toxics completed successfully");

    // Now we poison the connection
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
    // "Reading chunks as a pastime activity
    //  The toxicity of our city, of our city"
    let result =
        storage.fetch_chunk(&settings, &chunk_id, &(0..test_data.len() as u64)).await;

    let err = result.expect_err("Should have failed with minimum throughput error");
    assert!(format!("{err:?}").contains("ThroughputBelowMinimum"));

    // Now we remove the toxics *while* we are fetching a chunk.
    // The 7 second wait time on removing toxics is empirical but logs show that we consistently
    // remove it after a couple of retries
    // Logs:
    //    Fetching chunk again
    //      DEBUG fetch_chunk{id=5RJVAV34MK97ND5G4WAG range=0..51200}: icechunk::storage::s3: retrying on stalled stream error after 100ms.
    //      DEBUG fetch_chunk{id=5RJVAV34MK97ND5G4WAG range=0..51200}: icechunk::storage::s3: retrying on stalled stream error after 200.000003ms.
    //    Removing toxics
    //      DEBUG fetch_chunk{id=5RJVAV34MK97ND5G4WAG range=0..51200}: icechunk::storage::s3: retrying on stalled stream error after 400.000006ms.
    //    Successfully read data!
    let range = 0..test_data.len() as u64;
    let grab_data = {
        println!("Fetching chunk again");
        // "With a taste of your chunks, I'm on a ride
        // You're toxic, I'm slippin' under"
        storage.fetch_chunk(&settings, &chunk_id, &range)
    };
    let remove_toxics = async {
        // This will wait for a couple of retries
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
