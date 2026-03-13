/// Integration tests for network failure retry behavior with MinIO + toxiproxy
///
/// These tests verify that we retry on various network errors that occur while
/// streaming data, not during connection establishment. The AWS SDK's built-in
/// retry loop doesn't catch these post-connection errors, so we add our own
/// retry in `AssetManager::fetch_chunk`.
///
/// Tests require MinIO and Toxiproxy running via docker compose:
/// ```bash
/// docker compose up -d
/// cargo test --test test_bad_connections -- --nocapture
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

use crate::common::Permission;

/// Create S3 storage pointing to toxiproxy (which proxies to MinIO)
fn create_proxied_storage(
    proxy_port: u16,
    timeout_seconds: u32,
    prefix: &str,
) -> Result<Arc<S3Storage>, Box<dyn std::error::Error>> {
    let (access_key_id, secret_access_key) = Permission::Modify.keys();
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
        Some(format!("{}-{}", prefix, uuid::Uuid::new_v4())),
        S3Credentials::Static(S3StaticCredentials {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: None,
            expires_after: None,
        }),
        true,
        Vec::new(),
        Vec::new(),
    )?;

    Ok(Arc::new(storage))
}

/// Set up toxiproxy: create a new proxy on the given port, return (client, proxy_name).
/// If another proxy already holds the port (e.g. from a crashed test), it is
/// deleted first. Each test deletes its own proxy on cleanup.
async fn setup_toxiproxy(
    name: &str,
    port: u16,
) -> Result<(Client, String), Box<dyn std::error::Error>> {
    let client = Client::new("http://localhost:8474");

    // Delete any proxy already bound to this port.
    // This handles leftover proxies from crashed/aborted test runs without
    // wiping unrelated proxies.
    // Uncomment the block below to nuke all proxies during development:
    // let proxies = client.proxies().await.unwrap_or_default();
    // for (name, proxy) in proxies {
    //     println!("   Deleting old proxy: {}", name);
    //     proxy.delete().await.ok();
    // }
    let port_suffix = format!(":{port}");
    let proxies = client.proxies().await.unwrap_or_default();
    for (name, proxy) in proxies {
        if proxy.config.listen.ends_with(&port_suffix) {
            println!("Deleting stale proxy on :{}: {}", port, name);
            proxy.delete().await.ok();
        }
    }

    let listen = format!("0.0.0.0:{port}");
    let name = name.to_string();
    client.create_proxy(&name, &listen, "rustfs:9000").await?;
    println!("Created proxy: {} ({}) -> rustfs:9000", name, listen);

    Ok((client, name))
}

/// Create an AssetManager with fast retry settings for testing
fn create_test_manager(storage: Arc<S3Storage>) -> AssetManager {
    let settings = icechunk::storage::Settings {
        retries: Some(RetriesSettings {
            max_tries: Some(NonZeroU16::new(3).unwrap()),
            initial_backoff_ms: Some(100),
            max_backoff_ms: Some(1000),
        }),
        ..Default::default()
    };

    AssetManager::new_no_cache(
        storage as Arc<dyn Storage + Send + Sync>,
        settings,
        SpecVersionBin::default(),
        1,
        100,
    )
}

/// Write a test chunk and verify a clean read works
async fn write_and_verify_chunk(
    manager: &AssetManager,
) -> Result<(ChunkId, Bytes), Box<dyn std::error::Error>> {
    let chunk_id = ChunkId::random();
    let test_data = Bytes::from(vec![42u8; 50 * 1024]); // 50KB
    manager.write_chunk(chunk_id.clone(), test_data.clone()).await?;
    println!("Wrote {} bytes", test_data.len());

    let range = 0..test_data.len() as u64;
    let read_data = manager.fetch_chunk(&chunk_id, &range).await?;
    assert_eq!(test_data.len(), read_data.len());
    println!("Read without toxics completed successfully");

    Ok((chunk_id, test_data))
}

/// Add a reset_peer toxic via the toxiproxy REST API.
/// The noxious-client crate (v1.0) doesn't have a ResetPeer variant,
/// so we use the HTTP API directly via reqwest.
/// https://github.com/oguzbilgener/noxious/issues/1
async fn add_reset_peer_toxic(
    proxy_name: &str,
    toxic_name: &str,
    timeout_ms: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("http://localhost:8474/proxies/{}/toxics", proxy_name);
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({
            "name": toxic_name,
            "type": "reset_peer",
            "stream": "downstream",
            "toxicity": 1.0,
            "attributes": { "timeout": timeout_ms }
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        let text = resp.text().await?;
        return Err(format!("Failed to add reset_peer toxic: {}", text).into());
    }
    Ok(())
}

/// Remove a toxic via the toxiproxy REST API.
/// Again needed because noxious doesn't support ResetPeer
/// https://github.com/oguzbilgener/noxious/issues/1
async fn remove_toxic_via_api(
    proxy_name: &str,
    toxic_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url =
        format!("http://localhost:8474/proxies/{}/toxics/{}", proxy_name, toxic_name);
    let resp = reqwest::Client::new().delete(&url).send().await?;
    if !resp.status().is_success() {
        let text = resp.text().await?;
        return Err(format!("Failed to remove toxic: {}", text).into());
    }
    Ok(())
}

// ── Stalled stream test ─────────────────────────────────────────────────────
/// This test verifies that we retry on "stalled stream errors"
/// (ThroughputBelowMinimum) that occur while streaming data, not during
/// connection establishment. The AWS SDK's built-in retry loop doesn't catch
/// these, so we add our own retry in `AssetManager::fetch_chunk`.

#[icechunk_macros::tokio_test]
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn test_stalled_stream() -> Result<(), Box<dyn std::error::Error>> {
    let (client, name) = setup_toxiproxy("stalled_stream", 9002).await?;

    let storage = create_proxied_storage(9002, 2, "stalled-stream-test")?;
    let manager = create_test_manager(storage);
    let (chunk_id, test_data) = write_and_verify_chunk(&manager).await?;
    let range = 0..test_data.len() as u64;

    // Poison the connection: limit data then hold connection open
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

    // Remove the toxics *while* we are fetching a chunk.
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

    let (fetch_result, _) = tokio::join!(grab_data, remove_toxics);

    assert_eq!(fetch_result.unwrap().len(), test_data.len());
    println!("Successfully read data after stalled stream recovery!");

    client.proxy(&name).await?.delete().await?;
    Ok(())
}

// ── Connection reset test ───────────────────────────────────────────────────

#[icechunk_macros::tokio_test]
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn test_connection_reset() -> Result<(), Box<dyn std::error::Error>> {
    let (client, proxy_name) = setup_toxiproxy("connection_reset", 9003).await?;

    let storage = create_proxied_storage(9003, 5, "connection-reset-test")?;
    let manager = create_test_manager(storage);
    let (chunk_id, test_data) = write_and_verify_chunk(&manager).await?;
    let range = 0..test_data.len() as u64;

    // Inject reset_peer toxic — toxiproxy sends TCP RST immediately (timeout=0)
    add_reset_peer_toxic(&proxy_name, "reset", 10).await?;
    println!("Added reset_peer toxic");

    // With the toxic active persistently, fetch must fail
    println!("Reading with reset_peer toxic (expecting connection error)...");
    let result = manager.fetch_chunk(&chunk_id, &range).await;

    match &result {
        Ok(_) => println!("WARNING: Read succeeded despite reset_peer toxic!"),
        Err(e) => println!("Got expected error: {e:?}"),
    }
    assert!(result.is_err(), "Should have failed with connection reset error");

    // Remove the toxic *while* we are fetching a chunk.
    // If retry logic handles connection resets, the fetch should eventually
    // succeed once the toxic is removed.
    println!(
        "\nTesting retry recovery: fetching with reset_peer, will remove toxic after delay..."
    );

    let pname = proxy_name.clone();
    let grab_data = manager.fetch_chunk(&chunk_id, &range);
    let remove_toxic_task = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("Removing reset_peer toxic");
        remove_toxic_via_api(&pname, "reset").await.unwrap();
    };

    let (fetch_result, _) = tokio::join!(grab_data, remove_toxic_task);

    match &fetch_result {
        Ok(data) => {
            println!("Successfully read {} bytes after toxic removed!", data.len())
        }
        Err(e) => println!("Fetch failed even after toxic removed: {e:?}"),
    }

    assert_eq!(
        fetch_result.unwrap().len(),
        test_data.len(),
        "Should have recovered after toxic was removed (retries should handle connection reset)"
    );

    client.proxy(&proxy_name).await?.delete().await?;
    Ok(())
}
