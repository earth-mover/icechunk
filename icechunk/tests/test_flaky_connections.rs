/// Integration tests for network failure retry behavior with `MinIO` + toxiproxy
///
/// These tests verify that we retry on various network errors that occur while
/// streaming data, not during connection establishment. The AWS SDK's built-in
/// retry loop doesn't catch these post-connection errors, so we add our own
/// retry in `AssetManager::fetch_chunk`.
///
/// Tests require `MinIO` and Toxiproxy running via docker compose:
/// ```bash
/// docker compose up -d
/// cargo test --test test_bad_connections -- --nocapture
/// ```
use std::{num::NonZeroU16, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::TryStreamExt as _;
use icechunk::{
    Storage,
    asset_manager::AssetManager,
    config::{S3Credentials, S3Options, S3StaticCredentials},
    format::{ChunkId, format_constants::SpecVersionBin},
    storage::{RetriesSettings, S3Storage, TimeoutSettings},
};
use noxious_client::{Client, StreamDirection, Toxic, ToxicKind};

use crate::common::Permission;

/// Create S3 storage pointing to toxiproxy (which proxies to `MinIO`)
fn create_proxied_storage(
    proxy_port: u16,
    timeout_seconds: u32,
    prefix: &str,
) -> Result<Arc<S3Storage>, Box<dyn std::error::Error>> {
    let (access_key_id, secret_access_key) = Permission::Modify.keys();
    let storage = S3Storage::new(
        S3Options::default()
            .with_region("us-east-1")
            .with_endpoint_url(format!("http://localhost:{proxy_port}"))
            .with_allow_http(true)
            .with_force_path_style(true)
            .with_network_stream_timeout_seconds(timeout_seconds),
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
        None,
    )?;

    Ok(Arc::new(storage))
}

/// Delete a proxy by name via the toxiproxy REST API, ignoring a 404.
///
/// We can't use `noxious_client` for this: its deserializer chokes on any proxy
/// that has toxics attached (e.g. left behind by a crashed run), which is
/// exactly the case we need to clean up. The raw REST endpoint doesn't
/// deserialize toxics, so it always works.
async fn delete_proxy_via_api(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("http://localhost:8474/proxies/{name}");
    let resp = reqwest::Client::new().delete(&url).send().await?;
    // 204 = deleted, 404 = wasn't there; both are fine.
    if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
        let text = resp.text().await?;
        return Err(format!("Failed to delete proxy {name}: {text}").into());
    }
    Ok(())
}

/// Set up toxiproxy: create a new proxy on the given port, return (client, `proxy_name`).
/// Any existing proxy with the same name (e.g. left by a crashed test run) is
/// deleted first so `create_proxy` can't 409. Each test deletes its own proxy
/// on cleanup.
async fn setup_toxiproxy(
    name: &str,
    port: u16,
) -> Result<(Client, String), Box<dyn std::error::Error>> {
    let client = Client::new("http://localhost:8474");

    // Delete by name before creating. This survives a crashed run that left a
    // toxic-laden proxy — unlike scanning `client.proxies()`, whose deserializer
    // fails on proxies that have toxics attached.
    delete_proxy_via_api(name).await?;

    let listen = format!("0.0.0.0:{port}");
    let name = name.to_string();
    client.create_proxy(&name, &listen, "rustfs:9000").await?;
    println!("Created proxy: {name} ({listen}) -> rustfs:9000");

    Ok((client, name))
}

/// Create an `AssetManager` with fast retry settings for testing
fn create_test_manager(storage: Arc<S3Storage>) -> AssetManager {
    let settings = icechunk::storage::Settings {
        retries: Some(RetriesSettings {
            #[expect(clippy::unwrap_used)]
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

/// Add a `reset_peer` toxic via the toxiproxy REST API.
/// The noxious-client crate (v1.0) doesn't have a `ResetPeer` variant,
/// so we use the HTTP API directly via reqwest.
/// <https://github.com/oguzbilgener/noxious/issues/1>
async fn add_reset_peer_toxic(
    proxy_name: &str,
    toxic_name: &str,
    timeout_ms: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("http://localhost:8474/proxies/{proxy_name}/toxics");
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
        return Err(format!("Failed to add reset_peer toxic: {text}").into());
    }
    Ok(())
}

/// Remove a toxic via the toxiproxy REST API.
/// Again needed because noxious doesn't support `ResetPeer`
/// <https://github.com/oguzbilgener/noxious/issues/1>
async fn remove_toxic_via_api(
    proxy_name: &str,
    toxic_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("http://localhost:8474/proxies/{proxy_name}/toxics/{toxic_name}");
    let resp = reqwest::Client::new().delete(&url).send().await?;
    if !resp.status().is_success() {
        let text = resp.text().await?;
        return Err(format!("Failed to remove toxic: {text}").into());
    }
    Ok(())
}

// ── Stalled stream test ─────────────────────────────────────────────────────
/// This test verifies that we retry on "stalled stream errors"
/// (`ThroughputBelowMinimum`) that occur while streaming data, not during
/// connection establishment. The AWS SDK's built-in retry loop doesn't catch
/// these, so we add our own retry in `AssetManager::fetch_chunk`.

#[icechunk_macros::tokio_test]
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
            println!("Successfully read {} bytes after toxic removed!", data.len());
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

// ── Conditional PUT test ───────────────────────────────────────────────────

#[icechunk_macros::tokio_test]
async fn conditional_put() -> Result<(), Box<dyn std::error::Error>> {
    let (client, proxy_name) = setup_toxiproxy("conditional-put-test", 9004).await?;

    let storage = create_proxied_storage(9004, 3, "conditional-put-test")?;

    // Truncates the downstream connection after 300 bytes -- enough for the
    // small response payloads of the snapshot/transaction PUTs to fit, but
    // cuts the response of the conditional repo_info PUT mid-flight, so the AWS
    // SDK retries it and trips the spurious-conflict bug.
    let toxic_bytes: u64 = 300;
    let url = format!("http://localhost:8474/proxies/conditional-put-test/toxics");
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({
            "name": "conditional_put",
            "type": "limit_data",
            "stream": "downstream",
            "toxicity": 1.0,
            "attributes": { "bytes": toxic_bytes }
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        let text = resp.text().await?;
        return Err(format!("Failed to add limit_data toxic: {text}").into());
    }

    println!("Added limit_data toxic");

    let settings = icechunk::storage::Settings {
        retries: Some(RetriesSettings {
            #[expect(clippy::unwrap_used)]
            max_tries: Some(NonZeroU16::new(5).unwrap()),
            initial_backoff_ms: Some(50),
            max_backoff_ms: Some(500),
        }),
        timeouts: Some(TimeoutSettings {
            #[expect(clippy::unwrap_used)]
            read_timeout_ms: Some(2000),
            operation_attempt_timeout_ms: Some(3000),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Keep a handle to the storage so we can inspect what landed in the backend
    // after the (expected) failure.
    let storage_for_list = Arc::clone(&storage);

    let err = icechunk::Repository::create(
        Some(icechunk::RepositoryConfig {
            storage: Some(settings),
            ..Default::default()
        }),
        storage,
        std::collections::HashMap::new(),
        Some(SpecVersionBin::default()),
        false,
    )
    .await;

    // Remove the toxic so the listing below isn't itself truncated.
    remove_toxic_via_api(&proxy_name, "conditional_put").await?;

    // The conditional repo_info PUT actually succeeded on the server; only its
    // response was lost. So all three init objects (snapshot, transaction log,
    // repo info) should be present in storage.
    let list_settings = storage_for_list.default_settings().await?;
    let keys: Vec<String> = storage_for_list
        .list_objects(&list_settings, "")
        .await?
        .map_ok(|li| li.id)
        .try_collect()
        .await?;
    println!("keys committed to storage: {}/3", keys.len());
    for k in &keys {
        println!("  - {k}");
    }

    // The bug: despite all three PUTs landing, icechunk surfaces a spurious
    // conflict error because the AWS SDK retried the conditional repo_info PUT
    // after its response was lost, and the retry saw the object it had just
    // written as a pre-existing conflict.
    let err = err.expect_err("create should surface the spurious conflict bug");
    let msg = format!("{err}");
    println!("icechunk surfaced error: {msg}");
    assert!(
        [
            "repo info object was updated",
            "branch update conflict",
            "config was updated by other session",
            "expected parent",
        ]
        .iter()
        .any(|marker| msg.contains(marker)),
        "expected a spurious conflict error, got: {msg}"
    );
    assert_eq!(
        keys.len(),
        3,
        "all three init objects should have landed in storage despite the error"
    );

    client.proxy(&proxy_name).await?.delete().await?;
    Ok(())
}
