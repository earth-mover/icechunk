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
    )?;

    Ok(Arc::new(storage))
}

/// Set up toxiproxy: create a new proxy on the given port, return (client, `proxy_name`).
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
            println!("Deleting stale proxy on :{port}: {name}");
            let _ = proxy.delete().await;
        }
    }

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

// ── Conditional PUT regression tests for issue #2099 ───────────────────────
//
// Model a lost-response on a conditional PUT: the body reaches the server but
// the ack doesn't reach the client, so the underlying HTTP client retries
// and the retry trips the conditional header against the just-written object.
// Without the fix the spurious 412 surfaces as a `NotOnLatestVersion`
// ("repo info object was updated..."); the `*_long_blip` variant additionally
// covers the `object_store` case where every retry's response also gets cut
// and the failure arrives on the generic-error arm instead of `Precondition`.

#[derive(Clone, Copy)]
enum ConditionalPutBackend {
    IcechunkS3,
    ArrowObjectStore,
}

const LOST_RESPONSE_TOXIC_NAME: &str = "lost_response";

async fn build_proxied_storage(
    backend: ConditionalPutBackend,
    proxy_label: &str,
    port: u16,
) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
    let (access_key_id, secret_access_key) = Permission::Modify.keys();
    let credentials = S3Credentials::Static(S3StaticCredentials {
        access_key_id: access_key_id.into(),
        secret_access_key: secret_access_key.into(),
        session_token: None,
        expires_after: None,
    });
    let s3_options = S3Options::default()
        .with_region("us-east-1")
        .with_endpoint_url(format!("http://localhost:{port}"))
        .with_allow_http(true)
        .with_force_path_style(true)
        .with_network_stream_timeout_seconds(3);
    let prefix = format!("{proxy_label}-{}", uuid::Uuid::new_v4());
    Ok(match backend {
        ConditionalPutBackend::IcechunkS3 => Arc::new(S3Storage::new(
            s3_options,
            "testbucket".to_string(),
            Some(prefix),
            credentials,
            true,
            Vec::new(),
            Vec::new(),
        )?),
        ConditionalPutBackend::ArrowObjectStore => Arc::new(
            icechunk::ObjectStorage::new_s3(
                "testbucket".to_string(),
                Some(prefix),
                Some(credentials),
                Some(s3_options),
            )
            .await?,
        ),
    })
}

/// Retry/timeout knobs tight enough to keep the test under a second on a
/// healthy machine, generous enough for the readback's retry budget to
/// outlast the modelled blip.
fn default_lost_response_settings() -> icechunk::storage::Settings {
    icechunk::storage::Settings {
        retries: Some(RetriesSettings {
            #[expect(clippy::unwrap_used)]
            max_tries: Some(NonZeroU16::new(5).unwrap()),
            initial_backoff_ms: Some(50),
            max_backoff_ms: Some(500),
        }),
        timeouts: Some(TimeoutSettings {
            read_timeout_ms: Some(2000),
            operation_attempt_timeout_ms: Some(3000),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn install_limit_data_toxic(
    proxy_label: &str,
    toxic_name: &str,
    bytes: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("http://localhost:8474/proxies/{proxy_label}/toxics");
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({
            "name": toxic_name,
            "type": "limit_data",
            "stream": "downstream",
            "toxicity": 1.0,
            "attributes": { "bytes": bytes }
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        let text = resp.text().await?;
        return Err(format!("Failed to add limit_data toxic: {text}").into());
    }
    Ok(())
}

/// Serializes the lost-response tests within one process (`cargo test`) so the
/// concurrent load doesn't starve the toxic-removal timer they race. No-op
/// under nextest (process-per-test) — `.config/nextest.toml` serializes there.
static LOST_RESPONSE_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Sets up the toxic, then hands `(storage, settings, toxic_remover)` to
/// `action`; the closure is responsible for `.await??`-ing the
/// `toxic_remover` before any post-action storage ops that need the toxic
/// cleared. The harness deletes the proxy on return either way.
async fn with_lost_response_harness<F, Fut>(
    backend: ConditionalPutBackend,
    proxy_label: &str,
    port: u16,
    toxic_bytes: u64,
    toxic_removal_delay_ms: u64,
    action: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(
        Arc<dyn Storage + Send + Sync>,
        icechunk::storage::Settings,
        tokio::task::JoinHandle<Result<(), String>>,
    ) -> Fut,
    Fut: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let _serial = LOST_RESPONSE_SERIAL.lock().await;
    let (client, proxy_name) = setup_toxiproxy(proxy_label, port).await?;
    let storage = build_proxied_storage(backend, proxy_label, port).await?;
    // Warm up credentials/identity-cache and the pooled connection before
    // arming the toxic, so the byte budget is spent on the trigger path
    // (PUT responses) and not on cold-start traffic.
    let warmup_settings = storage.default_settings().await?;
    let _: Vec<_> =
        storage.list_objects(&warmup_settings, "").await?.try_collect().await?;
    install_limit_data_toxic(proxy_label, LOST_RESPONSE_TOXIC_NAME, toxic_bytes).await?;

    let removal_proxy = proxy_name.clone();
    let toxic_remover = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(toxic_removal_delay_ms)).await;
        remove_toxic_via_api(&removal_proxy, LOST_RESPONSE_TOXIC_NAME)
            .await
            .map_err(|e| e.to_string())
    });

    let result =
        action(Arc::clone(&storage), default_lost_response_settings(), toxic_remover)
            .await;

    client.proxy(&proxy_name).await?.delete().await?;
    result
}

async fn conditional_put_repro(
    backend: ConditionalPutBackend,
    proxy_label: &str,
    port: u16,
    toxic_removal_delay_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // 300 bytes: snapshot/transaction PUT responses fit, repo_info doesn't.
    with_lost_response_harness(
        backend,
        proxy_label,
        port,
        300,
        toxic_removal_delay_ms,
        |storage, settings, toxic_remover| async move {
            let storage_for_list = Arc::clone(&storage);
            let result = icechunk::Repository::create(
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

            toxic_remover.await??;

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

            if let Err(err) = &result {
                panic!(
                    "create should succeed despite the lost conditional PUT response, got: {err}"
                );
            }
            assert_eq!(
                keys.len(),
                3,
                "all three init objects should have landed in storage"
            );
            Ok(())
        },
    )
    .await
}

#[icechunk_macros::tokio_test]
async fn conditional_put() -> Result<(), Box<dyn std::error::Error>> {
    conditional_put_repro(
        ConditionalPutBackend::IcechunkS3,
        "conditional-put-test",
        9004,
        100,
    )
    .await
}

/// Short blip: `object_store`'s retry lands cleanly and surfaces a 412.
#[icechunk_macros::tokio_test]
async fn conditional_put_object_store_short_blip()
-> Result<(), Box<dyn std::error::Error>> {
    conditional_put_repro(
        ConditionalPutBackend::ArrowObjectStore,
        "conditional-put-object-store-short-blip",
        9005,
        50,
    )
    .await
}

/// Longer blip: `object_store` gives up retrying before observing a clean
/// 412 (every retry's response is also cut), so the failure lands on the
/// generic-error arm. The toxic clears before the readback's retry budget
/// runs out, so the recovery still works.
#[icechunk_macros::tokio_test]
async fn conditional_put_object_store_long_blip() -> Result<(), Box<dyn std::error::Error>>
{
    conditional_put_repro(
        ConditionalPutBackend::ArrowObjectStore,
        "conditional-put-object-store-long-blip",
        9006,
        250,
    )
    .await
}

/// Regression for the 0-byte readback: `refs::delete_tag` PUTs
/// `Bytes::new()` conditionally, so the readback must not use a 1-byte
/// range (which 416s on empty objects). Exercises `put_object` directly
/// with the same arguments — going through `Repository::delete_tag` adds
/// two parallel GETs in `fetch_tag` and makes the toxic-timing window too
/// narrow to test reliably.
#[icechunk_macros::tokio_test]
async fn zero_byte_conditional_put_lost_response()
-> Result<(), Box<dyn std::error::Error>> {
    // 80 bytes cuts even the empty-PUT response (a 300-byte budget would
    // fit a single tiny response — the other tests need 300 because they
    // accumulate the cost of three PUTs on the same connection).
    with_lost_response_harness(
        ConditionalPutBackend::ArrowObjectStore,
        "zero-byte-conditional-put",
        9007,
        80,
        50,
        |storage, settings, toxic_remover| async move {
            let result = storage
                .put_object(
                    &settings,
                    "zero-byte-test",
                    Bytes::new(),
                    None,
                    Default::default(),
                    Some(&icechunk::storage::VersionInfo::for_creation()),
                )
                .await;
            toxic_remover.await??;

            match result {
                Ok(icechunk::storage::VersionedUpdateResult::Updated { .. }) => Ok(()),
                Ok(icechunk::storage::VersionedUpdateResult::NotOnLatestVersion) => {
                    panic!(
                        "0-byte conditional PUT readback failed: caller reports \
                         NotOnLatestVersion for a write that landed. This is the \
                         0-byte readback bug — readback's ranged GET returns 416 on \
                         an empty object and the helper returns None."
                    )
                }
                Err(err) => panic!(
                    "conditional 0-byte PUT should succeed despite the lost response, got: {err}"
                ),
            }
        },
    )
    .await
}
