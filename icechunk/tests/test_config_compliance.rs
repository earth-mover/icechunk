//! Spec 2.2 configuration rules: reading never fails on configuration this
//! library can't decode, and saving the configuration keeps it.

use std::{error::Error, sync::Arc};

use bytes::Bytes;
use icechunk::{
    ObjectStoreConfig, Repository, RepositoryConfig, Storage,
    config::{CompressionConfig, S3Options},
    format::{
        ByteRange, ChunkIndices, Path,
        format_constants::SpecVersionBin,
        manifest::{ChunkPayload, VirtualChunkLocation, VirtualChunkRef},
        snapshot::ArrayShape,
    },
    new_in_memory_storage,
    repository::VersionInfo,
    session::get_chunk,
    virtual_chunks::VirtualChunkContainer,
};
use icechunk_macros::tokio_test;
use icechunk_types::error::ICResultCtxExt as _;
use pretty_assertions::assert_eq;
use serde_json::{Value, json};

type TestResult<A = ()> = Result<A, Box<dyn Error>>;

const KNOWN_PREFIX: &str = "s3://bucket/";
const FUTURE_PREFIX: &str = "s3://bucket/new/";
const FUTURE_STORE: &str = "storage_from_the_future";

fn s3_container(name: &str, url_prefix: &str) -> VirtualChunkContainer {
    let store = ObjectStoreConfig::S3(S3Options::default().with_region("us-east-1"));
    VirtualChunkContainer::new_named(name.to_string(), url_prefix.to_string(), store)
        .unwrap()
}

async fn stored_config(storage: &Arc<dyn Storage + Send + Sync>) -> TestResult<Value> {
    let repo = Repository::open(Arc::clone(storage)).execute().await?;
    let (repo_info, _) = repo.asset_manager().fetch_repo_info().await?;
    let bytes = repo_info.config_bytes_raw()?.ok_or("no stored configuration")?;
    Ok(flexbuffers::from_slice(&bytes)?)
}

/// Commits virtual refs to both containers while they still decode, then
/// rewrites the stored configuration as a later library would: a key this
/// library doesn't know, a known key with a value it can't decode and a
/// container store of a kind it doesn't know. Returns that configuration.
async fn repo_with_later_configuration(
    storage: &Arc<dyn Storage + Send + Sync>,
) -> TestResult<Value> {
    let mut config =
        RepositoryConfig { inline_chunk_threshold_bytes: Some(1), ..Default::default() };
    config.set_virtual_chunk_container(s3_container("known", KNOWN_PREFIX))?;
    config.set_virtual_chunk_container(s3_container("future", FUTURE_PREFIX))?;
    let repo = Repository::create(Arc::clone(storage))
        .config(config)
        .spec_version(SpecVersionBin::current())
        .execute()
        .await?;

    let mut session = repo.writable_session("main").await?;
    let path: Path = "/array".try_into()?;
    session
        .add_array(
            path.clone(),
            ArrayShape::new(vec![(2, 2)]).ok_or("bad shape")?,
            None,
            Bytes::new(),
        )
        .await?;
    for (index, location) in
        ["vcc://future/a.nc", "s3://bucket/new/b.nc"].iter().enumerate()
    {
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![index as u32]),
                Some(ChunkPayload::Virtual(VirtualChunkRef {
                    location: VirtualChunkLocation::from_url(location)?,
                    offset: 0,
                    length: 4,
                    checksum: None,
                })),
            )
            .await?;
    }
    session.commit("virtual refs").execute().await?;

    let mut later = stored_config(storage).await?;
    later["key_from_the_future"] = json!({"nested": [1, "two", {"three": 3.5}]});
    later["compression"] = json!({"algorithm": "algorithm_from_the_future", "level": 3});
    later["storage"] = json!({"retries": "retries_from_the_future"});
    later["virtual_chunk_containers"][FUTURE_PREFIX]["store"] =
        json!({FUTURE_STORE: {"endpoint": "https://example.com"}});
    write_stored_config(&repo, &later).await?;
    Ok(later)
}

/// Writes `config` as the stored configuration, as another library would.
async fn write_stored_config(repo: &Repository, config: &Value) -> TestResult {
    let spec_version = repo.spec_version();
    repo.asset_manager()
        .update_repo_info(
            repo.config().repo_update_retries().retries(),
            |info, backup, _| {
                Ok(Arc::new(info.set_config(spec_version, config, backup, 100).inject()?))
            },
        )
        .await?;
    Ok(())
}

async fn read_chunk(repo: &Repository, index: u32) -> TestResult<Option<Bytes>> {
    let session =
        repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;
    let path: Path = "/array".try_into()?;
    let reader = session
        .get_chunk_reader(&path, &ChunkIndices(vec![index]), &ByteRange::ALL)
        .await?;
    Ok(get_chunk(reader).await?)
}

#[tokio_test]
async fn opens_a_configuration_it_cannot_fully_decode() -> TestResult {
    let storage = new_in_memory_storage().await?;
    repo_with_later_configuration(&storage).await?;

    let repo = Repository::open(Arc::clone(&storage)).execute().await?;
    assert_eq!(repo.config().inline_chunk_threshold_bytes(), 1);
    assert_eq!(repo.config().compression(), &Default::default());
    let decoded: Vec<_> = repo.config().virtual_chunk_containers().cloned().collect();
    assert_eq!(decoded, vec![s3_container("known", KNOWN_PREFIX)]);

    let session =
        repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;
    assert_eq!(
        session.resolve_virtual_location(&VirtualChunkLocation::from_url(
            "vcc://future/a.nc"
        )?)?,
        "s3://bucket/new/a.nc"
    );

    // Both refs resolve to the container whose store doesn't decode, the
    // absolute one by the longest matching prefix.
    for index in [0, 1] {
        let err = read_chunk(&repo, index).await.err().ok_or("read must fail")?;
        let message = err.to_string();
        assert!(
            message.contains("can't decode") && message.contains(FUTURE_PREFIX),
            "unexpected error: {message}"
        );
    }
    Ok(())
}

#[tokio_test]
async fn save_config_keeps_what_it_cannot_decode() -> TestResult {
    let storage = new_in_memory_storage().await?;
    let later = repo_with_later_configuration(&storage).await?;

    let repo = Repository::open(Arc::clone(&storage))
        .config(RepositoryConfig {
            inline_chunk_threshold_bytes: Some(2),
            ..Default::default()
        })
        .execute()
        .await?;
    repo.save_config().await?;

    let saved = stored_config(&storage).await?;
    assert_eq!(saved["inline_chunk_threshold_bytes"], json!(2));
    // `storage` too, although opening injects the storage defaults
    for key in
        ["key_from_the_future", "compression", "storage", "virtual_chunk_containers"]
    {
        assert_eq!(saved[key], later[key], "{key}");
    }

    // and the saved configuration still opens and resolves the container
    let repo = Repository::open(Arc::clone(&storage)).execute().await?;
    let err = read_chunk(&repo, 0).await.err().ok_or("read must fail")?;
    assert!(err.to_string().contains("can't decode"), "unexpected error: {err}");
    Ok(())
}

#[tokio_test]
async fn save_config_replaces_a_container_it_cannot_decode_only_by_name() -> TestResult {
    let storage = new_in_memory_storage().await?;
    let later = repo_with_later_configuration(&storage).await?;
    let containers = |saved: &Value| -> TestResult<Vec<String>> {
        let containers = saved["virtual_chunk_containers"]
            .as_object()
            .ok_or("containers must be a map")?;
        Ok(containers.keys().cloned().collect())
    };

    // clearing affects only the containers this library can decode
    let repo = Repository::open(Arc::clone(&storage)).execute().await?;
    let mut config = repo.config().clone();
    config.clear_virtual_chunk_containers();
    let repo = repo.reopen(Some(config), None).await?;
    repo.save_config().await?;
    let saved = stored_config(&storage).await?;
    assert_eq!(containers(&saved)?, vec![FUTURE_PREFIX]);
    assert_eq!(
        saved["virtual_chunk_containers"][FUTURE_PREFIX],
        later["virtual_chunk_containers"][FUTURE_PREFIX]
    );
    assert_eq!(saved["key_from_the_future"], later["key_from_the_future"]);

    // a container with the same name replaces it
    let mut config = repo.config().clone();
    config.set_virtual_chunk_container(s3_container("future", "s3://bucket/newer/"))?;
    let repo = repo.reopen(Some(config), None).await?;
    repo.save_config().await?;
    let saved = stored_config(&storage).await?;
    assert_eq!(containers(&saved)?, vec!["s3://bucket/newer/"]);
    let repo = Repository::open(Arc::clone(&storage)).execute().await?;
    assert_eq!(repo.config().undecodable_virtual_chunk_containers().count(), 0);
    Ok(())
}

#[tokio_test]
async fn save_config_keeps_a_container_a_concurrent_writer_added() -> TestResult {
    let storage = new_in_memory_storage().await?;
    let mut config = RepositoryConfig::default();
    config.set_virtual_chunk_container(s3_container("known", KNOWN_PREFIX))?;
    let repo = Repository::create(Arc::clone(&storage)).config(config).execute().await?;

    // a newer library adds a container this library can't decode
    let other = Repository::open(Arc::clone(&storage)).execute().await?;
    let mut later = stored_config(&storage).await?;
    later["virtual_chunk_containers"][FUTURE_PREFIX] = json!({
        "name": "future",
        "url_prefix": FUTURE_PREFIX,
        "store": {FUTURE_STORE: {}},
    });
    write_stored_config(&other, &later).await?;

    let repo = repo
        .reopen(
            Some(RepositoryConfig {
                inline_chunk_threshold_bytes: Some(3),
                ..Default::default()
            }),
            None,
        )
        .await?;
    repo.save_config().await?;
    let saved = stored_config(&storage).await?;
    assert_eq!(saved["inline_chunk_threshold_bytes"], json!(3));
    assert_eq!(saved["virtual_chunk_containers"], later["virtual_chunk_containers"]);
    Ok(())
}

/// Bindings convert the configuration to their own types and back, losing
/// what this library couldn't decode; saving must keep it anyway.
#[tokio_test]
async fn save_config_after_a_binding_round_trip_keeps_what_it_cannot_decode() -> TestResult
{
    let storage = new_in_memory_storage().await?;
    let later = repo_with_later_configuration(&storage).await?;

    let repo = Repository::open(Arc::clone(&storage)).execute().await?;
    let round_tripped =
        RepositoryConfig { undecodable: Default::default(), ..repo.config().clone() };
    let repo = repo.reopen(Some(round_tripped), None).await?;
    repo.save_config().await?;
    let saved = stored_config(&storage).await?;
    for key in
        ["key_from_the_future", "compression", "storage", "virtual_chunk_containers"]
    {
        assert_eq!(saved[key], later[key], "{key}");
    }

    // setting a value by intent replaces what didn't decode
    let mut config = repo.config().clone();
    config.compression = Some(CompressionConfig { algorithm: None, level: Some(9) });
    let mut settings = config.storage.clone().unwrap_or_default();
    settings.unsafe_use_metadata = Some(false);
    config.storage = Some(settings);
    let repo = repo.reopen(Some(config), None).await?;
    repo.save_config().await?;
    let saved = stored_config(&storage).await?;
    assert_eq!(saved["compression"]["level"], json!(9));
    assert_eq!(saved["storage"]["unsafe_use_metadata"], json!(false));
    Ok(())
}

/// Writes the repository `icechunk-js/__test__/data/gcs-vcc-repo`, read by
/// the JS tests: its configuration has a GCS virtual chunk container, which
/// the WASM build doesn't include. Refuses to overwrite.
///
/// Does nothing unless `ICECHUNK_GENERATE_JS_FIXTURE` is set, so that
/// `just test-integration` (which runs every ignored test) leaves the fixture
/// alone. To regenerate it, delete the directory, then run
/// `ICECHUNK_GENERATE_JS_FIXTURE=1 cargo test -p icechunk --test main generate_js_gcs_vcc_fixture -- --ignored`.
#[cfg(all(feature = "object-store-gcs", feature = "object-store-fs"))]
#[tokio_test]
#[ignore = "generates a checked-in fixture"]
async fn generate_js_gcs_vcc_fixture() -> TestResult {
    use std::collections::HashMap;

    use icechunk::{Store, new_local_filesystem_storage};
    use tokio::sync::RwLock;

    if std::env::var_os("ICECHUNK_GENERATE_JS_FIXTURE").is_none() {
        println!("skipped: set ICECHUNK_GENERATE_JS_FIXTURE to generate the fixture");
        return Ok(());
    }
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../icechunk-js/__test__/data/gcs-vcc-repo");
    if dir.exists() {
        return Err(format!("{} exists, delete it first", dir.display()).into());
    }
    let storage = new_local_filesystem_storage(&dir).await?;
    let mut config =
        RepositoryConfig { inline_chunk_threshold_bytes: Some(0), ..Default::default() };
    config.set_virtual_chunk_container(VirtualChunkContainer::new_named(
        "gcs-data".to_string(),
        "gs://some-bucket/data/".to_string(),
        ObjectStoreConfig::Gcs(HashMap::new()),
    )?)?;
    let repo = Repository::create(storage).config(config).execute().await?;

    let session = Arc::new(RwLock::new(repo.writable_session("main").await?));
    let store = Store::from_session(Arc::clone(&session)).await;
    store
        .set("zarr.json", Bytes::from_static(br#"{"zarr_format":3,"node_type":"group"}"#))
        .await?;
    let array_meta = |name: &str| {
        Bytes::from(format!(
            r#"{{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"float32","chunk_grid":{{"name":"regular","configuration":{{"chunk_shape":[4]}}}},"chunk_key_encoding":{{"name":"default","configuration":{{"separator":"/"}}}},"fill_value":0,"codecs":[{{"name":"bytes","configuration":{{"endian":"little"}}}}],"attributes":{{"name":"{name}"}}}}"#
        ))
    };
    store.set("temperature/zarr.json", array_meta("temperature")).await?;
    let values: Vec<u8> =
        [1.5f32, 2.5, 3.5, 4.5].iter().flat_map(|v| v.to_le_bytes()).collect();
    store.set("temperature/c/0", Bytes::from(values)).await?;
    store.set("virtual/zarr.json", array_meta("virtual")).await?;
    store
        .set_virtual_ref(
            "virtual/c/0",
            VirtualChunkRef {
                location: VirtualChunkLocation::from_url("vcc://gcs-data/chunk.bin")?,
                offset: 0,
                length: 16,
                checksum: None,
            },
            true,
        )
        .await?;
    session.write().await.commit("temperature and a virtual array").execute().await?;
    Ok(())
}
