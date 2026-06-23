//! A transactional storage engine for Zarr.
//!
//! Icechunk provides version-controlled, transactional access to Zarr data on cloud
//! object storage. It adds a layer of indirection between Zarr keys and on-disk storage,
//! enabling:
//!
//! - **Serializable isolation** - Reads are isolated from concurrent writes and always
//!   use a committed snapshot. Writes are committed atomically and never partially visible.
//! - **Time travel** - Previous snapshots remain accessible after new ones are written.
//! - **Version control** - Repositories support branches (mutable) and tags (immutable)
//!   that reference snapshots.
//!
//! # Core concepts
//!
//! A **repository** contains a Zarr hierarchy (groups and arrays) with full version history.
//! Each update creates a new **snapshot** with a unique ID. **Branches** are mutable
//! references to snapshots (like Git branches), while **tags** are immutable references
//! (like Git tags). The default branch is `main`.
//!
//! # Key types
//!
//! - [`Repository`] - Handle to a versioned data store
//! - [`session::Session`] - Transaction context for reading/writing data
//! - [`Store`] - Zarr-compatible key-value interface backed by a Session
//!
//! # Architecture
//!
//! ```text
//! Repository ─creates─► Session
//!                           ├── ChangeSet (uncommitted modifications)
//!                           └── AssetManager (typed I/O with caching)
//!                                   └── Storage (S3, GCS, Azure, local, etc.)
//! ```
//!

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

pub mod asset_manager;
pub mod change_set;
pub mod cli;
pub mod compat;
pub mod config;
pub mod conflicts;
pub mod diff;
pub mod display;
pub mod error;
pub mod feature_flags;
pub use icechunk_format as format;
pub mod inspect;
pub mod migrations;
pub mod ops;
pub mod refs;
pub mod repository;
pub mod session;
pub mod storage;
pub mod store;
#[cfg(test)]
pub mod strategies;
mod stream_utils;
pub mod virtual_chunks;

pub use config::{ObjectStoreConfig, RepositoryConfig};
pub use repository::Repository;
#[cfg(feature = "object-store-fs")]
pub use storage::new_local_filesystem_storage;
#[cfg(feature = "object-store-s3")]
pub use storage::new_s3_object_store_storage;
#[cfg(feature = "s3")]
pub use storage::new_s3_storage;
pub use storage::{ObjectStorage, Storage, StorageError, new_in_memory_storage};
pub use store::Store;

pub use icechunk_types::user_agent;

#[cfg(test)]
pub(crate) mod test_utils {
    #[expect(unused_imports)]
    use rstest::rstest;
    use rstest_reuse::{self, *};

    #[expect(unused_imports)]
    use crate::format::format_constants::SpecVersionBin;

    #[template]
    #[rstest]
    #[case::v1(SpecVersionBin::V1)]
    #[case::v2(SpecVersionBin::V2)]
    pub fn spec_version_cases(#[case] spec_version: SpecVersionBin) {}
}

mod private {
    /// Used to seal traits we don't want user code to implement, to maintain compatibility.
    /// See <https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed>
    pub trait Sealed {}
}

#[cfg(feature = "logs")]
#[expect(clippy::type_complexity)]
static LOG_FILTER: std::sync::LazyLock<
    std::sync::Mutex<
        Option<
            tracing_subscriber::reload::Handle<
                tracing_subscriber::EnvFilter,
                tracing_subscriber::layer::Layered<
                    tracing_error::ErrorLayer<tracing_subscriber::Registry>,
                    tracing_subscriber::Registry,
                >,
            >,
        >,
    >,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(None));

/// Holds the OTLP tracer provider so spans can be flushed at shutdown.
#[cfg(feature = "otel")]
static TRACER_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

/// Set once `shutdown_telemetry` has run, so repeated calls are a no-op.
#[cfg(feature = "otel")]
static TELEMETRY_SHUTDOWN: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Build the OTLP span exporter and tracer when an endpoint is configured.
///
/// Returns `None` when no endpoint env var is set (export disabled) or when the
/// exporter fails to build. The icechunk-specific `ICECHUNK_OTLP_ENDPOINT` takes
/// precedence over the standard `OTEL_EXPORTER_OTLP_ENDPOINT`, so a global
/// collector can be overridden just for icechunk.
///
/// Must be called from within a Tokio runtime context: the tonic channel spawns
/// a background driver task on the current runtime, which must stay alive for
/// spans to be exported and flushed.
#[cfg(feature = "otel")]
fn build_otel_tracer() -> Option<opentelemetry_sdk::trace::SdkTracer> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::{SpanExporter, WithExportConfig as _};
    use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};

    let endpoint = std::env::var("ICECHUNK_OTLP_ENDPOINT")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .ok()?;

    let exporter =
        match SpanExporter::builder().with_tonic().with_endpoint(endpoint).build() {
            Ok(exporter) => exporter,
            Err(err) => {
                eprintln!("Error building OpenTelemetry exporter: {err}");
                return None;
            }
        };

    let service_name = std::env::var("ICECHUNK_OTEL_SERVICE_NAME")
        .or_else(|_| std::env::var("OTEL_SERVICE_NAME"))
        .unwrap_or_else(|_| "icechunk".to_string());
    let resource = Resource::builder().with_service_name(service_name).build();
    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();

    let tracer = provider.tracer("icechunk");
    // Keep the provider alive so `shutdown_telemetry` can flush it.
    let _ = TRACER_PROVIDER.set(provider);
    Some(tracer)
}

/// The filter controlling which spans are exported over OTLP. Fixed at startup.
#[cfg(feature = "otel")]
fn otel_export_filter() -> tracing_subscriber::EnvFilter {
    // TODO: allow changing this at runtime
    std::env::var("ICECHUNK_OTEL_FILTER")
        .ok()
        .map(tracing_subscriber::EnvFilter::new)
        .unwrap_or_else(|| tracing_subscriber::EnvFilter::new("icechunk=info"))
}

/// Flush and shut down the OpenTelemetry exporter, if one is active.
///
/// Call before process exit so the final batch of spans is exported. Idempotent,
/// and a no-op when the `otel` feature is disabled or no exporter was started.
#[cfg(feature = "otel")]
pub fn shutdown_telemetry() {
    use std::sync::atomic::Ordering;
    if TELEMETRY_SHUTDOWN.swap(true, Ordering::SeqCst) {
        return;
    }
    if let Some(provider) = TRACER_PROVIDER.get()
        && let Err(err) = provider.shutdown()
    {
        eprintln!("Error shutting down telemetry: {err}");
    }
}

/// No-op operation for callers with otel feature disabled
#[cfg(not(feature = "otel"))]
pub fn shutdown_telemetry() {}

#[cfg(feature = "logs")]
pub fn initialize_tracing(log_filter_directive: Option<&str>) {
    use tracing::Level;
    use tracing_error::ErrorLayer;
    use tracing_subscriber::{
        EnvFilter, Layer as _, Registry, layer::SubscriberExt as _, reload,
        util::SubscriberInitExt as _,
    };

    // We have two Layers. One keeps track of the spans to feed the ICError instances.
    // The other is the one spitting logs to stdout. Filtering only applies to the second Layer.

    let filter = log_filter_directive.map(EnvFilter::new).unwrap_or_else(|| {
        EnvFilter::from_env("ICECHUNK_LOG").add_directive(Level::WARN.into())
    });
    match LOG_FILTER.lock() {
        Ok(mut guard) => match guard.as_ref() {
            Some(handle) => {
                if let Err(err) = handle.reload(filter) {
                    eprintln!("Error reloading log settings: {err}");
                }
            }
            None => {
                let (filter, handle) = reload::Layer::new(filter);
                *guard = Some(handle);
                let stdout_layer =
                    tracing_subscriber::fmt::layer().pretty().with_filter(filter);

                let error_span_layer = ErrorLayer::default();

                // Appended last so the console filter's `S` type — and thus the
                // `LOG_FILTER` reload handle stored above — is unchanged. A `None`
                // layer is a no-op, so "OTel disabled" needs no separate path.
                #[cfg(feature = "otel")]
                let otel_layer = build_otel_tracer().map(|tracer| {
                    tracing_opentelemetry::OpenTelemetryLayer::new(tracer)
                        .with_filter(otel_export_filter())
                });
                #[cfg(not(feature = "otel"))]
                let otel_layer: Option<
                    tracing_subscriber::layer::Identity,
                > = None;

                if let Err(err) = Registry::default()
                    .with(error_span_layer)
                    .with(stdout_layer)
                    .with(otel_layer)
                    .try_init()
                {
                    eprintln!("Error initializing logs: {err}");
                }
            }
        },
        Err(err) => {
            eprintln!("Error setting up logs: {err}");
        }
    }
}
