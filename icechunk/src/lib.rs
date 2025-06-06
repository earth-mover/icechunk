//! General design:
//! - Most things are async even if they don't need to be. Async propagates unfortunately. If
//!   something can be async sometimes it needs to be async always. In our example: fetching from
//!   storage.
//! - There is a high level interface that knows about arrays, groups, user attributes, etc. This
//!   is the [`repository::Repository`] type.
//! - There is a low level interface that speaks zarr keys and values, and is used to provide the
//!   zarr store that will be used from python. This is the [`zarr::Store`] type.
//! - There is a translation language between low and high levels. When user writes to a zarr key,
//!   we need to convert that key to the language of arrays and groups. This is implemented it the
//!   [`zarr`] module
//! - There is an abstract type for loading and saving of the Arrow datastructures.
//!   This is the [`Storage`] trait. It knows how to fetch and write arrow.
//!   We have:
//!     - an in memory implementation
//!     - an s3 implementation that writes to parquet
//!     - a caching wrapper implementation
//! - The datastructures are represented by concrete types in the [`mod@format`] modules.
//!   These datastructures use Arrow RecordBatches for representation.
pub mod asset_manager;
pub mod change_set;
pub mod cli;
pub mod config;
pub mod conflicts;
pub mod error;
pub mod format;
pub mod ops;
pub mod refs;
pub mod repository;
pub mod session;
pub mod storage;
pub mod store;
#[cfg(test)]
pub mod strategies;
pub mod virtual_chunks;

pub use config::{LogsConfig, ObjectStoreConfig, RepositoryConfig, TelemetryConfig};
pub use repository::Repository;
pub use storage::{
    ObjectStorage, Storage, StorageError, new_in_memory_storage,
    new_local_filesystem_storage, new_s3_storage,
};
pub use store::Store;

mod private {
    /// Used to seal traits we don't want user code to implement, to maintain compatibility.
    /// See https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed
    pub trait Sealed {}
}

#[cfg(feature = "logs")]
pub async fn initialize_tracing(
    logs_config: Option<&LogsConfig>,
    telemetry_config: Option<&TelemetryConfig>,
) {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler};
    use tracing_error::ErrorLayer;
    use tracing_opentelemetry::OpenTelemetryLayer;
    use tracing_subscriber::{
        EnvFilter, Layer, Registry, layer::SubscriberExt, util::SubscriberInitExt,
    };

    // We have two Layers. One keeps track of the spans to feed the ICError instances.
    // The other is the one spitting logs to stdout. Filtering only applies to the second Layer.

    let stdout_layer = logs_config.map(|config| {
        tracing_subscriber::fmt::layer()
            .pretty()
            .with_filter(EnvFilter::from_env(config.env_var_name.as_str()))
    });

    let error_span_layer = ErrorLayer::default();

    let telemetry_layer = telemetry_config.and_then(|config| {
        let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(config.endpoint.as_str())
            .build()
            .map_err(|e| {
                println!(
                    "Warning, failed initializing telemetry, continuing without it: {e}",
                );
                e
            })
            .ok()?;

        let tracer = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(1.0))))
            // If export trace to AWS X-Ray, you can use XrayIdGenerator
            .with_id_generator(RandomIdGenerator::default())
            .with_batch_exporter(otlp_exporter)
            .build()
            .tracer(config.tracer_name.clone());

        Some(OpenTelemetryLayer::new(tracer))
    });

    let res = Registry::default()
        .with(error_span_layer)
        .with(stdout_layer)
        .with(telemetry_layer)
        .try_init();

    if let Err(err) = res {
        println!(
            "Warning, failed initializing logging or telemetry, continuing without: {}",
            err
        );
    }
}
