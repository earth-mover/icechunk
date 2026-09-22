//! Command line definitions and presets.

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(name = "gc_bench", about = "GC and stats benchmark on a synthetic repo")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    /// Build a synthetic repository on `RustFS`
    Build(BuildArgs),
    /// Run garbage collection against a built repository (dry run unless --delete)
    Gc(GcArgs),
    /// Run chunk storage stats against a built repository
    Stats(StatsArgs),
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub(crate) enum Preset {
    Small,
    Medium,
    Large,
    /// Few, very large manifests (~50 MB): reproduces the production stall pattern
    Huge,
}

#[derive(Args, Debug)]
pub(crate) struct BuildArgs {
    /// Dataset name; objects live under `s3://testbucket/gc-bench/<name>/`
    #[arg(long)]
    pub(crate) name: String,
    #[arg(long, value_enum, default_value_t = Preset::Small)]
    pub(crate) preset: Preset,
    /// Delete an existing dataset with this name first
    #[arg(long)]
    pub(crate) force: bool,

    /// Number of independent manifest lineages
    #[arg(long)]
    pub(crate) splits: Option<usize>,
    /// Chunk refs in every manifest
    #[arg(long)]
    pub(crate) refs_per_manifest: Option<usize>,
    /// Fraction of a split's chunk ids replaced by fresh ids in each new manifest version
    #[arg(long)]
    pub(crate) churn: Option<f64>,
    /// Length of the snapshot chain that main points to
    #[arg(long)]
    pub(crate) reachable_snapshots: Option<usize>,
    /// Splits that get a new manifest version in each reachable snapshot
    #[arg(long)]
    pub(crate) rewrites_per_snapshot: Option<usize>,
    /// Snapshot objects written to storage but absent from repo info
    #[arg(long)]
    pub(crate) garbage_snapshots: Option<usize>,
    /// Manifests referenced only by one garbage snapshot
    #[arg(long)]
    pub(crate) garbage_manifests_per_snapshot: Option<usize>,
    /// Zero-byte chunk objects written
    #[arg(long)]
    pub(crate) chunk_objects: Option<usize>,
    /// Fraction of chunk objects whose ids no manifest references
    #[arg(long)]
    pub(crate) garbage_chunk_fraction: Option<f64>,
    /// Maximum in-flight storage writes
    #[arg(long)]
    pub(crate) write_concurrency: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct DatasetParams {
    pub(crate) splits: usize,
    pub(crate) refs_per_manifest: usize,
    pub(crate) churn: f64,
    pub(crate) reachable_snapshots: usize,
    pub(crate) rewrites_per_snapshot: usize,
    pub(crate) garbage_snapshots: usize,
    pub(crate) garbage_manifests_per_snapshot: usize,
    pub(crate) chunk_objects: usize,
    pub(crate) garbage_chunk_fraction: f64,
    pub(crate) write_concurrency: usize,
}

impl DatasetParams {
    fn preset(preset: Preset) -> Self {
        match preset {
            Preset::Small => Self {
                splits: 4,
                refs_per_manifest: 1_000,
                churn: 0.05,
                reachable_snapshots: 20,
                rewrites_per_snapshot: 1,
                garbage_snapshots: 20,
                garbage_manifests_per_snapshot: 1,
                chunk_objects: 2_000,
                garbage_chunk_fraction: 0.5,
                write_concurrency: 64,
            },
            Preset::Medium => Self {
                splits: 20,
                refs_per_manifest: 10_000,
                churn: 0.05,
                reachable_snapshots: 500,
                rewrites_per_snapshot: 2,
                garbage_snapshots: 1_000,
                garbage_manifests_per_snapshot: 1,
                chunk_objects: 50_000,
                garbage_chunk_fraction: 0.5,
                write_concurrency: 128,
            },
            Preset::Large => Self {
                splits: 50,
                refs_per_manifest: 100_000,
                churn: 0.05,
                reachable_snapshots: 5_000,
                rewrites_per_snapshot: 1,
                garbage_snapshots: 2_000,
                garbage_manifests_per_snapshot: 1,
                chunk_objects: 500_000,
                garbage_chunk_fraction: 0.5,
                write_concurrency: 256,
            },
            // A 2.5M-ref manifest costs a few hundred MB while it is being built,
            // so concurrency stays low to keep the build's memory in check.
            Preset::Huge => Self {
                splits: 5,
                refs_per_manifest: 2_500_000,
                churn: 0.05,
                reachable_snapshots: 200,
                rewrites_per_snapshot: 1,
                garbage_snapshots: 100,
                garbage_manifests_per_snapshot: 1,
                chunk_objects: 50_000,
                garbage_chunk_fraction: 0.5,
                write_concurrency: 8,
            },
        }
    }

    pub(crate) fn resolve(args: &BuildArgs) -> Self {
        let p = Self::preset(args.preset);
        Self {
            splits: args.splits.unwrap_or(p.splits),
            refs_per_manifest: args.refs_per_manifest.unwrap_or(p.refs_per_manifest),
            churn: args.churn.unwrap_or(p.churn),
            reachable_snapshots: args
                .reachable_snapshots
                .unwrap_or(p.reachable_snapshots),
            rewrites_per_snapshot: args
                .rewrites_per_snapshot
                .unwrap_or(p.rewrites_per_snapshot),
            garbage_snapshots: args.garbage_snapshots.unwrap_or(p.garbage_snapshots),
            garbage_manifests_per_snapshot: args
                .garbage_manifests_per_snapshot
                .unwrap_or(p.garbage_manifests_per_snapshot),
            chunk_objects: args.chunk_objects.unwrap_or(p.chunk_objects),
            garbage_chunk_fraction: args
                .garbage_chunk_fraction
                .unwrap_or(p.garbage_chunk_fraction),
            write_concurrency: args.write_concurrency.unwrap_or(p.write_concurrency),
        }
    }
}

#[derive(Args, Debug, Clone)]
pub(crate) struct NetArgs {
    /// Toxiproxy latency toxic in milliseconds, both directions
    #[arg(long)]
    pub(crate) latency_ms: Option<u64>,
    /// Toxiproxy bandwidth toxic in KiB/s per connection, both directions
    #[arg(long)]
    pub(crate) bandwidth_kbps: Option<u64>,
}

/// Maps one to one onto the parameters of `garbage_collect` and `repo_chunks_storage`.
#[derive(Args, Debug, Clone)]
pub(crate) struct WalkArgs {
    #[arg(long, default_value_t = 50)]
    pub(crate) max_snapshots_in_memory: u16,
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    pub(crate) max_manifest_mem_bytes: usize,
    #[arg(long, default_value_t = 50)]
    pub(crate) max_concurrent_manifest_fetches: u16,
}

#[derive(Args, Debug)]
pub(crate) struct GcArgs {
    #[arg(long)]
    pub(crate) name: String,
    #[command(flatten)]
    pub(crate) net: NetArgs,
    #[command(flatten)]
    pub(crate) walk: WalkArgs,
    /// Actually delete objects (default is a dry run)
    #[arg(long)]
    pub(crate) delete: bool,
    /// GC cutoff is now minus this many seconds
    #[arg(long, default_value_t = 0)]
    pub(crate) cutoff_age_secs: u64,
    #[arg(long, default_value_t = 10)]
    pub(crate) max_concurrent_deletes: u16,
    #[arg(long, default_value_t = 50)]
    pub(crate) max_consecutive_delete_failures: u16,
}

#[derive(Args, Debug)]
pub(crate) struct StatsArgs {
    #[arg(long)]
    pub(crate) name: String,
    #[command(flatten)]
    pub(crate) net: NetArgs,
    #[command(flatten)]
    pub(crate) walk: WalkArgs,
}
