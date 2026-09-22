//! Span timing layer, peak memory, and report printing.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use icechunk::storage::metering::MeteringReport;
use tracing::{Subscriber, span};
use tracing_subscriber::{
    EnvFilter, Layer,
    layer::{Context, SubscriberExt as _},
    registry::LookupSpan,
    util::SubscriberInitExt as _,
};

/// Spans reported in the phase table. Every name must match a `#[instrument]`ed
/// function in `icechunk::ops`.
const TRACKED: &[&str] = &[
    "garbage_collect_one_attempt",
    "find_retained",
    "gc_snapshots",
    "gc_transaction_logs",
    "gc_manifests",
    "gc_chunks",
    "pointed_snapshots_v2",
    "repo_chunks_storage",
    "walk_manifests",
];

/// The walker's own completion event, whose fields the report prints.
const WALK_DONE: &str = "manifest walk done";

/// span name -> (times closed, total wall time)
#[derive(Debug, Default)]
pub(crate) struct PhaseTimes {
    spans: Mutex<BTreeMap<&'static str, (u64, Duration)>>,
    /// Fields of the last [`WALK_DONE`] event, as rendered strings. Taken from
    /// the event rather than the walk's result types, which stay free of it.
    walk: Mutex<BTreeMap<&'static str, String>>,
}

struct Created(Instant);

struct PhaseLayer {
    times: Arc<PhaseTimes>,
}

/// Renders every field of an event into a map, whatever its type.
#[derive(Default)]
struct FieldGrab(BTreeMap<&'static str, String>);

impl tracing::field::Visit for FieldGrab {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name(), value.to_string());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name(), value.to_string());
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0.insert(field.name(), format!("{value:.2}"));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name(), value.to_string());
    }

    fn record_debug(
        &mut self,
        field: &tracing::field::Field,
        value: &dyn std::fmt::Debug,
    ) {
        self.0.insert(field.name(), format!("{value:?}"));
    }
}

/// Wall time from span creation to close. `on_enter`/`on_exit` would measure
/// only the polled (busy) time of an async span, which is not what we want.
impl<S> Layer<S> for PhaseLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        _attrs: &span::Attributes<'_>,
        id: &span::Id,
        ctx: Context<'_, S>,
    ) {
        if let Some(span) = ctx.span(id)
            && TRACKED.contains(&span.name())
        {
            span.extensions_mut().insert(Created(Instant::now()));
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(&id)
            && let Some(Created(start)) = span.extensions_mut().remove::<Created>()
        {
            let mut times = self.times.spans.lock().unwrap_or_else(|p| p.into_inner());
            let entry = times.entry(span.name()).or_default();
            entry.0 += 1;
            entry.1 += start.elapsed();
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut grab = FieldGrab::default();
        event.record(&mut grab);
        if grab.0.get("message").map(String::as_str) != Some(WALK_DONE) {
            return;
        }
        grab.0.remove("message");
        *self.times.walk.lock().unwrap_or_else(|p| p.into_inner()) = grab.0;
    }
}

/// Install the global subscriber: the phase layer plus stderr logs filtered by
/// `ICECHUNK_LOG` (default `warn`). Call once, before opening the repository.
/// Do not also call `icechunk::initialize_tracing`; only one global subscriber can exist.
pub(crate) fn install_tracing() -> Arc<PhaseTimes> {
    let times = Arc::new(PhaseTimes::default());
    let filter = EnvFilter::try_from_env("ICECHUNK_LOG")
        .unwrap_or_else(|_| EnvFilter::new("warn"));
    let console =
        tracing_subscriber::fmt::layer().with_writer(std::io::stderr).with_filter(filter);
    tracing_subscriber::registry()
        .with(PhaseLayer { times: Arc::clone(&times) })
        .with(console)
        .init();
    times
}

/// Peak resident set size in MiB from `/proc/self/status`; `None` off Linux.
fn peak_rss_mib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib / 1024)
}

/// (user, system) CPU seconds from `/proc/self/stat`; `None` off Linux.
pub(crate) fn cpu_seconds() -> Option<(f64, f64)> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // comm may contain spaces; fields start after the closing paren
    let rest = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // fields[0] is state (field 3 in proc(5)); utime is field 14, stime field 15
    let utime: f64 = fields.get(11)?.parse().ok()?;
    let stime: f64 = fields.get(12)?.parse().ok()?;
    const TICKS_PER_SEC: f64 = 100.0;
    Some((utime / TICKS_PER_SEC, stime / TICKS_PER_SEC))
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

pub(crate) fn print_report(
    total: Duration,
    phases: &PhaseTimes,
    metering: &MeteringReport,
) {
    println!();
    println!("total wall time: {total:.2?}");

    println!();
    println!("{:<32} {:>8} {:>14}", "phase", "count", "wall time");
    let times = phases.spans.lock().unwrap_or_else(|p| p.into_inner());
    for name in TRACKED {
        if let Some((count, duration)) = times.get(name) {
            println!("{name:<32} {count:>8} {duration:>14.2?}");
        }
    }

    let walk = phases.walk.lock().unwrap_or_else(|p| p.into_inner());
    if !walk.is_empty() {
        println!();
        println!("{:<32} {:>14}", "manifest walk", "value");
        for (field, value) in walk.iter() {
            println!("{field:<32} {value:>14}");
        }
    }

    println!();
    println!(
        "{:<36} {:>10} {:>12} {:>12} {:>10} {:>12}",
        "operation", "requests", "MiB read", "MiB written", "deleted", "keys listed"
    );
    for (op, stats) in &metering.per_op {
        println!(
            "{op:<36} {:>10} {:>12.2} {:>12.2} {:>10} {:>12}",
            stats.requests,
            mib(stats.bytes_read),
            mib(stats.bytes_written),
            stats.objects_deleted,
            stats.keys_listed
        );
    }

    println!();
    println!(
        "{:>6} {:>12} {:>10} {:>12}",
        "second", "MiB read", "requests", "keys listed"
    );
    let mut idle = 0usize;
    for (second, stats) in metering.read_timeline.iter().enumerate() {
        if stats.bytes_read == 0 && stats.keys_listed == 0 {
            idle += 1;
        }
        println!(
            "{second:>6} {:>12.2} {:>10} {:>12}",
            mib(stats.bytes_read),
            stats.requests_started,
            stats.keys_listed
        );
    }
    println!(
        "idle seconds (no bytes read, no keys listed): {idle} of {}",
        metering.read_timeline.len()
    );

    println!();
    match peak_rss_mib() {
        Some(rss) => println!("peak RSS: {rss} MiB"),
        None => println!("peak RSS: n/a"),
    }
    match cpu_seconds() {
        Some((user, sys)) => {
            let cores =
                std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0);
            println!("cpu time: {user:.1}s user, {sys:.1}s sys, {cores} cores available");
        }
        None => println!("cpu time: n/a"),
    }
}
