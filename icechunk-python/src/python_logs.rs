//! Sends `tracing` events to Python `logging` through one forwarder thread.
//! Rust threads never wait for the GIL. Thus a caller that holds the GIL cannot deadlock.

use std::{
    fmt::{self, Write as _},
    process,
    sync::{
        Arc, Mutex, OnceLock, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use pyo3::{exceptions::PyValueError, prelude::*, types::PyTuple, wrap_pyfunction};
use tracing::{
    Event, Level, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{EnvFilter, Layer, Registry, layer::Context, reload};

/// Python `logging` has no TRACE level. This value is below DEBUG (10).
const PY_TRACE: u8 = 5;

/// The default channel size. The layer drops and counts the events that do not fit.
/// `ICECHUNK_PYTHON_LOG_QUEUE_SIZE` overrides it.
const DEFAULT_QUEUE_SIZE: usize = 10_000;
const QUEUE_SIZE_VAR: &str = "ICECHUNK_PYTHON_LOG_QUEUE_SIZE";
/// The exit flush waits at most this long for a slow or stalled handler.
const EXIT_FLUSH_TIMEOUT_SECS: f64 = 2.0;
const FLUSH_POLL: Duration = Duration::from_millis(10);

static FILTER: OnceLock<reload::Handle<EnvFilter, Registry>> = OnceLock::new();
static FORWARDER: OnceLock<Forwarder> = OnceLock::new();
static FLUSH_AT_EXIT: AtomicBool = AtomicBool::new(false);
/// `true` when logs go to Python, `false` when they go to the console.
static TO_PYTHON: AtomicBool = AtomicBool::new(false);
/// The last filter directive. `None` means `ICECHUNK_LOG`, or `warn`.
static DIRECTIVE: Mutex<Option<String>> = Mutex::new(None);

#[derive(Debug)]
struct Record {
    level: Level,
    target: String,
    message: String,
    file: Option<&'static str>,
    line: Option<u32>,
}

/// Connects process `pid` to its forwarder thread.
#[derive(Debug)]
struct Forwarder {
    /// A forked child gets the channel but not the thread. The child must not use them.
    pid: u32,
    sender: mpsc::SyncSender<Record>,
    state: Arc<ForwarderState>,
}

#[derive(Debug, Default)]
struct ForwarderState {
    thread: OnceLock<thread::ThreadId>,
    sent: AtomicU64,
    handled: AtomicU64,
    dropped: AtomicU64,
    stopped: AtomicBool,
}

#[derive(Debug)]
struct PythonLogs;

impl<S: Subscriber> Layer<S> for PythonLogs {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let record = Record {
            level: *metadata.level(),
            target: metadata.target().to_owned(),
            message: visitor.message + &visitor.fields,
            file: metadata.file(),
            line: metadata.line(),
        };
        let forwarder = FORWARDER.get_or_init(Forwarder::start);
        if forwarder.pid != process::id() {
            // A forked child drops its events.
            return;
        }
        match forwarder.sender.try_send(record) {
            Ok(()) => {
                forwarder.state.sent.fetch_add(1, Ordering::SeqCst);
            }
            Err(mpsc::TrySendError::Full(_)) => {
                forwarder.state.dropped.fetch_add(1, Ordering::SeqCst);
            }
            // The forwarder stopped at interpreter shutdown.
            Err(mpsc::TrySendError::Disconnected(_)) => {}
        }
    }
}

/// Collects the `message` field, and the other fields as ` key=value`.
#[derive(Debug, Default)]
struct MessageVisitor {
    message: String,
    fields: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // Writes to a `String` cannot fail.
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.fields, " {}={value:?}", field.name());
        }
    }
}

impl Forwarder {
    fn start() -> Forwarder {
        let (sender, receiver) = mpsc::sync_channel(queue_size_from_env());
        let state = Arc::new(ForwarderState::default());
        let thread_state = Arc::clone(&state);
        if let Err(err) = thread::Builder::new()
            .name("icechunk-python-logs".to_owned())
            .spawn(move || forward(receiver, &thread_state))
        {
            eprintln!("Error starting the Python logs thread: {err}");
            state.stopped.store(true, Ordering::SeqCst);
        }
        Forwarder { pid: process::id(), sender, state }
    }
}

fn queue_size_from_env() -> usize {
    let Ok(value) = std::env::var(QUEUE_SIZE_VAR) else {
        return DEFAULT_QUEUE_SIZE;
    };
    match value.trim().parse::<usize>() {
        Ok(size) if size > 0 => size,
        _ => {
            eprintln!(
                "WARNING: {QUEUE_SIZE_VAR} must be a positive integer, not {value:?}. \
                 Using {DEFAULT_QUEUE_SIZE}."
            );
            DEFAULT_QUEUE_SIZE
        }
    }
}

fn forward(receiver: mpsc::Receiver<Record>, state: &ForwarderState) {
    let _ = state.thread.set(thread::current().id());
    for record in receiver {
        let attached = Python::try_attach(|py| {
            if let Err(err) = emit(py, &record) {
                err.write_unraisable(py, None);
            }
            let dropped = state.dropped.swap(0, Ordering::SeqCst);
            if dropped > 0
                && let Err(err) = warn_dropped(py, dropped)
            {
                err.write_unraisable(py, None);
            }
        });
        state.handled.fetch_add(1, Ordering::SeqCst);
        // `None` means that the interpreter shuts down.
        if attached.is_none() {
            break;
        }
    }
    state.stopped.store(true, Ordering::SeqCst);
}

fn warn_dropped(py: Python<'_>, dropped: u64) -> PyResult<()> {
    let message = format!(
        "Dropped {dropped} log records because the Python logging handlers are too slow"
    );
    let logger = py.import("logging")?.call_method1("getLogger", ("icechunk",))?;
    logger.call_method1("warning", (message,))?;
    Ok(())
}

fn emit(py: Python<'_>, record: &Record) -> PyResult<()> {
    let name = record.target.replace("::", ".");
    let level = python_level(record.level);
    let logger = py.import("logging")?.call_method1("getLogger", (&name,))?;
    if !logger.call_method1("isEnabledFor", (level,))?.is_truthy()? {
        return Ok(());
    }
    let py_record = logger.call_method1(
        "makeRecord",
        (
            name,
            level,
            record.file.unwrap_or("(unknown file)"),
            record.line.unwrap_or(0),
            &record.message,
            PyTuple::empty(py),
            py.None(),
        ),
    )?;
    logger.call_method1("handle", (py_record,))?;
    Ok(())
}

fn python_level(level: Level) -> u8 {
    match level {
        Level::ERROR => 40,
        Level::WARN => 30,
        Level::INFO => 20,
        Level::DEBUG => 10,
        Level::TRACE => PY_TRACE,
    }
}

/// Calls [`icechunk::initialize_tracing_with_layer`] with the Python layer.
/// The layer is off until the destination is Python.
fn initialize_tracing(log_filter_directive: Option<&str>) {
    let mut layer = None;
    FILTER.get_or_init(|| {
        let (filter, handle) = reload::Layer::new(EnvFilter::new("off"));
        layer = Some(PythonLogs.with_filter(filter).boxed());
        handle
    });
    icechunk::initialize_tracing_with_layer(log_filter_directive, layer);
}

/// Stores the directive, and sets it on the active destination.
pub(crate) fn set_filter(log_filter_directive: Option<String>) {
    let mut directive = DIRECTIVE.lock().unwrap_or_else(PoisonError::into_inner);
    *directive = log_filter_directive;
    reload_filters(directive.as_deref());
}

/// Gives the directive to the active destination. The other destination gets `off`.
fn reload_filters(log_filter_directive: Option<&str>) {
    let to_python = TO_PYTHON.load(Ordering::SeqCst);
    initialize_tracing(if to_python { Some("off") } else { log_filter_directive });
    let python_filter = if to_python {
        icechunk::log_filter(log_filter_directive)
    } else {
        EnvFilter::new("off")
    };
    if let Some(handle) = FILTER.get()
        && let Err(err) = handle.reload(python_filter)
    {
        eprintln!("Error reloading Python log settings: {err}");
    }
}

/// Parses a destination name. `true` means Python.
fn parse_destination(destination: &str) -> Option<bool> {
    match destination {
        "console" => Some(false),
        "python" => Some(true),
        _ => None,
    }
}

fn set_destination(py: Python<'_>, to_python: bool) -> PyResult<()> {
    if to_python {
        py.import("logging")?.call_method1("addLevelName", (PY_TRACE, "TRACE"))?;
        if !FLUSH_AT_EXIT.swap(true, Ordering::SeqCst) {
            let flush = wrap_pyfunction!(flush_python_logs, py)?;
            py.import("atexit")?
                .call_method1("register", (flush, EXIT_FLUSH_TIMEOUT_SECS))?;
        }
    }
    TO_PYTHON.store(to_python, Ordering::SeqCst);
    let directive = DIRECTIVE.lock().unwrap_or_else(PoisonError::into_inner);
    reload_filters(directive.as_deref());
    Ok(())
}

/// Sets up logs at import from `log_filter_directive` and `ICECHUNK_LOG_DESTINATION`.
pub(crate) fn initialize(
    py: Python<'_>,
    log_filter_directive: Option<String>,
    destination: Option<&str>,
) -> PyResult<()> {
    let to_python = destination.is_some_and(|name| {
        parse_destination(name).unwrap_or_else(|| {
            eprintln!(
                "WARNING: ICECHUNK_LOG_DESTINATION must be 'console' or 'python', \
                 not {name:?}. Using 'console'."
            );
            false
        })
    });
    *DIRECTIVE.lock().unwrap_or_else(PoisonError::into_inner) = log_filter_directive;
    set_destination(py, to_python)
}

#[pyfunction]
pub(crate) fn set_logs_destination(py: Python<'_>, destination: &str) -> PyResult<()> {
    let to_python = parse_destination(destination).ok_or_else(|| {
        PyValueError::new_err(format!(
            "destination must be 'console' or 'python', not {destination:?}"
        ))
    })?;
    set_destination(py, to_python)
}

/// Waits until the forwarder handles all earlier records, or `timeout` seconds pass.
/// Ctrl+C stops the wait. On the forwarder thread, it returns at once: no deadlock.
#[pyfunction]
#[pyo3(name = "_flush_python_logs", signature = (timeout = None))]
pub(crate) fn flush_python_logs(py: Python<'_>, timeout: Option<f64>) -> PyResult<()> {
    let deadline =
        timeout.map(|secs| Instant::now() + Duration::from_secs_f64(secs.max(0.0)));
    let Some(forwarder) = FORWARDER.get().filter(|f| f.pid == process::id()) else {
        return Ok(());
    };
    let state = &forwarder.state;
    if state.thread.get() == Some(&thread::current().id()) {
        return Ok(());
    }

    let target = state.sent.load(Ordering::SeqCst);
    loop {
        let handled = state.handled.load(Ordering::SeqCst);
        if handled >= target || state.stopped.load(Ordering::SeqCst) {
            return Ok(());
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            eprintln!(
                "icechunk: {} log records did not reach Python logging in time",
                target - handled
            );
            return Ok(());
        }
        // The forwarder needs the GIL to handle the earlier records.
        py.detach(|| thread::sleep(FLUSH_POLL));
        py.check_signals()?;
    }
}
