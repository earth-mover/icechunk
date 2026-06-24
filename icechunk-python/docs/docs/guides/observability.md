# Observability

Icechunk is instrumented with the Rust [`tracing`](https://docs.rs/tracing) framework. The same instrumentation drives two independent outputs:

- **Console logs** — human-readable logs printed to stderr, controlled by `ICECHUNK_LOG`. Enabled by default at the `warn` level.
- **OpenTelemetry trace export** *(experimental)* — structured spans and traces are exported over OTLP/gRPC to a collector or service. Off by default, opt-in.
  Telemetry can be configured with an endpoint to use any OpenTelemetry compatible collector or service. If no endpoint is set
  by the user, there is zero exporter overhead and no spans are exported.

The two are configured separately and don't affect each other.

## Logging

Icechunk prints logs to stderr, so they don't interfere with your program's own output on stdout. Set the verbosity with the `ICECHUNK_LOG` environment variable before importing `icechunk`:

```bash
export ICECHUNK_LOG=icechunk=info
```

Levels, from least to most verbose, are `error`, `warn`, `info`, `debug`, and `trace`. The default is `warn`.

`ICECHUNK_LOG` uses [`tracing-subscriber`'s `EnvFilter` syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives), so you can target specific modules:

- `ICECHUNK_LOG=trace` — `trace` level for icechunk **and** all of its dependencies
- `ICECHUNK_LOG=icechunk=trace` — `trace` level for icechunk only
- `ICECHUNK_LOG=debug,icechunk=trace,rustls=info,h2=info,hyper=info` — `trace` for icechunk, `info` for `rustls`/`h2`/`hyper`, and `debug` for everything else

### Changing the level at runtime

`ICECHUNK_LOG` is read once, when `icechunk` is imported. To change the filter afterwards, call `set_logs_filter`, which takes the same directive syntax (or `None` to re-read `ICECHUNK_LOG` from the environment):

```python
import icechunk

icechunk.set_logs_filter("debug,icechunk=trace")
```

To skip logging setup entirely, set `ICECHUNK_NO_LOGS` (to any value) before importing `icechunk`.

To send logs to stdout instead of stderr, set `ICECHUNK_LOG_TO_STDOUT` (to any value) before importing `icechunk`. The destination is fixed when logging is first initialized at import, so this variable has no effect if set afterwards; `set_logs_filter` only changes the verbosity, not the destination.

## OpenTelemetry tracing

!!! warning "Experimental"

    OpenTelemetry export is experimental and its configuration may change in future releases.

Icechunk's internal `tracing` spans offer a lot of information into the inner work of Icechunk. They can be exported using the vendor-neutral [OpenTelemetry](https://opentelemetry.io/) protocol (OTLP/gRPC). Because it speaks standard OTLP, traces can be sent to **any** OpenTelemetry-compatible backend, either directly or through the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/). Examples are: [Jaeger](https://www.jaegertracing.io/), [Grafana Tempo](https://grafana.com/oss/tempo/), [Datadog](https://www.datadoghq.com/), [Honeycomb](https://www.honeycomb.io/), or [New Relic](https://newrelic.com/). Icechunk is not tied to any particular vendor.

**Export is opt-in and off by default** — nothing is collected or transmitted unless you turn it on:

- In the **Python wheel**, the exporter is compiled in but stays inactive until you configure an endpoint.
- In the **Rust crate**, you must additionally enable the `otel` Cargo feature (off by default, so the OTLP dependencies aren't compiled in at all).

Icechunk will never export traces or logs to any service unless you opt-in and configure the desired service. Icechunk doesn't run a collector, it can only optionally export to one if you configure it.

### Enabling export

Run and set a collector endpoint before importing `icechunk`:

```bash
export ICECHUNK_OTLP_ENDPOINT=http://localhost:4317
```

With no endpoint set there is zero exporter overhead, and behavior is identical to a build without the feature.

### Configuration

Currently, all export configuration is via environment variables, read once at import. Each Icechunk-specific variable falls back to the corresponding standard OpenTelemetry variable:

| Variable | Falls back to | Default | Description |
|----------|---------------|---------|-------------|
| `ICECHUNK_OTLP_ENDPOINT` | `OTEL_EXPORTER_OTLP_ENDPOINT` | *unset (export disabled)* | OTLP/gRPC endpoint of the collector. Setting either variable enables export. |
| `ICECHUNK_OTEL_SERVICE_NAME` | `OTEL_SERVICE_NAME` | `icechunk` | The `service.name` reported to the collector. |
| `ICECHUNK_OTEL_FILTER` | — | `icechunk=info` | Which spans are exported, using the same `EnvFilter` directive syntax as `ICECHUNK_LOG`. |

`ICECHUNK_OTEL_FILTER` is independent of `ICECHUNK_LOG` / `set_logs_filter`: it controls only what is exported (not console output) and is fixed at startup. The default `icechunk=info` captures icechunk's higher-level operations. Lower it (e.g. `icechunk=debug` or `icechunk=trace`) to also export finer-grained spans, such as individual store operations, and the diagnostic events logged inside calls.

### Flushing

Spans are batched and exported in the background. The final batch is flushed automatically when the interpreter exits, via an `atexit` handler registered on import. You can also flush manually:

```python
import icechunk

icechunk.shutdown_telemetry()
```

### Trying it locally

Run any OTLP-compatible collector. Jaeger, for example, exposes an OTLP receiver and a UI:

```bash
docker run --rm -p 16686:16686 -p 4317:4317 jaegertracing/jaeger:2.19.0
```

Then point Icechunk at it and run your workload:

```bash
export ICECHUNK_OTLP_ENDPOINT=http://localhost:4317
python your_script.py
```

Open <http://localhost:16686>, select the `icechunk` service, and browse the traces.
