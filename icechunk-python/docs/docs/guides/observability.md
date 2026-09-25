# Observability

Icechunk is instrumented with the Rust [`tracing`](https://docs.rs/tracing) framework. The same instrumentation drives two independent outputs:

- **Console logs** — human-readable logs printed to stderr, controlled by `ICECHUNK_LOG`. Enabled by default at the `warn` level.
- **OpenTelemetry trace export** *(experimental)* — structured spans and traces are exported over OTLP/gRPC to a collector or service. Off by default, opt-in.
  Telemetry can be configured with an endpoint to use any OpenTelemetry compatible collector or service. If no endpoint is set
  by the user, there is zero exporter overhead and no spans are exported.

The two are configured separately and don't affect each other.

A third view comes from the object store itself: every request Icechunk makes
identifies the array and chunk it is for in its `User-Agent` header, so the
bucket's own logs can attribute traffic. See [Attributing requests in bucket
logs](#attributing-requests-in-bucket-logs).

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
| ---------- | --------------- | --------- | ------------- |
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

## Attributing requests in bucket logs

Object stores record the `User-Agent` of every request in their access logs: S3
server access logs and CloudTrail data events, GCS usage logs, and Azure
diagnostic logs. Icechunk uses that header to say what each request is for, so
the bucket owner can break traffic down by array, chunk, application, workload
and user without any cooperation from the reader.

Every request carries an `icechunk/<version>` product token. Chunk reads and
writes add the array path and the chunk coordinates, and manifest reads and
writes add the array path:

```
aws-sdk-rust/1.3.14 os/linux lang/rust/1.98.0 weatherlib/0.9 icechunk/2.3.0 (workload=nightly-ingest; principal=u_123; array=g/temperature; chunk=0/1/2)
```

The tokens before `weatherlib/0.9` come from the AWS SDK and are absent on the
other backends. Parse the header by locating the `icechunk/` token and the
comment in parentheses rather than by position.

### Labels

The array and chunk are added automatically. Three optional labels describe who
is making the requests, and are set once when opening the repository:

```python
import icechunk

repo = icechunk.Repository.open(
    storage,
    attribution=icechunk.Attribution(
        client="weatherlib/0.9",    # which software embeds icechunk
        workload="nightly-ingest",  # what it is doing
        principal="u_123",          # on whose behalf
    ),
)
```

`client` is a product token, `name` or `name/version`. `workload` and
`principal` are free-form labels: printable ASCII, at most 128 bytes, without
`(`, `)`, `\`, `;`, `=` or `"`.

### Limitations

- Listings carry no attribution, and deletes carry none on the `object_store`
  backends (`gcs_storage`, `azure_storage`, `s3_object_store_storage`). Both are
  used mostly by garbage collection.
- Virtual chunk reads are attributed like any other chunk read, but reads made
  in a browser through `icechunk-js` are not, since browsers do not let a page
  set `User-Agent`.
- Metadata reads by the garbage collector and by chunk statistics carry the
  labels but no array.
