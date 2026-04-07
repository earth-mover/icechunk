# Flaky Networks

Icechunk provides configurable retries and timeouts to handle unreliable networks.

!!! note

    If you encounter errors or operations that you believe should be retried but aren't, please [open an issue](https://github.com/earth-mover/icechunk/issues).

## Retries: [`StorageRetriesSettings`](./reference.md#icechunk.StorageRetriesSettings)

Controls retry behavior for all storage operations (reads, writes, deletes).

| Parameter            | Default         | Description                                                |
|----------------------|-----------------|------------------------------------------------------------|
| `max_tries`          | 10              | Maximum number of attempts (including the initial request) |
| `initial_backoff_ms` | 100             | Initial delay (ms) for the exponential backoff stage       |
| `max_backoff_ms`     | 180,000 (3 min) | Maximum delay (ms) between retries                         |

```python
import icechunk

config = icechunk.RepositoryConfig.default()
config.storage.retries = icechunk.StorageRetriesSettings(
    max_tries=20,
    initial_backoff_ms=200,
    max_backoff_ms=60_000,
)
```

## Timeouts: [`StorageTimeoutSettings`](./reference.md#icechunk.StorageTimeoutSettings)

Controls connect, read, and operation timeouts for the underlying S3 client. All values are in milliseconds. By default, these are unset and the [AWS SDK defaults](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/timeouts.html) apply.

| Parameter                      | Description                                                        |
|--------------------------------|--------------------------------------------------------------------|
| `connect_timeout_ms`           | Time allowed to establish a TCP connection                         |
| `read_timeout_ms`              | Time allowed to read a single response                             |
| `operation_timeout_ms`         | Total time allowed for an entire operation (including all retries) |
| `operation_attempt_timeout_ms` | Time allowed for a single attempt of an operation                  |

```python
config.storage.timeouts = icechunk.StorageTimeoutSettings(
    connect_timeout_ms=5_000,
    read_timeout_ms=30_000,
    operation_timeout_ms=300_000,
    operation_attempt_timeout_ms=60_000,
)
```

## Stalled stream detection

A stalled network stream is an HTTP connection which does not transfer any data over a certain period.
Stalled connections may occur in the following situations:

- When the client is connecting to a remote object store behind a slow network connection.
- When the client is behind a VPN or proxy server which is limiting the number or throughput of connections between the client and the remote object store.

The Amazon S3 SDK allows detection and retrying on stalled stream errors, which typically look like
```
  |-> I/O error
  |-> streaming error
  `-> minimum throughput was specified at 1 B/s, but throughput of 0 B/s was observed
```

This behavior is configurable when creating a new `Storage` option, via the `network_stream_timeout_seconds` parameter.
The default is 60 seconds.
To set a different value, you may specify as follows

```python
storage = icechunk.s3_storage(
    **other_storage_kwargs,
    network_stream_timeout_seconds=50,
)
repo = icechunk.Repository.open(storage=storage)
```

Specifying a value of 0 disables this check entirely.

## Applying the configuration

Pass the configured `RepositoryConfig` when creating or opening a repository:

```python
repo = icechunk.Repository.open(
    storage=storage,
    config=config,
)
```

See [Configuration](./configuration.md) for full details on applying and persisting configuration.
