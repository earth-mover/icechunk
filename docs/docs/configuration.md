# Configuration

When creating and opening Icechunk repositories, there are many configuration options available to control the behavior of the repository and the storage backend. This page will guide you through the available options and how to use them.

## [`RepositoryConfig`](./reference.md#icechunk.RepositoryConfig)

The `RepositoryConfig` object is used to configure the repository. For convenience, this can be constructed using some sane defaults:

```python exec="on" session="config" source="material-block"
import icechunk

config = icechunk.RepositoryConfig.default()
```

or it can be optionally loaded from an existing repository:

```python
config = icechunk.Repository.fetch_config(storage)
```

It allows you to configure the following parameters:

### [`inline_chunk_threshold_bytes`](./reference.md#icechunk.RepositoryConfig.inline_chunk_threshold_bytes)

The threshold for when to inline a chunk into a manifest instead of storing it as a separate object in the storage backend.

### [`get_partial_values_concurrency`](./reference.md#icechunk.RepositoryConfig.get_partial_values_concurrency)

The number of concurrent requests to make when getting partial values from storage.

### [`max_concurrent_requests`](./reference.md#icechunk.RepositoryConfig.max_concurrent_requests)

The maximum total number of concurrent requests this repo will allow.
See [Performance | Concurrency](../performance#Concurrency) for details.

### [`compression`](./reference.md#icechunk.RepositoryConfig.compression)

Icechunk uses Zstd compression to compress its metadata files. [`CompressionConfig`](./reference.md#icechunk.CompressionConfig) allows you to configure the [compression level](./reference.md#icechunk.CompressionConfig.level) and [algorithm](./reference.md#icechunk.CompressionConfig.algorithm). Currently, the only algorithm available is [`Zstd`](https://facebook.github.io/zstd/).

```python exec="on" session="config" source="material-block"
config.compression = icechunk.CompressionConfig(
    level=3,
    algorithm=icechunk.CompressionAlgorithm.Zstd,
)
```

### [`caching`](./reference.md#icechunk.RepositoryConfig.caching)

Icechunk caches files (metadata and chunks) to speed up common operations. [`CachingConfig`](./reference.md#icechunk.CachingConfig) allows you to configure the caching behavior for the repository.

```python exec="on" session="config" source="material-block"
config.caching = icechunk.CachingConfig(
    num_snapshot_nodes=100,
    num_chunk_refs=100,
    num_transaction_changes=100,
    num_bytes_attributes=10_000,
    num_bytes_chunks=1_000_000,
)
```

### [`storage`](./reference.md#icechunk.RepositoryConfig.storage)

This configures how Icechunk loads data from the storage backend. [`StorageSettings`](./reference.md#icechunk.StorageSettings) allows you to configure the storage settings.

```python exec="on" session="config" source="material-block"
config.storage = icechunk.StorageSettings(
    concurrency=icechunk.StorageConcurrencySettings(
        max_concurrent_requests_for_object=10,
        ideal_concurrent_request_size=1_000_000,
    ),
    storage_class="STANDARD",
    metadata_storage_class="STANDARD_IA",
    chunks_storage_class="STANDARD_IA",
)
```

### [`virtual_chunk_containers`](./reference.md#icechunk.RepositoryConfig.virtual_chunk_containers)

Icechunk allows repos to contain [virtual chunks](./virtual.md). To allow for referencing these virtual chunks, you must configure the `virtual_chunk_containers` parameter to specify the storage locations and configurations for any virtual chunks. Each virtual chunk container is specified by a [`VirtualChunkContainer`](./reference.md#icechunk.VirtualChunkContainer) object which contains a url prefix, and a storage configuration. When a container is added to the settings, any virtual chunks with a url that starts with the configured prefix will use the storage configuration for that matching container.

!!! note

    Currently only `s3` compatible storage, `gcs`, `local_filesystem` and `http[s]` storages are supported for virtual chunk containers. Other storage backends such as `azure` are on the roadmap.

#### Example

For example, if we wanted to configure an icechunk repo to be able to contain virtual chunks from an `s3` bucket called `my-s3-bucket` in `us-east-1`, we would do the following:

```python exec="on" session="config" source="material-block"
config.set_virtual_chunk_container(
    icechunk.VirtualChunkContainer(
        "s3://my-s3-bucket/",
        store=icechunk.s3_store(region="us-east-1"),
    ),
)
```

If we also wanted to configure the repo to be able to contain virtual chunks from another `s3` bucket called `my-other-s3-bucket` in `us-west-2`, we would do the following:

```python exec="on" session="config" source="material-block"
config.set_virtual_chunk_container(
  icechunk.VirtualChunkContainer(
      "s3://my-other-s3-bucket/",
      store=icechunk.s3_store(region="us-west-2")
  )
)
```

This will add a second `VirtualChunkContainer` but not overwrite the first one that was added because they have different url prefixes.  Now at read time, if Icechunk encounters a virtual chunk url that starts with `s3://my-other-s3-bucket/`, it will use the storage configuration for the `my-other-s3-bucket` container.

!!! note

    While virtual chunk containers specify the storage configuration for any virtual chunks, they do not contain any authentication information. The credentials must also be specified when opening the repository using the [`authorize_virtual_chunk_access`](./reference.md#icechunk.Repository.open) parameter. This parameter also serves as a way for the user to authorize the access to the virtual chunk containers, containers that are not explicitly allowed with `authorize_virtual_chunk_access` won't be able to fetch their chunks. See the [Virtual Chunk Credentials](#virtual-chunk-credentials) section for more information.

### [`manifest`](./reference.md#icechunk.RepositoryConfig.manifest)

The manifest configuration for the repository. [`ManifestConfig`](./reference.md#icechunk.ManifestConfig) allows you to configure behavior for how manifests are loaded. In particular, the `preload` parameter allows you to configure the preload behavior of the manifest using a [`ManifestPreloadConfig`](./reference.md#icechunk.ManifestPreloadConfig). This allows you to control the number of references that are loaded into memory when a session is created, along with which manifests are available to be preloaded.

#### Example

For example, if we have a repo which contains data that we plan to open as an [`Xarray`](./xarray.md) dataset, we may want to configure the manifest preload to only preload manifests that contain arrays that are coordinates, in our case `time`, `latitude`, and `longitude`.

```python exec="on" session="config" source="material-block"
config.manifest = icechunk.ManifestConfig(
    preload=icechunk.ManifestPreloadConfig(
        max_total_refs=100_000_000,
        preload_if=icechunk.ManifestPreloadCondition.name_matches(".*time|.*latitude|.*longitude"),
    ),
)
```

### Applying Configuration

Now we can now create or open an Icechunk repo using our config.

#### Creating a new repo

If no config is provided, the repo will be created with the [default configuration](./reference.md#icechunk.RepositoryConfig.default).

!!! note

    Icechunk repos cannot be created in the same location where another store already exists.

=== "Creating with S3 storage"

    ```python
    storage = icechunk.s3_storage(
        bucket='earthmover-sample-data',
        prefix='icechunk/oisst.2020-2024/',
        region='us-east-1',
        from_env=True,
    )

    repo = icechunk.Repository.create(
        storage=storage,
        config=config,
    )
    ```

=== "Creating with Google Cloud Storage"

    ```python
    storage = icechunk.gcs_storage(
        bucket='earthmover-sample-data',
        prefix='icechunk/oisst.2020-2024/',
        from_env=True,
    )

    repo = icechunk.Repository.create(
        storage=storage,
        config=config,
    )
    ```

=== "Creating with Azure Blob Storage"

    ```python
    storage = icechunk.azure_storage(
        container='earthmover-sample-data',
        prefix='icechunk/oisst.2020-2024/',
        from_env=True,
    )

    repo = icechunk.Repository.create(
        storage=storage,
        config=config,
    )
    ```

=== "Creating with local filesystem"

    ```python
    repo = icechunk.Repository.create(
        storage=icechunk.local_filesystem_storage("/path/to/my/dataset"),
        config=config
    )
    ```

#### Opening an existing repo

When opening an existing repo, the config will be loaded from the repo if it exists. If no config exists and no config was specified, the repo will be opened with the [default configuration](./reference.md#icechunk.RepositoryConfig.default).

However, if a config was specified when opening the repo AND a config was previously persisted in the repo, the two configurations will be merged. The config specified when opening the repo will take precedence over the persisted config.

=== "Opening from S3 Storage"

    ```python
    storage = icechunk.s3_storage(
        bucket='earthmover-sample-data',
        prefix='icechunk/oisst.2020-2024/',
        region='us-east-1',
        from_env=True,
    )

    repo = icechunk.Repository.open(
        storage=storage,
        config=config,
    )
    ```

=== "Opening from Google Cloud Storage"

    ```python
    storage = icechunk.gcs_storage(
        bucket='earthmover-sample-data',
        prefix='icechunk/oisst.2020-2024/',
        from_env=True,
    )

    repo = icechunk.Repository.open(
        storage=storage,
        config=config,
    )
    ```

=== "Opening from Azure Blob Storage"

    ```python
    storage = icechunk.azure_storage(
        container='earthmover-sample-data',
        prefix='icechunk/oisst.2020-2024/',
        from_env=True,
    )

    repo = icechunk.Repository.open(
        storage=storage,
        config=config,
    )
    ```

=== "Opening from local filesystem"

    ```python
    storage = icechunk.local_filesystem_storage("/path/to/my/dataset")
    store = icechunk.IcechunkStore.open(
        storage=storage,
        config=config,
    )
    ```

### Persisting Configuration

Once the repo is opened, the current config can be persisted to the repo by calling [`save_config`](./reference.md#icechunk.Repository.save_config).

```python
repo.save_config()
```

The next time this repo is opened, the persisted config will be loaded by default.

## Virtual Chunk Credentials

When using virtual chunk containers, the containers must be authorized by the repo user, and the credentials for the storage backend must be specified. This is done using the [`authorize_virtual_chunk_access`](./reference.md#icechunk.Repository.open) parameter when creating or opening the repo. Credentials are specified as a dictionary of container url prefixes mapping to credential objects or `None`. A `None` credential will fetch credentials from the process environment or it will use anonymous credentials if the container allows it. A helper function, [`containers_credentials`](./reference.md#icechunk.containers_credentials), is provided to make it easier to specify credentials for multiple containers.

### Example

Expanding on the example from the [Virtual Chunk Containers](#virtual_chunk_containers) section, we can configure the repo to use the credentials for the `my-s3-bucket` and `my-other-s3-bucket` containers.

```python
credentials = icechunk.containers_credentials(
    { "s3://my_s3_bucket": icechunk.s3_credentials(bucket="my-s3-bucket", region="us-east-1"),
      "s3://my_other_s3_bucket": icechunk.s3_credentials(bucket="my-other-s3-bucket", region="us-west-2"),
    }
)

repo = icechunk.Repository.open(
    storage=storage,
    config=config,
    authorize_virtual_chunk_access=credentials,
)
```

## Proxy configuration

Icechunk supports routing its requests through a proxy. This is useful when your object storage service itself is behind a proxy.

Icechunk automatically detects proxy settings from environment variables:

```bash
export HTTPS_PROXY=http://proxy.corp.com:8080
export HTTP_PROXY=http://proxy.corp.com:8080
```

No code changes are required. Icechunk checks proxy sources in this order:
1. `HTTPS_PROXY` / `https_proxy` - highest priority
2. `HTTP_PROXY` / `http_proxy`
3. `ALL_PROXY` / `all_proxy`

Note that you must set the environment variable at the OS level, not via the python interpreter.
