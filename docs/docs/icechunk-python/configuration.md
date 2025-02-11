# Configuration

When creating and opening Icechunk repositories, there are many configuration options available to control the behavior of the repository and the storage backend. This page will guide you through the available options and how to use them.

## [`RepositoryConfig`](./reference.md#icechunk.RepositoryConfig)

The `RepositoryConfig` object is used to configure the repository. Within, it allows you to configure the following:

- [`inline_chunk_threshold_bytes`](./reference.md#icechunk.RepositoryConfig.inline_chunk_threshold_bytes): The threshold for when to inline a chunk into a manifest instead of storing it as a separate object in the storage backend.
- [`unsafe_overwrite_refs`](./reference.md#icechunk.RepositoryConfig.unsafe_overwrite_refs): Whether to allow overwriting references in the repository.
- [`get_partial_values_concurrency`](./reference.md#icechunk.RepositoryConfig.get_partial_values_concurrency): The number of concurrent requests to make when getting partial values from storage.
- [`compression`](./reference.md#icechunk.RepositoryConfig.compression): The compression configuration for the repository.
- [`caching`](./reference.md#icechunk.RepositoryConfig.caching): The caching configuration for the repository.
- [`storage`](./reference.md#icechunk.RepositoryConfig.storage): The storage configuration for the repository.
- [`virtual_chunk_containers`](./reference.md#icechunk.RepositoryConfig.virtual_chunk_containers): The virtual chunk containers for the repository.
- [`manifest`](./reference.md#icechunk.RepositoryConfig.manifest): The manifest configuration for the repository.

### [`CompressionConfig`](./reference.md#icechunk.CompressionConfig)

### [`CachingConfig`](./reference.md#icechunk.CachingConfig)

### [`StorageSettings`](./reference.md#icechunk.StorageSettings)

### [`VirtualChunkContainer`](./reference.md#icechunk.VirtualChunkContainer)

### [`ManifestConfig`](./reference.md#icechunk.ManifestConfig)

## Creating and Opening Repos

Now we can now create or open an Icechunk repo using our config.

### Creating a new repo

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
    )
    ```

=== "Creating with local filesystem"

    ```python
    repo = icechunk.Repository.create(
        storage=icechunk.local_filesystem_storage("/path/to/my/dataset"),
    )
    ```

If you are not sure if the repo exists yet, an `icechunk Repository` can created or opened if it already exists:

=== "Open or creating with S3 storage"

    ```python
    storage = icechunk.s3_storage(
        bucket='earthmover-sample-data',
        prefix='icechunk/oisst.2020-2024/',
        region='us-east-1',
        from_env=True,
    )

    repo = icechunk.Repository.open_or_create(
        storage=storage,
    )
    ```

=== "Open or creating with Google Cloud Storage"

    ```python
    storage = icechunk.gcs_storage(
        bucket='earthmover-sample-data',
        prefix='icechunk/oisst.2020-2024/',
        from_env=True,
    )

    repo = icechunk.Repository.open_or_create(
        storage=storage,
    )
    ```

=== "Open or creating with Azure Blob Storage"

    ```python
    storage = icechunk.azure_storage(
        container='earthmover-sample-data',
        prefix='icechunk/oisst.2020-2024/',
        from_env=True,
    )

    repo = icechunk.Repository.open_or_create(
        storage=storage,
    )
    ```

=== "Open or creating with local filesystem"

    ```python
    repo = icechunk.Repository.open_or_create(
        storage=icechunk.local_filesystem_storage("/path/to/my/dataset"),
    )
    ```

### Opening an existing repo

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
    )
    ```

=== "Opening from local filesystem"

    ```python
    storage = icechunk.local_filesystem_storage("/path/to/my/dataset")
    store = icechunk.IcechunkStore.open(
        storage=storage,
    )
    ```
