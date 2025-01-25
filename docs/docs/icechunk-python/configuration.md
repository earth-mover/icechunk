# Configuration

When creating and opening Icechunk repositories, there are a two different sets of configuration to be aware of:

- [`Storage`](./reference.md#icechunk.Storage) - for configuring access to the object store or filesystem
- [`RepositoryConfig`](./reference.md#icechunk.RepositoryConfig) - for configuring the behavior of the Icechunk Repository itself

## Storage

Icechunk can be configured to work with both object storage and filesystem backends. The storage configuration defines the location of an Icechunk store, along with any options or information needed to access data from a given storage type.

### S3 Storage

When using Icechunk with s3 compatible storage systems, credentials must be provided to allow access to the data on the given endpoint. Icechunk allows for creating the storage config for s3 in three ways:

=== "From environment"

    With this option, the credentials for connecting to S3 are detected automatically from your environment.
    This is usually the best choice if you are connecting from within an AWS environment (e.g. from EC2). [See the API](./reference.md#icechunk.s3_storage)

    ```python
    icechunk.s3_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        from_env=True
    )
    ```

=== "Provide credentials"

    With this option, you provide your credentials and other details explicitly. [See the API](./reference.md#icechunk.s3_storage)

    ```python
    icechunk.s3_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        region='us-east-1',
        access_key_id='my-access-key',
        secret_access_key='my-secret-key',
        # session token is optional
        session_token='my-token',
        endpoint_url=None,  # if using a custom endpoint
        allow_http=False,  # allow http connections (default is False)
    )
    ```

=== "Anonymous"

    With this option, you connect to S3 anonymously (without credentials).
    This is suitable for public data. [See the API](./reference.md#icechunk.StorageConfig.s3_anonymous)

    ```python
    icechunk.s3_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        region='us-east-1,
        anonymous=True,
    )
    ```

### Filesystem Storage

Icechunk can also be used on a [local filesystem](./reference.md#icechunk.local_filesystem_storage) by providing a path to the location of the store

=== "Local filesystem"

    ```python
    icechunk.local_filesystem_storage("/path/to/my/dataset")
    ```

## Repository Config

Separate from the storage config, the Repository can also be configured with options which control its runtime behavior.

!!! note
    This section is under construction and coming soon.

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
