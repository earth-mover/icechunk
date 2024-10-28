# Configuration

When creating and opening Icechunk stores, there are a two different sets of configuration to be aware of:

- [`StorageConfig`](./reference.md#icechunk.StorageConfig) - for configuring access to the object store or filesystem
- [`StoreConfig`](./reference.md#icechunk.StoreConfig) - for configuring the behavior of the Icechunk Store itself

## Storage Config

Icechunk can be confirgured to work with both object storage and filesystem backends. The storage configuration defines the location of an Icechunk store, along with any options or information needed to access data from a given storage type.

### S3 Storage

When using Icechunk with s3 compatible storage systems, credentials must be provided to allow access to the data on the given endpoint. Icechunk allows for creating the storage config for s3 in three ways:

=== "From environment"

    With this option, the credentials for connecting to S3 are detected automatically from your environment.
    This is usually the best choice if you are connecting from within an AWS environment (e.g. from EC2). [See the API](./reference.md#icechunk.StorageConfig.s3_from_env)
    
    ```python
    icechunk.StorageConfig.s3_from_env(
        bucket="icechunk-test",
        prefix="quickstart-demo-1"
    )
    ```

=== "Provide credentials"

    With this option, you provide your credentials and other details explicitly. [See the API](./reference.md#icechunk.StorageConfig.s3_from_config)
    
    ```python
    icechunk.StorageConfig.s3_from_config(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        region='us-east-1',
        credentials=S3Credentials(
            access_key_id='my-access-key',
            secret_access_key='my-secret-key',
            # session token is optional
            session_token='my-token',
        ),
        endpoint_url=None,
        allow_http=False,
    )
    ```

=== "Anonymous"

    With this option, you connect to S3 anonymously (without credentials).
    This is suitable for public data. [See the API](./reference.md#icechunk.StorageConfig.s3_anonymous)
    
    ```python
    icechunk.StorageConfig.s3_anonymous(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        region='us-east-1,
    )
    ```

### Filesystem Storage

Icechunk can also be used on a [local filesystem](./reference.md#icechunk.StorageConfig.filesystem) by providing a path to the location of the store

=== "Local filesystem"

    ```python
    icechunk.StorageConfig.filesystem("/path/to/my/dataset")
    ```

## Store Config

Separate from the storage config, the Store can also be configured with options which control its runtime behavior.

### Writing chunks inline

Chunks can be written inline alongside the store metadata if the size of a given chunk falls within the configured threshold.
Inlining allows these small chunks (often used to store small coordinate variables) to be accessed more quickly.
This is the default behavior for chunks smaller than 512 bytes, but it can be overridden using the `inline_chunk_threshold_bytes` option:

=== "Never write chunks inline"

    ```python
    StoreConfig(
        inline_chunk_threshold_bytes=0,
        ...
    )
    ```

=== "Write bigger chunks inline"

    ```python
    StoreConfig(
        inline_chunk_threshold_bytes=1024,
        ...
    )
    ```

### Virtual Reference Storage Config

Icechunk allows for reading "Virtual" data from [existing archival datasets](./virtual.md). This requires creating a distinct `VirtualRefConfig` (similar to `StorageConfig`) giving Icechunk the necessary permissions to access the archival data. This can be configured using the `virtual_ref_config` option:

=== "S3 from environment"

    ```python
    StoreConfig(
        virtual_ref_config=VirtualRefConfig.s3_from_env(),
        ...
    )
    ```

=== "S3 with credentials"

    ```python
    StoreConfig(
        virtual_ref_config=VirtualRefConfig.s3_from_config(
            credential=S3Credentials(
                access_key_id='my-access-key',
                secret_access_key='my-secret-key',
            ),
            region='us-east-1'
        ),
        ...
    )
    ```

=== "S3 Anonymous"

    ```python
    StoreConfig(
        virtual_ref_config=VirtualRefConfig.s3_anonymous(region='us-east-1'),
        ...
    )
    ```

## Creating and Opening Repos

Now we can now create or open an Icechunk store using our config.

### Creating a new store

!!! note

    Icechunk stores cannot be created in the same location where another store already exists.

=== "Creating with S3 storage"

    ```python
    storage = icechunk.StorageConfig.s3_from_env(
        bucket='earthmover-sample-data',
        prefix='icechunk/oisst.2020-2024/',
        region='us-east-1',
    )

    store = icechunk.IcechunkStore.create(
        storage=storage, 
        mode="w", 
    )
    ```

=== "Creating with local filesystem"

    ```python
    storage = icechunk.StorageConfig.filesystem("/path/to/my/dataset")
    config = icechunk.StoreConfig(
        inline_chunk_threshold_bytes=1024,
    )

    store = icechunk.IcechunkStore.create(
        storage=storage, 
        mode="w", 
    )
    ```

### Opening an existing store

=== "Opening from S3 Storage"

    ```python
    storage = icechunk.StorageConfig.s3_anonymous(
        bucket='earthmover-sample-data',
        prefix='icechunk/oisst.2020-2024/',
        region='us-east-1',
    )

    config = icechunk.StoreConfig(
        virtual_ref_config=icechunk.VirtualRefConfig.s3_anonymous(region='us-east-1'),
    )

    store = icechunk.IcechunkStore.open_existing(
        storage=storage, 
        mode="r+", 
        config=config,
    )
    ```

=== "Opening from local filesystem"

    ```python
    storage = icechunk.StorageConfig.filesystem("/path/to/my/dataset")
    config = icechunk.StoreConfig(
        inline_chunk_threshold_bytes=1024,
    )

    store = icechunk.IcechunkStore.open_existing(
        storage=storage,
        mode='r+',
        config=config,
    )
    ```

#### Access Mode

Note that in all of the above examples, a `mode` is provided to instruct the access level of the user to the store. This mode instructs whether the store should be opened in read only mode, and the store should start with a clean slate (although Icechunk prevents the possibility of accidentally overwriting any data that was previously comimtted to the store forever). For more about the access modes, see the [`zarr-python` docs](https://zarr.readthedocs.io/en/v3/_autoapi/zarr/abc/store/index.html#zarr.abc.store.AccessMode).
