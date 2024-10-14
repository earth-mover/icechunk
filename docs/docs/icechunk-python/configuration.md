# Configuration

When creating and opening Icechunk stores, there are a few different ways to configure your store. The two types of configuration are `StorageConfig` and `StoreConfig`. 

## Storage Config

Icechunk can be used with many different storage backends. The storage configuration defines the location of an Icechunk stor, along with any options or information needed to access data from a given storage type.

### S3 Storage

When using Icechunk with s3 compatible storage systems, credentials must be provided to allow access to the data on the given endpoint. Icechunk allows for creating the storage config for s3 in three ways:

=== "From environment"

    ```python
    icechunk.StorageConfig.s3_from_env(
        bucket="icechunk-test",
        prefix="quickstart-demo-1"
    )
    ```

=== "Provide credentials"

    ```python
    icechunk.StorageConfig.s3_from_config(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        region='us-east-1',
        credentials=S3Credentials(
            access_key_id='my-access-key',
            secret_access_key='my-secret-key',
        ),
        endpoint_url=None,
    )
    ```

=== "Anonymous"

    ```python
    icechunk.StorageConfig.s3_anonymous(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        region='us-east-1,
    )
    ```

### Filesystem Storage

Icechunk can also be used on a local filesystem by providing a path to the location of the store

=== "Local filesystem"

    ```python
    icechunk.StorageConfig.filesystem("/path/to/my/dataset")
    ```

## Store Config

Separate from the storage config, the store can also be configured with a few different options.

## Creating and Opening Repos

