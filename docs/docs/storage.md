# Storage

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
    This is suitable for public data. [See the API](./reference.md#icechunk.s3_storage)

    ```python
    icechunk.s3_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        region='us-east-1,
        anonymous=True,
    )
    ```

=== "Refreshable Credentials"

    With this option, you provide a callback function that will be called to obtain S3 credentials when needed. This is useful for workloads that depend on retrieving short-lived credentials from AWS or similar authority, allowing for credentials to be refreshed as needed without interrupting any workflows. [See the API](./reference.md#icechunk.s3_storage)

    ```python
    def get_credentials() -> S3StaticCredentials:
        # In practice, you would use a function that actually fetches the credentials and returns them
        # along with an optional expiration time which will trigger this callback to run again
        return icechunk.S3StaticCredentials(
            access_key_id="xyz",
            secret_access_key="abc",å
            expires_after=datetime.now(UTC) + timedelta(days=1)
        )

    icechunk.s3_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        region='us-east-1',
        get_credentials=get_credentials,
    )
    ```

#### Tigris

[Tigris](https://www.tigrisdata.com/) is available as a storage backend for Icechunk. Icechunk provides a helper function specifically for [creating Tigris storage configurations](./reference.md#icechunk.tigris_storage).

```python
icechunk.tigris_storage(
    bucket="icechunk-test",
    prefix="quickstart-demo-1",
    access_key_id='my-access-key',
    secret_access_key='my-secret-key',
)
```

Even is Tigris is API-compatible with S3, this function is needed because Tigris implements a different form of consistency. If instead you use `s3_storage` with the Tigris endpoint, Icechunk won't be able to achieve all its consistency guarantees.

#### Cloudflare R2

Icechunk can use Cloudflare R2's S3-compatible API. You will need to:

1. provide either the account ID or set the [endpoint URL](https://developers.cloudflare.com/r2/api/s3/api/) specific to your bucket: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`;
2. [create an API token](https://developers.cloudflare.com/r2/api/s3/tokens/) to generate a secret access key and access key ID; and

```python
icechunk.r2_storage(
    bucket="bucket-name",
    prefix="icechunk-test/quickstart-demo-1",
    access_key_id='my-access-key',
    secret_access_key='my-secret-key',
    account_id='my-account-id',
)
```

For buckets with public access,

```python
icechunk.r2_storage(
    prefix="icechunk-test/quickstart-demo-1",
    endpoint_url="https://public-url,
)
```

#### Minio

[Minio](https://min.io/) is available as a storage backend for Icechunk. Functionally this storage backend is the same as S3 storage, but with a different endpoint.

For example, if we have a Minio server running at `http://localhost:9000` with access key `minio` and
secret key `minio123` we can create a storage configuration as follows:

```python
icechunk.s3_storage(
    bucket="icechunk-test",
    prefix="quickstart-demo-1",
    region='us-east-1',
    access_key_id='minio',
    secret_access_key='minio123',
    endpoint_url='http://localhost:9000',
    allow_http=True,
    force_path_style=True,
```

A few things to note:

1. The `endpoint_url` parameter is set to the URL of the Minio server.
2. If the Minio server is running over HTTP and not HTTPS, the `allow_http` parameter must be set to `True`.
3. Even though this is running on a local server, the `region` parameter must still be set to a valid region. [By default use `us-east-1`](https://github.com/minio/minio/discussions/15063).

#### Object stores lacking conditional writes

Some object stores don't have support for conditional writes, so they don't work with Icechunk out of the box. This is changing rapidly since AWS added support for these operations, and most major object store have had support for a long time.

If you are trying to use one of these object stores, like [JASMIN](https://help.jasmin.ac.uk/docs/short-term-project-storage/using-the-jasmin-object-store/) for example, you'll need to accept some trade-offs.
Icechunk can work on them, but you'll lose the consistency guarantee in the presence of multiple concurrent committers. If two sessions commit at the same time, one of them could get lost. If you decide
to accept this risk, you can configure Icechunk like so:

```python
storage = icechunk.s3_storage(...)
storage_config = icechunk.StorageSettings(
    unsafe_use_conditional_update=False,
    unsafe_use_conditional_create=False,
)
config = icechunk.RepositoryConfig(
    storage = storage_config
)
repo = icechunk.Repository.create(
    storage=storage,
    config= config
)
```

### Google Cloud Storage

Icechunk can be used with [Google Cloud Storage](https://cloud.google.com/storage?hl=en).

=== "From environment"

    With this option, the credentials for connecting to GCS are detected automatically from your environment.  [See the API](./reference.md#icechunk.gcs_storage)

    ```python
    icechunk.gcs_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        from_env=True
    )
    ```

=== "Service Account File"

    With this option, you provide the path to a [service account file](https://cloud.google.com/iam/docs/service-account-creds#key-types). [See the API](./reference.md#icechunk.gcs_storage)

    ```python
    icechunk.gcs_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        service_account_file="/path/to/service-account.json"
    )
    ```

=== "Service Account Key"

    With this option, you provide the service account key as a string. [See the API](./reference.md#icechunk.gcs_storage)

    ```python
    icechunk.gcs_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        service_account_key={
            "type": "service_account",
            "project_id": "my-project",
            "private_key_id": "my-private-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nmy-private-key\n-----END PRIVATE KEY-----\n",
            "client_email": "
            },
    )
    ```

=== "Application Default Credentials"

    With this option, you use the [application default credentials (ADC)](https://cloud.google.com/docs/authentication/provide-credentials-adc) to authentication with GCS. Provide the path to the credentials. [See the API](./reference.md#icechunk.gcs_storage)

    ```python
    icechunk.gcs_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        application_credentials="/path/to/application-credentials.json"
    )
    ```

=== "Bearer Token"

    With this option, you provide a bearer token to use for the object store. This is useful for short lived workflows where expiration is not relevant or when the bearer token will not expire [See the API](./reference.md#icechunk.gcs_storage)

    ```python
    icechunk.gcs_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        bearer_token="my-bearer-token"
    )
    ```

=== "Refreshable Credentials"

    With this option, you provide a callback function that will be called to obtain GCS credentials when needed. This is useful for workloads that depend on retrieving short-lived credentials from GCS or similar authority, allowing for credentials to be refreshed as needed without interrupting any workflows. This works at a lower level than the other methods, and accepts a bearer token and expiration time. These are the same credentials that are created for you when specifying the service account file, key, or ADC. [See the API](./reference.md#icechunk.gcs_storage)

    ```python
    def get_credentials() -> GcsBearerCredential:
        # In practice, you would use a function that actually fetches the credentials and returns them
        # along with an optional expiration time which will trigger this callback to run again
        return icechunk.GcsBearerCredential(bearer="my-bearer-token", expires_after=datetime.now(UTC) + timedelta(days=1))

    icechunk.gcs_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        get_credentials=get_credentials,
    )
    ```

#### Limitations

- The consistency guarantees for GCS function differently than S3. Specifically, GCS uses the [generation](https://cloud.google.com/storage/docs/request-preconditions#compose-preconditions) instead of etag for `if-match` `put` requests. Icechunk has not wired this through yet and thus [configuration updating](https://github.com/earth-mover/icechunk/issues/533) is potentially unsafe. This is not a problem for most use cases that are not frequently updating the configuration.
- GCS does not yet support [`bearer` tokens and auth refreshing](https://github.com/earth-mover/icechunk/issues/637). This means currently auth is limited to service account files.

### Azure Blob Storage

Icechunk can be used with [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).

=== "From environment"

    With this option, the credentials for connecting to Azure Blob Storage are detected automatically from your environment. [See the API](./reference.md#icechunk.azure_storage)

    ```python
    icechunk.azure_storage(
        account="my-account-name",
        container="icechunk-test",
        prefix="quickstart-demo-1",
        from_env=True
    )
    ```

=== "Provide credentials"

    With this option, you provide your credentials and other details explicitly. [See the API](./reference.md#icechunk.azure_storage)

    ```python
    icechunk.azure_storage(
        account_name='my-account-name',
        container="icechunk-test",
        prefix="quickstart-demo-1",
        account_key='my-account-key',
        access_token=None,  # optional
        sas_token=None,  # optional
        bearer_token=None,  # optional
    )
    ```

### Filesystem Storage

Icechunk can also be used on a [local filesystem](./reference.md#icechunk.local_filesystem_storage) by providing a path to the location of the store

=== "Local filesystem"

    ```python
    icechunk.local_filesystem_storage("/path/to/my/dataset")
    ```

#### Limitations

!!! warning
    File system Storage is not safe in the presence of concurrent commits. If two sessions are trying to commit at the same time, both operations may return successfully but one of the commits can be lost. Don't use file system storage in production if there is the possibility of concurrent commits.

- Icechunk currently does not work with a local filesystem storage backend on Windows. See [this issue](https://github.com/earth-mover/icechunk/issues/665) for more discussion. To work around, try using [WSL](https://learn.microsoft.com/en-us/windows/wsl/about) or a cloud storage backend.

### In Memory Storage

While it should never be used for production data, Icechunk can also be used with an in-memory storage backend. This is useful for testing and development purposes. This is volatile and when the Python process ends, all data is lost.

```python
icechunk.in_memory_storage()
```
