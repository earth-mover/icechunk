# Storage

Icechunk can be configured to work with both object storage and filesystem backends. The storage configuration defines the location of an Icechunk store, along with any options or information needed to access data from a given storage type.

### S3 Storage

When using Icechunk with s3 compatible storage systems, credentials must be provided to allow access to the data on the given endpoint. Icechunk allows for creating the storage config for s3 in three ways:

=== "From environment"

    With this option, the credentials for connecting to S3 are detected automatically from your environment.
    This is usually the best choice if you are connecting from within an AWS environment (e.g. from EC2). [See the API](../reference/storage.md#icechunk.storage.s3_storage)

    ```python
    icechunk.s3_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        from_env=True
    )
    ```

=== "Provide credentials"

    With this option, you provide your credentials and other details explicitly. [See the API](../reference/storage.md#icechunk.storage.s3_storage)

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
    This is suitable for public data. [See the API](../reference/storage.md#icechunk.storage.s3_storage)

    ```python
    icechunk.s3_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        region='us-east-1,
        anonymous=True,
    )
    ```

=== "Refreshable Credentials"

    With this option, you provide a callback function that will be called to obtain S3 credentials when needed. This is useful for workloads that depend on retrieving short-lived credentials from AWS or similar authority, allowing for credentials to be refreshed as needed without interrupting any workflows. [See the API](../reference/storage.md#icechunk.storage.s3_storage)

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

[Tigris](https://www.tigrisdata.com/) is available as a storage backend for Icechunk. Icechunk provides a helper function specifically for [creating Tigris storage configurations](../reference/storage.md#icechunk.storage.tigris_storage).

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

#### Hugging Face

Icechunk can use [Hugging Face Storage Buckets](https://huggingface.co/docs/hub/storage-buckets) through their [S3-compatible gateway](https://huggingface.co/docs/hub/storage-buckets-s3). You will need to:

1. [create a bucket](https://huggingface.co/docs/hub/storage-buckets#creating-a-bucket) under your username or one of your organizations; and
2. [generate S3 credentials](https://huggingface.co/docs/hub/storage-buckets-s3#generating-s3-credentials) from a Hugging Face access token. The access key ID starts with `HFAK`. An access token alone is not an S3 credential.

Icechunk provides a helper function for [creating Hugging Face storage configurations](../reference/storage.md#icechunk.storage.hf_storage).

```python
icechunk.hf_storage(
    bucket="my-bucket",
    prefix="quickstart-demo-1",
    namespace="my-username",
    access_key_id="HFAK...",
    secret_access_key="my-secret-key",
)
```

`namespace` is the owner of the bucket: your username or an organization name. The gateway scopes every request to the namespace in the endpoint path. `hf_storage` therefore builds the endpoint as `https://s3.hf.co/<namespace>`. It sends the bare bucket name. It sets the region to `us-east-1`. It forces path-style URLs. The gateway requires that region and that URL style.

The Hugging Face gateway discards user metadata which imposes some limitations on Icechunk: A conditional write whose response is lost in transit can report a conflict that never happened. Retry the commit if that happens.

#### Minio

[Minio](https://min.io/) is available as a storage backend for Icechunk. Functionally this storage backend is the same as S3 storage, but with a different endpoint.

For example, if we have a Minio server running at `http://localhost:4200` with access key `minio` and
secret key `test123` we can create a storage configuration as follows:

```python
icechunk.s3_storage(
    bucket="icechunk-test",
    prefix="quickstart-demo-1",
    region='us-east-1',
    access_key_id='minio',
    secret_access_key='test123',
    endpoint_url='http://localhost:4200',
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

    With this option, the credentials for connecting to GCS are detected automatically from your environment.  [See the API](../reference/storage.md#icechunk.storage.gcs_storage)

    ```python
    icechunk.gcs_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        from_env=True
    )
    ```

=== "Service Account File"

    With this option, you provide the path to a [service account file](https://cloud.google.com/iam/docs/service-account-creds#key-types). [See the API](../reference/storage.md#icechunk.storage.gcs_storage)

    ```python
    icechunk.gcs_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        service_account_file="/path/to/service-account.json"
    )
    ```

=== "Service Account Key"

    With this option, you provide the service account key as a string. [See the API](../reference/storage.md#icechunk.storage.gcs_storage)

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

    With this option, you use the [application default credentials (ADC)](https://cloud.google.com/docs/authentication/provide-credentials-adc) to authentication with GCS. Provide the path to the credentials. [See the API](../reference/storage.md#icechunk.storage.gcs_storage)

    ```python
    icechunk.gcs_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        application_credentials="/path/to/application-credentials.json"
    )
    ```

=== "Bearer Token"

    With this option, you provide a bearer token to use for the object store. This is useful for short lived workflows where expiration is not relevant or when the bearer token will not expire [See the API](../reference/storage.md#icechunk.storage.gcs_storage)

    ```python
    icechunk.gcs_storage(
        bucket="icechunk-test",
        prefix="quickstart-demo-1",
        bearer_token="my-bearer-token"
    )
    ```

=== "Refreshable Credentials"

    With this option, you provide a callback function that will be called to obtain GCS credentials when needed. This is useful for workloads that depend on retrieving short-lived credentials from GCS or similar authority, allowing for credentials to be refreshed as needed without interrupting any workflows. This works at a lower level than the other methods, and accepts a bearer token and expiration time. These are the same credentials that are created for you when specifying the service account file, key, or ADC. [See the API](../reference/storage.md#icechunk.storage.gcs_storage)

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

    With this option, the credentials for connecting to Azure Blob Storage are detected automatically from your environment. [See the API](../reference/storage.md#icechunk.storage.azure_storage)

    ```python
    icechunk.azure_storage(
        account="my-account-name",
        container="icechunk-test",
        prefix="quickstart-demo-1",
        from_env=True
    )
    ```

=== "Provide credentials"

    With this option, you provide your credentials and other details explicitly. [See the API](../reference/storage.md#icechunk.storage.azure_storage)

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

Icechunk can also store a repository in a filesystem, given the path to the location of the store. There are two filesystem backends:

- [`local_filesystem_storage`](../reference/storage.md#icechunk.storage.local_filesystem_storage). It is not safe for concurrent commits.
- [`filesystem_storage`](../reference/storage.md#icechunk.storage.filesystem_storage), experimental. It is safe for concurrent writers on filesystems that meet [its requirements](#requirements-and-limits-of-filesystem_storage), including network filesystems such as NFS.

=== "Local filesystem"

    ```python
    icechunk.local_filesystem_storage("/path/to/my/dataset")
    ```

=== "Filesystem (safe for concurrent writers)"

    ```python
    icechunk.filesystem_storage("/path/to/my/dataset")
    ```

!!! warning "Experimental"

    `filesystem_storage` is experimental. Feedback on how it behaves on different filesystems is welcome on the [issue tracker](https://github.com/earth-mover/icechunk/issues).

#### Limitations

!!! warning
    `local_filesystem_storage` is not safe in the presence of concurrent commits. If two sessions are trying to commit at the same time, both operations may return successfully but one of the commits can be lost. Don't use `local_filesystem_storage` in production if there is the possibility of concurrent commits; use [`filesystem_storage`](#requirements-and-limits-of-filesystem_storage) instead.

- Icechunk currently does not work with a local filesystem storage backend on Windows. See [this issue](https://github.com/earth-mover/icechunk/issues/665) for more discussion. To work around, try using [WSL](https://learn.microsoft.com/en-us/windows/wsl/about) or a cloud storage backend.

#### Requirements and limits of `filesystem_storage`

- **Filesystem.** The filesystem must provide hard links, atomic rename by name and exclusive directory rename. Icechunk doesn't check for them. Local POSIX filesystems, NFSv3 and NFSv4 with a Linux client, Lustre, GPFS, CephFS and JuiceFS are expected to work. SMB is untested. Non-Linux NFS clients (macOS, FreeBSD) are untested and may see new commits late.
- **Repository format.** `filesystem_storage` stores the repository's `repo` file in its own format: a stub file named `repo` and a `repo.d/` directory holding its versions. Everything else is stored as with any other backend. Other tools and other storage backends can't open these repositories: `local_filesystem_storage` and the object-store backends fail with a [`FormatError`](../reference/index.md#icechunk.FormatError) saying the repository uses the filesystem format, and older Icechunk versions fail on an unknown file type.
- **Unix only.** `filesystem_storage` is available on Unix platforms only; elsewhere it raises an error when constructed.
- **Every writer.** Concurrent-writer safety holds only if every writer of the repository uses `filesystem_storage`. `local_filesystem_storage` and older Icechunk versions overwrite `repo` without the protocol `filesystem_storage` relies on: they must never write to its repositories. After such a write, `filesystem_storage` refuses to open the repository, which then needs manual recovery. The refusal happens at the first use of `repo` by each storage instance (opening a repository creates one), so an overwrite while an instance is already in use goes unnoticed until the next instance.
- **Icechunk 1 repositories** are refused. See [Switching an existing repo](#switching-an-existing-repo).
- **Spurious conflicts.** On NFSv3 or NFSv4.0, a commit can be reported as a conflict (or a tag or branch creation as "already exists") although it succeeded. This needs the reply to the commit's final rename to be lost, the server's duplicate-request cache to miss the retry, and either another writer to commit within milliseconds or the directory listing to lag. The repository stays consistent and no data is lost. After a conflict error the commit may already have landed: look for it in the branch history ([`Repository.ancestry`](../reference/index.md#icechunk.Repository.ancestry), for example by its message) before retrying it.
- **Frozen repositories.** A repository can be [frozen](#freezing-a-repo), for example to copy it consistently. While it is frozen, it stays readable and every change to the repository fails with a [`StorageError`](../reference/index.md#icechunk.StorageError) of kind `repository-frozen` ([`ErrorKind.REPOSITORY_FROZEN`](../reference/index.md#icechunk.ErrorKind)), until it is unfrozen.
- **Symlinks.** Reads follow symlinks inside the repository, but listings don't. Garbage collection never deletes objects reached through a symlink.
- **Listing order** is unspecified.
- **Conditional writes.** Repositories whose config disables `unsafe_use_conditional_create` or `unsafe_use_conditional_update` log a warning; the setting is ignored and the backend always uses its own conditional writes.
- **Garbage collection** also deletes the leftovers of `filesystem_storage`'s commit protocol: versions of `repo` that are no longer current, whatever their age, and files of crashed or abandoned writes and creations (`repo.d/stage.*`, `repo.d.tmp.*`, `repo.stub.*` and staging files in the top-level directory) older than the `delete_object_older_than` cutoff, but never younger than 24 hours. `GCSummary.storage_leftovers_deleted` counts them.

`filesystem_storage` takes one optional setting, `fsync` (default `True`): whether to fsync data files after writes, and their directories before each commit. Turning it off risks losing writes after an OS crash or power loss; it never affects safety between concurrent writers. The fsyncs of the commit itself always run. On NFS, crash safety also needs exports with the `sync` option (the default).

```python
icechunk.filesystem_storage("/path/to/my/dataset", fsync=False)
```

#### Switching an existing repo

Repositories created with `local_filesystem_storage`, or with an older Icechunk version, keep `repo` as a single file. `filesystem_storage` refuses to open them, with an error that names `migrate_legacy_repo`, until they are converted once:

1. Stop every writer of the repository. No other process may write to it from when the conversion starts until every writer uses `filesystem_storage`.
2. Convert it with [`FilesystemStorage.migrate_legacy_repo`](../reference/storage.md#icechunk.storage.FilesystemStorage.migrate_legacy_repo). It returns `True` if it converted the repository and `False` if there was nothing to convert. If it is interrupted, run it again.

    ```python
    storage = icechunk.filesystem_storage("/path/to/my/dataset")
    storage.migrate_legacy_repo()
    repo = icechunk.Repository.open(storage)
    ```

3. From then on, open the repository only with `filesystem_storage`.

Also note:

- Icechunk 1 repositories must first be migrated to Icechunk 2 with `local_filesystem_storage`. See [Migrate to 2.0](../reference/migration-2.md).
- Converting a repository back to the single-file format, or copying it to an object store, isn't supported yet.
- A repository config that disables conditional writes only logs a harmless warning; see **Conditional writes** in [Requirements and limits](#requirements-and-limits-of-filesystem_storage).
- Pickled storage carries the absolute root path. With Dask or distributed, every worker must see the filesystem at the same path.

#### Freezing a repo

[`FilesystemStorage.freeze_repo`](../reference/storage.md#icechunk.storage.FilesystemStorage.freeze_repo) stops all changes to a `filesystem_storage` repository while keeping it readable, for example to copy the directory consistently. While it is frozen, every update of the repository (commits, creating or deleting branches and tags, saving the config) fails with an error saying the repository is frozen. [`FilesystemStorage.unfreeze_repo`](../reference/storage.md#icechunk.storage.FilesystemStorage.unfreeze_repo) accepts changes again.

```python
storage = icechunk.filesystem_storage("/path/to/my/dataset")
assert storage.freeze_repo()
# copy /path/to/my/dataset
storage.unfreeze_repo()
```

Both return `True` if the call changed the repository.

### HTTP Storage

Icechunk can read repositories served over HTTP or HTTPS. This is useful for accessing publicly available datasets or repositories hosted on static file servers. [See the API](../reference/storage.md#icechunk.storage.http_storage)

```python
icechunk.http_storage("https://example.com/path/to/repo")
```

You can also pass optional configuration options to customize the HTTP client behavior:

```python
icechunk.http_storage(
    "https://example.com/path/to/repo",
    opts={
        "timeout": "30s",
        "connect_timeout": "5s",
    }
)
```

See the [object_store ClientConfigKey documentation](https://docs.rs/object_store/latest/object_store/client/enum.ClientConfigKey.html#variants) for a list of possible configuration keys (use snake_case format).

#### Limitations

!!! warning
    HTTP Storage is read-only. You cannot create new repositories or commit changes to repositories accessed via HTTP storage. Use this backend only for reading existing data.

- HTTP storage does not support write operations. Attempting to commit changes to a repository opened with HTTP storage will fail.
- The HTTP server must serve the repository files as static content at the expected paths.

### Redirect Storage

Icechunk supports a redirect storage backend that resolves the actual storage location at runtime by following HTTP redirects. Rather than pointing directly at a bucket or path, you provide an HTTP URL that returns a redirect (3xx response with a `Location` header) to a supported storage scheme (`s3://`, `gs://`, `r2://`, `tigris://`, `http+icechunk://`, etc.). [See the API](../reference/storage.md#icechunk.storage.redirect_storage)

```python
icechunk.redirect_storage("https://example.com/my-repo-location")
```

When the repository is first accessed, Icechunk will follow redirects from the given URL until it reaches a recognized storage scheme, and then use that as the underlying backend. The resolved backend is cached for the lifetime of the storage instance.

This is useful when a service controls which bucket or path a repository lives in. For example, a catalog or registry can serve a redirect so that clients don't need to know the final storage location ahead of time — they only need the stable redirect URL. If the data moves to a different bucket or provider, the redirect can be updated without changing any client code.

#### Example redirect server

The server side of redirect storage is straightforward, any HTTP server that returns a 3xx redirect with a `Location` header will work. Here is a minimal example using Python's built-in `http.server`:

```python
from http.server import BaseHTTPRequestHandler, HTTPServer

REPOS = {
    "/my-repo": "s3://my-bucket/icechunk/my-repo?region=us-east-1",
    "/public-era5": "gs://era5-zarr/icechunk/era5",
}


class RedirectHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        location = REPOS.get(self.path)
        if location is None:
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(302)
        self.send_header("Location", location)
        self.end_headers()


HTTPServer(("127.0.0.1", 8989), RedirectHandler).serve_forever()
```

With this server running, you can open a repository through it:

```python
storage = icechunk.redirect_storage("http://127.0.0.1:8989/my-repo")
repo = icechunk.Repository.open(storage=storage)
```

#### Limitations

- Redirect Storage only ever resolves to a read-only backend.
- The resolved storage will use anonymous credentials, so the target data must be publicly accessible.
- The URL you provide must return an HTTP redirect. If the server returns a non-redirect response, opening the repository will fail.
- The redirect target must use one of the supported schemes: `s3://`, `gs://`, `gcs://`, `r2://`, `tigris://`, `http+icechunk://`, `http+ic://`, `https+icechunk://`, or `https+ic://`.
- A maximum of 10 redirects will be followed before failing.

### In Memory Storage

While it should never be used for production data, Icechunk can also be used with an in-memory storage backend. This is useful for testing and development purposes. This is volatile and when the Python process ends, all data is lost.

```python
icechunk.in_memory_storage()
```

### Custom request headers

You can attach arbitrary HTTP headers to the object-store requests Icechunk makes.
A common use is forcing an ACL on every write:

```python
storage = icechunk.s3_storage(
    bucket="b",
    prefix="p",
    region="us-east-1",
    write_headers={"x-amz-acl": "bucket-owner-full-control"},
)
```

Three keyword arguments are available on `s3_storage`, `s3_object_store_storage`,
`tigris_storage`, `r2_storage`, and `gcs_storage`:

- `read_headers` — sent on read requests (GET/HEAD/list).
- `write_headers` — sent on write requests (PUT/POST/DELETE/copy).
- `headers` — sent on both; merged with the role-specific maps, which take
  precedence per role on a name conflict:
  `effective_read = {**headers, **read_headers}`,
  `effective_write = {**headers, **write_headers}`.

The `User-Agent` header is managed by Icechunk: it names the array and chunk
each request is for, plus optional labels set when opening the repository. See
[Attributing requests in bucket logs][attribution].

[attribution]: observability.md#attributing-requests-in-bucket-logs

### Empty prefix and legacy repositories

Creating a repository at an empty `prefix` (`None` or `""`) — i.e. at the bucket root — is not supported on object stores since Icechunk v2.1.0 and raises an error; use a non-empty `prefix`. In-memory and local-filesystem storage are unaffected.

Existing empty-prefix repositories can still be **opened and updated**: Icechunk detects their on-disk layout automatically, so no action is needed to keep using them.

When opening, the `legacy_rooted_keys` parameter overrides that detection:

- `None` (default) — auto-detect the layout by probing storage. Use this unless you have a specific reason not to.
- `True` — force the legacy leading-slash layout, skipping the probe (for example, for a write-only worker that cannot probe storage). Only valid with an empty prefix.
- `False` — force the standard layout, skipping the probe. Users shouldn't need to pass this value.
