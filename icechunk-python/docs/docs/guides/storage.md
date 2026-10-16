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
- [`filesystem_storage`](../reference/storage.md#icechunk.storage.filesystem_storage), experimental. It is safe for concurrent writers on filesystems that meet [its requirements](#requirements-and-limits-of-filesystem_storage), including network filesystems such as NFS. It stores repositories in [the filesystem layout](#filesystem-format), which other backends and older Icechunk versions can read but not modify.

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

- **Filesystem.** The filesystem must provide hard links, atomic rename by name and exclusive directory rename, and the whole repository must be on one filesystem, with no mount point inside it. Icechunk doesn't check for them. Local POSIX filesystems, NFSv3 and NFSv4 with a Linux client, Lustre, GPFS, and CephFS are expected to work; others are untested.
- <a id="filesystem-format"></a>**Repository format.** `filesystem_storage` stores the repository info in the [filesystem layout](../reference/spec-filesystem-layout-v1.md): the `repo.d/` directory holds its versions, and the file `repo` is the [mirror](#the-mirror), a copy of a version that was current. Every _change_, that is every update of the repository info (a commit, a branch, tag, status or configuration change, expiration, garbage collection or a conversion), publishes the mirror before returning. Everything else is stored as with any other backend.
- **Other backends and older versions.** The repository lists the `filesystem-layout-v1` [writer feature](../reference/spec-v2-2.md#writer-features), which only `filesystem_storage` implements. Other backends, and Icechunk older than 2.4.0 with any backend, read the repository through the mirror and refuse to modify it. From Icechunk 2.4.0, the refusal is a [`ReadOnlyError`](../reference/index.md#icechunk.ReadOnlyError) of kind `missing-writer-features` that names the feature. Icechunk 2.0 to 2.3 fail with a message that names the feature and the version it needs, except garbage collection and expiration, which say only that the repository is read-only. Those versions can still change the status, and, while `repo` is missing, create a repository at the same path with `check_clean_root=False`; both [fork](#forks) the repository. Never change the status with Icechunk older than 2.4.0, and never write `repo` by hand.
- **Unix only.** `filesystem_storage` is available on Unix platforms only; elsewhere it raises an error when constructed.
- <a id="reads-can-lag"></a>**Commit visibility can lag.** Unlike object stores, a commit made on one machine isn't always visible on another right away. Commits are never lost, but a reader can open a branch and get its previous tip without any error. How long a reader can lag depends on the filesystem and the client, but Icechunk makes a best effort to minimize this time, and the time collapses to zero in some filesystems. To hand a commit over between machines, pass its snapshot id and open it with [`Repository.readonly_session(snapshot_id=...)`](../reference/index.md#icechunk.Repository.readonly_session).
- **Spurious conflicts.** In rare cases, when the filesystem loses the reply to a change and then many other changes land, or directory listings stay stale, the change reports a conflict although it landed. A change is never reported as done when it didn't land. Check the branch before retrying a commit that failed this way.
- **Clock.** Creating a repository, or converting one to the filesystem layout, numbers its versions from the clock of the host doing it. If an earlier `repo.d/` existed at the same path, for example before a conversion to the standard layout and back, that clock must not lag behind the clocks of the hosts that created the earlier one: a stale directory listing that still shows it can then lead garbage collection to delete the current version.
- **Garbage collection cutoff.** As with every backend, `delete_object_older_than` in [garbage collection](../understanding/expiration.md) must be earlier than the start of every session that may still commit: a commit that started before the cutoff and lands after garbage collection can reference objects it deleted. On a shared filesystem under load, a commit can take minutes, so leave a margin of hours, not minutes.
- **Icechunk 1 repositories** are refused. See [Switching an existing repo](#switching-an-existing-repo).
- **Conditional writes.** Repositories whose config disables `unsafe_use_conditional_create` or `unsafe_use_conditional_update` log a warning; the setting is ignored and the backend always uses its own conditional writes.

`filesystem_storage` takes one optional setting, `fsync` (default `True`): whether to fsync data files after writes, and their directories before each change. Turning it off risks losing writes after an OS crash or power loss; it never affects safety between concurrent writers. The fsyncs of the repository info and the mirror always run.

```python
icechunk.filesystem_storage("/path/to/my/dataset", fsync=False)
```

#### The mirror

Other backends and older Icechunk versions read the repository through the mirror `repo`. Opening a repository never publishes the mirror, and every change publishes it before returning, but it can lag behind the current version:

- after a process crashes between its change and the publication;
- under heavy write contention, when a publication gives up after a few rounds; it logs a warning that names the version it left in `repo`;
- briefly, while a slow publisher, or one working from a stale directory listing on NFS or FUSE, catches up.

A lagging mirror can reference objects that garbage collection has deleted, so reads through it can fail. Garbage collection makes a change and publishes the mirror before deleting anything, and publishes it again afterwards.

To make the mirror current, make a change once every process that changed the repository earlier has finished: its publication makes the mirror current. Setting the status to read-only is the change to make, since nothing changes the repository after it. Do it before serving a finished repository read-only through the mirror:

```python
storage = icechunk.filesystem_storage("/path/to/my/dataset")
repo = icechunk.Repository.open(storage)
repo.set_status(icechunk.RepoStatus(icechunk.RepoAvailability.read_only))
```

[`FilesystemStorage.publish_mirror`](../reference/storage.md#icechunk.storage.FilesystemStorage.publish_mirror) repairs a lagging mirror without changing the repository, and is safe while writers run. It is best effort: it publishes the version it lists, which a change landing meanwhile, or a stale listing, can make older than the current one. Its result's `current` is `True` if `repo` holds the version that was current in its last listing; otherwise `left` names the version it left in `repo`, if any.

```python
outcome = storage.publish_mirror()
if not outcome.current:
    print("the mirror holds", outcome.left)
```

#### Stopping writers and copying a repository

While the status is read-only, every modification of the repository is refused, garbage collection and expiration included, except status changes and the conversions. To stop every writer, for example before copying the repository, set the status to read-only. To copy a repository consistently:

1. Set the status to read-only, as above, and keep it until the copy is complete.
2. Copy every entry under the repository's root, `repo.d/` and `repo` included, in any order.
3. Check the copy: its `repo.d/` has exactly one marker, `latest.<K>.<N>.<h>`; `repo.d/<N>.<h>` has the SHA-256 `h` and the status read-only; and the copied `repo` has the SHA-256 `h`. If the marker or its version fails the check, start over. If only `repo` differs, run `publish_mirror()` on the original, copy `repo` again and check again; if it still differs, set the status to read-only again and start over.

    ```python
    import hashlib
    from pathlib import Path

    def sha256(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    copy = Path("/path/to/the/copy")
    markers = [p.name for p in (copy / "repo.d").iterdir() if p.name.startswith("latest.")]
    assert len(markers) == 1, "start over"
    _, _, n, h = markers[0].split(".")
    assert sha256(copy / "repo.d" / f"{n}.{h}") == h, "start over"
    copied = icechunk.Repository.open(icechunk.filesystem_storage(str(copy)))
    assert copied.get_status().availability == icechunk.RepoAvailability.read_only, "start over"
    assert sha256(copy / "repo") == h, "publish the mirror, copy repo again"
    ```

4. Set the status of the original back to online.

The copy keeps the read-only status. On a filesystem, `filesystem_storage` can set it online and write to it. A copy on other storage, such as an object store, can be read through its mirror, but every modification is refused. To get a copy that can be modified on other storage:

1. Set the status of the original to read-only.
2. Convert the original to the standard layout with `convert_to_standard_layout()`, as described in [Switching an existing repo](#switching-an-existing-repo).
3. Copy it.
4. If the original should stay in the filesystem layout, convert it back with `convert_to_filesystem_layout()`.
5. Set the statuses of the original and the copy to online.

#### Switching an existing repo

Repositories created with `local_filesystem_storage`, another backend or an Icechunk version older than 2.4.0 are in the standard layout: `repo` is a single file. `filesystem_storage` refuses to open them, with an error that names `convert_to_filesystem_layout`, until they are converted once. [`FilesystemStorage.convert_to_standard_layout`](../reference/storage.md#icechunk.storage.FilesystemStorage.convert_to_standard_layout) converts a repository back.

!!! warning "Conversions are administrative operations"

    Run a conversion with nothing else using the repository: no writer in any process, no process still publishing the mirror after its change, and no other conversion. What happens otherwise is undefined. If a conversion is interrupted, run the same command again, under the same condition, before anything else uses the repository: the rerun completes it. Errors from an interrupted conversion name the conversion to rerun.

Both conversions change only the writer features and the ops log, and keep the repository's status, whatever it is. Neither copies or rewrites data.

To convert a repository to the filesystem layout:

1. Stop every writer of the repository.
2. Convert it with [`FilesystemStorage.convert_to_filesystem_layout`](../reference/storage.md#icechunk.storage.FilesystemStorage.convert_to_filesystem_layout). It returns `True` if it converted the repository and `False` if it was in the filesystem layout already. If it is interrupted, run it again.

    ```python
    storage = icechunk.filesystem_storage("/path/to/my/dataset")
    storage.convert_to_filesystem_layout()
    repo = icechunk.Repository.open(storage)
    ```

3. From then on, modify the repository only with `filesystem_storage`.

To convert it back to the standard layout, stop every writer and run `convert_to_standard_layout()`. It changes the repository to a version without the writer feature, publishes it as `repo`, then deletes `repo.d/`. It returns `True` if it converted the repository and `False` if it was in the standard layout already. A run interrupted before its change leaves the repository usable. If a run is interrupted after its change landed, while `repo.d/` still exists, the current version lacks the writer feature, and every change and garbage collection through `filesystem_storage`, and `publish_mirror()`, fail with a [`SessionStateError`](../reference/index.md#icechunk.SessionStateError) of kind `layout-conversion-in-progress` that says to rerun it. On NFS or FUSE, the conversion can also fail with a [`StorageError`](../reference/index.md#icechunk.StorageError) saying that directory listings are stale and that nothing was changed: run it again once they refresh. A rerun whose stale listing misses `repo.d/` returns `False` and leaves it in place: run it again once listings refresh, and it finishes, or reports a fork if `repo` was written meanwhile.

```python
storage = icechunk.filesystem_storage("/path/to/my/dataset")
storage.convert_to_standard_layout()
repo = icechunk.Repository.open(icechunk.local_filesystem_storage("/path/to/my/dataset"))
```

While a conversion runs, readers of `repo.d/` and readers of the mirror can see different versions of the repository.

Also note:

- Icechunk 1 repositories must first be migrated to Icechunk 2 with `local_filesystem_storage`. See [Migrate to 2.0](../reference/migration-2.md).
- A repository config that disables conditional writes only logs a harmless warning; see **Conditional writes** in [Requirements and limits](#requirements-and-limits-of-filesystem_storage).
- Pickled storage carries the absolute root path. With Dask or distributed, every worker must see the filesystem at the same path.

#### Forks

A _fork_ is a `repo` file that doesn't list the `filesystem-layout-v1` writer feature, next to a `repo.d/` that doesn't hold it (unrelated to [`Session.fork`](../reference/session.md#icechunk.session.Session.fork)). `repo` and `repo.d/` then hold diverging histories. To other backends and older Icechunk versions, the `repo` side is an ordinary repository in the standard layout: they can commit to it and collect garbage in it, and that garbage collection deletes files that only the `repo.d/` history references.

Forks come only from unsupported writes of `repo`:

- **Icechunk older than 2.4.0 changing the status.** Those versions can't modify the repository, except its status, and a status change removes the writer feature and the read-only marking from `repo`. Never change the status of a repository in the filesystem layout with Icechunk older than 2.4.0. Creating a repository at the same path with those versions and `check_clean_root=False` while `repo` is missing forks it too.
- **Writing `repo` by hand or with other tools**, such as copying a backup over it. Never write `repo` yourself: only `filesystem_storage` writes it.
- **Writes through other backends that skip the writer-feature check**, such as the Rust function `force_write_repo_info`.

From Icechunk 2.4.0, `local_filesystem_storage` refuses garbage collection and every write of `repo` while `repo.d/` exists, whatever `repo` holds, so it can neither fork a repository nor make a fork diverge further.

**Detection is best effort.** Before every publication of the mirror, and before the change of a conversion to the standard layout, `filesystem_storage` checks `repo`. A missing `repo`, a copy of a version in `repo.d/`, or a repository info that lists the writer feature, as a lagging mirror does, is not a fork. On a fork, nothing is published or deleted, and the operation raises:

- [`CommitDurableButRepositoryForkedError`](../reference/index.md#icechunk.CommitDurableButRepositoryForkedError) from a commit or a flush: it landed in `repo.d/` and is durable, so don't retry it. Its `snapshot_id` is the new snapshot.
- [`UpdateDurableButRepositoryForkedError`](../reference/index.md#icechunk.UpdateDurableButRepositoryForkedError) from other changes, such as branch, tag, configuration or status changes: the change is durable, so don't retry it.
- [`RepositoryForkedError`](../reference/index.md#icechunk.RepositoryForkedError) from `publish_mirror()` and from the conversions.
- Garbage collection raises `UpdateDurableButRepositoryForkedError` when the publication of its own change finds the fork, and `RepositoryForkedError` when the check before its deletes does. Either way, it deletes nothing.

Both durable errors are subclasses of `RepositoryForkedError`, so `except icechunk.RepositoryForkedError` catches every fork report.

Nothing checks before a change: a change made after a fork lands in `repo.d/`, and its publication reports the fork. A fork written between the check and the publication is overwritten without a report. A `repo` that can't be read or decoded after a few tries is never overwritten, but isn't called a fork either: garbage collection, the conversions and `publish_mirror()` fail with a [`StorageError`](../reference/index.md#icechunk.StorageError) of kind `mirror-unreadable`, and a change that landed logs an error.

A fork report doesn't always mean a fork:

- After an interrupted conversion to the standard layout, a stale directory listing on NFS or FUSE can make `publish_mirror()` or the conversion's rerun report a fork that doesn't exist. Nothing changes; running the operation again once listings refresh succeeds.
- If a conversion to the filesystem layout was interrupted after it put `repo.d/` in place, `repo` still holds the repository info in the standard layout. A writer that runs before the conversion's rerun gets a fork report, although its change is durable, and so does `convert_to_standard_layout()`, which changes nothing. Rerun `convert_to_filesystem_layout()` to complete the conversion. If more changes landed meanwhile than its check can follow, the rerun stops and recovery is manual.

**Recovery** is manual and keeps one of the two histories. First stop every process that uses the repository, older Icechunk versions included.

_Keep `repo.d/`_, dropping the changes made through the fork:

1. Check that no garbage collection or expiration ran on the fork side: Icechunk 2.0 to 2.3 garbage collection deletes objects that only `repo.d/` references. If one did, check that every object the current version in `repo.d/` references still exists; if some don't, keep `repo` instead.
2. Move `repo` out of the repository's root, and keep it until the recovery is done.
3. Run `publish_mirror()`. A missing `repo` is not a fork, so it publishes the current version.
4. The changes made through the fork are lost. Garbage collection deletes the objects only they referenced.

_Keep `repo`_, dropping the changes made through `filesystem_storage` after the fork:

1. Check that every object `repo` references exists: a mirror that lagged behind garbage collection can reference deleted objects.
2. Archive `repo.d/`, then delete it.
3. The repository is now in the standard layout. To return to the filesystem layout, convert it with `convert_to_filesystem_layout()`.

_Orphan mirror._ `filesystem_storage` refuses to open a repository whose `repo` lists the writer feature without a `repo.d/`, unless it is an interrupted creation, which only has `RepoInitializedUpdate` in its ops log: `repo.d/` was deleted or wasn't copied. Restore `repo.d/` if a copy exists. Otherwise create `repo.d/` holding a version `<N>.<h>` with the bytes of `repo`, where `N` is the current Unix time in milliseconds and `h` their SHA-256, and an empty marker `latest.<K>.<N>.<h>`, where `K` is `999999999999999 - N` in 15 digits. The changes made after the version the mirror held are lost.

```python
import hashlib
import time
from pathlib import Path

root = Path("/path/to/my/dataset")
data = (root / "repo").read_bytes()
n = time.time_ns() // 1_000_000
h = hashlib.sha256(data).hexdigest()
(root / "repo.d").mkdir()
(root / "repo.d" / f"{n}.{h}").write_bytes(data)
(root / "repo.d" / f"latest.{999999999999999 - n:015d}.{n}.{h}").touch()
```

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
