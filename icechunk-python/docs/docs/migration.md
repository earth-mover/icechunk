# Migration guide

## Parallel Writes

Icechunk is a stateful store and requires care when executing distributed writes.
Icechunk 1.0 introduces new API for safe [_coordinated_ distributed writes](./parallel.md#cooperative-distributed-writes) where a Session is distributed to remote workers:

1. Create a [`ForkSession`](./reference.md#icechunk.ForkSession) using [`Session.fork`](./reference.md#icechunk.Session.fork) instead of the [`Session.allow_pickling`](./reference.md#icechunk.Session.allow_pickling) context manager. ForkSessions can be pickled and written to in remote distributed workers.
1. Only Sessions with _no_ changes can be forked. You may need to insert commits in your current workflows.
1. [`Session.merge`](./reference.md#icechunk.Session.merge) can now merge multiple sessions, so the use of [`merge_sessions`](./reference.md#icechunk.distributed.merge_sessions) is discouraged.

The tabs below highlight typical code changes required:

=== "After"

    ```python {hl_lines="7 10 17"}
    from concurrent.futures import ProcessPoolExecutor


    session = repo.writable_session("main")
    with ProcessPoolExecutor() as executor:
        # obtain a writable session that can be pickled.
        fork = session.fork()
        # submit the writes, distribute `fork`
        futures = [
            executor.submit(write_timestamp, itime=i, session=fork)
            for i in range(ds.sizes["time"])
        ]
        # grab the Session objects from each individual write task
        remote_sessions = [f.result() for f in futures]

    # manually merge the remote sessions in to the local session
    session.merge(*remote_sessions)
    session.commit("finished writes")
    ```

=== "Before"

    ```python {hl_lines="2 7 10 17"}
    from concurrent.futures import ProcessPoolExecutor
    from icechunk.distributed import merge_sessions

    session = repo.writable_session("main")
    with ProcessPoolExecutor() as executor:
        # obtain a writable session that can be pickled.
        with session.allow_pickling():
            # submit the writes, distribute `session`
            futures = [
                executor.submit(write_timestamp, itime=i, session=session)
                for i in range(ds.sizes["time"])
            ]
            # grab the Session objects from each individual write task
            remote_sessions = [f.result() for f in futures]

        # manually merge the remote sessions in to the local session
        session = merge_sessions(session, *sessions)
        session.commit("finished writes")
    ```

## Virtual Datasets

Icechunk 1.0 gives the user more control over what virtual chunks can be resolved at runtime.
Virtual chunks are associated with a virtual chunk container based on their url. Each virtual
chunk container must declare its url prefix. In versions before 1.0, Icechunk had a list of
default virtual chunk containers. To give the user more control before Icechunk tries to resolve
virtual chunks, since version 1.0 Icechunk requires repository creators to explicitly declare their
virtual chunk containers, no defaults are provided.

You can follow this example to declare a virtual chunk container in your repo

```python
# The store defines the type of Storage that will be used for the virtual chunks
# other options are gcs_store, local_filesystem_store and http_store
store_config = s3_store(region="us-east-1")

# we create a container by giving it the url prefix and the store config
container = VirtualChunkContainer("s3://testbucket", store_config)

# we add it to the repo config
config = RepositoryConfig.default()
config.set_virtual_chunk_container(container)

# we set credentials for the virtual chunk container
# repo readers will also need this to be able to resolve the virtual chunks
credentials = containers_credentials(
    {
        # we identify for which container we are passing credentials
        # by using its url_prefx
        # If the value in the map is None, Icechunk will use the "natural" credential
        # type, usually fetching them from the process environment
        "s3://testbucket": s3_credentials(
            access_key_id="abcd", secret_access_key="0123"
        )
    }
)

# When we create the repo, its configuration will be saved to disk
# including its virtual chunk containers
repo = Repository.create(
    storage=...
    config=config,
    authorize_virtual_chunk_access=credentials,
)
```

In previous Icechunk versions, `Repository.create` and `Repository.open` had a
`virtual_chunk_credentials` argument. This argument is replaced by the new
`authorize_virtual_chunk_access`. If a container is not present in the
`authorize_virtual_chunk_access` dictionary, Icechunk will refuse to resolve
chunks matching its url prefix.
