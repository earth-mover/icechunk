# Migration guide

## Parallel Writes

Icechunk is a stateful store and requires care when executing distributed writes.
Icechunk 1.0 introduces new API for safe distributed writes:
1. Use [`Session.fork`](./reference.md#icechunk.Session.fork) instead of [`Session.allow_pickling`](./reference.md#icechunk.Session.allow_pickling).
2. [`Session.merge`](./reference.md#icechunk.Session.merge) can now merge multiple sessions, so the use of [`merge_sessions`](./reference.md#icechunk.distributed.merge_sessions) is discouraged.

```{important}
Code that uses `to_icechunk` does _not_ need changes.
```

=== After

    ```python {hl_lines="5 7 16"}
    from concurrent.futures import ProcessPoolExecutor

    session = repo.writable_session("main")
    with ProcessPoolExecutor() as executor:
        # obtain a writable session that can be pickled.
        fork = session.fork()
        # submit the writes
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

=== Before

    ```python {hl_lines="5 7 16"}
    from concurrent.futures import ProcessPoolExecutor
    from icechunk.distributed import merge_sessions

    session = repo.writable_session("main")
    with ProcessPoolExecutor() as executor:
        # obtain a writable session that can be pickled.
        with session.allow_pickling():
            # submit the writes
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
