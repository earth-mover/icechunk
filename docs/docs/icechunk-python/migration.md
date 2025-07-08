# Migration guide

## Parallel Writes

Icechunk is a stateful store and requires care when executing distributed writes.
Icechunk 1.0 introduces new API for safe [_coordinated_ distributed writes](./parallel.md#cooperative-distributed-writes) where a Session is distributed to remote workers:

1. Create a [`ForkSession`](./reference.md#icechunk.session.ForkSession) using [`Session.fork`](./reference.md#icechunk.Session.fork) instead of the [`Session.allow_pickling`](./reference.md#icechunk.Session.allow_pickling) context manager. ForkSessions can be pickled and written to in remote distributed workers.
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
