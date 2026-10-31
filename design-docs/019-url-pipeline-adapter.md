# `icechunk:` URL pipeline adapter (ZEP 8)

Issue: [#576](https://github.com/earth-mover/icechunk/issues/576)

Status: draft, implemented in [#1161](https://github.com/earth-mover/icechunk/pull/1161). Needs a zarr release with URL pipelines.

## Summary

[URL pipelines](https://github.com/jbms/url-pipeline) are a `|`-separated
syntax for addressing data inside nested containers, read outer to inner:

```text
s3://bucket/repo|icechunk://tag.v1/path/to/group|zarr3:
```

zarr-python adds the machinery in
[zarr-developers/zarr-python#4192](https://github.com/zarr-developers/zarr-python/pull/4192):
third-party packages register a `URLPipelineAdapter` under the
`zarr.url_adapters` entry-point group. This design adds Icechunk's adapter,
so that

```python
zarr.open_group(URLPipeline.from_url("s3://bucket/repo|icechunk://tag.v1/path/to/group"), mode="r")
xr.open_zarr(URLPipeline.from_url("file:///data/repo|icechunk:"))
```

open a read-only session on the selected ref and path. The same syntax is
already understood by TensorStore and Neuroglancer, so a URL can be passed
between tools.

The Rust crate does the parsing, the root-URL → `Storage` mapping and the
session opening (`icechunk::url_pipeline`). Python has a thin binding and the
zarr adapter class (`icechunk.zarr_url_adapter.IcechunkURLAdapter`).

This PR supersedes the earlier approach in #1161, a pure-Python adapter that
targeted a zarr API that no longer exists (`StoreAdapter`, `from_url_segment`,
the `zarr.stores` group), used the older `@ref` syntax, and parsed snapshot ids
as hex.

## Syntax

Per [`schemes/icechunk.md`](https://github.com/jbms/url-pipeline/blob/main/schemes/icechunk.md):

```abnf
icechunk-url    =  "icechunk" [ ":" icechunk-suffix ]
icechunk-suffix =  "//" icechunk-ref [ "/" icechunk-path ]
                /  [ "/" ] icechunk-path
icechunk-ref    =  %s"branch." branch-name / %s"tag." tag-name / snapshot-id
snapshot-id     =  20( b32char )   ; Crockford base32, uppercase
```

| Segment | Ref | Path |
|---|---|---|
| `icechunk`, `icechunk:`, `icechunk:/` | `branch.main` | root |
| `icechunk:a/b`, `icechunk:/a/b/` | `branch.main` | `a/b` |
| `icechunk://branch.dev/a` | branch `dev` | `a` |
| `icechunk://tag.v1/` | tag `v1` | root |
| `icechunk://FWWFQGAW742XMX0F5MF0/a` | that snapshot | `a` |

Details:

- `branch.` and `tag.` are case-sensitive. Anything else after `//` must be a
  snapshot id: exactly 20 characters from `0-9A-Z` minus `I L O U`. The
  `base32` decoder is lenient (lowercase, `I`→`1`, …), so the spelling is
  checked before decoding. A 20-character id carries 100 bits for a 96-bit
  id; ids whose 4 spare bits are set (the spec's own example
  `ABCDEFGHJKMNPQRSTVWX`) are accepted and decode to the canonical id.
- Branch and tag names are percent-decoded and must not contain `/`, even
  as `%2F`. Empty names are errors.
- The residual path is returned **without** percent-decoding and without
  leading or trailing `/`. This matches zarr-python, which does not decode
  escapes anywhere in a pipeline today (its follow-up F4). If zarr starts
  decoding, we should follow.
- A query string on the `icechunk:` segment is an error.

Neuroglancer implemented an earlier proposal, `icechunk:@ref/path`. We do
**not** accept it: `@` is a valid path character, so accepting both forms
makes `icechunk:@x` ambiguous, and the spec has settled on `//ref`.

## Root URL mapping

`icechunk:` is a *native* adapter: it does not call
`PipelineContext.resolve_preceding()` (Icechunk does not sit on a zarr
`Store`), so it owns validation of every preceding segment. Allowed: exactly
one root URL. zarr reads `zarr:`/`zarr2:`/`zarr3:` format segments itself and
accepts them only as the last segment, so they never reach the adapter.
zarr's parser also rejects schemeless and single-letter (Windows drive) roots
before any adapter runs. Any
other adapter before `icechunk:` is an error; for example
`x.zip|zip:|icechunk:` is out of scope for v1.

| Root URL | Storage |
|---|---|
| `file:/p`, `file:///p`, `file://localhost/p` | `new_local_filesystem_storage` |
| `memory:`, `memory:name`, `memory://name` | in-memory, shared per name (below) |
| `s3://bucket[/prefix]` | `new_s3_storage` |
| `s3+http(s)://endpoint/bucket[/prefix]` | `new_s3_storage`, `endpoint_url`, path-style, `allow_http` for `s3+http` |
| `s3+https://bucket.s3[.region].amazonaws.com[/prefix]`, `s3+https://s3[.region].amazonaws.com/bucket[/prefix]` | `new_s3_storage` on AWS (region from the host) |
| `gs://bucket[/prefix]` | `new_gcs_storage` |
| `http(s)://host[:port][/path]` | `new_http_storage`, read-only |

Anything else (including schemeless local paths, a zarr-python extension)
is a typed error that Python reports as `URLPipelineError`, unless an opener
is registered for the scheme (see "Repository openers").

- `file:` follows the spec: absolute paths only, empty or `localhost`
  authority only, no query.
- `memory:` storages are kept in a process-wide map keyed by name, so a
  repository created with `icechunk.storage_from_url("memory:x")` can be
  reopened by `memory:x|icechunk:`. Entries live for the life of the process.
  This namespace is separate from zarr's `memory:` root, and the semantics
  differ: zarr has one process-wide store and reads the body as a key prefix
  (`memory:a/b` is the prefix `a/b`), while Icechunk reads the whole body as
  the name (`memory:a/b` is a storage named `a/b`, unrelated to `memory:a`).
  Icechunk parses `context.preceding_url` itself, so zarr's rules for its own
  `file:` and `memory:` roots do not apply here.
- `s3+http(s):` is ambiguous in the spec: the endpoint may serve one bucket
  (`https://bucket.example.com/path`) or many (`https://example.com/bucket/path`),
  and implementations are asked to disambiguate, e.g. by trying a LIST both
  ways. S3 clients cannot address a bucket without its name, so we use the
  AWS hostname patterns when they match and otherwise assume the
  multi-bucket, path-style form. An endpoint with no path (`s3+http://host`)
  is an error, since there is no bucket.
- Azure has no scheme in the spec, so it is not mapped.

## Credentials and `storage_options`

The Rust side takes an explicit `UrlStorageOptions` (credentials, anonymous,
region, endpoint override) and never reads the environment itself; when no
credentials and no `anonymous` flag are given, S3 and GCS fall back to
`FromEnv`, the same default as `s3_storage()`/`gcs_storage()`. Credentials
must match the root (S3 credentials for `s3:`, GCS for `gs:`); `file:`,
`memory:` and `http(s):` take none.

The Python adapter accepts these `storage_options` keys, namespaced per the
zarr convention, and rejects every other key for built-in roots:

| Key | Meaning |
|---|---|
| `icechunk_credentials` | S3 or GCS credentials object |
| `icechunk_anonymous` | anonymous access |
| `icechunk_region`, `icechunk_endpoint_url` | S3 region / endpoint |
| `icechunk_config` | `RepositoryConfig` for `Repository.open` |
| `icechunk_virtual_chunk_credentials` | mapping passed to `authorize_virtual_chunk_access` |

fsspec-style keys (`anon`, `key`, `secret`, ...) are not accepted as aliases;
`token`, for one, means different things in s3fs and gcsfs.

Following zarr's adapter guidance, the adapter consumes only these keys, passes repository
openers a copy with every `icechunk_*` key removed, and never logs the options
or puts their values in error messages; errors name keys only.

Root URLs for built-in schemes take no query. Errors that quote a URL show
its query as `?…`, as zarr's do, since a query may carry a token (for example
a signed `https:` URL). Rust rejects root queries and redacts the URL in its
error.

This follows the review of #1161, which asked for credentials, including
virtual chunk container credentials, to come through `storage_options`
rather than being hardcoded to "from environment".

## Mode policy

zarr has no commit step, so the adapter cannot commit a writable session.

- `"r"`: a read-only session on the selected ref.
- Every other mode, including `"a"` (zarr's default for `open`/`open_group`)
  and `None` (`open_array`'s default): `URLPipelineError`, before any I/O,
  suggesting `mode="r"` and pointing to `repo.writable_session(branch)` +
  `session.commit(...)`. This follows zarr's guidance that adapters for
  read-only resources raise for every mode except `"r"`.

The Rust `open_from_url` opens read-only sessions only. Write support,
whichever form it takes, is out of scope:

- (b) Return a writable session and hand it to the caller through an
  `icechunk_session_callback` storage option so they can commit. Workable,
  but easy to misuse: an uncommitted session silently loses writes.
- (c) A commit hook in zarr or the spec. Needs a zarr design discussion.

## Repository openers

Arraylake (and similar services) want `arraylake://org/repo|icechunk://tag.v1/path`
to work without Icechunk depending on them. Openers are async callables
registered under the `icechunk.url_repository_openers` entry-point group,
named by root scheme (or via `register_url_repository_opener`):

```python
async def open_repository(
    root: PipelineSegment, *, read_only: bool, storage_options: dict
) -> icechunk.Repository: ...
```

The adapter validates the pipeline and mode, calls the opener with the root
segment and the non-`icechunk_*` storage options, optionally reopens the
repository with `icechunk_config`/`icechunk_virtual_chunk_credentials`, then
applies the `icechunk:` ref and path itself. The opener owns its credentials
and root-URL validation, must not block the loop, and namespaces its own
keys. Built-in schemes cannot be overridden; duplicate entry points warn
and the first wins. Entry points are scanned and loaded lazily, the first
time an unknown root scheme is used.

## Async and blocking

The adapter runs on zarr's IO event loop. Every I/O step is a
`future_into_py` awaitable: `_open_session_from_url_async` builds storage,
opens the repository and session in Rust, and returns `(session, path)`.
URL errors are raised synchronously, before any I/O. The existing
`PyStorage.new_in_memory`/`new_local_filesystem` constructors use
`block_on` and are not used. A test serves a repository over a deliberately
slow HTTP server and checks that other tasks keep running while it opens.

`session.store` is a Rust getter that takes `blocking_read()` on the session
lock. The session was created a moment earlier and nothing else holds it, so
the lock is uncontended and the call does no I/O. We consider that
acceptable; if it ever matters, the binding can return the store alongside
the session.

## zarr version dependency

The adapter needs `zarr.abc.url_pipeline` (zarr-python#4192, unreleased).
#4192 reads only `URLPipeline` objects as pipelines; plain pipeline strings
are left to a follow-on zarr PR.

- `icechunk/zarr_url_adapter.py` is the only module that imports it; it is
  not imported by `import icechunk`, and zarr only imports it when a pipeline
  names `icechunk:`. Older zarr never reads the entry-point group.
- `icechunk.storage_from_url` has no zarr dependency.
- The dev environment pins `zarr<3.2`, so the adapter tests skip there and
  the adapter module is excluded from the default mypy run. The
  `url-pipeline-adapter` job in `python-check.yaml` installs zarr `main`
  (pinned to `01fa6d98`) and runs mypy and the tests. Once a zarr release
  has URL pipelines: set a dependency floor (or keep the import guard), move
  the tests into the normal run, and drop the job.

## Out of scope

- Repositories inside other containers (`x.zip|zip:|icechunk:`): would need
  a `Storage` over a zarr store or a file-resource primitive.
- Writes (see "Mode policy").
- Neuroglancer's legacy `@ref` syntax (recommend not accepting it).
- `as_of` (time travel by date): the spec has no syntax for it.
- Azure roots, until the spec defines a scheme.
- Query parameters on roots (e.g. `?region=`): the spec leaves root queries
  to each scheme and none define them for these schemes.
