# Conditional PUTs and lost responses

Icechunk's consistency story rests on conditional PUTs. Every write to a
ref, the repo config, or the repo info object is a compare-and-swap:
create-only writes send `If-None-Match: *`, updates send
`If-Match: <etag>` (or the generation equivalent). If the condition
fails, the caller learns it lost a race (`NotOnLatestVersion`) and can
rebase or report a conflict.

## The problem

Conditional PUTs are not idempotent under retry, and retries happen
below us: both the AWS SDK and `object_store` transparently re-issue
requests on transient failures. This is the failure sequence
(reported as [issue #2099](https://github.com/earth-mover/icechunk/issues/2099)):

1. We send a conditional PUT.
2. The server writes the object, but the success response never
   reaches us — the connection drops, a proxy returns 499, etc.
3. The retry layer re-sends the same request.
4. The retry's precondition fails **against the object we just wrote**.
5. A spurious `NotOnLatestVersion` surfaces to the user as a conflict
   that never happened: "tag already exists", "config was updated by
   other session", "repo info object was updated after this session
   started", commit parent mismatches.

Tigris is hit disproportionately (it produces many
response-interruption errors), but nothing here is Tigris-specific.

Two variants of the same issue:

* **Multipart**: `CompleteMultipartUpload` succeeds but the response is
  lost. The retried complete gets `NoSuchUpload` (the upload is
  finished, so its id is gone) — an error that looks fatal but can hide
  a success.
* **Exhausted retries** (`object_store`): if every retry's response is
  also cut, the failure surfaces as a generic transport error rather
  than a precondition failure.

## Design

We stamp every conditional PUT with a unique write-id and read the
object back when a failure might be hiding a landed write.

* Each conditional PUT carries user metadata
  `icechunk_write_id: <uuid4>`, generated once per logical write. The
  transparent retry layers re-send the same request, so the original
  attempt and its retries share the id. Unconditional PUTs are
  idempotent and carry no stamp. For multipart, the id is sent on
  `CreateMultipartUpload` and exposed on the final object.
* On a suspicious failure, we HEAD the object and classify:
  * the stored write-id is ours → our write landed; **success**, using
    the etag/generation returned by the HEAD;
  * a different or missing write-id, or no object at all → not ours;
  * our write-id but no usable version identity → error;
  * the HEAD itself fails → inconclusive; we return the HEAD error
    rather than guessing.

What "not ours" and "inconclusive" resolve to depends on which failure
triggered the read-back. There are two rules:

1. **Precondition failures** — the object provably exists, the only
   question is who wrote it: HTTP 412/409, `PreconditionFailed`,
   `ConditionalRequestConflict`, and `ConcurrentModification` (Ceph
   Object Gateway). Ours → success; not ours → a genuine race,
   `NotOnLatestVersion`.
1. **Lost-response-shaped failures** — our write may or may not have
   landed: `NoSuchUpload` (or a raw 404) from a multipart complete, and
   generic transport errors from `object_store`. Only a provably-ours
   object rescues the operation; anything else propagates the original
   error so the caller can retry.

Rule 1 may convert the failure into an answer either way; rule 2 must
never convert a real failure into a conflict or a success it can't
prove.

The classification and resolution logic lives in one shared module
(`icechunk_storage::readback`) used by both storage implementations:
the native S3 backend (single-PUT and multipart paths) and the
`object_store` backend, which covers S3, GCS, Azure and HTTP.

## Alternatives considered

* **HEAD and compare the etag** (suggested in the issue). There is no
  etag to compare: the response carrying it was lost, and we cannot
  compute it client-side — multipart etags are not content hashes, and
  server-side encryption changes etags.
* **Retry with `If-Match` instead of `If-None-Match`**. The retries are
  issued by the SDK's transparent retry layer, which re-sends the same
  request; and again, we would not know which etag to match.
* **Treat a precondition failure after a retry as success.** Masks
  exactly the genuine concurrent-writer races that conditional PUTs
  exist to catch.

## Caveats

* **Recovery requires user metadata.** With `unsafe_use_metadata`
  disabled, conditional PUTs still work but carry no write-id, so a
  lost response surfaces as a spurious conflict again. We log a
  one-time warning for this combination. Backends with no
  user-metadata support simply keep the old behavior.
* **Recovered versions are fresh.** On recovery we return the version
  identity read back from the HEAD, not anything remembered from the
  failed PUT, so follow-on conditional updates operate on the current
  etag/generation.
* **Overwrite race.** If another writer overwrites the object between
  our landed PUT and our HEAD, the write-id no longer matches and we
  report `NotOnLatestVersion`. That is correct: our version is stale
  either way.
* **Identical bodies.** Recovery assumes all retries of a write send
  the same body. True for transparent SDK retries, which are the only
  retries in play below this layer.
* **Key naming.** The key uses underscores (`icechunk_write_id`)
  because Azure requires metadata names to be valid C# identifiers;
  S3 additionally lowercases metadata keys.

## Testing

Two complementary strategies:

* **Deterministic** (against rustfs): seed an object whose metadata
  carries a known write-id, then force the next conditional create to
  reuse that id via a test-only injection hook. The 412 triggers the
  read-back, which must recover success and return a fresh etag —
  verified by a follow-on conditional update. Covers the single-PUT
  and multipart paths.
* **Network-level** (toxiproxy): a `limit_data` toxic cuts the PUT
  response mid-flight during repository creation, modelling the real
  lost-ack sequence end to end for both storage backends. A long-blip
  variant also cuts every retry's response, landing the failure on the
  generic-transport-error rule.

## Future work

* Conditional-PUT settings could be coupled to metadata support so the
  metadata-off footgun cannot be configured at all.
* The `object_store` write path does not use multipart uploads yet;
  when it does, the multipart complete will need the same treatment as
  the native S3 backend.
