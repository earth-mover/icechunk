"""Demo: how `obstore.list(store, prefix=, offset=)` lets us list a
specific slab of a zarr chunk grid without scanning everything.

Run:
    uv run --with obstore python /tmp/list_with_offset_demo.py

KEY FINDING this demo is built around:

  `prefix` in obstore (and the underlying object_store crate) is a
  **PATH prefix**, not a raw string prefix. It splits on `/` and
  matches at directory boundaries. This means:

  * For zarr keys with the v3 DEFAULT separator `/` (i.e., `c/0/0/0`,
    `c/0/0/1`, …), `prefix='c/0/'` works exactly as you'd expect —
    you get every chunk whose first axis index is 0.

  * For zarr keys with the `.` separator (i.e., `c/0.0.0`,
    `c/0.0.1`, …), `prefix='c/0.'` returns NOTHING. There's no
    directory `c/0.`; everything lives under `c/`.

  Implications for region-listing in the ingest:
  - With `/` separator: `prefix` is a clean per-axis filter — listing
    cost is O(returned keys).
  - With `.` separator: you can't slice axes via prefix at all.
    You'd have to fall back to listing under `c/` and either filtering
    client-side, or using `offset` alone with cursor-style traversal.
"""

from __future__ import annotations

import obstore
import obstore.store


def show(label: str, store, **kwargs) -> None:
    print(f"\n--- {label} ---")
    print(f"  list args: {kwargs}")
    listed: list[str] = []
    for batch in obstore.list(store, **kwargs):
        for meta in batch:
            listed.append(meta["path"])
    if not listed:
        print("    (no results)")
    for path in listed:
        print(f"    {path}")


# ============================================================================
# CASE A: zarr v3 with `/` separator (the default). prefix-as-path works.
# ============================================================================

print("=" * 68)
print("CASE A: chunk keys use '/' separator (zarr v3 default)")
print("=" * 68)

store_a = obstore.store.MemoryStore()
for k in [
    "zarr.json",
    "c/0/0/0",
    "c/0/0/1",
    "c/0/1/0",
    "c/0/1/1",
    "c/1/0/0",
    "c/1/0/1",
    "c/1/1/0",
    "c/1/1/1",
]:
    obstore.put(store_a, k, b"x")

show("A1. all chunks (prefix='c/')", store_a, prefix="c/")
show("A2. axis-0 = 0 plane (prefix='c/0/') — 4 chunks", store_a, prefix="c/0/")
show(
    "A3. axis-0 = 0, axis-1 = 1 row (prefix='c/0/1/') — 2 chunks",
    store_a,
    prefix="c/0/1/",
)

# offset is exclusive: keys > offset are returned. Combine with prefix to
# resume mid-axis.
show(
    "A4. axis-0 = 0, skip first row (prefix='c/0/', offset='c/0/0/\\xff')",
    store_a,
    prefix="c/0/",
    offset="c/0/0/\xff",
)

# ============================================================================
# CASE B: zarr v3 with `.` separator. prefix-as-path DOES NOT slice axes.
# ============================================================================

print()
print("=" * 68)
print("CASE B: chunk keys use '.' separator")
print("=" * 68)

store_b = obstore.store.MemoryStore()
for k in [
    "zarr.json",
    "c/0.0.0",
    "c/0.0.1",
    "c/0.1.0",
    "c/0.1.1",
    "c/1.0.0",
    "c/1.0.1",
    "c/1.1.0",
    "c/1.1.1",
]:
    obstore.put(store_b, k, b"x")

show("B1. all chunks (prefix='c/') — works the same as case A", store_b, prefix="c/")
show("B2. tries axis-0 = 0 via prefix='c/0.' — RETURNS NOTHING", store_b, prefix="c/0.")

# Workaround: list under c/ and use offset to start at the right place.
# offset is a STRING comparison in this layer, so cursor traversal works.
show(
    "B3. workaround: prefix='c/' + offset='c/0.0.\\xff' "
    "(skips c/0.0.*, returns c/0.1.* and after)",
    store_b,
    prefix="c/",
    offset="c/0.0.\xff",
)

# To stop at a region boundary, the consumer has to break out of the loop
# when keys no longer match the desired axis-0/axis-1. That's client-side
# filtering, but you only paid for keys you actually consumed.
print()
print("--- B4. consumer-side stop on prefix='c/0.1.' boundary ---")
print(
    "  list args: prefix='c/', offset='c/0.0.\\xff', stop when key not "
    "starting with 'c/0.1.'"
)
listed: list[str] = []
for batch in obstore.list(store_b, prefix="c/", offset="c/0.0.\xff"):
    done = False
    for meta in batch:
        path = meta["path"]
        if not path.startswith("c/0.1."):
            done = True
            break
        listed.append(path)
    if done:
        break
for p in listed:
    print(f"    {p}")


# ============================================================================
# Takeaway summary
# ============================================================================

# ============================================================================
# CASE C: list_with_delimiter — directory-style listing
# ============================================================================
#
# `obstore.list_with_delimiter` returns the immediate "directory entries"
# at a prefix: both leaf objects and "common prefixes" (subdirectories).
# This is useful for walking a zarr hierarchy one level at a time without
# pulling all descendants.

print()
print("=" * 68)
print("CASE C: list_with_delimiter on the '/' store from Case A")
print("=" * 68)

# At prefix='c/' we expect to see two common prefixes (c/0/ and c/1/) and
# zero direct objects (chunks are nested another level).
result = obstore.list_with_delimiter(store_a, prefix="c/")
print()
print("--- C1. list_with_delimiter(prefix='c/') ---")
print("  objects:")
for obj in result["objects"]:
    print(f"    {obj['path']}")
print("  common prefixes:")
for p in result["common_prefixes"]:
    print(f"    {p}")

# At prefix='c/0/1/' the two leaves are direct children, no common prefixes.
result = obstore.list_with_delimiter(store_a, prefix="c/0/1/")
print()
print("--- C2. list_with_delimiter(prefix='c/0/1/') ---")
print("  objects:")
for obj in result["objects"]:
    print(f"    {obj['path']}")
print("  common prefixes:")
for p in result["common_prefixes"]:
    print(f"    {p}")


# ============================================================================
# Note: PaginatedListStore (Rust-only)
# ============================================================================

print()
print("=" * 68)
print("PaginatedListStore (Rust trait — not all knobs are exposed in Python)")
print("=" * 68)
print("""
The Rust object_store crate exposes a lower-level `PaginatedListStore`
trait with `list_paginated(prefix, opts: PaginatedListOptions)`. Knobs:

  * prefix        — same as obstore.list, path prefix
  * offset        — same as obstore.list (Option<String>, exclusive)
  * delimiter     — same as list_with_delimiter, but pageable
  * max_keys      — SERVER-side cap on returned paths.  ★
  * page_token    — explicit pagination cursor for stateless resume. ★

The starred items are NOT exposed in obstore Python's `list()` today.
They'd be useful for:

  * `max_keys`: server-side cap means "give me exactly N keys for this
    region" without paying for any beyond. Works in tandem with offset
    for sliding-window listings. Today we approximate with a client-side
    `take` and break out early; that pays for at most one extra page.

  * `page_token`: a cursor that survives process boundaries. Useful for
    distributed ingest where workers each need to pick up where another
    left off without re-listing. We don't have a distributed-ingest
    story yet; if we add one, exposing `page_token` to Python would be
    on the path.

Backends: AmazonS3, AzureBlob, GoogleCloud only. NOT local filesystem
or memory store. So even if Python exposed it, our LocalStore-based
tests couldn't exercise it.

Reference: https://docs.rs/object_store/latest/object_store/list/trait.PaginatedListStore.html
""")


# ============================================================================
# Takeaway summary
# ============================================================================

print("=" * 68)
print("Summary")
print("=" * 68)
print("""
- `prefix` is a PATH (directory) prefix. It splits on '/' only.
- With '/' separator chunk keys (zarr v3 default): you get clean
  per-axis slicing with prefix='c/i/' or 'c/i/j/' etc. Each call is
  O(returned keys) at the server.
- With '.' separator chunk keys: prefix slicing collapses to just
  prefix='c/'. Slabs of a region must be done with `offset=` plus
  consumer-side early stop, or by listing the whole `c/` subtree
  once and filtering client-side.
- `offset` is exclusive — keys k > offset are returned. Strings are
  compared lexicographically; "c/0.0.\\xff" works as a "just past
  everything starting with c/0.0." marker.
- `list_with_delimiter` returns immediate-children only (objects +
  common-prefix subdirectories), good for walking a hierarchy.
- The Rust `PaginatedListStore` trait offers `max_keys` and
  `page_token` for tighter server-side caps and stateless resume; not
  exposed in obstore Python today.

For a region-aware ingest, the implementation strategy depends on the
source's `chunk_key_encoding` separator:
  - '/' (default): prefix-walked axis iteration, near-optimal.
  - '.': single `c/`-prefixed list + client-side filter, or a sequence
    of offset/take cursor walks with break-on-region-exit.
""")
