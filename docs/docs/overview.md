---
title: Overview
---
# Icechunk

Icechunk is an open-source [transactional](concepts.md#transactions) [storage engine](concepts.md#what-is-icechunk) for [Zarr](https://zarr.dev/). Icechunk enables using Zarr as a true database for array data.

[Get started with the Quickstart →](quickstart.md)

## Why Icechunk?

Zarr is a [cloud-optimized](https://earthmover.io/blog/fundamentals-what-is-cloud-optimized-scientific-data/) format for multidimensional arrays that is a significant advance over legacy formats like NetCDF, GRIB, and TIFF. However, Zarr has three critical limitations:

1. **[Safety](#safety)**: You can accidentally delete or corrupt your data in an unrecoverable way
2. **[Consistency](#consistency)**: Concurrent readers and writers can see partial, inconsistent data
3. **[Reproducibility](#reproducibility)**: Data modifications happen silently with no version tracking

These limitations mean Zarr cannot be reliably used as a database—a critical capability for modern data-intensive workflows like weather forecasting, climate modeling, and geospatial analysis where multiple teams need to safely read and update shared datasets.

Icechunk provides git-like [snapshots](concepts.md#snapshots) combined with [ACID transactions](concepts.md#acid-transactions). This provides:

- **[Safety](#safety)**: Recover from corrupted or accidentally deleted data using version history
- **[Consistency](#consistency)**: Ensure readers always see complete, valid snapshots—never partial writes
- **[Reproducibility](#reproducibility)**: Reference any version of your data permanently via commits or tags

Every write operation creates an immutable snapshot, while [branches and tags](concepts.md#branches-and-tags) provide familiar version control semantics.

```python
# Write and commit atomically - readers never see partial updates
with repo.transaction("main", message="Add new forecast") as store:
    group = zarr.open_group(store)
    group["temperature"][:] = new_temp_data
    group["pressure"][:] = new_pressure_data
    # Both arrays updated together or not at all

# Time travel to any previous version
historical_session = repo.readonly_session(snapshot_id="abc123")

# Reproducible analysis with permanent references
repo.create_tag("v1.0-release", snapshot_id="abc123")  # Immutable reference
session = repo.readonly_session(tag="v1.0-release")
```

Learn more about [Icechunk's core concepts](concepts.md).

### Safety

No matter how careful you are, it is always possible to unintentionally write to the wrong chunk or update an existing chunk with corrupted data.

=== "❌ Zarr (Unrecoverable)"

    ```python
    import zarr

    # Open existing group
    root = zarr.open_group("my_data", mode="a")

    # Accidentally overwrite critical data
    root["temperature"][:] = corrupted_data

    # Data is permanently lost! 💥
    # No way to recover the original values
    ```

=== "✅ Icechunk (Recoverable)"

    ```python
    import icechunk, zarr
    repo = icechunk.Repository.open(storage)

    with repo.transaction("main", message="Update temp") as store:
        root = zarr.open_group(store)
        root["temperature"][:] = corrupted_data

    # Oh no! The data was corrupted. But we can recover:
    previous_snapshot = list(repo.ancestry("main"))[1].id
    repo.reset_branch("main", previous_snapshot)
    # Data restored! ✅
    ```

With **Icechunk**, you cannot permanently destroy or corrupt your data. If you commit bad data, you can always revert using version history.

### Consistency

When working with the output of a computational model, such as a weather forecast model, variables must be internally consistent. For example, if the pressure variable has one more time step than the air temperature variable, then analysis pipelines will break.

This situation can occur with the output of a weather model using Zarr. When the process is writing multiple variables, each with its own array, they will never be written at exactly the same time. So any reader that accesses during this process will get inconsistent data.

This is a serious problem as weather model writes are large enough that there can be disparities in timing. Furthermore, readers will try to read the data as soon as it comes out for latency-critical applications such as emergency weather monitoring and trading off of weather forecasts.

=== "❌ Zarr (Inconsistent Reads)"

    ```python
    # Writer process
    forecast["temperature"][:, new_time] = temp_data  # Written
    forecast["pressure"][:, new_time] = press_data    # Written
    # ... still writing ...

    # Reader process running at the same time
    temp = forecast["temperature"][:, new_time]   # ✓ Gets new data
    pressure = forecast["pressure"][:, new_time]  # ✓ Gets new data
    humidity = forecast["humidity"][:, new_time]  # ✗ Gets old data!

    # Analysis breaks: variables are from different time snapshots
    ```

=== "✅ Icechunk (Always Consistent)"

    ```python
    # Writer process
    with repo.transaction("main", message="Add forecast") as store:
        forecast = zarr.open_group(store)
        forecast["temperature"][:, new_time] = temp_data
        forecast["pressure"][:, new_time] = press_data
        forecast["humidity"][:, new_time] = humid_data
    # All variables updated atomically

    # Reader process (concurrent with writer)
    session = repo.readonly_session("main")
    forecast = zarr.open_group(session.store)
    # Always sees a complete, consistent snapshot
    # Either all old values OR all new values, never mixed
    ```

There are other consistency issues with Zarr that you can read about in more detail in the [Multi-Player Mode](https://earthmover.io/blog/multi-player-mode-why-teams-that-use-zarr-need-icechunk) blog post.

In contrast, **Icechunk** is, by design, always consistent. It implements ACID transactions, which ensures you will never run into these issues.

### Reproducibility

Weather forecast datasets not only regularly add new timepoints, but also backfill and fix errors in prior timepoints. **Zarr** has no mechanism for tracking when the data was last updated. This creates a potential reproducibility crisis for any analysis based on the data. For example, an insurance company must be able to precisely reproduce historical pricing. If they rely on a public zarr dataset, then they have no guarantees that the data is the same as they used, unless they copy the entire (possibly petabyte-large) data and store it in perpetuity.

=== "❌ Zarr (No Version Tracking)"

    ```python
    # January: Run analysis for paper
    forecast = zarr.open_group("s3://public-weather/forecast")
    results_jan = analyze(forecast["temperature"][:])
    # Published results based on this data

    # March: Try to reproduce results
    forecast = zarr.open_group("s3://public-weather/forecast")
    results_mar = analyze(forecast["temperature"][:])
    # Data was updated! Results don't match!
    # No way to know what changed or get back to January's data
    ```

=== "✅ Icechunk (Permanent References)"

    ```python
    # January: Run analysis using specific tagged version
    repo = icechunk.Repository.open(
        icechunk.s3_storage(bucket="public-weather", prefix="forecast")
    )
    session = repo.readonly_session(tag="2025-01-15")
    forecast = zarr.open_group(session.store)
    results_jan = analyze(forecast["temperature"][:])
    # Record in paper: "Analysis used tag 2025-01-15"

    # March: Reproduce results using same tagged version
    session = repo.readonly_session(tag="2025-01-15")
    forecast = zarr.open_group(session.store)
    results_mar = analyze(forecast["temperature"][:])
    # Exact same data, guaranteed reproducible results! ✓
    ```

**Icechunk** requires a commit for every change to the data. This allows referencing an immutable version of the data by its commit, or a tag. Thereby making any analysis, even based on a dataset you don't control, permanently reproducible.

### Storage Space

But will this version history balloon storage costs? **No** it will not. Icechunk is storage-optimal as it only stores the chunks that were modified with each new commit. If maintaining the full history is too much, you can expire old commits and branches via Icechunk's built-in [garbage collection](https://earthmover.io/blog/everything-you-need-to-know-about-icechunk-garbage-collection).

## Next Steps

Ready to use Icechunk?

- **[Quickstart](quickstart.md)** - Get up and running with Icechunk in 5 minutes
- **[Core Concepts](concepts.md)** - Understand transactions, snapshots, branches, and tags
- **[Configuration](configuration.md)** - Set up storage backends and tune performance
