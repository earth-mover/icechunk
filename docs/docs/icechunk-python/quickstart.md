# Quickstart

Icechunk is designed to be mostly in the background.
As a Python user, you'll mostly be interacting with Zarr.
If you're not familiar with Zarr, you may want to start with the [Zarr Tutorial](https://zarr.readthedocs.io/en/latest/tutorial.html)

## Installation

Install Icechunk with pip

```python
pip install icechunk 
```

!!! note

    Icechunk is currently designed to support the [Zarr V3 Specification](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html).
    Using it today requires installing the latest pre-release of Zarr Python 3.


## Create a new store

To get started, let's create a new Icechunk store.
We recommend creating your store on S3 to get the most out of Icechunk's cloud-native design.
However, you can also create a store on your local filesystem.

=== "S3 Storage"

    ```python
    storage_config = icechunk.StorageConfig.s3_from_env(
        bucket="icechunk-test",
        prefix="quickstart-demo-1"
    )
    store = icechunk.IcechunkStore.create(storage_config)
    ```

=== "Local Storage"

    ```python
    storage_config = icechunk.StorageConfig.filesystem("./icechunk-local")
    store = icechunk.IcechunkStore.create(storage_config)
    ```

## Write some data and commit

We can now use our Icechunk `store` with Zarr.
Let's first create a group and an array within it.

```python
group = zarr.group(store)
array = group.create("my_array", shape=10, dtype=int)
```

Now let's write some data

```python
array[:] = 1
```

Now let's commit our update

```python
store.commit("first commit")
```

🎉 Congratulations! You just made your first Icechunk snapshot.

## Make a second commit

Let's now put some new data into our array, overwriting the first five elements.

```python
array[:5] = 2
```

...and commit the changes

```python
store.commit("overwrite some values")
```

## Explore version history

We can see the full version history of our repo:

```python
hist = store.ancestry()
for anc in hist:
    print(anc.id, anc.message, anc.written_at)

# Output:
# AHC3TSP5ERXKTM4FCB5G overwrite some values 2024-10-14 14:07:27.328429+00:00
# Q492CAPV7SF3T1BC0AA0 first commit 2024-10-14 14:07:26.152193+00:00
# T7SMDT9C5DZ8MP83DNM0 Repository initialized 2024-10-14 14:07:22.338529+00:00
```

...and we can go back in time to the earlier version.

```python
# latest version
assert array[0] == 2
# check out earlier snapshot
store.checkout(snapshot_id=hist[1].id)
# verify data matches first version
assert array[0] == 1
```

---

That's it! You now know how to use Icechunk!
For your next step, dig deeper into [configuration](./configuration.md),
explore the [version control system](./version-control.md), or learn how to
[use Icechunk with Xarray](./xarray.md).