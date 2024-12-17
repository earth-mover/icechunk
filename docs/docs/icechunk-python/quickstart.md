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


## Create a new Icechunk repository

To get started, let's create a new Icechunk repository.
We recommend creating your repo on S3 to get the most out of Icechunk's cloud-native design.
However, you can also create a repo on your local filesystem.

=== "S3 Storage"

    ```python
    storage_config = icechunk.StorageConfig.s3_from_env(
        bucket="icechunk-test",
        prefix="quickstart-demo-1"
    )
    repo = icechunk.Repository.create(storage_config)
    ```

=== "Local Storage"

    ```python
    storage_config = icechunk.StorageConfig.filesystem("./icechunk-local")
    repo = icechunk.Repository.create(storage_config)
    ```

## Accessing the Icechunk store

Once the repository is created, we can use `Session`s to read and write data. Since there is no data in the repository yet,
let's create a writable session on the default `main` branch.

```python
session = repo.writable_session("main")
```

Now that we have a session, we can access the `IcechunkStore` from it to interact with the underlying data using `zarr`:

```python
store = session.store()
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

Now let's commit our update using the session

```python
session.commit("first commit")
```

🎉 Congratulations! You just made your first Icechunk snapshot.

!!! note

    Once a writable `Session` has been successfully committed to, it becomes read only to ensure that all writing is done explicitly.


## Make a second commit

At this point, we have already committed using our session, so we need to get a new session and store to make more changes.

```python
session_2 = repo.writable_session("main")
store_2 = session_2.store()
group = zarr.open_group(store_2)
array = group["my_array"]
```

Let's now put some new data into our array, overwriting the first five elements.

```python
array[:5] = 2
```

...and commit the changes

```python
snapshot_id_2 = session_2.commit("overwrite some values")
```

## Explore version history

We can see the full version history of our repo:

```python
hist = repo.ancestry(snapshot_id_2)
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
earlier_session = repo.readonly_session(snapshot_id=hist[1].id)
store = earlier_session.store()

# get the array
group = zarr.open_group(store)
array = group["my_array]

# verify data matches first version
assert array[0] == 1
```

---

That's it! You now know how to use Icechunk!
For your next step, dig deeper into [configuration](./configuration.md),
explore the [version control system](./version-control.md), or learn how to
[use Icechunk with Xarray](./xarray.md).
