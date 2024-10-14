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
    # TODO
    ```

=== "Local Storage"

    ```python
    # TODO
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
# TODO: update when we change the API to be async
await store.commit("first commit")
```

🎉 Congratulations! You just made your first Icechunk snapshot.

## Make a second commit

Let's now put some new data into our array, overwriting the first five elements.

```python
array[:5] = 2
```

...and commit the changes

```
await store.commit("overwrite some values")
```

### Explore version history

We can now see both versions of our array

```python
# TODO: use ancestors
```