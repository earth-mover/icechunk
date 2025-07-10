# Icechunk for Git Users

While Icechunk does not work the same way as [git](https://git-scm.com/), it borrows from a lot of the same concepts. This guide will talk through the version control features of Icechunk from the perspective of a user that is familiar with git.

## Repositories

The main primitive in Icechunk is the [repository](reference.md#icechunk.Repository). Similar to git, the repository is the entry point for all operations and the source of truth for the data. However there are many important differences.

When developing with git, you will commonly have a local and remote copy of the repository. The local copy is where you do all of your work. The remote copy is where you push your changes when you are ready to share them with others. In Icechunk, there is not local or remote repository, but a single repository that typically exists in a cloud storage bucket. This means that every transaction is saved to the same repository that others may be working on. Icechunk uses the consistency guarantees from storage systems to provide strong consistency even when multiple users are working on the same repository.

## Working with branches

Icechunk has [branches](version-control.md#branches) similar to git.

### Creating a branch

In practice, this means the workflow is different from git. For instance, I wanted to make a new branch based on the `main` branch on my existing git repository and then commit my changes in git this is how I would do it:

```bash
# Assume currently on main branch
# create branch
git checkout -b my-new-branch
# stage changes
git add myfile.txt
# commit changes
git commit -m "My new branch"
# push to remote
git push origin -u my-new-branch
```

In Icechunk, you would do the following:

```python
# We create the branch
repo.create_branch("my-new-branch", repo.lookup_branch("main"))
# create a writable session
session = repo.writable_session("my-new-branch")
...  # make some changes
# commit the changes
session.commit("My new branch")
```

Two things to note:

1. When we create a branch, the branch is now available for any other instance of this `Repository` object. It is not a local branch, it is created in the repositories storage backend.
2. When we commit the changes are immediately visible to other users of the repository. There is not concept of a local commit, all snapshots happen in the storage backend.

### Checking out a branch

In git, you can check out a branch by using the `git checkout` command. Icechunk does not have the concept of checking out a branch, instead you create [`Session`s](reference.md#icechunk.Session) that are based on the tip of a branch.

We can either check out a branch for [read-only access](reference.md#icechunk.Repository.readonly_session) or for [read-write access](reference.md#icechunk.Repository.writable_session).

```python
# check out a branch for read-only access
session = repo.readonly_session(branch="my-new-branch")
# readonly_session accepts a branch name by default
session = repo.readonly_session("my-new-branch")
# check out a branch for read-write access
session = repo.writable_session("my-new-branch")
```

Once we have checked out a session, the [`store`](reference.md#icechunk.Session.store) method will return a [`Store`](reference.md#icechunk.Store) object that we can use to read and write data to the repository with `zarr`.

### Resetting a branch

In git, you can reset a branch to previous commit. Similarly, in Icechunk you can [reset a branch to a previous snapshot](reference.md#icechunk.Repository.reset_branch).

```python
# reset the branch to the previous snapshot
repo.reset_branch("my-new-branch", "198273178639187")
```

!!! warning
    This is a destructive operation. It will overwrite the branch reference with the snapshot immediately. It can only be undone by resetting the branch again.

At this point, the tip of the branch is now the snapshot `198273178639187` and any changes made to the branch will be based on this snapshot. This also means the history of the branch is now same as the ancestry of this snapshot.

### Branch History

In Icechunk, you can view the history of a branch by using the [`repo.ancestry()`](reference.md#icechunk.Repository.ancestry) command, similar to the `git log` command.

```python
[ancestor for ancestor in repo.ancestry(branch="my-new-branch")]

#[Snapshot(id='198273178639187', ...), ...]
```

### Listing branches

We can also [list all branches](reference.md#icechunk.Repository.list_branches) in the repository.

```python
repo.list_branches()

# ['main', 'my-new-branch']
```

You can also view the snapshot that a branch is based on by using the [`repo.lookup_branch()`](reference.md#icechunk.Repository.lookup_branch) command.

```python
repo.lookup_branch("my-new-branch")

# '198273178639187'
```

### Deleting a branch

You can delete a branch by using the [`repo.delete_branch()`](reference.md#icechunk.Repository.delete_branch) command.

```python
repo.delete_branch("my-new-branch")
```

## Working with tags

Icechunk [tags](version-control.md#tags) are also similar to git tags.

### Creating a tag

We [create a tag](reference.md#icechunk.Repository.create_tag) by providing a name and a snapshot id, similar to the `git tag` command.

```python
repo.create_tag("my-new-tag", "198273178639187")
```

Just like git tags, Icechunk tags are immutable and cannot be modified. They can however be [deleted like git tags](reference.md#icechunk.Repository.delete_tag):

```python
repo.delete_tag("my-new-tag")
```

However, unlike git tags once a tag is deleted it cannot be recreated. This will now raise an error:

```python
repo.create_tag("my-new-tag", "198273178639187")

# IcechunkError: Tag with name 'my-new-tag' already exists
```

### Listing tags

We can also [list all tags](reference.md#icechunk.Repository.list_tags) in the repository.

```python
repo.list_tags()

# ['my-new-tag']
```

### Viewing tag history

We can also view the history of a tag by using the [`repo.ancestry()`](reference.md#icechunk.Repository.ancestry) command.

```python
repo.ancestry(tag="my-new-tag")
```

This will return an iterator of snapshots that are ancestors of the tag. Similar to branches we can lookup the snapshot that a tag is based on by using the [`repo.lookup_tag()`](reference.md#icechunk.Repository.lookup_tag) command.

```python
repo.lookup_tag("my-new-tag")

# '198273178639187'
```

## Merging and Rebasing

Git supports merging and rebasing branches together. Icechunk currently does not support merging and rebasing branches together. It does support [rebasing sessions that share the same branch](version-control.md#conflict-resolution).
