# Transactions and Version Control

Icechunk was designed to solve several interrelated problems typically encountered
by teams working with Zarr data in cloud object storage (e.g. AWS S3):
- challenges making safe, consistent updates to active Zarr groups and arrays
- difficulty rolling back changes in the case of a failed or interrupted write operation
- no easy way to manage multiple related versions of Zarr data

To understand how Icechunk solves these problems, we need to first review how Zarr storage works.

## How Zarr Stores Data

[Zarr V3](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html) works by storing both metadata and chunk data into a physical storage device
according to a specified system of "keys".
For example, a Zarr array called `myarray`,  within a group called `mygroup`, might generate
the following keys in the storage device:

```
# metadata for mygroup
mygroup/zarr.json
# metadata for myarray
mygroup/myarray/zarr.json
# chunks of data
mygroup/myarray/c/0/0
mygroup/myarray/c/0/1
```

In standard Zarr usage, these keys are filenames in a filesystem or object keys in an object storage system.

When writing data, a Zarr implementation will create these keys and populate them with data.
When modifying existing arrays or groups, a Zarr implementation will potentially overwrite existing keys with new data.

:::tip
An important point is that **the state of a Zarr dataset is spread over many different keys**, both metadata and chunks.
:::

This is generally not a problem, as long there is only one person or process coordinating access to the data.
However, when multiple uncoordinated readers and writers attempt to access the same Zarr data at the same time,
consistency problems emerge.
These consistency problems can occur in both file storage and object storage; they are particularly severe in
a cloud setting where Zarr is being used as an active store for data that are frequently changed while also
being read.

## Consistency Problems with Zarr

In these examples, we refer to independent, uncoordinated processes as different _clients_.
- Client A reads from an array while Client B is in the process of overwriting chunks.
  It's ambiguous which data Client A will see.
- Client A and Client B both attempt to modify array contents in different ways.
  Some of A's modifications will overwrite B's, and vice versa, depending on the exact timing of the updates.
  There is no way to guarantee that the resulting data are correct.
- Client A is writing to an array while client B is resizing the same array to a smaller size.
  Some of client A's modifications will be lost.
- A more complex example, involving consistency across multiple arrays:
  - Client A reads the contents of array `foo` and then writes certain data to array `bar` which are dependent
    on what was found in `foo`.
  - While Client A is writing `bar`, Client B modifies `foo`.
  - The changes client A made to `bar` are now no longer correct.

All of these problems arise from the fact that the state of the data is distributed over multiple keys within
the store, each of which can be modified independently by any process with write access to the store.
While most popular object storage services offer some consistency guarantees around access to a specific key
(e.g. [AWS S3 consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)),
these do not extend to operations that involve multiple keys.

## Icechunk is a Database for Zarr

These problems can all be solved by using well-established ideas from databases--and this is exactly what Icechunk does.
The theory of Database management systems (DMBS) defines specific [isolation levels](https://www.geeksforgeeks.org/transaction-isolation-levels-dbms/)
which explain how the actions of one client interact with another.
Viewed as a DBMS for array data, Standard Zarr in files storage or object storage offers the lowest level of isolation ("read uncommitted", which means that one client may read uncommitted changes made by another client)
and no guarantees around atomicity and consistency.

You can think of Icechunk as a key-value database specialized to the Zarr data model which provides
**atomicity**, **consistency** and **serializable isolation** for transactions over multiple keys.
Icechunk is designed to be light weight and cloud-native, relying on object storage for the actual data (chunks)
and a lower-latency service for the metadata (which are generally very small compared to the chunks).
This allows Icechunk to leverage the cloud-scale performance of object storage for scale-out workloads
(clients access chunk data directly from the object store), while also solving the consistency problems
outlined above.

The design of Icechunk was loosely inspired by [Apache Iceberg](https://iceberg.apache.org/);
readers familiar with Iceberg will note similar concepts in what follows.
In particular, the use of a metastore database or catalog (in addition to the object store) is required to provide
transactional guarantees in both systems.

Icechunk is designed to support analytical workloads (OLAP) over transactional workloads (OLTP).
In particular, the design assumes the following:

- Reading will happen much more frequently than writing.
- Data ingestion will occur via relatively infrequent batch jobs.
- Throughput is more important than latency.

### Commits and Branches

Rather than allowing independent clients to modify the Zarr keys directly (which leads to consistency problems),
Icechunk packages a series of changes within a repo into a single unit called a **commit**.
Each commit is identified by a unique ID, the `commit_id`, and is associated with an exact timestamp
for when the commit was made. The `commit_id` is a string generated by Icechunk.
Each commit also records its **parent commit**, a pointer to the commit that came before.
The commit history allows all clients to agree on the order in which changes are made for each repo.

The `commit_id` can be considered an immutable identifier for the state of the repo.
Multiple clients can check out any commit and are guaranteed to always see the exact same data.
This is a very important property when building systems that require verification and auditability.
Such guarantees are impossible when using file storage or object storage, since keys in both systems are mutable.

To keep track of the "current" version of each repo, Icechunk has **branches**.
A branch is a mutable pointer to a commit.
By default, each repo has a single branch called `main`.
As the state of the repo evolves over time, the branch pointer is moved to newer commits.

:::caution
Support for multiple branches in Icechunk is currently experimental.
Specifically, we don't yet implement a merge operation, limiting the utility of having multiple branches.
For production systems, we recommend sticking with a single branch until this feature set matures.
:::

The relationship between commits, branches, and tags is illustrated below.

```mermaid
flowchart TD

		subgraph commits
    2bd5abcd --> 9218a4d7
		9886524e --> 2bd5abcd
		069579d0 --> 9886524e
		3b27b2ab --> 069579d0
		033268a2 --> 9886524e
		d37b2363 --> 033268a2
    adf25293 --> d37b2363
    3b4f0aa6 --> d37b2363
		end

		subgraph branches
    main -.-> 3b27b2ab
    staging -.-> adf25293
    dev -.-> 3b4f0aa6
		end

		subgraph tags
    2022-01 -.-> 9886524e
    2022-02 -.-> 069579d0
		end

```

### Sessions, Transactions and Isolation

A key concept required to solve the consistency and isolation problems is the idea of a **session**.
From a DBMS point of view, operations that occur within a session are part of a single transaction.

Whenever a client connects to an Icechunk repo, it initiates a session. This session records:
- The most recent latest `commit_id` for the active branch. This is called the **base commit**.
  This will be used as a the parent commit for the next commit.
- A new, unique `session_id` for the session.
- An `expiration` timestamp, at which point the session will be automatically
  closed for writing. Session expiration is configurable. If not explicitly
  specified, a session will last 24 hours by default, but can be set to expire
  after any duration up to seven (7) days.

When writing new keys or overwriting existing keys, Icechunk uses a _copy-on-write_ strategy.
New records are created in the metastore and associated with the `session_id`.

When reading data, both the `commit_id` and the `session_id` are used to resolve the correct record.
If a record exists for `session_id`, that always takes precedence.
Otherwise, Icechunk will fetch the most recent version of the document according to the commit history.

Within a single session, Icechunk offers the same isolation as just using object storage: read uncommitted,
meaning that one client can see another clients changes before they have been committed.
For this reason, all of the updates to the repo within a session must be cooperative and coordinated by the user.
They can still be concurrent, as long as the concurrent processes are cooperating not to clobber each other's data.
This allows distributed compute engines to do parallel I/O on the repo within the context of a session.

After a client has finished making changes to a repo, it can finalize the changes by making a commit.
Alternatively, the client can abandon the session (by simply never calling `commit()`), and the changes will be lost.

### Commit Process and Conflict Resolution

To illustrated the commit process, let's assume that we are on the branch `main` inside a session with a base commit of **C1**.
We make changes to our repo by writing or deleting records, and we are now ready to commit.

Committing to a branch is a two step process.

1. Create a new commit record. Icechunk issues a new `commit_id`. Let's call this **C2**.
   The parent for this commit is the session's current base commit: **C1**.
   This operation will always succeed.
2. Move the branch pointer to the new `commit_id`.
   This can fail if a new commit was made to the branch since the current session was initiated.


To detect whether moving the branch is possible, Icechunk verifies that the new commit's parent `commit_id` (**C1**)
matches `commit_id` of the branch. If the answer is "yes", then the branch is updated, and the commit process succeeds.

If the answer is "no", that means that a different session has committed to the same branch since our last checkout.
(Let's call this commit **C3**.)
At this point, Icechunk adopts an "optimistic concurrency" strategy to retry the commit.
If there are conflicts between the changes in **C2** and **C3**, the commit fails.
(The changes made in the session are still available to read via **C2**.)
If there are no conflicts, then the session creates a new commit (call this **C4**) with **C3** as the parent.
(This is equivalent to a "fast forward" operation in Git parlance.)
The session then again requests to move the branch pointer to **C4**, and the process repeats until
eventual success, failure (due to conflicts), or a configurable timeout.

This process is illustrated via the flowchart below:


```mermaid
graph TD;
    A[check out repo at <b>C1</b>]-->B;
    B[make changes]-->C;
    C[create new commit <b>C2</b>]-->D;
    D[request to move branch pointer]-->E;
    E{new commits on\n branch since <b>C1</b>?}-->|no| F;
    F([success\n move branch to <b>C2</b>]);
    E-->|yes: <b>C3</b>| G;
    G{can <b>C2</b> be safely\nrebased on <b>C3</b>?};
    G-->|no| H([fail\n <b>C2</b> left detached])
    G-->|yes| I[create new commit <b>C4</b>\n with parent <b>C3</b>]
    I-->|retry| D
```

:::caution
The "optimistic concurrency" approach is relatively expensive and assumes that conflicts
are unlikely. It makes the assumption that users will design their workloads to avoid
deliberately creating many simultaneous commits at the same time.
This is elaborated further in [Best Practices](best-practices).
:::

### Content Addressable Chunk Storage

The Zarr chunk data are not stored directly in Icechunk. Instead, Icechunk stores a _reference_ for each chunk
pointing at an object in object storage (including a potential byte range within the object where the chunk will be found).
When writing new chunk data, Icechunk uses a "content addressable" strategy to pick the keys to use in the object store;
it computes a hash for each piece of data and then uses that hash as the key.
The process of writing a chunk is as follows:

1. Compute the hash of the chunk using a deterministic cryptographic hash (e.g. SHA265)
2. Write the chunk data to the object store using the hash as a key
3. Create a "chunk reference" record in the metastore for the specific chunk name (e.g. `foo/bar/c/2/3`)
   which points at the hash. These records are transacted and versioned the same as any
   other metadata record.

When reading a chunk, the client first queries the metastore to find the chunk reference it needs,
and then retrieves the chunk data from the object store.

This approach also provides automatic deduplication of data at the chunk level.
If two session happen to write different chunks that contain the same exact data, both
will be stored under the same key. It doesn't matter if one overwrites the other--the
content addressability ensures that the data are the same regardless of who wrote it.


## Concurrency Modes

Concurrency refers to when multiple operations are performing I/O operations on Icechunk at the same time.
Concurrency is desirable when doing large-scale data processing, as it allows parallel reading / writing,
greatly increasing the throughput of a workflow. Concurrency can take many forms:
- Asynchronous operations
- Multithreading (multiple threads within a single machine)
- Multiprocessing (multiple processes within a single machine)
- Distributed processing (multiple machines working together)

Concurrent reading is never a problem with Icechunk and works well in any scenario.
Concurrent writing is more complicated.
Icechunk allows for two distinct approaches to concurrent writing to repos:

### Cooperative Mode

In **cooperative mode**, the concurrent writes occurs as part of a single job in which the user
can plan how each worker will act. Specifically, the software doing the writing should take
care not to overwrite chunks written by other workers, e.g. by aligning writes with
chunk boundaries. These concurrent writes can happen within a single session
and be committed via a single commit. Cooperative mode is simpler and less expensive,
but requires more planning by the user.

Cooperative writes often occur in jobs managed by a workflow scheduling system like
Dask, Apache Airflow, Prefect, Apache Beam, etc. All of these systems use
directed acyclic graphs (DAGs) to represent computations. A DAG for a cooperative write to an
array with chunk size 10 would look something like this:

```mermaid
graph TD;
    CO[check out repo];
    CO-->|session ID| A[write region 0:10]
    CO-->|session ID| B[write region 10:20]
    CO-->|session ID| C[write region 20:30]
    CO-->|session ID| D[write region 30:40]
    A-->CM
    B-->CM
    C-->CM
    D-->CM
    CM([commit session])
```

(Note that each worker's write evenly aligns with the chunk boundaries.)

For frameworks that allow serialization of Python objects and passing them directly between tasks
(e.g. Dask, Prefect), the `arraylake.Repo` object can be passed directly between tasks to maintain
a single session.

Cooperative mode is the recommended way to make large updates to arrays in Icechunk.
Non-cooperative mode should only be used when cooperative mode is not feasible.

### Non-cooperative mode

In **non-cooperative mode**, the concurrent writes come from distinct, uncoordinated processes
which may potentially conflict with each other. In this case, it may be better to have one process
fail rather than make an inconsistent change to the repository.

#### Example 1: No Conflicts

As an example, consider two sessions writing to to the same array with chunk size 10.
In the first example, the two processes write to different regions of the array (`0:20` and `20:30`)
in a way that evenly aligns with the chunk boundaries:

```mermaid
sequenceDiagram
    actor S1 as Session 1 (`0:20`)
    participant A as Icechunk
    actor S2 as Session 2 (`20:30`)
    Note over A: main: C1
    S1->>A: check out main
    A->>S1: commit C1
    S2->>A: check out main
    A->>S2: commit C1
    S1->>A: write array region 0:20
    S2->>A: write array region 20:30
    S1->>A: commit session
    Note over A: main: C2
    A->>S1: success: new commit C2
    S2->>A: commit session
    A->>S2: sorry, your branch is out of date
    S2->>A: can I fast forward?
    A->>S2: yes
    S2->>A: fast forward and commit session
    Note over A: main: C3
    A->>S2: success: new commit C3
```

In this case, the optimistic concurrency system is able to resolve the situation and both sessions
can commit successfully.
However, this is significantly more complex and expensive in terms of communication with Icechunk
than using cooperative mode. This approach would not scale well to hundreds of simultaneous commits,
since the fast-forward block would have to loop over and over until finding a consistent state it can commit.
_If possible, it would have been better to use cooperative mode for this update,_
since the writes were aligned with chunk boundaries.

#### Example 1: Chunk Conflicts

In a second example, let's consider what happens when the two sessions write to _the same chunk_.
One session writes to `0:20` while the other writes to `15:30`.
They overlap writes on the chunk spanning the range `10:20`.
In this case, only one commit will succeed, and the other will raise an error.
_This is good!_ It means Icechunk helped us avoid a potentially inconsistent update to the array
that would have produced an incorrect end state.
:::tip
This sort of consistency problem is not possible to detect when using Zarr directly on object storage.
:::

It is now up to the user to decide what to do next.
In the example below, the user's code implements a manual retry by checking out the repo in its
latest state and re-applying the update.

```mermaid
sequenceDiagram
    actor S1 as Session 1 (`0:20`)
    participant A as Icechunk
    actor S2 as Session 2 (`15:30`)
    Note over A: main: C2
    S1->>A: check out main
    A->>S1: commit C1
    S2->>A: check out main
    A->>S2: commit C1
    S1->>A: write array region 0:20
    S2->>A: write array region 15:30
    S1->>A: commit session
    Note over A: main: C2
    A->>S1: success: new commit C2
    S2->>A: commit session
    A->>S2: sorry, your branch is out of date
    S2->>A: can I fast forward?
    A->>S2: no: conflict detected
    Note over S2: At this point, Icechunk raises an error.
    Note over S2: User can implement a manual retry.
    S2->>A: check out main
    A->>S2: commit C2
    Note over S2: Now Session 2 can see Session 1 changes.
    S2->>A: rewrite array region 15:30
    S2->>A: commit session
    Note over A: main: C3
    A->>S2: success: new commit C3
```

It would not have been possible to have these two updates occur within a single session,
since the updates from 2 would overwrite the updates from 2, or vice versa, in an
unpredictable way.
