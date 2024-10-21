# Design document

## Goal

Goal here is to describe and finish designing the mechanism to "rebase" changes.

When user `A` tries to commit a change, currently the commit will fail if user
`B` committed since `A`'s session started. This is the best and safest
default, but it's not necessarily what `A` wants every time. For example
maybe `A` wrote to array `/array_a` and `B` wrote to `/array_b` and those
changes are unrelated. In a case like that, `A` may decide to still
do the commit, accepting the risks if they know exactly what `B` changes were.

A rebase is then, the process of "merging" a change, potentially modifying it,
on top of other pre-existing changes.

We want to provide:

* A mechanism for users to execute a rebase after a failed commit.
* Users can define what changes are OK to rebase and which are not, and how
  their changes must be modified for a clean rebase. Example: if user wrote
  to an array but a previous commit deleted that array, the user may
  indicate to either fail their commit, or to simply rebase ignoring any
  writes to the array.
* If a rebase fails we need to explain why.

## Transaction logs

As part of this change we will introduce the concept of `TransactionLog`.
These are files we will store on-disk, in their own prefix, and with the same
id as the corresponding snapshot. The transaction log contains a serialization,
somewhat expanded, of the `ChangeSet`.

They provide at least two utilities:

* An easy way to know what the conflicting commits changed, to be able to
  execute rebases without having to compare snapshots (it would be very
  expensive).
* In the future, an easy way to provide `diff` functionality.

Transaction logs will be generated from the `ChangeSet` (and probably a bit
of extra information, like the list of existing nodes), and they will be
written during the commit process.

Transaction logs can be made optional. For ultimate performance users may choose
not to use them, but in that case, they'll be giving up on rebase and diff
functionality.

## Conflict resolution

In the most detailed case, conflict resolution could be done interactively.
Users may want to investigate their own change, together with the diffs of
the conflicting changes, and decide with full detail how to modify their
change for the rebase. This sounds like a very advanced usage, and we don't need
to support it initially. We just need to make sure it is possible in the future.

In the simpler case, the user will run rebase after a commit failed with
conflict. They will call a `rebase` function, passing a `ConflictSolver`
that includes the policy on how to deal with different types of conflicts.

## Some conflict resolution examples

* If two changes write to the same chunk, user can select `ours` or `theirs`
* If two changes write to the same array, but different chunk coordinates,
  user may decide it's OK to merge both writes.
* If writes happen to an entity deleted in a previous change, we may
  support either ignoring the write or fail the rebase
* TODO: more

## Conflict resolution process

* We find all the paths affected by the current change
  * This includes nested paths, for example, all nested nodes in a deleted group
* We sort them lexicographically
* For each path:
  * Compute conflicts on the current `ChangeSet`, affecting the path
  * We apply a policy to update the change, the policy can fail the rebase or
    modify the `ChangeSet` to continue
  * We abort at the first path where the policy fails the rebase
  * We update the current `ChangeSet`
* If the loop finished, we have a successful rebase and a modified `ChangeSet`.
* We need to double check there are no more conflicts. If still there are
  conflicts the rebase fails

## Exhaustive list of conflicts detected and resolutions

1. If current change creates a node (group or array)
    1. if previous_repo has an explicit node on the same path
       * No solution offered
    2. if previous_repo has an array in any of the implicit parents
       * No solution offered

2. If current change updates an array metadata
    1. if previous_change updates the same array
       * No solution offered
    2. if previous_change deletes the same array
       * No solution offered

3. If current change updates user attributes of a node
    1. if previous_change update user attributes on the same node
       * Rebase by keeping their changes
       * Rebase by keeping our changes
    2. if previous_change deletes the node
       * No solution offered

4. If current change updates chunk coordinate C in array A
    1. If previous_change also updated array A at coord C
       * Rebase by keeping their changes
       * Rebase by keeping our changes
    2. If previous_change deleted A
       * No solution offered
    3. If previous_change updated zarr metadata for A
       * No solution offered

5. If current change deletes an array
    1. If previous_change updated user atts, zarr metadata or chunks for the array
       * Rebase by skipping the delete
       * Rebase by still deleting

6. If current change deletes a group
    1. If previous_change updated user attributes for the group
       * Rebase by skipping the delete
       * Rebase by still deleting
    2. If previous_change made any changes to the group descendants
       * Rebase by skipping the delete
       * Rebase by still deleting

### Conflict resolutions

1.
    1. No solution
    2. No solution

2.
    1. No solution
    2. No solution

3.
    1. No solution
    2. No solution

This is WIP

* When a previous change deleted an array:
  * if chunks were written to it: recoverable by not applying the change
  * if user attributes were set: recoverable by not applying the change
  * if metadata was changed: recoverable by not applying the change
  * if the same array was delete: recoverable by dropping the change

* When a previous change deleted a group:
  * if user attributes were set on it: recoverable by not applying the change
  * if a new nodes are created inside of it: recoverable by re creating the
  implicit group, or dropping the changes
  * if nodes are modified: recoverable by dropping the change

* When a previous change creates an array
  * if an array is created on the same path: recoverable by not applying the change
  * if a node is created on the same path: recoverable by not applying the change
  * if an implicit group is created on the same path: recoverable by not
    applying the change

* When a previous change creates a group
  * if a node is created on the same path, except if it's implicit

* When a previous change updates user attributes
  * if the same node attributes are also updated
  * if the same node attributes are also updated

* When a previous change updates zarr metadata
* When a previous change writes/deletes a chunk

# Authz questions for a design doc

* Are we providing an HTTP service, a library (in what language) or both?
* What is the API? Examples:
  * Can it only answer atomic questions like "does user A have access to
  resource B with permissions C"?
  * Or does it answer more comprehensive questions such as "What are all the
  repos user A has read access to"?
* What is the general architecture? What services run where, how do they
  communicate, how does this interact with different regions and AZs.
* What order of magnitude latency are we targeting? 2ms? 20ms?
* How do we provide low latency from a query service deployed far from our main
  database
* How do the different parts interact and communicate? What is the flow of
  information, for example, when we want to render all repos a user can see.
  What happens when we delete a repo?
* The way code interacts with authz in current AL service is not great.
  Examples:
  * Dependencies are not "typed",
  * They depend on function argument names,
  * It's hard to predict what the effect of adding a dependency is, what data
      it will pull from the database, etc
  * It's very hard to maintain dependencies, small changes to requirements
      trigger large code changes
  How are we planning to change this?
* Same is true for tests, they are hard to write today. Examples:
  * Our fixture system for user creation and authz doesn't really work, lots of
    copy/pasting and very brittle. Very high maintenance cost.
  * A lot of duplication between tests, high cost to set them up.
  * Difficulties running tests in parallel because of interacting tests
  If we don't actively provide support for tests, they will only get much
  worse in a distributed system. How are we going to deal with this.
* Are there caches involved? Where, how do they get invalidated?
* Do we use a single database or one for business entities and a different one
  for authz tuples? How do we back them up in sync and consistently.
* How do we ensure consistency between our main database and the authz database?
  If this is only eventual consistency, how does the client code request
  different levels of consistency to ensure read-your-own-writes?
* How do we deploy changes to the authz model while maintaining availability in
  multiple regions?

----------------------------------------------------

## Seba's consistency warnings

Consistency in distributed systems is really hard, probably the hardest problem
of them all. When you have more than one system of record you need to make sure
their views of the world agree. A lot of the guarantees we usually trust can no
longer be used. Examples: transactions, clocks (no, not even if you sync them),
write-read consistency, delete-read consistency, etc. Simple algorithms that
would be trivial in a non distributed system, become multi-week projects, and the
result is usually buggy and leaky.

Not having consistency makes it really hard to reason about the properties of
your system, hard to impossible. Trivial example: create a user, list users, the
new user shows in the list. Even something as trivial as that is not guaranteed
without the right level of consistency. It can be a problem for a UI, but maybe
that's a minor problem; but it can also be a problem for other more important
processes. Maybe you send a welcome e-mail, but the code getting didn't get
the new user. That's a more important bug, and it only gets worse.

There is no way around it, at some point you need to face the difficulties. But
it slows down development very significantly. For this reason, it's always
important to delay the appearance of distributed systems and inconsistencies for
as long as possible. Move fast while it's easy, then make it hard to move fast.

The two main sources of inconsistencies I see in the architecture are:

* The authz database separate from the main business entities database.
* The main database being far from the QS deployment region, which could require
  caches

--------------------------------------------------------

## Sylvera chunk gc

```python
def extract_session(key):
    return key.split(".").last


committed_sessions: set[str] = <load file Seba sent>

for chunk in s3.list(repo_id + "/chunks"):
   chunk_session = extract_session(chunk.key)
   if not chunk_session in committed_sessions and chunk.last_updated_at < 7 days ago:
       s3.delete(chunk.key)
```

```
use 66f1668210b4c41864bdcc93

db.commits.aggregate([
  {$unwind: {path: "$session_ids"}},
  {$group: {_id: null, session_id: {$addToSet: "$session_ids"}}},
  {$unwind: {path: "$session_id"}},
  {$project: {_id: "$session_id"}},
  {$out: "committed_sessions"}
])

use 66f1668610b4c41864bdcc9d
   
db.commits.aggregate([
  {$unwind: {path: "$session_ids"}},
  {$group: {_id: null, session_id: {$addToSet: "$session_ids"}}},
  {$unwind: {path: "$session_id"}},
  {$project: {_id: "$session_id"}},
  {$out: "committed_sessions"}
])



mongoexport \
  --noHeaderLine \
  --uri "$MONGO_METASTORE_URI" \
  --db=66f1668210b4c41864bdcc93 \
  --collection=committed_sessions \
  --type=csv \
  --fields=_id \
  --out=/tmp/66f1668210b4c41864bdcc93.csv

mongoexport \
  --noHeaderLine \
  --uri "$MONGO_METASTORE_URI" \
  --db=66f1668610b4c41864bdcc9d \
  --collection=committed_sessions \
  --type=csv \
  --fields=_id \
  --out=/tmp/66f1668610b4c41864bdcc9d.csv


6605b8fb9c42a38db23426f4
```

------------------------------------------

## Mentoring Orestis

Folks, I thought a little bit last night about how to help Orestis be more effective, and I came up with a few points. Hopefully it will be of use to you.

### Technical

* Encourage Orestis to write design documents, get feedback and iterate ideas, changing as needed. This could be achieved by pairing him with somebody else during the design phase, at least so he gets help and/or private feedback, which is easier to deal with.

### Team work

* More interaction with the team. To more effectively lead the development of our catalog, Orestis needs to be very aware of everything that is going on in the rest of the system: QS, IC, credentials, ingestion, etc. This requires more understanding of the technical details than he currently has. A possible way to improve this: encourage him to gather specific questions and schedule monthly check-ins with other project leads to get answers and explanations.
* Orestis is very "surprise driven". He keeps things secrets and then he does a big reveal. I don't think this is good as a default mode, it's fine once or twice a year, but you cannot constantly surprise people with your work. At certain times (metrics project for instance) this becomes very frustrating. I feel we encourage this behavior a bit too. One way to improve this would be to have Orestis providing _real_ updates to his projects. There is usually no way to know where he's at, and I don't think he's taking the weekly updates seriously. This would help him a lot, he would get feedback much earlier, and avoid really bad situations such as the catalog-of-catalogs thing. Same applies to his standup updates, it's usually just something like "authz research".

### Communication

* This is the hardest one, and I don't know how to approach it. Orestis has a very particular style of communication, which is a core part of his personality, makes him a fun person to be around, and we shouldn't try to change it. But in professional contexts he needs to learn how to be more succinct and to the point. I have heard this from several others too. My previous points, that suggest more interaction, are not going to work well if we cannot make those meetings more effective. This is the kind of feedback that is very hard to pass to the person, but maybe you can drive the point by having Orestis write short and specific documents, and schedule short and informative meetings about his work. Also, insisting on him answering questions in documents and comments.

Boa tarde. Ainda nao recebi contato do seguro.
entendi, obrigado, so isso por enquanto

## Performance review

* How to be able to contribute meaningfully to other projects without distracting me for my main objective
* Developing a deeper understanding of the existing Xarray / Dask / Zarr Python
  * yes, but not main area of interest
  * pending since I joined, it feels like a big time investment
* Conflict and frustration
  * Part of the impact of Icechunk, 12-14 hours days for 2.5 months
  * Also felt it at family interactions
* Proposing alternatives not only criticism in design docs
  * How to do it without people thinking I'm dictating the design
  * I use questions as a way to direct thinking towards the good parts
  * I'm trying to get people to ask, before I tell
  * It's always a struggle, different people want different things
  * I'm very open to get more public or private feedback, when you notice things
* Word choice and tone
  * Yes, working on that
  * Just being more relaxed will help
  * I'm very open to get more public or private feedback, when you notice things

  sbeeu02

## First ticket feedback

> Alright, it doesn't look like I am able to publish my branch for this (permissions error?)
You need to fork the repository, push a new branch there, and then create a pull request once it's ready. I can also review the fork branch before you want to create a PR.

> add a method to ZarrArrayMetadata that checks if a given set of coordinates is valid for the array.
Great

> Let me know if this makes sense
Yes, very much so. That sound like a fine approach.

A few notes:

* I'd probably name the method something like `valid_chunk_coordinates`
* Rust has a mechanism for documentation strings, google around and you'll find a whole syntax using stuff like ///
* You could use `debug_assert_eq` instead. Maybe we should add this validation when metadata is written and updated too, to make sure we fail early.
* With a quick look your code seems to do the right thing, it's just not idiomatic (as expected as you learn the language). In Rust we usually try to avoid mutable variables, unless they are really necessary. We also try to avoid explicit index based looping. Take a look at the [`Iterator`](https://doc.rust-lang.org/std/iter/trait.Iterator.html) trait, it has a bunch of very useful methods that get used all the time for this kind of thing.
* Here are some more tips:
  * You could write a helper function that returns an iterator to the maximum chunk index permitted (or even a `Vec` would work, but an Iterator may be better practice)
  * Then you can `zip` the results of that function with the request.
  * Then you verify that `all` elements in the zip result are valid.
  * Relevant functions: [`zip`](https://doc.rust-lang.org/std/iter/fn.zip.html), [`all`](https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.all)

-------------------------------------------------------------------------

## Icechunk update

# Where are we on the roadmap?

* [On the road to 1.0](https://www.notion.so/earthmover/The-Road-to-Icechunk-1-0-12f492ee309f801587e6f1e32fdef0a1)
* Closer to virtual dataset support upstream
* Trying to gather more collaboration from the community, it's hard

# What's next?

* Transaction logs
* Conflict resolution lite
* Good interface to dask distributed writes
* More testing
* Better Python API

# Questions / Blockers

* What should we target for the NASA/DevSeed collaboration? (GC? General performance?)

--------------------------------------------------

## Goals for Authz success

* We support the new features we need
* Nobody outside of the Authz team needs to learn OpenFGA, or interact with Auth0
* Nobody outside of the Authz team understand the fine grained details of the Authz model implementation
* Ergonomics for service endpoint authz are very good:
  * Easy to read and write
  * Documented
  * Testable
  * Auditable
* Ergonomics for tests setup/teardown are significantly better than what we have today
* On authz failures, our error messages and error codes are improved
* We have a short design document of how Authn & Authz work in QS, including:
  * a latency target that works for the QS team,
  * a QS deployment in Europe
* Catalog and QS use the same authz mechanism
* We have a target authz latency documented before the implementation, and a mechanism to measure it. Including stuff QS deployments in different regions.
* We have code and support to deal with eventual consistency: read-after-write, read-after-delete, etc
* Eventual consistency is documented when it cannot be avoided
* We have a path to disaster recovery with some documented level of consistency

-----------------------------------------------

## Nassa links

DevSeed

Open Veda hub: <https://hub.openveda.cloud/>
Login with GitHub
-----------------------------------------------------------------------------------------------------

## Icechunk project update

# Where are we on the roadmap?

* [On the road to 1.0](https://www.notion.so/earthmover/The-Road-to-Icechunk-1-0-12f492ee309f801587e6f1e32fdef0a1)
* We updated the [public roadmap](https://icechunk.io/contributing/#roadmap) to reflect the details of 1.0
* We got a contributions with an example Rust -> C -> Julia binding

# What's next?

* Transaction logs
* Conflict resolution lite
* Good interface to dask distributed writes (very close)
* Better Python API
* We decided we'll prioritize early AL customer migration
* Working with DevSeed to prioritize use cases/demos
  * Trying to close on a dataset. Probably it will have half-hourly updates.
  * We'll have to decide virtual vs. native
  * We successfully steered away from variable length chunks
  * Interest in checksumming virtual datasets
  * Interest in expiration/GC
  * Established biweekly meetings

# Questions / Blockers

* Seba out most of next week
