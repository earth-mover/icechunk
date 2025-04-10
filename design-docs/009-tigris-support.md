# Tigris support

[Tigris](https://www.tigrisdata.com/) is a globally distributed S3-compatible object store. Writes and reads get dispatched
automatically to the geographically closest available region, and then the data gets automatically
replicated to other regions.

## Why is support needed

Tigris offers strong consistency within the same region and eventual consistency globally. This means that if write happens in region 'A' and read from the same region, the read data will be up to date. However, if the read happens from a different region, such as region 'B', there is a possibility that the data may be stale, and an older version could be served.

This means basic Icechunk guarantees cannot be hold. For example, something as simple as creating a repo can fails
a high percentage of the time in certain geographic configurations.

Tigris has built for us the ability to recover consistency by passing an `X-Tigris-Regions` header in the request which allows writes and reads to be served by the same region. This allows requests to be strongly consistent. We also need to send `Cache-Control:no-cache` in the get requests in order to avoid reading from the cache. Of course, this comes at the price of performance, because reads no longer happen from the closest region anywhere in the world.

An alternative would be to set the bucket as region-restricted. In that case all writes and reads go to the single region.
This doesn't work well for Icechunk because:

* It doesn't have access to modifying the bucket configuration
* Users may have other data in their buckets, data they don't want to serve from a single region
* It's not flexible, we lose Tigris benefit of data locality

## What we want

Ideally most of the time users can use Icechunk in the most performant way, at least for reads. Even if it's
slightly unsafe, users will want to make all reads from the closest regions.

By default Icechunk should be perfectly safe and consistent, even at the price of slower performance.

## How to achieve it

* By default:
  * When users create a Tigris `Storage` instance we force them to pass a region.
  * All writes and reads use the `X-Tigris-Regions` header set to the initialized region.
  * All reads use the `Cache-Control:no-cache` header.
  * This will make Icechunk safe, but it will direct all writes and reads to a single region, which may cause performance degradation.

* Setting a `Storage` configuration variable to "`unsafe_read_from_local_region: True`":
  * Region is ignored if passed
  * No `X-Tigris-Regions` header is passed.
  * No `Cache-Control:no-cache` header.
  * No write sessions are allowed
  * This is a good configuration to use, for example, for "read services". Where data being read has been written at least minutes ago.
  * User accepts the following trade-offs:
    * Undefined behavior if an object read happens before the object has been propagated to the closest region
    * No writes allowed

## Icechunk changes (WIP)

* We currently don't have a way to limit the creation of write sessions depending on `Storage`
* How to avoid people using S3 compatible storage instead of Tigris specific and getting consistency issues?
