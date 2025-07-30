# Async Usage

Icechunk includes an optional asynchronous interface for orchestrating repos and sessions. However, the Icechunk core is fully asynchronous and delivers full parallelism and performance whether you choose to use the synchronous or asynchronous interface. Most users, particularly those doing interactive data science and analytics, should use the synchronous interface.

## When to use async

The async interface allows for icechunk operations to run concurrently, without blocking the current thread while waiting for IO operations. The most common reason to use async is that you are working within a server context, or anywhere that work may be happening across multiple Icechunk repositories at a time.

## Using the async interface

Because of Icehcunk's rust based runtime, you can call both sync and async methods on a `Repository`, `Session`, or `Store` as needed, there is no need to worry about sync contexts or event loops. Methods that support async are named with an `_async` postfix:

```python exec="on" session="async_usage" source="material-block"
import icechunk

async def get_branches(storage: icechunk.Storage): set[string]
    repo = await icechunk.Repository.open_async(storage)
    return await repo.list_branches_async()
```
