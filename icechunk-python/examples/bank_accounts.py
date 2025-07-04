"""
This scripts is the subject of a post in the Earthmover blog.

It uses Icechunk to simulate a series of concurrent bank transactions, and
explores different approaches to conflict resolution.
"""

import multiprocessing
import random
import time
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from enum import Enum

import icechunk
import zarr

MAX_INITIAL_BALANCE = 100_000  # cents


class WireResult(Enum):
    DONE = 1
    NOT_ENOUGH_BALANCE = 2


def create_repo(num_accounts: int) -> icechunk.Repository:
    storage = icechunk.s3_storage(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        force_path_style=True,
        bucket="testbucket",
        prefix="bank_accounts_example_" + str(int(time.time() * 1000)),
        access_key_id="minio123",
        secret_access_key="minio123",
    )
    repo = icechunk.Repository.create(storage=storage)

    session = repo.writable_session("main")
    group = zarr.group(store=session.store, overwrite=True)
    group.create_array(
        "accounts",
        shape=(num_accounts,),
        chunks=(1,),
        dtype="uint64",  # cents
        fill_value=0,
        compressors=None,
    )
    session.commit("Accounts array created")
    return repo


def initial_balances(max_initial_balance: int, repo: icechunk.Repository) -> int:
    """Add initial random balance to every account.

    Make a commit when done.
    """

    session = repo.writable_session("main")
    group = zarr.open_group(store=session.store, mode="r")
    array = group["accounts"]
    num_accounts = array.shape[0]
    for account in range(num_accounts):
        balance = random.randint(0, max_initial_balance + 1)
        array[account] = balance
    total_balance = sum(array)
    session.commit("Account balances initialized")
    return total_balance


def transfer(
    from_account: int, to_account: int, amount: int, array: zarr.Array
) -> WireResult:
    """Transfer amount cents from from_account to to_account.

    It records the operation in the array but it doesn't attempt to commit the session.
    """

    # pretend we are doing some actual work
    time.sleep(random.uniform(0.5, 1))

    balance = array[from_account]
    res = WireResult.DONE
    if balance < amount:
        res = WireResult.NOT_ENOUGH_BALANCE
    else:
        array[from_account] = balance - amount
        array[to_account] = array[to_account] + amount
    return res


def unsafe_transfer_task(repo: icechunk.Repository) -> WireResult:
    """Naif approach to transfers.num_accounts.

    This approach fails by generating commit conflicts.
    """

    session = repo.writable_session("main")
    group = zarr.open_group(store=session.store, mode="r")
    array = group["accounts"]
    num_accounts = array.shape[0]
    from_account = random.randint(0, num_accounts - 1)
    to_account = random.randint(0, num_accounts - 1)
    amount = random.randint(0, int(MAX_INITIAL_BALANCE / 5))
    res = transfer(from_account, to_account, amount, array)
    if res == WireResult.DONE:
        session.commit(f"wired ${amount}: {from_account} -> {to_account}")
    return res


def slow_transfer_task(repo: icechunk.Repository) -> WireResult:
    """This approach to concurrent transfers is safe but very slow.

    It simply retries transactions when they conflict.
    """
    session = repo.readonly_session("main")
    group = zarr.open_group(store=session.store, mode="r")
    array = group["accounts"]
    num_accounts = array.shape[0]
    from_account = random.randint(0, num_accounts - 1)
    to_account = random.randint(0, num_accounts - 1)
    amount = random.randint(0, int(MAX_INITIAL_BALANCE / 5))

    while True:
        session = repo.writable_session("main")
        group = zarr.open_group(store=session.store, mode="r")
        array = group["accounts"]
        res = transfer(from_account, to_account, amount, array)
        if res == WireResult.NOT_ENOUGH_BALANCE:
            return res
        if res == WireResult.DONE:
            try:
                session.commit(f"wired ${amount}: {from_account} -> {to_account}")
                return res
            except icechunk.ConflictError:
                pass
    return res


def rebase_transfer_task(repo: icechunk.Repository) -> WireResult:
    """Safe and fast approach to concurrent transfers.

    Rebases conflicting transactions. Retries transactions that cannot be rebased.
    """
    session = repo.readonly_session("main")
    group = zarr.open_group(store=session.store, mode="r")
    array = group["accounts"]
    num_accounts = array.shape[0]
    from_account = random.randint(0, num_accounts - 1)
    to_account = random.randint(0, num_accounts - 1)
    amount = random.randint(0, int(MAX_INITIAL_BALANCE / 5))

    while True:
        session = repo.writable_session("main")
        group = zarr.open_group(store=session.store, mode="r")
        array = group["accounts"]
        res = transfer(from_account, to_account, amount, array)
        if res == WireResult.NOT_ENOUGH_BALANCE:
            return res
        if res == WireResult.DONE:
            try:
                session.commit(
                    f"wired ${amount}: {from_account} -> {to_account}",
                    rebase_with=icechunk.ConflictDetector(),
                )
                return WireResult.DONE
            except icechunk.RebaseFailedError:
                pass


def calculate_total_balance(repo: icechunk.Repository) -> int:
    session = repo.readonly_session(branch="main")
    store = session.store
    group = zarr.open_group(store=store, mode="r")
    array = group["accounts"]
    assert all(balance >= 0 for balance in array)
    return sum(array)


def concurrent_transfers(
    num_accounts: int,
    num_transfers: int,
    total_balance: int,
    transfer_task: Callable[[icechunk.Repository], WireResult],
    repo: icechunk.Repository,
) -> None:
    """Launch concurrent transfers using multi processing.

    When done, it walidates invariants of the system and runs GC.
    """

    # Execute the transfers
    results = []
    with ProcessPoolExecutor(max_workers=10) as exec:
        for res in exec.map(transfer_task, [repo] * num_transfers):
            results.append(res)
            print(f"Done: {len(results)}/{num_transfers}")

    # Verify the end result is valid
    assert len(results) == num_transfers
    succeeded = sum(1 for res in results if res != WireResult.NOT_ENOUGH_BALANCE)
    failed = sum(1 for res in results if res == WireResult.NOT_ENOUGH_BALANCE)
    assert succeeded + failed == num_transfers
    print(f"Succeeded: {succeeded}. Failed: {failed}")

    assert calculate_total_balance(repo) == total_balance
    num_commits = sum(1 for _ in repo.ancestry(branch="main"))
    assert num_commits == succeeded + 3

    # Garbage collect to delete any leftovers
    print(repo.garbage_collect(datetime.now(UTC)))


def main() -> None:
    multiprocessing.set_start_method("forkserver")
    num_accounts = 50
    num_transfers = 50

    repo = create_repo(num_accounts)
    total_balance = initial_balances(MAX_INITIAL_BALANCE, repo)

    ## uncomment to try the different approaches

    # concurrent_transfers(
    #     num_accounts, num_transfers, total_balance, unsafe_transfer_task, repo
    # )
    # concurrent_transfers(
    #     num_accounts, num_transfers, total_balance, slow_transfer_task, repo
    # )
    concurrent_transfers(
        num_accounts, num_transfers, total_balance, rebase_transfer_task, repo
    )


if __name__ == "__main__":
    main()
