"""Resolve Arraylake repos into per-module icechunk Storage objects.

The arraylake client is hard-bound to the module literally named
``icechunk``, so it cannot build storage for the renamed baseline package.
Instead we call the metastore's ``open_repo`` endpoint directly (one HTTP
call returning bucket config + resolved prefix + vended credentials) and
mirror the small storage dispatch from
``arraylake/repos/icechunk/storage.py``, parameterized on the module.

Static credentials only: rounds re-resolve when credentials near expiry
instead of installing per-module refresh callbacks.
"""

from __future__ import annotations

import datetime
import os
from dataclasses import dataclass
from types import ModuleType
from typing import Any

BENCH_DESCRIPTION = "icechunk governor A/B benchmark data (safe to delete)"
CREDS_REFRESH_MARGIN = datetime.timedelta(minutes=10)

S3_PLATFORMS = ("s3", "s3c", "s3-compatible", "minio")


def make_client(token: str | None = None) -> Any:
    try:
        from arraylake import Client
    except ModuleNotFoundError:
        raise SystemExit(
            "the arraylake package is not installed; run "
            "`uv sync --group io-governor-ab` (or `uv pip install arraylake`)"
        ) from None
    return Client(token=token or os.environ.get("ARRAYLAKE_TOKEN"))


@dataclass(frozen=True)
class ResolvedRepo:
    org: str
    name: str
    platform: str
    bucket: str
    prefix: str
    region: str | None
    endpoint_url: str | None
    use_ssl: bool
    force_path_style: bool
    credentials: Any  # arraylake S3Credentials | GSCredentials | None

    @property
    def full_name(self) -> str:
        return f"{self.org}/{self.name}"

    @property
    def expires_soon(self) -> bool:
        expiration = getattr(self.credentials, "expiration", None)
        if expiration is None:
            return False
        now = datetime.datetime.now(datetime.UTC)
        return expiration.replace(tzinfo=datetime.UTC) <= now + CREDS_REFRESH_MARGIN


def resolve_repo(client: Any, org: str, name: str) -> ResolvedRepo:
    from arraylake.asyn import sync

    # there is no public API returning storage config without constructing
    # icechunk objects, hence the private metastore access
    mstore = client.aclient._metastore_for_org(org)
    response = sync(mstore.open_repo, name)
    bucket = response.repo_bucket
    extra = bucket.extra_config or {}
    use_ssl = bool(extra.get("use_ssl", True))
    region = extra.get("region_name")
    endpoint_url = extra.get("endpoint_url")
    return ResolvedRepo(
        org=org,
        name=name,
        platform=bucket.platform,
        bucket=bucket.name,
        prefix=response.repo_prefix,
        region=str(region) if region is not None else None,
        endpoint_url=str(endpoint_url) if endpoint_url is not None else None,
        use_ssl=use_ssl,
        force_path_style=bool(extra.get("force_path_style", not use_ssl)),
        credentials=response.repo_credentials,
    )


def storage_for(mod: ModuleType, repo: ResolvedRepo) -> Any:
    """Build ``mod``'s Storage for a resolved repo (S3/Tigris/R2 and GCS)."""
    creds = repo.credentials
    expires = getattr(creds, "expiration", None)
    if expires is not None:
        expires = expires.replace(tzinfo=datetime.UTC)
    if repo.platform in S3_PLATFORMS:
        url = repo.endpoint_url or ""
        common = dict(
            bucket=repo.bucket,
            prefix=repo.prefix,
            region=repo.region,
            endpoint_url=repo.endpoint_url,
            allow_http=not repo.use_ssl,
            access_key_id=creds.aws_access_key_id if creds else None,
            secret_access_key=creds.aws_secret_access_key if creds else None,
            session_token=creds.aws_session_token if creds else None,
            expires_after=expires,
            anonymous=creds is None,
            from_env=False,
        )
        if "fly.storage.tigris.dev" in url or "t3.storage.dev" in url:
            if repo.region in (None, "auto"):
                raise SystemExit(
                    f"{repo.full_name}: tigris buckets need an explicit region"
                )
            return mod.tigris_storage(**common)
        if ".r2." in url:
            return mod.r2_storage(account_id=None, **common)
        return mod.s3_storage(force_path_style=repo.force_path_style, **common)
    if repo.platform == "gs":
        return mod.gcs_storage(
            bucket=repo.bucket,
            prefix=repo.prefix,
            bearer_token=creds.access_token if creds else None,
            anonymous=creds is None,
            from_env=False,
        )
    raise SystemExit(
        f"unsupported bucket platform {repo.platform!r} for {repo.full_name}"
    )


def list_repo_names(client: Any, org: str) -> set[str]:
    return {r.name for r in client.list_repos(org)}


def get_bucket_config(client: Any, org: str, nickname: str | None) -> Any:
    """The org's BucketResponse for `nickname`, or its default bucket."""
    from arraylake.asyn import sync

    buckets = sync(client.aclient.list_bucket_configs, org)
    if nickname is None:
        default = [b for b in buckets if getattr(b, "is_default", False)]
        if not default:
            raise SystemExit(f"org {org} has no default bucket; pass --bucket-nickname")
        return default[0]
    for bucket in buckets:
        if bucket.nickname == nickname:
            return bucket
    raise SystemExit(
        f"no bucket named {nickname!r} in {org}; "
        f"available: {sorted(b.nickname for b in buckets)}"
    )


def create_repo(client: Any, org: str, name: str, bucket_nickname: str | None) -> None:
    client.create_repo(
        f"{org}/{name}",
        bucket_config_nickname=bucket_nickname,
        description=BENCH_DESCRIPTION,
    )


def delete_repo(client: Any, org: str, name: str) -> None:
    # immediate: benchmark garbage, skip the recoverable-ghost grace period
    client.delete_repo(f"{org}/{name}", imsure=True, imreallysure=True, immediate=True)
