import platformdirs

PUBLIC_DATA_BUCKET = "icechunk-public-data"

TEST_BUCKETS: dict[str, dict] = {
    "s3": dict(store="s3", bucket="icechunk-ci", region="us-east-1"),
    "gcs": dict(store="gcs", bucket="icechunk-test-gcp", region="us-east1"),
    "r2": dict(
        store="r2",
        bucket="icechunk-test-r2",
        region="us-east-1",
        endpoint_url="https://caa3022c13c9823de0d22b3b6c249494.r2.cloudflarestorage.com",
    ),
    "tigris": dict(
        store="tigris",
        bucket="test-icechunk-github-actions",
        region="us-east-2",
        endpoint_url="https://t3.storage.dev",
    ),
    "local": dict(store="local", bucket=platformdirs.site_cache_dir()),
}
TEST_BUCKETS["s3_ob"] = TEST_BUCKETS["s3"]

BUCKETS = {
    "s3": dict(store="s3", bucket=PUBLIC_DATA_BUCKET, region="us-east-1"),
    "gcs": dict(store="gcs", bucket=PUBLIC_DATA_BUCKET + "-gcs", region="us-east1"),
    "tigris": dict(store="tigris", bucket=PUBLIC_DATA_BUCKET + "-tigris", region="iad"),
    "r2": dict(store="r2", bucket=PUBLIC_DATA_BUCKET + "-r2", region="us-east-1"),
}

# Object store URLs and regions for benchmark result upload/download.
_SCHEMES = {"s3": "s3", "gcs": "gs", "tigris": "s3", "r2": "s3"}
RESULT_STORE_URLS: dict[str, dict[str, str]] = {
    name: {
        "url": f"{_SCHEMES[info['store']]}://{info['bucket']}",
        "region": info.get("region", ""),
        "endpoint_url": info.get("endpoint_url", ""),
    }
    for name, info in TEST_BUCKETS.items()
    if info.get("store") in _SCHEMES
}
