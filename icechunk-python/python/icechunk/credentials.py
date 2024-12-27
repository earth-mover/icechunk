import pickle
from collections.abc import Callable

from icechunk._icechunk_python import (
    S3Credentials,
    S3StaticCredentials,
)


def s3_refreshable_credentials(
    get_credentials: Callable[[], S3StaticCredentials],
) -> S3Credentials.Refreshable:
    return S3Credentials.Refreshable(pickle.dumps(get_credentials))
