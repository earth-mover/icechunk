import pickle

from icechunk import s3_earthdata_credentials
from icechunk.credentials import S3Credentials, _EarthdataCredentialFetcher


def test_earthdata_fetcher_pickle_roundtrip() -> None:
    fetcher = _EarthdataCredentialFetcher("https://example.com/s3credentials", "host", "token")
    unpickled = pickle.loads(pickle.dumps(fetcher))
    assert unpickled.credentials_url == fetcher.credentials_url
    assert unpickled.host == fetcher.host
    assert unpickled.auth == fetcher.auth


def test_earthdata_credentials_returns_refreshable() -> None:
    creds = s3_earthdata_credentials("https://example.com/s3credentials", auth="token")
    assert isinstance(creds, S3Credentials.Refreshable)
