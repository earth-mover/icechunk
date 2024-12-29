import pickle
from collections.abc import Callable, Mapping
from datetime import datetime

from icechunk._icechunk_python import (
    Credentials,
    S3Credentials,
    S3StaticCredentials,
)

AnyS3Credential = (
    S3Credentials.Static
    | S3Credentials.DontSign
    | S3Credentials.FromEnv
    | S3Credentials.Refreshable
)
AnyCredential = Credentials.S3


def s3_refreshable_credentials(
    get_credentials: Callable[[], S3StaticCredentials],
) -> S3Credentials.Refreshable:
    return S3Credentials.Refreshable(pickle.dumps(get_credentials))


def s3_static_credentials(
    *,
    access_key_id: str,
    secret_access_key: str,
    session_token: str | None = None,
    expires_after: datetime | None = None,
) -> S3Credentials.Static:
    return S3Credentials.Static(
        S3StaticCredentials(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            expires_after=expires_after,
        )
    )


def s3_dont_sign_credentials() -> S3Credentials.DontSign:
    return S3Credentials.DontSign()


def s3_from_env_credentials() -> S3Credentials.FromEnv:
    return S3Credentials.FromEnv()


def s3_credentials(
    *,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    session_token: str | None = None,
    expires_after: datetime | None = None,
    dont_sign: bool | None = None,
    from_env: bool | None = None,
    get_credentials: Callable[[], S3StaticCredentials] | None = None,
) -> AnyS3Credential:
    if (
        (from_env is None or from_env)
        and access_key_id is None
        and secret_access_key is None
        and session_token is None
        and expires_after is None
        and not dont_sign
        and get_credentials is None
    ):
        return s3_from_env_credentials()

    if (
        dont_sign
        and access_key_id is None
        and secret_access_key is None
        and session_token is None
        and expires_after is None
        and not from_env
        and get_credentials is None
    ):
        return s3_dont_sign_credentials()

    if (
        get_credentials is not None
        and access_key_id is None
        and secret_access_key is None
        and session_token is None
        and expires_after is None
        and not from_env
        and not dont_sign
    ):
        return s3_refreshable_credentials(get_credentials)

    if (
        access_key_id
        and secret_access_key
        and not from_env
        and not dont_sign
        and get_credentials is None
    ):
        return s3_static_credentials(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            expires_after=expires_after,
        )

    raise ValueError("Conflicting arguments to s3_credentials function")


def containers_credentials(
    m: Mapping[str, AnyS3Credential] = {}, **kwargs: AnyS3Credential
) -> dict[str, AnyCredential]:
    res = {}
    for name, cred in {**m, **kwargs}.items():
        if isinstance(cred, AnyS3Credential):
            res[name] = Credentials.S3(cred)
        else:
            raise ValueError(f"Unknown credential type {type(cred)}")
    return res
