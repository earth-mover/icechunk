//! TLS trust roots from `ICECHUNK_TLS_CA_FILE`, and the hint for TLS errors.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use rustls_pki_types::{CertificateDer, pem::PemObject as _};

use crate::{StorageError, other_error};

/// When set, names a PEM bundle that replaces the system trust roots of every client.
pub const TLS_CA_FILE_ENV: &str = "ICECHUNK_TLS_CA_FILE";

pub const TLS_HINT: &str = "TLS certificate verification failed. If this process cannot use the \
    system trust store (for example, in a macOS sandbox), set the environment variable \
    ICECHUNK_TLS_CA_FILE to a PEM CA bundle, such as the path from certifi.where(), and \
    create the storage again.";

/// Read the PEM bundle at `path`. Fail if it has no certificate or a malformed one.
pub fn load_tls_ca_file(path: &Path) -> Result<Vec<u8>, StorageError> {
    let fail = |reason: String| {
        other_error(format!("cannot use {TLS_CA_FILE_ENV}={path:?}: {reason}"))
    };
    let pem = std::fs::read(path).map_err(|e| fail(e.to_string()))?;
    let mut count = 0;
    for cert in CertificateDer::pem_slice_iter(&pem) {
        let cert = cert.map_err(|e| fail(format!("bad certificate: {e}")))?;
        // the same check as rustls RootCertStore::add, which the AWS SDK unwraps
        webpki::anchor_from_trusted_cert(&cert)
            .map_err(|e| fail(format!("bad certificate: {e}")))?;
        count += 1;
    }
    if count == 0 {
        return Err(fail("no certificate in the file".to_string()));
    }
    Ok(pem)
}

/// The PEM bundle named by `ICECHUNK_TLS_CA_FILE`, or `None` when it is unset or empty.
pub fn tls_ca_bundle() -> Result<Option<Vec<u8>>, StorageError> {
    ca_file_path(std::env::var_os(TLS_CA_FILE_ENV))
        .map(|path| load_tls_ca_file(&path))
        .transpose()
}

fn ca_file_path(value: Option<OsString>) -> Option<PathBuf> {
    value.filter(|v| !v.is_empty()).map(PathBuf::from)
}

/// A hint for the state of `ICECHUNK_TLS_CA_FILE` when an error in the chain of `err`
/// is a certificate rejection.
pub fn tls_hint(err: &(dyn std::error::Error + 'static)) -> Option<String> {
    let mut current = Some(err);
    while let Some(e) = current {
        if e.to_string().contains("invalid peer certificate") {
            return Some(hint_for(tls_ca_bundle()));
        }
        current = e.source();
    }
    None
}

fn hint_for(bundle: Result<Option<Vec<u8>>, StorageError>) -> String {
    match bundle {
        Ok(None) => TLS_HINT.to_string(),
        Err(err) => err.kind().to_string(),
        Ok(Some(_)) => format!(
            "TLS certificate verification failed with the trust roots from \
             {TLS_CA_FILE_ENV}. The file does not contain the CA certificate of the \
             server or of the HTTPS proxy."
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, ffi::OsString, fmt, io::Write as _, path::PathBuf};

    use super::*;

    const TEST_CA: &str = include_str!("../tests/data/test-ca.pem");

    fn pem_file(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file
    }

    #[test]
    fn loads_a_valid_bundle() {
        let contents = format!("{TEST_CA}{TEST_CA}");
        let file = pem_file(&contents);
        assert_eq!(load_tls_ca_file(file.path()).unwrap(), contents.as_bytes());
    }

    #[test]
    fn skips_non_certificate_blocks() {
        let contents = format!(
            "{TEST_CA}-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"
        );
        let file = pem_file(&contents);
        assert!(load_tls_ca_file(file.path()).is_ok());
    }

    #[test]
    fn rejects_a_missing_file() {
        let msg =
            load_tls_ca_file("does-not-exist.pem".as_ref()).unwrap_err().to_string();
        assert!(msg.contains(TLS_CA_FILE_ENV), "{msg}");
        assert!(msg.contains("does-not-exist.pem"), "{msg}");
    }

    #[test]
    fn rejects_a_file_without_certificates() {
        let file = pem_file("");
        let msg = load_tls_ca_file(file.path()).unwrap_err().to_string();
        assert!(msg.contains("no certificate"), "{msg}");
    }

    #[test]
    fn rejects_a_malformed_certificate_block() {
        let file = pem_file(
            "-----BEGIN CERTIFICATE-----\nnot base64!\n-----END CERTIFICATE-----\n",
        );
        assert!(load_tls_ca_file(file.path()).is_err());
    }

    #[test]
    fn rejects_a_certificate_block_with_bad_der() {
        // valid base64 of "not a certificate"
        let file = pem_file(
            "-----BEGIN CERTIFICATE-----\nbm90IGEgY2VydGlmaWNhdGU=\n-----END CERTIFICATE-----\n",
        );
        let msg = load_tls_ca_file(file.path()).unwrap_err().to_string();
        assert!(msg.contains("bad certificate"), "{msg}");
    }

    #[derive(Debug)]
    struct Outer(std::io::Error);

    impl fmt::Display for Outer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("dispatch failure")
        }
    }

    impl Error for Outer {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn hint_matches_nested_tls_error() {
        let err = Outer(std::io::Error::other(
            "client error (Connect): invalid peer certificate: UnknownIssuer",
        ));
        assert!(tls_hint(&err).is_some());
    }

    #[test]
    fn hint_ignores_other_errors() {
        assert_eq!(tls_hint(&std::io::Error::other("connection refused")), None);
    }

    #[test]
    fn hint_without_ca_file_suggests_the_variable() {
        assert_eq!(hint_for(Ok(None)), TLS_HINT);
    }

    #[test]
    fn hint_with_a_bad_ca_file_shows_the_load_error() {
        let hint = hint_for(load_tls_ca_file("missing.pem".as_ref()).map(Some));
        assert!(hint.contains(TLS_CA_FILE_ENV), "{hint}");
        assert!(hint.contains("missing.pem"), "{hint}");
    }

    #[test]
    fn hint_with_a_loaded_ca_file_says_it_lacks_the_ca() {
        let hint = hint_for(Ok(Some(TEST_CA.as_bytes().to_vec())));
        assert!(hint.contains(TLS_CA_FILE_ENV), "{hint}");
        assert!(hint.contains("does not contain"), "{hint}");
    }

    #[test]
    fn empty_variable_is_unset() {
        assert_eq!(ca_file_path(Some(OsString::new())), None);
        assert_eq!(ca_file_path(None), None);
        assert_eq!(ca_file_path(Some("a.pem".into())), Some(PathBuf::from("a.pem")));
    }
}
