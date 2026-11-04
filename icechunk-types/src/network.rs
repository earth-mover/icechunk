//! Wording for storage hosts that could not be reached at the network level.

use std::fmt::{self, Display};

/// How a request to a storage host failed at the network level.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NetworkFailure {
    Connect,
    Timeout,
    Tls,
    /// Something answered, but not the storage service.
    UnexpectedResponse {
        status: u16,
        content_type: Option<String>,
        snippet: Option<String>,
    },
}

impl Display for NetworkFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkFailure::Connect => write!(f, "could not connect"),
            NetworkFailure::Timeout => write!(f, "the request timed out"),
            NetworkFailure::Tls => write!(f, "the TLS handshake failed"),
            NetworkFailure::UnexpectedResponse { status, content_type, snippet } => {
                write!(
                    f,
                    "got an unexpected response (HTTP {status}, Content-Type: {}) instead of a storage service response",
                    content_type.as_deref().unwrap_or("none")
                )?;
                if let Some(snippet) = snippet {
                    write!(f, "; response begins: \"{snippet}\"")?;
                }
                Ok(())
            }
        }
    }
}

impl NetworkFailure {
    fn hint(&self) -> &'static str {
        match self {
            NetworkFailure::Tls => {
                " If your network uses a TLS-inspecting proxy, its CA certificate may need to be trusted, for example by setting ICECHUNK_TLS_CA_FILE to a PEM bundle that includes it."
            }
            _ => "",
        }
    }
}

/// The user-facing text for an unreachable storage host.
pub fn unreachable_message(host: &str, failure: &NetworkFailure) -> String {
    format!(
        "could not reach storage at {host}: {failure}. This can happen when a firewall or proxy blocks or intercepts traffic to {host}. If you are on a corporate or institutional network, ask your network administrator to allow access to this host.{}",
        failure.hint()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_message_points_at_ca_file() {
        assert!(unreachable_message("h.invalid", &NetworkFailure::Tls).ends_with(
            " If your network uses a TLS-inspecting proxy, its CA certificate may need to be trusted, for example by setting ICECHUNK_TLS_CA_FILE to a PEM bundle that includes it."
        ));
    }

    #[test]
    fn unexpected_response_without_snippet_or_content_type() {
        let failure = NetworkFailure::UnexpectedResponse {
            status: 407,
            content_type: None,
            snippet: None,
        };
        assert_eq!(
            failure.to_string(),
            "got an unexpected response (HTTP 407, Content-Type: none) instead of a storage service response"
        );
    }
}
