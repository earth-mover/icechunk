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

/// The user-facing text for an unreachable storage host.
///
/// Only failures that point at the network path (no connection, or a response
/// that did not come from the storage service) suggest a firewall or proxy.
/// Timeouts and TLS failures have common causes unrelated to the network, so
/// their wording names those causes too.
pub fn unreachable_message(host: &str, failure: &NetworkFailure) -> String {
    match failure {
        NetworkFailure::Connect => format!(
            "could not reach storage at {host}: {failure}. Check that the endpoint is correct. If it is, a firewall or proxy may be blocking traffic to {host}; on a corporate or institutional network, ask your network administrator to allow access to this host."
        ),
        NetworkFailure::Timeout => format!(
            "request to storage at {host} timed out. On a slow connection, large transfers can exceed the per-attempt timeout (operation_attempt_timeout_ms in the storage settings). If no request to {host} ever succeeds, a firewall or proxy may be silently dropping the traffic."
        ),
        NetworkFailure::Tls => format!(
            "could not establish a TLS connection to storage at {host}. If the server certificate is not trusted, either because this process cannot use the system trust store (for example, in a macOS sandbox) or because a TLS-inspecting proxy intercepts the traffic, set ICECHUNK_TLS_CA_FILE to a PEM CA bundle that includes the right CA certificate (for example, the path from certifi.where(), plus the proxy's CA if there is one)."
        ),
        NetworkFailure::UnexpectedResponse { .. } => format!(
            "could not reach storage at {host}: {failure}. This can happen when a firewall or proxy blocks or intercepts traffic to {host}. If you are on a corporate or institutional network, ask your network administrator to allow access to this host."
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_message_covers_trust_store_and_proxy() {
        let msg = unreachable_message("h.invalid", &NetworkFailure::Tls);
        assert!(msg.contains("ICECHUNK_TLS_CA_FILE"));
        assert!(msg.contains("macOS sandbox"));
        assert!(msg.contains("TLS-inspecting proxy"));
        assert!(!msg.contains("firewall"));
    }

    #[test]
    fn timeout_message_does_not_only_blame_the_network() {
        let msg = unreachable_message("h.invalid", &NetworkFailure::Timeout);
        assert!(msg.contains("operation_attempt_timeout_ms"));
        assert!(!msg.contains("network administrator"));
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
