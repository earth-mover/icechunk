const HTTPS_PROXY_ENV_VARS: [&str; 4] =
    ["HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TunnelFailure {
    AuthRequired,
    Refused,
    Unreachable,
    Other,
}

/// A hint that names the proxy when an error in the chain of `err` is a failed proxy tunnel.
pub fn proxy_hint(err: &(dyn std::error::Error + 'static)) -> Option<String> {
    let failure = std::iter::successors(Some(err), |e| e.source())
        .find_map(|e| tunnel_failure(&e.to_string()))?;
    Some(hint_for(failure, configured_proxy().as_ref()))
}

fn tunnel_failure(msg: &str) -> Option<TunnelFailure> {
    let (_, detail) = msg.split_once("tunnel error: ")?;
    Some(match detail {
        "proxy authorization required" => TunnelFailure::AuthRequired,
        "unsuccessful" => TunnelFailure::Refused,
        "failed to create underlying connection" => TunnelFailure::Unreachable,
        _ => TunnelFailure::Other,
    })
}

fn configured_proxy() -> Option<(&'static str, String)> {
    HTTPS_PROXY_ENV_VARS.iter().find_map(|&name| {
        std::env::var(name)
            .ok()
            .filter(|url| !url.is_empty())
            .map(|url| (name, redact_password(&url)))
    })
}

fn redact_password(url: &str) -> String {
    let (scheme, rest) = url.split_once("://").unwrap_or(("", url));
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, path) = rest.split_at(authority_end);
    match authority.rsplit_once('@').and_then(|(userinfo, host)| {
        userinfo.split_once(':').map(|(user, _)| (user, host))
    }) {
        Some((user, host)) => {
            let scheme =
                if scheme.is_empty() { String::new() } else { format!("{scheme}://") };
            format!("{scheme}{user}:***@{host}{path}")
        }
        None => url.to_string(),
    }
}

fn hint_for(failure: TunnelFailure, proxy: Option<&(&str, String)>) -> String {
    let proxy = match proxy {
        Some((name, url)) => format!("proxy {url} (from {name})"),
        None => "HTTPS proxy".to_string(),
    };
    match failure {
        TunnelFailure::AuthRequired => format!(
            "The {proxy} answered HTTP 407 Proxy Authentication Required. Put the user name and \
             password in the proxy URL, for example \
             http://user:password@proxy.example.com:8080. If the URL already has them, \
             the proxy rejected them."
        ),
        TunnelFailure::Refused => format!(
            "The CONNECT request to the {proxy} failed. The proxy can block the object store \
             host, or it cannot reach that host. Ask your network administrator to allow \
             the object store host. If the object store is reachable without the proxy, \
             add its host to NO_PROXY."
        ),
        TunnelFailure::Unreachable => {
            format!(
                "Cannot connect to the {proxy}. Check the host and port of the proxy URL."
            )
        }
        TunnelFailure::Other => format!("The connection through the {proxy} failed."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_tunnel_failures_anywhere_in_the_message() {
        assert_eq!(
            tunnel_failure("tunnel error: proxy authorization required"),
            Some(TunnelFailure::AuthRequired)
        );
        assert_eq!(
            tunnel_failure("client error (Connect): tunnel error: unsuccessful"),
            Some(TunnelFailure::Refused)
        );
        assert_eq!(
            tunnel_failure("tunnel error: failed to create underlying connection"),
            Some(TunnelFailure::Unreachable)
        );
        assert_eq!(
            tunnel_failure("tunnel error: unexpected end of file"),
            Some(TunnelFailure::Other)
        );
        assert_eq!(tunnel_failure("connection refused"), None);
    }

    #[derive(Debug)]
    struct Chained(&'static str, Option<Box<Chained>>);

    impl std::fmt::Display for Chained {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Chained {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|e| e as _)
        }
    }

    #[test]
    fn walks_the_error_chain() {
        let err = Chained(
            "dispatch failure",
            Some(Box::new(Chained("tunnel error: unsuccessful", None))),
        );
        let hint = proxy_hint(&err).unwrap_or_default();
        assert!(hint.contains("NO_PROXY"), "{hint}");
        assert_eq!(proxy_hint(&Chained("dispatch failure", None)), None);
    }

    #[test]
    fn hint_names_the_variable_and_the_url() {
        let proxy = ("HTTPS_PROXY", "http://proxy.corp:3128".to_string());
        let hint = hint_for(TunnelFailure::AuthRequired, Some(&proxy));
        assert!(hint.contains("http://proxy.corp:3128 (from HTTPS_PROXY)"), "{hint}");
        assert!(hint.contains("407"), "{hint}");
    }

    #[test]
    fn hint_without_a_variable_says_https_proxy() {
        let hint = hint_for(TunnelFailure::Unreachable, None);
        assert!(hint.starts_with("Cannot connect to the HTTPS proxy."), "{hint}");
    }

    #[test]
    fn redacts_the_password() {
        assert_eq!(
            redact_password("http://alice:s3cr@t@proxy.corp:3128/"),
            "http://alice:***@proxy.corp:3128/"
        );
        assert_eq!(redact_password("proxy.corp:3128"), "proxy.corp:3128");
        assert_eq!(
            redact_password("http://alice@proxy.corp:3128"),
            "http://alice@proxy.corp:3128"
        );
        assert_eq!(redact_password("http://proxy.corp/a@b"), "http://proxy.corp/a@b");
        assert_eq!(redact_password("bob:pw@proxy.corp"), "bob:***@proxy.corp");
    }
}
