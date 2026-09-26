//! `User-Agent` attribution for `object_store` clients.
//!
//! `object_store` clients are frozen at construction, so the per-request
//! fragment travels as a typed [`http::Extensions`] value on `GetOptions`,
//! `PutOptions` and `CopyOptions`. `object_store` copies request extensions
//! into the outgoing HTTP request, where the service installed by
//! [`AttributedHttpConnector`] reads it and writes the whole `user-agent`
//! header. Requests without the extension (list and delete, whose `object_store`
//! methods take no options) get only the icechunk product token. A user agent
//! configured through `ClientOptions` is kept as a prefix.

use async_trait::async_trait;
use object_store::{
    ClientConfigKey, ClientOptions,
    client::{
        HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService,
        ReqwestConnector,
    },
};

/// The icechunk part of the header for one request, as rendered by
/// `RequestAttribution::user_agent_fragment`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserAgentFragment(pub String);

/// Request extensions carrying `fragment` as the request's `user-agent`.
pub fn extensions_for(fragment: String) -> http::Extensions {
    let mut ext = http::Extensions::new();
    ext.insert(UserAgentFragment(fragment));
    ext
}

/// [`HttpConnector`] wrapping [`ReqwestConnector`] so every built client
/// writes the attribution into `user-agent`.
#[derive(Debug, Default)]
pub struct AttributedHttpConnector {
    inner: ReqwestConnector,
}

impl HttpConnector for AttributedHttpConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = self.inner.connect(options)?;
        // icechunk sets no user agent of its own, so any value is the user's
        let user_prefix = options.get_config_value(&ClientConfigKey::UserAgent);
        Ok(HttpClient::new(AttributedHttpService { inner, user_prefix }))
    }
}

#[derive(Debug)]
struct AttributedHttpService {
    inner: HttpClient,
    user_prefix: Option<String>,
}

impl AttributedHttpService {
    fn user_agent_for(&self, req: &HttpRequest) -> String {
        let fragment = match req.extensions().get::<UserAgentFragment>() {
            Some(UserAgentFragment(fragment)) => fragment.as_str(),
            None => icechunk_types::user_agent_product(),
        };
        match &self.user_prefix {
            Some(prefix) => format!("{prefix} {fragment}"),
            None => fragment.to_string(),
        }
    }
}

#[async_trait]
impl HttpService for AttributedHttpService {
    async fn call(&self, mut req: HttpRequest) -> Result<HttpResponse, HttpError> {
        // the fragment is ASCII by construction; a failure here can only come
        // from a caller-built extension, and then the header is left alone
        if let Ok(value) = http::HeaderValue::from_str(&self.user_agent_for(&req)) {
            req.headers_mut().insert(http::header::USER_AGENT, value);
        }
        self.inner.execute(req).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use object_store::client::{HttpRequestBody, HttpResponseBody};

    use super::*;

    /// Inner service recording the `user-agent` it receives.
    #[derive(Debug)]
    struct Recording(Arc<Mutex<Vec<Option<String>>>>);

    #[async_trait]
    impl HttpService for Recording {
        async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
            let ua = req
                .headers()
                .get(http::header::USER_AGENT)
                .map(|v| v.to_str().unwrap().to_string());
            self.0.lock().unwrap().push(ua);
            Ok(http::Response::builder()
                .status(200)
                .body(HttpResponseBody::from(Vec::new()))
                .unwrap())
        }
    }

    fn request(fragment: Option<&str>) -> HttpRequest {
        let mut req = http::Request::builder()
            .method(http::Method::GET)
            .uri("http://store.example/object")
            .body(HttpRequestBody::empty())
            .unwrap();
        if let Some(f) = fragment {
            req.extensions_mut().insert(UserAgentFragment(f.to_string()));
        }
        req
    }

    #[icechunk_macros::tokio_test]
    async fn fragment_becomes_the_user_agent() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let service = AttributedHttpService {
            inner: HttpClient::new(Recording(Arc::clone(&seen))),
            user_prefix: None,
        };
        service.call(request(Some("icechunk/9 (array=a)"))).await.unwrap();
        service.call(request(None)).await.unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![
                Some("icechunk/9 (array=a)".to_string()),
                Some(icechunk_types::user_agent_product().to_string()),
            ]
        );
    }

    #[icechunk_macros::tokio_test]
    async fn user_agent_option_is_kept_as_prefix() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let service = AttributedHttpService {
            inner: HttpClient::new(Recording(Arc::clone(&seen))),
            user_prefix: Some("myapp/2".to_string()),
        };
        service.call(request(Some("icechunk/9 (array=a)"))).await.unwrap();
        service.call(request(None)).await.unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![
                Some("myapp/2 icechunk/9 (array=a)".to_string()),
                Some(format!("myapp/2 {}", icechunk_types::user_agent_product())),
            ]
        );
    }

    #[test]
    fn connector_reads_the_user_agent_option() {
        let options =
            ClientOptions::new().with_config(ClientConfigKey::UserAgent, "myapp/2");
        assert_eq!(
            options.get_config_value(&ClientConfigKey::UserAgent).as_deref(),
            Some("myapp/2")
        );
        assert_eq!(
            ClientOptions::new().get_config_value(&ClientConfigKey::UserAgent),
            None
        );
    }
}
