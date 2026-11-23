//! HTTPS client that offers only HTTP/1.1. Hyper's HTTP/2 client reads 2-7x slower, and the SDK
//! client cannot turn HTTP/2 off: <https://github.com/earth-mover/icechunk/issues/2571>.

use std::{
    collections::HashMap,
    error::Error,
    fmt,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use aws_sdk_s3::{
    config::{RuntimeComponents, SharedHttpClient},
    error::BoxError,
};
use aws_smithy_runtime_api::client::{
    http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings,
        SharedHttpConnector,
    },
    orchestrator::{HttpRequest, HttpResponse},
    result::ConnectorError,
};
use aws_smithy_types::{body::SdkBody, retry::ErrorKind};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::{
        legacy::{
            Client,
            connect::{HttpConnector as TcpConnector, proxy::Tunnel},
        },
        proxy::matcher::Matcher,
    },
    rt::{TokioExecutor, TokioIo, TokioTimer},
};
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{CertificateDer, pem::PemObject as _};
use tokio::net::TcpStream;
use tower_service::Service;

#[derive(Debug)]
pub(crate) enum TrustRoots {
    System,
    Pem(Vec<u8>),
    Nothing,
}

pub(crate) fn https_client(roots: TrustRoots) -> SharedHttpClient {
    SharedHttpClient::new(Http1Client {
        tls: tls_config(roots),
        proxies: Arc::new(Matcher::from_env()),
        connectors: Mutex::default(),
    })
}

static SYSTEM_ROOTS: LazyLock<Arc<RootCertStore>> = LazyLock::new(|| {
    let native = rustls_native_certs::load_native_certs();
    for err in &native.errors {
        tracing::warn!("cannot load a system TLS root certificate: {err}");
    }
    let mut store = RootCertStore::empty();
    store.add_parsable_certificates(native.certs);
    Arc::new(store)
});

fn tls_config(roots: TrustRoots) -> ClientConfig {
    let roots = match roots {
        TrustRoots::System => Arc::clone(&SYSTEM_ROOTS),
        TrustRoots::Pem(pem) => {
            let mut store = RootCertStore::empty();
            store.add_parsable_certificates(
                CertificateDer::pem_slice_iter(&pem).filter_map(Result::ok),
            );
            Arc::new(store)
        }
        TrustRoots::Nothing => Arc::new(RootCertStore::empty()),
    };
    #[expect(clippy::expect_used)]
    ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("aws-lc-rs supports the default TLS versions")
    .with_root_certificates(roots)
    .with_no_client_auth()
}

type Timeouts = (Option<Duration>, Option<Duration>);

struct Http1Client {
    tls: ClientConfig,
    proxies: Arc<Matcher>,
    // The SDK asks for a connector per request; caching keeps the connection pool alive.
    connectors: Mutex<HashMap<Timeouts, SharedHttpConnector>>,
}

impl fmt::Debug for Http1Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Http1Client").finish_non_exhaustive()
    }
}

impl HttpClient for Http1Client {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        let timeouts = (settings.connect_timeout(), settings.read_timeout());
        let mut connectors =
            self.connectors.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        connectors
            .entry(timeouts)
            .or_insert_with(|| {
                SharedHttpConnector::new(Http1Connector::new(
                    self.tls.clone(),
                    Arc::clone(&self.proxies),
                    timeouts,
                ))
            })
            .clone()
    }
}

/// TCP connections, through a CONNECT tunnel when the proxy env vars select one for the host.
#[derive(Clone)]
struct Transport {
    tcp: TcpConnector,
    proxies: Arc<Matcher>,
}

impl Service<http::Uri> for Transport {
    type Response = TokioIo<TcpStream>;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: http::Uri) -> Self::Future {
        let mut tcp = self.tcp.clone();
        // A plain-HTTP endpoint connects directly: a proxy would need absolute-form requests.
        let proxy = (dst.scheme() == Some(&http::uri::Scheme::HTTPS))
            .then(|| self.proxies.intercept(&dst))
            .flatten();
        Box::pin(async move {
            match proxy {
                None => Ok(tcp.call(dst).await?),
                Some(proxy) => {
                    let mut tunnel = Tunnel::new(proxy.uri().clone(), tcp);
                    if let Some(auth) = proxy.basic_auth() {
                        tunnel = tunnel.with_auth(auth.clone());
                    }
                    Ok(tunnel.call(dst).await?)
                }
            }
        })
    }
}

struct Http1Connector {
    client: Client<HttpsConnector<Transport>, SdkBody>,
    read_timeout: Option<Duration>,
}

impl fmt::Debug for Http1Connector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Http1Connector")
            .field("read_timeout", &self.read_timeout)
            .finish_non_exhaustive()
    }
}

impl Http1Connector {
    fn new(
        tls: ClientConfig,
        proxies: Arc<Matcher>,
        (connect_timeout, read_timeout): Timeouts,
    ) -> Self {
        let mut tcp = TcpConnector::new();
        tcp.enforce_http(false);
        tcp.set_nodelay(true);
        tcp.set_connect_timeout(connect_timeout);
        let https = HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .wrap_connector(Transport { tcp, proxies });
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .build(https);
        Self { client, read_timeout }
    }
}

impl HttpConnector for Http1Connector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let request = match request.try_into_http1x() {
            Ok(request) => request,
            Err(err) => {
                return HttpConnectorFuture::ready(Err(ConnectorError::user(err.into())));
            }
        };
        let response = self.client.request(request);
        let read_timeout = self.read_timeout;
        HttpConnectorFuture::new(async move {
            let response = match read_timeout {
                None => response.await,
                Some(limit) => tokio::time::timeout(limit, response)
                    .await
                    .map_err(|elapsed| ConnectorError::timeout(elapsed.into()))?,
            }
            .map_err(|err| classify(err.into()))?;
            HttpResponse::try_from(response.map(SdkBody::from_body_1_x))
                .map_err(|err| ConnectorError::other(err.into(), None))
        })
    }
}

/// Sorts errors into the kinds the SDK retry policy reads, the same way the SDK client does.
fn classify(err: BoxError) -> ConnectorError {
    let kind = error_kind(err.as_ref());
    match kind {
        Kind::Timeout => ConnectorError::timeout(err),
        Kind::User => ConnectorError::user(err),
        Kind::Io => ConnectorError::io(err),
        Kind::Transient => ConnectorError::other(err, Some(ErrorKind::TransientError)),
        Kind::Other => ConnectorError::other(err, None),
    }
}

enum Kind {
    Timeout,
    User,
    Io,
    Transient,
    Other,
}

fn error_kind(err: &(dyn Error + 'static)) -> Kind {
    let chain = || std::iter::successors(Some(err), |&e| e.source());
    if let Some(hyper_err) = chain().find_map(|e| e.downcast_ref::<hyper::Error>()) {
        if hyper_err.is_timeout() {
            return Kind::Timeout;
        }
        if hyper_err.is_user() {
            return Kind::User;
        }
        if hyper_err.is_closed() || hyper_err.is_canceled() {
            return Kind::Io;
        }
        // S3 sometimes closes a connection mid-response.
        if hyper_err.is_incomplete_message() {
            return Kind::Transient;
        }
    }
    let connect_failed = chain()
        .find_map(|e| e.downcast_ref::<hyper_util::client::legacy::Error>())
        .is_some_and(hyper_util::client::legacy::Error::is_connect);
    if connect_failed || chain().any(|e| e.is::<std::io::Error>()) {
        Kind::Io
    } else {
        Kind::Other
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aws_smithy_runtime_api::client::{
        http::HttpConnector as _, orchestrator::HttpRequest,
    };
    use aws_smithy_types::body::SdkBody;
    use hyper_util::client::proxy::matcher::Matcher;
    use rustls::ServerConfig;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};
    use tokio::{io::AsyncWriteExt as _, net::TcpListener};
    use tokio_rustls::TlsAcceptor;

    use super::{Http1Connector, TrustRoots, tls_config};

    const CA: &[u8] = include_bytes!("../../icechunk-python/tests/data/tls/ca.pem");
    const CERT: &[u8] = include_bytes!("../../icechunk-python/tests/data/tls/server.pem");
    const KEY: &[u8] = include_bytes!("../../icechunk-python/tests/data/tls/server.key");

    /// Serves one empty `200` over TLS and returns the ALPN protocol the client picked.
    async fn negotiated_protocol(connector: Http1Connector) -> Option<Vec<u8>> {
        let mut server = ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            CertificateDer::pem_slice_iter(CERT).collect::<Result<_, _>>().unwrap(),
            PrivateKeyDer::from_pem_slice(KEY).unwrap(),
        )
        .unwrap();
        server.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let acceptor = TlsAcceptor::from(Arc::new(server));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let serve = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.unwrap();
            let alpn = tls.get_ref().1.alpn_protocol().map(<[u8]>::to_vec);
            tls.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n").await.unwrap();
            tls.shutdown().await.unwrap();
            alpn
        });

        let request = http::Request::get(format!("https://localhost:{port}/"))
            .body(SdkBody::empty())
            .unwrap();
        let response =
            connector.call(HttpRequest::try_from(request).unwrap()).await.unwrap();
        assert_eq!(response.status().as_u16(), 200);
        serve.await.unwrap()
    }

    #[tokio::test]
    async fn negotiates_http1_with_a_server_that_offers_h2() {
        let connector = Http1Connector::new(
            tls_config(TrustRoots::Pem(CA.to_vec())),
            Arc::new(Matcher::builder().build()),
            (None, None),
        );
        // hyper-rustls sends no ALPN for HTTP/1-only, and without ALPN no server can pick h2.
        assert_ne!(negotiated_protocol(connector).await.as_deref(), Some(&b"h2"[..]));
    }
}
