//! HTTP client that speaks only HTTP/1.1. Hyper's HTTP/2 client reads much slower, and the SDK
//! client cannot turn HTTP/2 off: <https://github.com/earth-mover/icechunk/issues/2571>.

use std::{
    borrow::Cow,
    collections::HashMap,
    error::Error,
    fmt, io,
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
    connector_metadata::ConnectorMetadata,
    http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings,
        SharedHttpConnector,
    },
    orchestrator::{HttpRequest, HttpResponse},
    result::ConnectorError,
};
use aws_smithy_types::{body::SdkBody, retry::ErrorKind};
use http::{Uri, header::PROXY_AUTHORIZATION, uri::Scheme};
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper_rustls::{HttpsConnector, MaybeHttpsStream};
use hyper_util::{
    client::{
        legacy::{
            Client,
            connect::{
                Connected, Connection, HttpConnector as TcpConnector, proxy::Tunnel,
            },
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
        tls: Arc::new(tls_config(roots)),
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
    if store.is_empty() {
        tracing::warn!("no system TLS root certificates found, HTTPS requests will fail");
    }
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
    let mut config = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("aws-lc-rs supports the default TLS versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config
}

type Timeouts = (Option<Duration>, Option<Duration>);

struct Http1Client {
    tls: Arc<ClientConfig>,
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
                    Arc::clone(&self.tls),
                    Arc::clone(&self.proxies),
                    timeouts,
                ))
            })
            .clone()
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        Some(ConnectorMetadata::new("hyper", Some(Cow::Borrowed("1.x"))))
    }
}

type ProxyIo = MaybeHttpsStream<TokioIo<TcpStream>>;

/// A TCP stream to the endpoint, or a stream to or through the proxy that the env vars select.
struct Conn {
    io: ProxyIo,
    // Hyper sends absolute-form request targets only on a connection marked as proxied.
    forward_proxy: bool,
}

impl Connection for Conn {
    fn connected(&self) -> Connected {
        self.io.connected().proxy(self.forward_proxy)
    }
}

impl Read for Conn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl Write for Conn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }
}

type Connecting<T> = Pin<Box<dyn Future<Output = Result<T, BoxError>> + Send>>;

#[derive(Clone)]
struct Transport {
    tcp: TcpConnector,
    // Speaks TLS to an `https://` proxy, so the CONNECT and its credentials stay encrypted.
    to_proxy: HttpsConnector<TcpConnector>,
    proxies: Arc<Matcher>,
}

impl Service<Uri> for Transport {
    type Response = Conn;
    type Error = BoxError;
    type Future = Connecting<Conn>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let mut tcp = self.tcp.clone();
        let mut to_proxy = self.to_proxy.clone();
        let proxy = self.proxies.intercept(&dst);
        Box::pin(async move {
            let Some(proxy) = proxy else {
                let io = MaybeHttpsStream::Http(tcp.call(dst).await?);
                return Ok(Conn { io, forward_proxy: false });
            };
            let scheme = proxy.uri().scheme_str().unwrap_or_default();
            if !matches!(scheme, "http" | "https") {
                return Err(format!("unsupported proxy scheme `{scheme}`").into());
            }
            if dst.scheme() == Some(&Scheme::HTTPS) {
                let mut tunnel = Tunnel::new(proxy.uri().clone(), to_proxy);
                if let Some(auth) = proxy.basic_auth() {
                    tunnel = tunnel.with_auth(auth.clone());
                }
                Ok(Conn { io: tunnel.call(dst).await?, forward_proxy: false })
            } else {
                let io = to_proxy.call(proxy.uri().clone()).await?;
                Ok(Conn { io, forward_proxy: true })
            }
        })
    }
}

#[derive(Debug)]
struct ConnectTimeout(Duration);

impl fmt::Display for ConnectTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HTTP connect timeout occurred after {:?}", self.0)
    }
}

impl Error for ConnectTimeout {}

/// One limit for DNS, TCP, the proxy handshake and TLS together, as in the SDK client.
#[derive(Clone)]
struct WithConnectTimeout {
    inner: HttpsConnector<Transport>,
    limit: Option<Duration>,
}

impl Service<Uri> for WithConnectTimeout {
    type Response = MaybeHttpsStream<Conn>;
    type Error = BoxError;
    type Future = Connecting<Self::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let connecting = self.inner.call(dst);
        let limit = self.limit;
        Box::pin(async move {
            match limit {
                None => connecting.await,
                Some(limit) => tokio::time::timeout(limit, connecting)
                    .await
                    .map_err(|_| ConnectTimeout(limit))?,
            }
        })
    }
}

struct Http1Connector {
    client: Client<WithConnectTimeout, SdkBody>,
    proxies: Arc<Matcher>,
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
        tls: Arc<ClientConfig>,
        proxies: Arc<Matcher>,
        (connect_timeout, read_timeout): Timeouts,
    ) -> Self {
        let mut tcp = TcpConnector::new();
        tcp.enforce_http(false);
        tcp.set_nodelay(true);
        let transport = Transport {
            to_proxy: HttpsConnector::from((tcp.clone(), Arc::clone(&tls))),
            tcp,
            proxies: Arc::clone(&proxies),
        };
        let connector = WithConnectTimeout {
            inner: HttpsConnector::from((transport, tls)),
            limit: connect_timeout,
        };
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .build(connector);
        Self { client, proxies, read_timeout }
    }
}

impl HttpConnector for Http1Connector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let mut request = match request.try_into_http1x() {
            Ok(request) => request,
            Err(err) => {
                return HttpConnectorFuture::ready(Err(ConnectorError::user(err.into())));
            }
        };
        // A tunnel sends the proxy credentials in its CONNECT; a forward proxy reads them here.
        if request.uri().scheme() == Some(&Scheme::HTTP)
            && !request.headers().contains_key(PROXY_AUTHORIZATION)
            && let Some(proxy) = self.proxies.intercept(request.uri())
            && let Some(auth) = proxy.basic_auth()
        {
            request.headers_mut().insert(PROXY_AUTHORIZATION, auth.clone());
        }
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
    if chain().any(|e| e.is::<ConnectTimeout>()) {
        return Kind::Timeout;
    }
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
    if connect_failed || chain().any(|e| e.is::<io::Error>()) {
        Kind::Io
    } else {
        Kind::Other
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use aws_smithy_runtime_api::client::{
        http::HttpConnector as _,
        orchestrator::{HttpRequest, HttpResponse},
        result::ConnectorError,
    };
    use aws_smithy_types::body::SdkBody;
    use hyper_util::client::proxy::matcher::Matcher;
    use icechunk_macros::tokio_test;
    use rustls::ServerConfig;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};
    use tokio::{
        io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
        net::{TcpListener, TcpStream},
        sync::mpsc::{UnboundedReceiver, unbounded_channel},
    };
    use tokio_rustls::TlsAcceptor;

    use super::{Http1Connector, TrustRoots, tls_config};

    const CA: &[u8] = include_bytes!("../../icechunk-python/tests/data/tls/ca.pem");
    const CERT: &[u8] = include_bytes!("../../icechunk-python/tests/data/tls/server.pem");
    const KEY: &[u8] = include_bytes!("../../icechunk-python/tests/data/tls/server.key");
    const OK: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
    const AUTH: &str = "proxy-authorization: basic dxnlcjpzzwnyzxq=";

    /// A TLS server side for `localhost` that offers `h2` before `http/1.1`.
    fn acceptor() -> TlsAcceptor {
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
        TlsAcceptor::from(Arc::new(server))
    }

    fn connector(proxies: Matcher, connect_timeout: Option<Duration>) -> Http1Connector {
        Http1Connector::new(
            Arc::new(tls_config(TrustRoots::Pem(CA.to_vec()))),
            Arc::new(proxies),
            (connect_timeout, None),
        )
    }

    fn proxy(url: String) -> Matcher {
        Matcher::builder().all(url).build()
    }

    async fn get(
        connector: &Http1Connector,
        url: &str,
    ) -> Result<HttpResponse, ConnectorError> {
        let request = http::Request::get(url).body(SdkBody::empty()).unwrap();
        connector.call(HttpRequest::try_from(request).unwrap()).await
    }

    /// Reads a request head in lower case, or `None` when the peer sends none.
    async fn read_head(io: &mut (impl AsyncRead + Unpin)) -> Option<String> {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(io.read_u8().await.ok()?);
        }
        Some(String::from_utf8_lossy(&head).to_lowercase())
    }

    /// Runs `handle` on every connection to a new local port. A handler that returns `None`
    /// is ignored, so a stray connection cannot fail a test.
    async fn serve<F, Fut>(handle: F) -> (u16, UnboundedReceiver<String>)
    where
        F: Fn(TcpStream) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<String>> + Send,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (seen, rx) = unbounded_channel();
        let handle = Arc::new(handle);
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let (handle, seen) = (Arc::clone(&handle), seen.clone());
                tokio::spawn(async move {
                    if let Some(observed) = handle(tcp).await {
                        let _ = seen.send(observed);
                    }
                });
            }
        });
        (port, rx)
    }

    async fn answer(mut io: impl AsyncRead + AsyncWrite + Unpin) -> Option<String> {
        let head = read_head(&mut io).await?;
        io.write_all(OK).await.ok()?;
        Some(head)
    }

    /// Answers a CONNECT, then serves the tunneled TLS connection. Returns the CONNECT head.
    async fn tunnel(mut io: impl AsyncRead + AsyncWrite + Unpin) -> Option<String> {
        let connect = read_head(&mut io).await?;
        io.write_all(b"HTTP/1.1 200 OK\r\n\r\n").await.ok()?;
        answer(acceptor().accept(io).await.ok()?).await?;
        Some(connect)
    }

    async fn stall(tcp: TcpStream) -> Option<String> {
        tokio::time::sleep(Duration::from_secs(60)).await;
        drop(tcp);
        None
    }

    async fn timeout_error(connector: &Http1Connector, url: &str) -> ConnectorError {
        tokio::time::timeout(Duration::from_secs(10), get(connector, url))
            .await
            .expect("the connect timeout must end the attempt")
            .unwrap_err()
    }

    #[tokio_test]
    async fn negotiates_http1_with_a_server_that_offers_h2() {
        let (port, mut seen) = serve(|tcp| async {
            let tls = acceptor().accept(tcp).await.ok()?;
            let alpn = tls.get_ref().1.alpn_protocol().map(<[u8]>::to_vec)?;
            answer(tls).await?;
            String::from_utf8(alpn).ok()
        })
        .await;
        let connector = connector(Matcher::builder().build(), None);
        let response = get(&connector, &format!("https://localhost:{port}/")).await;
        assert_eq!(response.unwrap().status().as_u16(), 200);
        assert_eq!(seen.recv().await.unwrap(), "http/1.1");
    }

    #[tokio_test]
    async fn connect_timeout_covers_a_stalled_tls_handshake() {
        let (port, _seen) = serve(stall).await;
        let connector =
            connector(Matcher::builder().build(), Some(Duration::from_millis(200)));
        let err = timeout_error(&connector, &format!("https://localhost:{port}/")).await;
        assert!(err.is_timeout(), "{err:?}");
    }

    #[tokio_test]
    async fn connect_timeout_covers_a_stalled_proxy() {
        let (port, _seen) = serve(stall).await;
        let connector = connector(
            proxy(format!("http://127.0.0.1:{port}")),
            Some(Duration::from_millis(200)),
        );
        let err = timeout_error(&connector, "https://localhost:1/").await;
        assert!(err.is_timeout(), "{err:?}");
    }

    #[tokio_test]
    async fn refused_connections_are_io_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let connector = connector(Matcher::builder().build(), None);
        let err =
            get(&connector, &format!("https://localhost:{port}/")).await.unwrap_err();
        assert!(err.is_io(), "{err:?}");
    }

    #[tokio_test]
    async fn https_endpoints_tunnel_through_an_http_proxy() {
        let (port, mut seen) = serve(tunnel).await;
        let connector =
            connector(proxy(format!("http://user:secret@127.0.0.1:{port}")), None);
        let response = get(&connector, "https://localhost:1/").await;
        assert_eq!(response.unwrap().status().as_u16(), 200);
        let connect = seen.recv().await.unwrap();
        assert!(connect.starts_with("connect localhost:1 "), "{connect}");
        assert!(connect.contains(AUTH), "{connect}");
    }

    #[tokio_test]
    async fn an_https_proxy_gets_the_connect_inside_tls() {
        let (port, mut seen) =
            serve(|tcp| async { tunnel(acceptor().accept(tcp).await.ok()?).await }).await;
        let connector =
            connector(proxy(format!("https://user:secret@localhost:{port}")), None);
        let response = get(&connector, "https://localhost:1/").await;
        assert_eq!(response.unwrap().status().as_u16(), 200);
        let connect = seen.recv().await.unwrap();
        assert!(connect.starts_with("connect localhost:1 "), "{connect}");
        assert!(connect.contains(AUTH), "{connect}");
    }

    #[tokio_test]
    async fn http_endpoints_use_a_forward_proxy() {
        let (port, mut seen) = serve(answer).await;
        let connector =
            connector(proxy(format!("http://user:secret@127.0.0.1:{port}")), None);
        let response = get(&connector, "http://endpoint.invalid/bucket/key").await;
        assert_eq!(response.unwrap().status().as_u16(), 200);
        let head = seen.recv().await.unwrap();
        assert!(head.starts_with("get http://endpoint.invalid/bucket/key "), "{head}");
        assert!(head.contains(AUTH), "{head}");
    }

    #[tokio_test]
    async fn no_proxy_hosts_connect_directly() {
        let (port, mut seen) =
            serve(|tcp| async { answer(acceptor().accept(tcp).await.ok()?).await }).await;
        let proxies =
            Matcher::builder().all("http://127.0.0.1:1").no("localhost").build();
        let connector = connector(proxies, None);
        let response = get(&connector, &format!("https://localhost:{port}/")).await;
        assert_eq!(response.unwrap().status().as_u16(), 200);
        assert!(seen.recv().await.unwrap().starts_with("get / "));
    }

    #[tokio_test]
    async fn other_proxy_schemes_are_rejected() {
        let connector = connector(proxy("socks5://127.0.0.1:1".to_string()), None);
        let err = get(&connector, "https://localhost:1/").await.unwrap_err();
        assert!(format!("{err:?}").contains("unsupported proxy scheme"), "{err:?}");
    }
}
