//! Minimal in-process HTTP/1.1 server for exercising the fast-list fetchers end
//! to end — auth headers, status handling, body scanning, credential refresh,
//! and timeouts — without a real cloud endpoint. Each connection serves exactly
//! one request and closes; a handler closure maps the request to a canned
//! response (optionally delayed, to drive timeout behavior).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub(crate) struct FakeRequest {
    pub authorization: Option<String>,
}

pub(crate) struct FakeResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub delay: Option<Duration>,
}

impl FakeResponse {
    pub(crate) fn ok(body: impl Into<Vec<u8>>) -> Self {
        Self { status: 200, body: body.into(), delay: None }
    }

    pub(crate) fn status(status: u16) -> Self {
        Self { status, body: Vec::new(), delay: None }
    }

    /// Accept the request but stall past any realistic client timeout without
    /// responding, so a small configured timeout fires.
    pub(crate) fn stall() -> Self {
        Self { status: 200, body: Vec::new(), delay: Some(Duration::from_secs(30)) }
    }
}

pub(crate) struct FakeServer {
    addr: SocketAddr,
    handle: JoinHandle<()>,
}

impl FakeServer {
    #[expect(clippy::expect_used, reason = "test harness: fail loudly on setup errors")]
    pub(crate) async fn start<H>(handler: H) -> Self
    where
        H: Fn(FakeRequest) -> FakeResponse + Send + Sync + 'static,
    {
        let listener =
            TcpListener::bind("127.0.0.1:0").await.expect("bind loopback listener");
        let addr = listener.local_addr().expect("resolve local addr");
        let handler = Arc::new(handler);
        let handle = tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let handler = Arc::clone(&handler);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 1024];
                    loop {
                        match sock.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&tmp[..n]),
                        }
                        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let resp = handler(parse_request(&buf));
                    if let Some(delay) = resp.delay {
                        tokio::time::sleep(delay).await;
                    }
                    let head = format!(
                        "HTTP/1.1 {} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        resp.status,
                        resp.body.len(),
                    );
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.write_all(&resp.body).await;
                    let _ = sock.flush().await;
                });
            }
        });
        Self { addr, handle }
    }

    pub(crate) fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn parse_request(buf: &[u8]) -> FakeRequest {
    let text = String::from_utf8_lossy(buf);
    let authorization = text
        .split("\r\n")
        .skip(1)
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.trim().to_string());
    FakeRequest { authorization }
}
