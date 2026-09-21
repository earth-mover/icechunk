//! Toxiproxy control: one proxy in front of `RustFS` with optional latency and bandwidth toxics.

use noxious_client::{Client, StreamDirection, Toxic, ToxicKind};

use crate::{BoxError, cli::NetArgs, storage::TOXIPROXY_PORT};

const API_URL: &str = "http://localhost:8474";
const PROXY_NAME: &str = "gc_bench";
/// `RustFS` as seen from inside the toxiproxy container.
const UPSTREAM: &str = "rustfs:9000";

pub(crate) fn enabled(net: &NetArgs) -> bool {
    net.latency_ms.is_some() || net.bandwidth_kbps.is_some()
}

fn direction_name(direction: &StreamDirection) -> &'static str {
    match direction {
        StreamDirection::Upstream => "upstream",
        StreamDirection::Downstream => "downstream",
    }
}

/// Create (or recreate) the proxy and add the requested toxics in both directions.
pub(crate) async fn setup(net: &NetArgs) -> Result<(), BoxError> {
    let client = Client::new(API_URL);
    let listen_suffix = format!(":{TOXIPROXY_PORT}");
    if let Ok(proxies) = client.proxies().await {
        for (name, proxy) in proxies {
            if name == PROXY_NAME || proxy.config.listen.ends_with(&listen_suffix) {
                proxy.delete().await.map_err(|e| e.to_string())?;
            }
        }
    }
    let listen = format!("0.0.0.0:{TOXIPROXY_PORT}");
    let proxy = client
        .create_proxy(PROXY_NAME, &listen, UPSTREAM)
        .await
        .map_err(|e| e.to_string())?;
    for direction in [StreamDirection::Upstream, StreamDirection::Downstream] {
        let dir = direction_name(&direction);
        if let Some(latency) = net.latency_ms {
            let toxic = Toxic {
                kind: ToxicKind::Latency { latency, jitter: 0 },
                name: format!("latency-{dir}"),
                toxicity: 1.0,
                direction,
            };
            proxy.add_toxic(&toxic).await.map_err(|e| e.to_string())?;
        }
        if let Some(rate) = net.bandwidth_kbps {
            let toxic = Toxic {
                kind: ToxicKind::Bandwidth { rate },
                name: format!("bandwidth-{dir}"),
                toxicity: 1.0,
                direction,
            };
            proxy.add_toxic(&toxic).await.map_err(|e| e.to_string())?;
        }
    }
    println!(
        "toxiproxy: {PROXY_NAME} (localhost:{TOXIPROXY_PORT} -> {UPSTREAM}) latency={:?}ms bandwidth={:?}KiB/s",
        net.latency_ms, net.bandwidth_kbps
    );
    Ok(())
}

/// Remove the proxy. Errors are ignored: the run already produced its report.
pub(crate) async fn teardown() {
    if let Ok(proxy) = Client::new(API_URL).proxy(PROXY_NAME).await {
        let _ = proxy.delete().await;
    }
}
