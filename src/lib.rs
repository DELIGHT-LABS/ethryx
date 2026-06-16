//! Ethryx — Ethereum EL/CL sidecar library entry.
//!
//! The binary in `src/main.rs` is a thin wrapper around [`run`]. Tests can call
//! [`run`] directly with a custom shutdown future to drive the full stack
//! in-process without spawning a subprocess.

#![forbid(unsafe_code)]

pub mod config;
mod headers;
mod health;
pub mod metrics;
#[cfg(feature = "otel")]
pub mod otel;
mod proxy;
mod state;

pub use config::Config;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use hyper::body::Incoming;
use hyper::http::Request;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use crate::state::AppState;

use std::sync::OnceLock;
pub static PROMETHEUS_HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> =
    OnceLock::new();

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Bind every `cfg.listen` address, serve all routes, and wait for `shutdown`
/// before draining for `cfg.shutdown_grace` seconds.
pub async fn run<F>(cfg: Config, shutdown: F) -> Result<(), BoxError>
where
    F: Future<Output = ()> + Send,
{
    // Install default rustls cryptoprovider to prevent panics during parallel tests
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let prometheus_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let prometheus_recorder = prometheus_builder.build_recorder();
    let prometheus_handle = prometheus_recorder.handle();

    #[cfg(feature = "otel")]
    let _otel_guard = if let Some(ref otel_endpoint) = cfg.otel_endpoint {
        let tp = crate::otel::init_otel(otel_endpoint)?;
        let otel_recorder = crate::otel::OtelRecorder::new();
        let fanout = metrics_util::layers::FanoutBuilder::default()
            .add_recorder(prometheus_recorder)
            .add_recorder(otel_recorder)
            .build();
        if ::metrics::set_global_recorder(fanout).is_ok() {
            let _ = PROMETHEUS_HANDLE.set(prometheus_handle.clone());
            let handle = prometheus_handle.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    handle.run_upkeep();
                }
            });
        }
        Some(tp)
    } else {
        if ::metrics::set_global_recorder(prometheus_recorder).is_ok() {
            let _ = PROMETHEUS_HANDLE.set(prometheus_handle.clone());
            let handle = prometheus_handle.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    handle.run_upkeep();
                }
            });
        }
        None
    };

    #[cfg(not(feature = "otel"))]
    if ::metrics::set_global_recorder(prometheus_recorder).is_ok() {
        let _ = PROMETHEUS_HANDLE.set(prometheus_handle.clone());
        let handle = prometheus_handle.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                handle.run_upkeep();
            }
        });
    }

    if cfg.listen.is_empty() {
        return Err("no listen addresses configured".into());
    }
    if cfg.health_poll_interval.is_zero() {
        return Err("health-poll-interval must be at least 1 second".into());
    }

    info!(
        version = concat!("v", env!("CARGO_PKG_VERSION")),
        git = env!("ETHRYX_GIT_DESCRIBE"),
        ?cfg,
        "starting ethryx"
    );

    let client = proxy::build_client(false);
    // A second, HTTP/2-only client for the EL hop; the health poller decides at
    // runtime (`el_use_h2`) whether to use it — preferring h2c and falling back
    // to HTTP/1.1 when the upstream doesn't speak it.
    let el_h2_client = proxy::build_client(true);
    let el_http_uri = cfg.el_http_url.parse()?;
    let cl_base = cfg.cl_beacon_url.trim_end_matches('/');
    let cl_syncing_uri = format!("{cl_base}/eth/v1/node/syncing").parse()?;
    let cl_peer_count_uri = format!("{cl_base}/eth/v1/node/peer_count").parse()?;
    let cl_genesis_time = cfg.resolve_cl_genesis_time()?;
    let cl_seconds_per_slot = cfg.resolve_cl_seconds_per_slot()?;
    let poll_interval = cfg.health_poll_interval;
    let shutdown_grace = cfg.shutdown_grace;
    let listen_addrs = cfg.listen.clone();

    let (probe_tx, probe_rx) = watch::channel(Arc::new(health::Probe::pending()));
    let (shutdown_tx, _) = watch::channel(false);

    let state = Arc::new(AppState {
        cfg,
        client,
        el_h2_client,
        el_use_h2: AtomicBool::new(true),
        el_http_uri,
        cl_syncing_uri,
        cl_peer_count_uri,
        cl_genesis_time,
        cl_seconds_per_slot,
        probe: probe_rx,
    });

    // Warm the health cache with one poll before serving, then refresh it in the
    // background so /healthz and /readyz read a snapshot instead of hitting
    // upstream on every probe.
    let first_probe = Arc::new(health::probe_once(&state).await);
    let first_report = health::evaluate_ready(&state, &first_probe);
    health::update_metrics(&first_probe, &first_report);
    let el_proto = if state.el_use_h2.load(Ordering::Relaxed) {
        "HTTP/2 (h2c)"
    } else {
        "HTTP/1.1"
    };
    info!(
        el_http = %state.cfg.el_http_url,
        el_http_proto = %el_proto,
        el_ws = %state.cfg.el_ws_url,
        cl_beacon = %state.cfg.cl_beacon_url,
        cl_proto = "HTTP/1.1",
        "upstreams connected"
    );
    let _ = probe_tx.send(first_probe);
    let poller = tokio::spawn(health::poll_loop(
        state.clone(),
        probe_tx,
        shutdown_tx.subscribe(),
        poll_interval,
    ));

    let mut listeners = Vec::with_capacity(listen_addrs.len());
    for addr in &listen_addrs {
        let listener = TcpListener::bind(addr).await?;
        info!(listen = %addr, "listening");
        listeners.push(listener);
    }

    let (conn_tx, mut conn_rx) = tokio::sync::mpsc::channel::<()>(1);

    let mut accept_handles = Vec::with_capacity(listeners.len());
    for listener in listeners {
        let state = state.clone();
        let shutdown_tx = shutdown_tx.clone();
        let conn_tx = conn_tx.clone();
        accept_handles.push(tokio::spawn(accept_loop(
            listener,
            state,
            shutdown_tx,
            conn_tx,
        )));
    }

    shutdown.await;
    info!("shutdown signal received");

    let _ = shutdown_tx.send(true);

    for h in accept_handles {
        let _ = h.await;
    }
    let _ = poller.await;

    // Drop primary sender so receiver returns None when all client connection senders drop
    drop(conn_tx);

    info!(
        grace_secs = shutdown_grace.as_secs(),
        "draining connections"
    );
    if !shutdown_grace.is_zero() {
        let _ = tokio::time::timeout(shutdown_grace, async move {
            let _ = conn_rx.recv().await;
        })
        .await;
    }
    #[cfg(feature = "otel")]
    if let Some(tp) = _otel_guard {
        let _ = tp.shutdown();
    }
    Ok(())
}

/// Health-probe paths, excluded from the access log: k8s liveness/readiness and
/// LB checks hit these every few seconds, so logging them would bury real traffic.
fn is_probe_path(path: &str) -> bool {
    matches!(path, "/livez" | "/readyz" | "/healthz" | "/metrics")
}

async fn accept_loop(
    listener: TcpListener,
    state: Arc<AppState>,
    shutdown_tx: watch::Sender<bool>,
    conn_tx: tokio::sync::mpsc::Sender<()>,
) {
    let mut rx = shutdown_tx.subscribe();
    loop {
        tokio::select! {
            biased;
            _ = rx.changed() => return,
            accept = listener.accept() => {
                let (stream, peer) = match accept {
                    Ok(x) => x,
                    Err(e) => {
                        warn!(error = %e, "accept failed");
                        // Back off so a persistent error (e.g. fd exhaustion)
                        // doesn't busy-spin a core retrying immediately.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                if let Err(e) = stream.set_nodelay(true) {
                    debug!(error = %e, "set_nodelay failed");
                }
                // Connection lifecycle is fine-grained, high-frequency detail, so
                // it lives at trace (accept/close) and on a dedicated `access_log`
                // target (the negotiated protocol + first request line) rather
                // than the main info log — which stays reserved for lifecycle and
                // state changes. See the `access_log` config flag.
                trace!(%peer, "connection accepted");
                let io = TokioIo::new(stream);
                let st = state.clone();
                // `auto::Builder` doesn't expose its h1/h2 choice, but the first
                // request's version is that choice, so log one access line on the
                // first dispatch — skipping health-probe paths so frequent k8s/LB
                // checks don't drown the access log.
                let access_log_enabled = state.cfg.access_log;
                let otel_enabled = {
                    #[cfg(feature = "otel")]
                    {
                        state.cfg.otel_endpoint.is_some()
                    }
                    #[cfg(not(feature = "otel"))]
                    {
                        false
                    }
                };
                let svc = service_fn(move |req: Request<Incoming>| {
                    let st = st.clone();
                    let raw_path = req.uri().path();
                    let is_probe = is_probe_path(raw_path);

                    let method = if !is_probe { Some(req.method().clone()) } else { None };
                    let version = if !is_probe { Some(req.version()) } else { None };
                    let path = if !is_probe && (access_log_enabled || otel_enabled) {
                        raw_path.to_owned()
                    } else {
                        String::new()
                    };
                    let upstream_data = if !is_probe {
                        Some(crate::proxy::classify_request(&req))
                    } else {
                        None
                    };

                    let span = if !is_probe {
                        let ut = upstream_data.unwrap().0;
                        tracing::info_span!(
                            "request",
                            method = %method.as_ref().unwrap(),
                            path = %path,
                            upstream = ut,
                        )
                    } else {
                        tracing::Span::none()
                    };

                    let start = std::time::Instant::now();
                    use tracing::Instrument;
                    let dispatch_fut = proxy::dispatch(req, st).instrument(span);

                    async move {
                        let res = dispatch_fut.await;
                        if is_probe {
                            return res;
                        }

                        let elapsed = start.elapsed();
                        let status = match &res {
                            Ok(resp) => resp.status().as_u16(),
                            Err(_) => 500,
                        };
                        let (upstream_type, upstream_proto) = upstream_data.unwrap();
                        let method = method.unwrap();

                        if access_log_enabled {
                            info!(
                                target: "access_log",
                                %peer,
                                version = ?version.unwrap(),
                                method = %method,
                                path = %path,
                                upstream = upstream_type,
                                proto = upstream_proto,
                                status,
                                latency_ms = elapsed.as_secs_f64() * 1000.0,
                                "request"
                            );
                        }
                        let status_str = status.to_string();
                        ::metrics::counter!(
                            "ethryx_proxy_requests_total",
                            "upstream" => upstream_type,
                            "method" => method.as_str().to_owned(),
                            "status" => status_str
                        )
                        .increment(1);

                        ::metrics::histogram!(
                            "ethryx_proxy_request_duration_seconds",
                            "upstream" => upstream_type
                        )
                        .record(elapsed.as_secs_f64());
                        res
                    }
                });
                let mut conn_rx = shutdown_tx.subscribe();
                let conn_tx = conn_tx.clone();
                tokio::spawn(async move {
                    let _conn_tx = conn_tx;
                    let _guard = crate::metrics::ActiveConnectionGuard::new("tcp");
                    // Auto-detect HTTP/1 vs HTTP/2 (incl. cleartext h2c preface);
                    // `_with_upgrades` keeps HTTP/1.1 WebSocket Upgrade working;
                    // `enable_connect_protocol` advertises RFC 8441 Extended CONNECT
                    // (HTTP/2 WebSocket).
                    let mut builder = auto::Builder::new(TokioExecutor::new());
                    builder.http2().enable_connect_protocol();
                    let conn = builder.serve_connection_with_upgrades(io, svc);
                    tokio::pin!(conn);
                    tokio::select! {
                        res = conn.as_mut() => {
                            match res {
                                Ok(()) => trace!(%peer, "connection closed"),
                                Err(e) => debug!(error = %e, %peer, "connection closed"),
                            }
                        }
                        _ = conn_rx.changed() => {
                            conn.as_mut().graceful_shutdown();
                            match conn.await {
                                Ok(()) => trace!(%peer, "connection closed after shutdown"),
                                Err(e) => debug!(error = %e, %peer, "connection closed after shutdown"),
                            }
                        }
                    }
                });
            }
        }
    }
}
