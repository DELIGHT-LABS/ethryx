mod config;
mod headers;
mod health;
mod proxy;
mod state;

use std::sync::Arc;

use clap::Parser;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::state::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();

    let cfg = Config::parse();
    info!(?cfg, "starting ethryx");

    if cfg.listen.is_empty() {
        return Err("no listen addresses configured".into());
    }

    let client = proxy::build_client();
    let el_http_uri = cfg.el_http_url.parse()?;
    let cl_base = cfg.cl_beacon_url.trim_end_matches('/');
    let cl_syncing_uri = format!("{cl_base}/eth/v1/node/syncing").parse()?;
    let cl_peer_count_uri = format!("{cl_base}/eth/v1/node/peer_count").parse()?;
    let cl_genesis_time = cfg.resolve_cl_genesis_time()?;
    let cl_seconds_per_slot = cfg.resolve_cl_seconds_per_slot()?;
    let shutdown_grace = cfg.shutdown_grace;
    let listen_addrs = cfg.listen.clone();
    let state = Arc::new(AppState {
        cfg,
        client,
        el_http_uri,
        cl_syncing_uri,
        cl_peer_count_uri,
        cl_genesis_time,
        cl_seconds_per_slot,
    });

    // Bind every listener before spawning, so binding failure aborts startup cleanly.
    let mut listeners = Vec::with_capacity(listen_addrs.len());
    for addr in &listen_addrs {
        let listener = TcpListener::bind(addr).await?;
        info!(listen = %addr, "listening");
        listeners.push(listener);
    }

    let (shutdown_tx, _) = watch::channel(false);
    let mut accept_handles = Vec::with_capacity(listeners.len());
    for listener in listeners {
        let state = state.clone();
        let shutdown_tx = shutdown_tx.clone();
        accept_handles.push(tokio::spawn(accept_loop(listener, state, shutdown_tx)));
    }

    let _ = signal::ctrl_c().await;
    info!("shutdown signal received");

    // Broadcasts to every accept loop and every in-flight connection.
    let _ = shutdown_tx.send(true);

    // Accept loops exit promptly once the watch fires.
    for h in accept_handles {
        let _ = h.await;
    }

    info!(
        grace_secs = shutdown_grace.as_secs(),
        "draining connections"
    );
    tokio::time::sleep(shutdown_grace).await;
    Ok(())
}

async fn accept_loop(
    listener: TcpListener,
    state: Arc<AppState>,
    shutdown_tx: watch::Sender<bool>,
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
                        error!(error = %e, "accept failed");
                        continue;
                    }
                };
                if let Err(e) = stream.set_nodelay(true) {
                    debug!(error = %e, "set_nodelay failed");
                }
                let io = TokioIo::new(stream);
                let st = state.clone();
                let svc = service_fn(move |req| proxy::dispatch(req, st.clone()));
                let mut conn_rx = shutdown_tx.subscribe();
                tokio::spawn(async move {
                    let conn = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, svc)
                        .with_upgrades();
                    tokio::pin!(conn);
                    tokio::select! {
                        res = conn.as_mut() => {
                            if let Err(e) = res {
                                debug!(error = %e, %peer, "connection ended");
                            }
                        }
                        _ = conn_rx.changed() => {
                            conn.as_mut().graceful_shutdown();
                            if let Err(e) = conn.await {
                                debug!(error = %e, %peer, "connection ended after shutdown");
                            }
                        }
                    }
                });
            }
        }
    }
}
