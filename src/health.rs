use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Full};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::proxy::{ProxyClient, ResBody, box_full};
use crate::state::AppState;

/// One readiness signal: a pass/fail verdict plus a human-readable detail.
/// Used only by `/readyz`, which must render a verdict to gate traffic.
#[derive(Serialize)]
pub struct Check {
    pub ok: bool,
    pub detail: String,
}

/// `/healthz` view: a verdict-free, machine-readable snapshot of current EL + CL
/// state. Numeric fields carry the live values (peer counts, block / slot age,
/// sync status) for the consumer to threshold; any upstream failure is recorded
/// in the layer's `errors`, and unavailable fields are omitted. This endpoint
/// **always returns 200** — it reports state, it does not judge it. Thresholding
/// / alerting belongs in the consumer (Prometheus, dashboards), and traffic
/// gating belongs in `/readyz`.
#[derive(Serialize)]
pub struct HealthSnapshot {
    pub el: ElHealth,
    pub cl: ClHealth,
}

#[derive(Serialize, Default)]
pub struct ElHealth {
    /// HTTP version ethryx uses for this upstream hop: `h2c` / `h2` / `http/1.1`.
    /// Reflects the poller's auto-detected EL transport verdict.
    pub transport: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub syncing: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_distance: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_age_secs: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

#[derive(Serialize, Default)]
pub struct ClHealth {
    /// HTTP version ethryx uses for the Beacon hop. The CL hop never uses h2c
    /// prior-knowledge, so a cleartext upstream is always `http/1.1`; an `https`
    /// upstream may negotiate `h2` via ALPN, which is not separately tracked and
    /// is reported conservatively as `http/1.1`.
    pub transport: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub syncing: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_distance: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot_age_secs: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

/// `/readyz` view: the load-balancer readiness verdict. Defaults to sync status
/// only; the freshness fields are populated (and gated on) only under
/// `--readyz-strict`. Peer count is intentionally never part of readiness — it
/// is a soft, fleet-correlated signal that belongs in `/healthz`.
#[derive(Serialize)]
pub struct ReadyReport {
    pub status: &'static str,
    pub el_syncing: Check,
    pub cl_syncing: Check,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub el_block_fresh: Option<Check>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cl_slot_fresh: Option<Check>,
}

#[derive(Clone)]
struct ClStatus {
    head_slot: u64,
    sync_distance: u64,
    is_syncing: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum ElSyncingResult {
    Synced(bool),
    Syncing {
        #[serde(rename = "currentBlock")]
        current_block: String,
        #[serde(rename = "highestBlock")]
        highest_block: String,
    },
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ElBlockResult {
    pub number: String,
    pub timestamp: String,
}

/// The five upstream probe results captured by one poll, shared with the request
/// handlers via a `watch` channel. `/healthz` and `/readyz` read the latest one
/// instead of querying upstream per request, so upstream load is decoupled from
/// probe rate. Stored as raw results so block / slot ages can be recomputed live.
#[derive(Clone)]
pub(crate) struct Probe {
    el_syncing: Result<ElSyncingResult, String>,
    el_peers: Result<String, String>,
    el_block: Result<ElBlockResult, String>,
    cl_status: Result<ClStatus, String>,
    cl_peers: Result<u64, String>,
}

impl Probe {
    /// Placeholder used before the first poll lands. The cache is warmed with a
    /// real poll before any listener accepts, so this is never actually served.
    pub(crate) fn pending() -> Self {
        Probe {
            el_syncing: Err("warming up".into()),
            el_peers: Err("warming up".into()),
            el_block: Err("warming up".into()),
            cl_status: Err("warming up".into()),
            cl_peers: Err("warming up".into()),
        }
    }
}

const EL_BATCH_REQ: &[u8] = br#"[{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1},{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":2},{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":3}]"#;

/// `/healthz` — verdict-free EL + CL snapshot, **always 200**. Serves the latest
/// background poll (see [`poll_loop`]); block / slot ages are recomputed live per
/// request. Intended for dashboards / alerting; use [`ready`] as the LB gate.
pub fn report(state: &AppState) -> Response<ResBody> {
    let probe = state.probe.borrow();
    let mut snapshot = build_snapshot(
        &probe.el_syncing,
        &probe.el_peers,
        &probe.el_block,
        &probe.cl_status,
        &probe.cl_peers,
        state.cl_genesis_time,
        state.cl_seconds_per_slot,
        now_unix(),
    );
    snapshot.el.transport = transport_label(
        state.el_use_h2.load(Ordering::Relaxed),
        state.el_http_uri.scheme_str(),
    );
    // The CL hop always uses the default client (never h2c prior-knowledge).
    snapshot.cl.transport = transport_label(false, state.cl_syncing_uri.scheme_str());
    json_response(StatusCode::OK, &snapshot)
}

/// HTTP-version label for an upstream hop from whether ethryx selected HTTP/2 and
/// whether the upstream is TLS. Cleartext h2 is `h2c`; h2 over TLS is `h2`.
fn transport_label(uses_h2: bool, scheme: Option<&str>) -> &'static str {
    match (uses_h2, scheme == Some("https")) {
        (true, true) => "h2",
        (true, false) => "h2c",
        (false, _) => "http/1.1",
    }
}

/// `/readyz` — load-balancer readiness gate. 200 when ready, else 503.
///
/// By default this gates **only on EL + CL sync status**. A node that is caught
/// up reports `eth_syncing == false` even when the chain itself stalls
/// network-wide, so freshness/peer dips that hit the whole fleet at once do not
/// pull every backend out of rotation (which would turn a chain incident into a
/// total RPC outage). Sync status is the node-local signal that genuinely
/// distinguishes "this node can serve" from "this node is behind its peers".
///
/// With `--readyz-strict`, EL block age and CL slot age must also be within
/// their thresholds — choose this when serving strictly-at-head data matters
/// more than fleet availability during a network-wide stall.
pub fn ready(state: &AppState) -> Response<ResBody> {
    let probe = state.probe.borrow();
    let report = evaluate_ready(state, &probe);
    let code = if report.status == "ready" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    json_response(code, &report)
}

/// Build the `/readyz` verdict from a probe snapshot. Default gates on EL + CL
/// sync only; `--readyz-strict` also gates on block / slot freshness. Shared by
/// the endpoint and the poller's transition logging.
pub(crate) fn evaluate_ready(state: &AppState, probe: &Probe) -> ReadyReport {
    let el_syncing = check_el_syncing(&probe.el_syncing);
    let cl_syncing = check_cl_syncing(&probe.cl_status);
    // `now` is only needed for the strict freshness checks; the default
    // sync-only path doesn't read the clock.
    let (el_block_fresh, cl_slot_fresh) = if state.cfg.readyz_strict {
        let now = now_unix();
        (
            Some(check_el_block_fresh(
                &probe.el_block,
                state.cfg.el_max_block_age_secs,
                now,
            )),
            Some(check_cl_slot_fresh(
                &probe.cl_status,
                state.cl_genesis_time,
                state.cl_seconds_per_slot,
                state.cfg.cl_max_slot_age_secs,
                now,
            )),
        )
    } else {
        (None, None)
    };
    let all_ok = el_syncing.ok
        && cl_syncing.ok
        && el_block_fresh.as_ref().is_none_or(|c| c.ok)
        && cl_slot_fresh.as_ref().is_none_or(|c| c.ok);
    ReadyReport {
        status: if all_ok { "ready" } else { "not_ready" },
        el_syncing,
        cl_syncing,
        el_block_fresh,
        cl_slot_fresh,
    }
}

/// What [`poll_loop`] should log given the previous and current readiness, so a
/// sustained state logs once (on transition) rather than once per poll.
enum Transition {
    Silent,
    BecameNotReady,
    Recovered,
}

fn readiness_transition(prev_ready: Option<bool>, ready: bool) -> Transition {
    match prev_ready {
        Some(p) if p == ready => Transition::Silent,
        None if ready => Transition::Silent, // first poll already healthy: stay quiet
        _ if ready => Transition::Recovered,
        _ => Transition::BecameNotReady, // first-poll-degraded or ready -> not-ready
    }
}

/// Query all five upstream signals once. Each call is bounded by
/// `--health-timeout`; failures land in the result rather than aborting.
pub(crate) async fn probe_once(state: &AppState) -> Probe {
    // Probe EL in a single batch request, which also performs transport detection.
    let batch = el_batch_detect(state).await;
    let (cl_status, cl_peers) = tokio::join!(cl_syncing_status(state), cl_peer_count(state));
    Probe {
        el_syncing: batch.syncing.map_err(|e| e.to_string()),
        el_peers: batch.peers.map_err(|e| e.to_string()),
        el_block: batch.block.map_err(|e| e.to_string()),
        cl_status,
        cl_peers,
    }
}

/// The EL client picked by the current `el_use_h2` verdict.
fn el_client(state: &AppState) -> &ProxyClient {
    if state.el_use_h2.load(Ordering::Relaxed) {
        &state.el_h2_client
    } else {
        &state.client
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HealthError {
    Timeout,
    Transport(String),
    HttpStatus(http::StatusCode),
    Decode(String),
    Rpc(String),
}

impl std::fmt::Display for HealthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => write!(f, "timeout"),
            Self::Transport(s) => write!(f, "transport: {s}"),
            Self::HttpStatus(status) => write!(f, "http {status}"),
            Self::Decode(s) => write!(f, "decode: {s}"),
            Self::Rpc(s) => write!(f, "rpc error: {s}"),
        }
    }
}

struct ElBatchResult {
    syncing: Result<ElSyncingResult, HealthError>,
    peers: Result<String, HealthError>,
    block: Result<ElBlockResult, HealthError>,
}

/// Query all three EL JSON-RPC methods in a batch request, maintaining the transport verdict.
async fn el_batch_detect(state: &AppState) -> ElBatchResult {
    let prefer_h2 = state.el_use_h2.load(Ordering::Relaxed);
    let payload = Bytes::from_static(EL_BATCH_REQ);
    let primary = el_batch_rpc(state, payload.clone(), el_client(state)).await;

    match primary {
        Ok(res) => res,
        Err(e) if is_transport(&e) => {
            // The preferred transport failed at the connection level — try the other one.
            let other = if prefer_h2 {
                &state.client
            } else {
                &state.el_h2_client
            };
            let alt = el_batch_rpc(state, payload, other).await;
            match alt {
                Ok(res) => {
                    state.el_use_h2.store(!prefer_h2, Ordering::Relaxed);
                    if prefer_h2 {
                        warn!("EL JSON-RPC upstream stopped speaking h2c — using HTTP/1.1");
                    } else {
                        info!("EL JSON-RPC upstream speaks h2c — using HTTP/2");
                    }
                    res
                }
                Err(alt_err) => {
                    if is_transport(&alt_err) {
                        // Both transports failed at the connection level → upstream down.
                        // Keep current verdict and report original error.
                        ElBatchResult {
                            syncing: Err(e.clone()),
                            peers: Err(e.clone()),
                            block: Err(e),
                        }
                    } else {
                        // Alternative transport reached upstream (non-transport error, e.g. 404) → switch.
                        state.el_use_h2.store(!prefer_h2, Ordering::Relaxed);
                        if prefer_h2 {
                            warn!("EL JSON-RPC upstream stopped speaking h2c — using HTTP/1.1");
                        } else {
                            info!("EL JSON-RPC upstream speaks h2c — using HTTP/2");
                        }
                        ElBatchResult {
                            syncing: Err(alt_err.clone()),
                            peers: Err(alt_err.clone()),
                            block: Err(alt_err),
                        }
                    }
                }
            }
        }
        Err(e) => ElBatchResult {
            syncing: Err(e.clone()),
            peers: Err(e.clone()),
            block: Err(e),
        },
    }
}

/// A hard transport/connection failure or a timeout (vs. an HTTP status or decode
/// error) — the only signal that the chosen HTTP version might be wrong for this
/// upstream. A timeout is included so that if the upstream hangs on the HTTP/2
/// connection preface, we can fall back to HTTP/1.1 if the alternate client succeeds.
/// If both timeout, we do not switch.
fn is_transport(e: &HealthError) -> bool {
    matches!(e, HealthError::Transport(_) | HealthError::Timeout)
}

/// Refresh the shared [`Probe`] until shutdown: poll, then sleep `interval`,
/// repeat. The cache is warmed by one poll before this loop starts, so the first
/// background refresh lands `interval` later. Sleeping *after* each poll gives a
/// slow upstream a full `interval` breather instead of being polled back-to-back.
/// An in-flight poll is cancelled promptly on shutdown.
///
/// This loop is also the single place readiness is logged: a transition is logged
/// once (bounded to the poll rate), independent of how often `/readyz` is probed,
/// and visible even if nothing probes it.
pub(crate) async fn poll_loop(
    state: Arc<AppState>,
    tx: watch::Sender<Arc<Probe>>,
    mut shutdown: watch::Receiver<bool>,
    interval: Duration,
) {
    let mut prev_ready: Option<bool> = None;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        let probe = tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            probe = probe_once(&state) => probe,
        };

        let report = evaluate_ready(&state, &probe);
        let ready = report.status == "ready";
        debug!(status = report.status, "health poll");

        match readiness_transition(prev_ready, ready) {
            Transition::Silent => {}
            Transition::Recovered => info!("readiness recovered"),
            Transition::BecameNotReady => {
                let el_block = report
                    .el_block_fresh
                    .as_ref()
                    .map_or("-", |c| c.detail.as_str());
                let cl_slot = report
                    .cl_slot_fresh
                    .as_ref()
                    .map_or("-", |c| c.detail.as_str());
                warn!(
                    el_syncing = %report.el_syncing.detail,
                    cl_syncing = %report.cl_syncing.detail,
                    el_block,
                    cl_slot,
                    "not ready"
                );
            }
        }
        prev_ready = Some(ready);

        if tx.send(Arc::new(probe)).is_err() {
            return; // all receivers dropped
        }
    }
}

fn json_response<T: Serialize>(code: StatusCode, body: &T) -> Response<ResBody> {
    let body_bytes = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(code)
        .header("content-type", "application/json")
        .body(box_full(Full::new(Bytes::from(body_bytes))))
        .expect("response builder")
}

// ---- readiness verdicts (Check): sync is a fact; freshness uses thresholds ----

fn check_el_syncing(r: &Result<ElSyncingResult, String>) -> Check {
    match r {
        Ok(ElSyncingResult::Synced(false)) => Check {
            ok: true,
            detail: "synced".into(),
        },
        Ok(ElSyncingResult::Synced(true)) => Check {
            ok: false,
            detail: "syncing (true)".into(),
        },
        Ok(ElSyncingResult::Syncing {
            current_block,
            highest_block,
        }) => {
            let current = hex_to_u64(current_block);
            let highest = hex_to_u64(highest_block);
            let detail = match (current, highest) {
                (Some(c), Some(h)) => {
                    let distance = h.saturating_sub(c);
                    format!("syncing (block {c}, distance {distance})")
                }
                _ => format!("syncing (current {current_block}, highest {highest_block})"),
            };
            Check { ok: false, detail }
        }
        Err(e) => Check {
            ok: false,
            detail: format!("eth_syncing: {e}"),
        },
    }
}

fn check_cl_syncing(r: &Result<ClStatus, String>) -> Check {
    match r {
        Ok(s) if !s.is_syncing => Check {
            ok: true,
            detail: format!(
                "synced (slot {}, distance {})",
                s.head_slot, s.sync_distance
            ),
        },
        Ok(s) => Check {
            ok: false,
            detail: format!(
                "syncing (slot {}, distance {})",
                s.head_slot, s.sync_distance
            ),
        },
        Err(e) => Check {
            ok: false,
            detail: format!("node/syncing: {e}"),
        },
    }
}

fn check_el_block_fresh(r: &Result<ElBlockResult, String>, max_age: u64, now: u64) -> Check {
    match r {
        Ok(block) => {
            let ts = hex_to_u64(&block.timestamp);
            let num = hex_to_u64(&block.number);
            match (num, ts) {
                (Some(n), Some(t)) => {
                    let age = now.saturating_sub(t);
                    if age <= max_age {
                        Check {
                            ok: true,
                            detail: format!("block {n}, age {age}s"),
                        }
                    } else {
                        Check {
                            ok: false,
                            detail: format!("block {n} stale: {age}s (max {max_age})"),
                        }
                    }
                }
                _ => Check {
                    ok: false,
                    detail: "block missing fields".into(),
                },
            }
        }
        Err(e) => Check {
            ok: false,
            detail: format!("eth_getBlockByNumber: {e}"),
        },
    }
}

fn check_cl_slot_fresh(
    r: &Result<ClStatus, String>,
    genesis: u64,
    seconds_per_slot: u64,
    max_age: u64,
    now: u64,
) -> Check {
    match r {
        Ok(s) if genesis == 0 => Check {
            ok: true,
            detail: format!("slot {} (age check disabled)", s.head_slot),
        },
        Ok(s) => {
            let age = slot_age(s.head_slot, genesis, seconds_per_slot, now);
            if age <= max_age {
                Check {
                    ok: true,
                    detail: format!("slot {}, age {age}s", s.head_slot),
                }
            } else {
                Check {
                    ok: false,
                    detail: format!("slot {} stale: {age}s (max {max_age})", s.head_slot),
                }
            }
        }
        Err(e) => Check {
            ok: false,
            detail: format!("node/syncing: {e}"),
        },
    }
}

// ---- /healthz: verdict-free numeric snapshot ----

/// Fold the five upstream probe results into a numeric snapshot. Successful
/// signals populate their fields; failed ones are pushed onto the layer's
/// `errors` and leave the field unset.
#[allow(clippy::too_many_arguments)]
fn build_snapshot(
    sync_r: &Result<ElSyncingResult, String>,
    peers_r: &Result<String, String>,
    block_r: &Result<ElBlockResult, String>,
    cl_status_r: &Result<ClStatus, String>,
    cl_peers_r: &Result<u64, String>,
    genesis: u64,
    seconds_per_slot: u64,
    now: u64,
) -> HealthSnapshot {
    let mut el = ElHealth::default();
    match sync_r {
        Ok(ElSyncingResult::Synced(false)) => el.syncing = Some(false),
        Ok(ElSyncingResult::Synced(true)) => el.syncing = Some(true),
        Ok(ElSyncingResult::Syncing {
            current_block,
            highest_block,
        }) => {
            el.syncing = Some(true);
            let current = hex_to_u64(current_block);
            let highest = hex_to_u64(highest_block);
            el.sync_distance = match (current, highest) {
                (Some(c), Some(h)) => Some(h.saturating_sub(c)),
                _ => None,
            };
        }
        Err(e) => el.errors.push(format!("eth_syncing: {e}")),
    }
    match peers_r {
        Ok(hex) => match hex_to_u64(hex) {
            Some(n) => el.peers = Some(n),
            None => el.errors.push(format!("net_peerCount invalid hex: {hex}")),
        },
        Err(e) => el.errors.push(format!("net_peerCount: {e}")),
    }
    match block_r {
        Ok(block) => {
            let ts = hex_to_u64(&block.timestamp);
            let num = hex_to_u64(&block.number);
            match (num, ts) {
                (Some(n), Some(t)) => {
                    el.block_number = Some(n);
                    el.block_age_secs = Some(now.saturating_sub(t));
                }
                _ => el
                    .errors
                    .push("eth_getBlockByNumber: missing fields".into()),
            }
        }
        Err(e) => el.errors.push(format!("eth_getBlockByNumber: {e}")),
    }

    let mut cl = ClHealth::default();
    match cl_status_r {
        Ok(s) => {
            cl.syncing = Some(s.is_syncing);
            cl.sync_distance = Some(s.sync_distance);
            cl.head_slot = Some(s.head_slot);
            if genesis != 0 {
                cl.slot_age_secs = Some(slot_age(s.head_slot, genesis, seconds_per_slot, now));
            }
        }
        Err(e) => cl.errors.push(format!("node/syncing: {e}")),
    }
    match cl_peers_r {
        Ok(n) => cl.peers = Some(*n),
        Err(e) => cl.errors.push(format!("node/peer_count: {e}")),
    }

    HealthSnapshot { el, cl }
}

fn slot_age(head_slot: u64, genesis: u64, seconds_per_slot: u64, now: u64) -> u64 {
    // Saturate: a garbage `head_slot` from upstream must not overflow-panic
    // (debug) or wrap (release).
    let expected = genesis.saturating_add(head_slot.saturating_mul(seconds_per_slot));
    now.saturating_sub(expected)
}

/// Send a prepared probe request with the health-poll timeout and return the
/// body of a successful response. Shared by the EL JSON-RPC and CL REST probes.
async fn fetch_bytes(
    state: &AppState,
    req: Request<ResBody>,
    client: &ProxyClient,
) -> Result<Bytes, HealthError> {
    let resp = tokio::time::timeout(state.cfg.health_timeout, client.request(req))
        .await
        .map_err(|_| HealthError::Timeout)?
        .map_err(|e| HealthError::Transport(e.to_string()))?;
    if !resp.status().is_success() {
        return Err(HealthError::HttpStatus(resp.status()));
    }
    let body = resp.into_body();
    let limited = http_body_util::Limited::new(body, 10 * 1024 * 1024);
    limited
        .collect()
        .await
        .map(|c| c.to_bytes())
        .map_err(|e| HealthError::Decode(e.to_string()))
}

async fn el_batch_rpc(
    state: &AppState,
    payload: Bytes,
    client: &ProxyClient,
) -> Result<ElBatchResult, HealthError> {
    #[derive(serde::Deserialize)]
    struct RpcResponse {
        id: i64,
        result: Option<Box<serde_json::value::RawValue>>,
        error: Option<serde_json::Value>,
    }

    let req = Request::builder()
        .method(Method::POST)
        .uri(state.el_http_uri.clone())
        .header("content-type", "application/json")
        .body(box_full(Full::new(payload)))
        .map_err(|e| HealthError::Transport(format!("build: {e}")))?;
    let body = fetch_bytes(state, req, client).await?;
    let mut arr: Vec<RpcResponse> =
        serde_json::from_slice(&body).map_err(|e| HealthError::Decode(e.to_string()))?;

    if arr.len() != 3 {
        return Err(HealthError::Decode(format!(
            "expected 3 responses, got {}",
            arr.len()
        )));
    }

    let mut parse_res = |id_val: i64| -> Result<Box<serde_json::value::RawValue>, HealthError> {
        let idx = arr
            .iter()
            .position(|res| res.id == id_val)
            .ok_or_else(|| HealthError::Decode(format!("missing response for id {id_val}")))?;
        let mut resp = arr.remove(idx);
        if let Some(err) = resp.error.take() {
            return Err(HealthError::Rpc(err.to_string()));
        }
        resp.result
            .take()
            .ok_or_else(|| HealthError::Decode("missing result".into()))
    };

    let syncing = parse_res(1).and_then(|raw| {
        serde_json::from_str(raw.get()).map_err(|e| HealthError::Decode(e.to_string()))
    });
    let peers = parse_res(2).and_then(|raw| {
        serde_json::from_str(raw.get()).map_err(|e| HealthError::Decode(e.to_string()))
    });
    let block = parse_res(3).and_then(|raw| {
        serde_json::from_str(raw.get()).map_err(|e| HealthError::Decode(e.to_string()))
    });

    Ok(ElBatchResult {
        syncing,
        peers,
        block,
    })
}

async fn cl_syncing_status(state: &AppState) -> Result<ClStatus, String> {
    let v = cl_get_json(state, &state.cl_syncing_uri).await?;
    let data = v.get("data").ok_or("missing data")?;
    let is_syncing = data
        .get("is_syncing")
        .and_then(Value::as_bool)
        .ok_or("missing is_syncing")?;
    let head_slot = parse_decimal_str(data.get("head_slot")).ok_or("missing head_slot")?;
    let sync_distance =
        parse_decimal_str(data.get("sync_distance")).ok_or("missing sync_distance")?;
    Ok(ClStatus {
        head_slot,
        sync_distance,
        is_syncing,
    })
}

async fn cl_peer_count(state: &AppState) -> Result<u64, String> {
    let v = cl_get_json(state, &state.cl_peer_count_uri).await?;
    let data = v.get("data").ok_or("missing data")?;
    parse_decimal_str(data.get("connected")).ok_or_else(|| "missing connected".into())
}

async fn cl_get_json(state: &AppState, uri: &Uri) -> Result<Value, String> {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri.clone())
        .body(box_full(Full::new(Bytes::new())))
        .map_err(|e| format!("build: {e}"))?;
    let body = fetch_bytes(state, req, &state.client)
        .await
        .map_err(|e| e.to_string())?;
    serde_json::from_slice(&body).map_err(|e| format!("decode: {e}"))
}

fn parse_decimal_str(v: Option<&Value>) -> Option<u64> {
    v.and_then(Value::as_str)
        .and_then(|s| s.parse::<u64>().ok())
}

fn hex_to_u64(s: &str) -> Option<u64> {
    u64::from_str_radix(s.trim_start_matches("0x"), 16).ok()
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hex_with_0x_prefix() {
        assert_eq!(hex_to_u64("0x10"), Some(16));
        assert_eq!(hex_to_u64("0xff"), Some(255));
        assert_eq!(hex_to_u64("0x0"), Some(0));
    }

    #[test]
    fn hex_without_prefix() {
        assert_eq!(hex_to_u64("ff"), Some(255));
        assert_eq!(hex_to_u64("a"), Some(10));
    }

    #[test]
    fn hex_invalid_returns_none() {
        assert_eq!(hex_to_u64("0xZZ"), None);
        assert_eq!(hex_to_u64(""), None);
        assert_eq!(hex_to_u64("not hex"), None);
    }

    #[test]
    fn hex_handles_block_timestamp_width() {
        assert_eq!(hex_to_u64("0x671E0000"), Some(0x671E_0000));
    }

    #[test]
    fn decimal_str_parses_beacon_format() {
        let v = json!("9412341");
        assert_eq!(parse_decimal_str(Some(&v)), Some(9_412_341));
    }

    #[test]
    fn decimal_str_rejects_numeric_json() {
        let v = json!(42);
        assert_eq!(parse_decimal_str(Some(&v)), None);
    }

    #[test]
    fn decimal_str_handles_missing_field() {
        assert_eq!(parse_decimal_str(None), None);
    }

    #[test]
    fn decimal_str_rejects_garbage_string() {
        let v = json!("not a number");
        assert_eq!(parse_decimal_str(Some(&v)), None);
    }

    #[test]
    fn decimal_str_handles_zero() {
        let v = json!("0");
        assert_eq!(parse_decimal_str(Some(&v)), Some(0));
    }

    #[test]
    fn el_syncing_false_is_synced() {
        let c = check_el_syncing(&Ok(ElSyncingResult::Synced(false)));
        assert!(c.ok);
        assert_eq!(c.detail, "synced");
    }

    #[test]
    fn el_syncing_object_reports_block_progress() {
        let c = check_el_syncing(&Ok(ElSyncingResult::Syncing {
            current_block: "0x10".to_string(),
            highest_block: "0x20".to_string(),
        }));
        assert!(!c.ok);
        assert_eq!(c.detail, "syncing (block 16, distance 16)");
    }

    #[test]
    fn el_syncing_error_is_prefixed() {
        let c = check_el_syncing(&Err("boom".into()));
        assert!(!c.ok);
        assert_eq!(c.detail, "eth_syncing: boom");
    }

    #[test]
    fn cl_syncing_synced_is_ok() {
        let s = ClStatus {
            head_slot: 42,
            sync_distance: 0,
            is_syncing: false,
        };
        let c = check_cl_syncing(&Ok(s));
        assert!(c.ok);
        assert_eq!(c.detail, "synced (slot 42, distance 0)");
    }

    #[test]
    fn cl_syncing_in_progress_fails() {
        let s = ClStatus {
            head_slot: 42,
            sync_distance: 5,
            is_syncing: true,
        };
        let c = check_cl_syncing(&Ok(s));
        assert!(!c.ok);
        assert_eq!(c.detail, "syncing (slot 42, distance 5)");
    }

    #[test]
    fn el_block_fresh_within_threshold_is_ok() {
        let block = ElBlockResult {
            number: "0x5".to_string(),
            timestamp: "0x3e2".to_string(),
        };
        let c = check_el_block_fresh(&Ok(block), 60, 1000);
        assert!(c.ok);
        assert_eq!(c.detail, "block 5, age 6s");
    }

    #[test]
    fn el_block_fresh_past_threshold_is_stale() {
        let block = ElBlockResult {
            number: "0x5".to_string(),
            timestamp: "0x384".to_string(),
        };
        let c = check_el_block_fresh(&Ok(block), 60, 1000);
        assert!(!c.ok);
        assert_eq!(c.detail, "block 5 stale: 100s (max 60)");
    }

    #[test]
    fn cl_slot_fresh_disabled_when_genesis_zero() {
        let s = ClStatus {
            head_slot: 7,
            sync_distance: 0,
            is_syncing: false,
        };
        let c = check_cl_slot_fresh(&Ok(s), 0, 12, 60, 9_999_999);
        assert!(c.ok);
        assert_eq!(c.detail, "slot 7 (age check disabled)");
    }

    #[test]
    fn cl_slot_fresh_past_threshold_is_stale() {
        let s = ClStatus {
            head_slot: 10,
            sync_distance: 0,
            is_syncing: false,
        };
        let c = check_cl_slot_fresh(&Ok(s), 1000, 12, 60, 1300);
        assert!(!c.ok);
        assert_eq!(c.detail, "slot 10 stale: 180s (max 60)");
    }

    #[test]
    fn slot_age_computes_wall_clock_age() {
        assert_eq!(slot_age(10, 1000, 12, 1300), 180);
    }

    #[test]
    fn slot_age_saturates_on_garbage_head_slot() {
        assert_eq!(slot_age(u64::MAX, 1_606_824_023, 12, 2_000_000_000), 0);
    }

    #[test]
    fn readiness_transitions_log_only_on_change() {
        use Transition::*;
        assert!(matches!(readiness_transition(None, false), BecameNotReady));
        assert!(matches!(readiness_transition(None, true), Silent));
        assert!(matches!(readiness_transition(Some(true), true), Silent));
        assert!(matches!(readiness_transition(Some(false), false), Silent));
        assert!(matches!(
            readiness_transition(Some(true), false),
            BecameNotReady
        ));
        assert!(matches!(readiness_transition(Some(false), true), Recovered));
    }

    #[test]
    fn snapshot_reports_numeric_values_when_healthy() {
        let snap = build_snapshot(
            &Ok(ElSyncingResult::Synced(false)),
            &Ok("0x10".to_string()),
            &Ok(ElBlockResult {
                number: "0x5".to_string(),
                timestamp: "0x3e2".to_string(),
            }),
            &Ok(ClStatus {
                head_slot: 10,
                sync_distance: 0,
                is_syncing: false,
            }),
            &Ok(64u64),
            1000,
            12,
            1300,
        );
        assert_eq!(snap.el.syncing, Some(false));
        assert_eq!(snap.el.peers, Some(16));
        assert_eq!(snap.el.block_number, Some(5));
        assert_eq!(snap.el.block_age_secs, Some(306));
        assert!(snap.el.errors.is_empty());
        assert_eq!(snap.cl.syncing, Some(false));
        assert_eq!(snap.cl.peers, Some(64));
        assert_eq!(snap.cl.head_slot, Some(10));
        assert_eq!(snap.cl.slot_age_secs, Some(180));
        assert!(snap.cl.errors.is_empty());
    }

    #[test]
    fn snapshot_records_errors_and_omits_values_on_failure() {
        let snap = build_snapshot(
            &Err("timeout".to_string()),
            &Err("http 500".to_string()),
            &Err("timeout".to_string()),
            &Err("http 404".to_string()),
            &Err("http 404".to_string()),
            1000,
            12,
            1300,
        );
        assert_eq!(snap.el.syncing, None);
        assert_eq!(snap.el.peers, None);
        assert_eq!(snap.el.block_number, None);
        assert_eq!(snap.el.errors.len(), 3);
        assert_eq!(snap.cl.head_slot, None);
        assert_eq!(snap.cl.errors.len(), 2);
    }

    #[test]
    fn snapshot_reports_el_sync_distance_while_syncing() {
        let snap = build_snapshot(
            &Ok(ElSyncingResult::Syncing {
                current_block: "0x10".to_string(),
                highest_block: "0x20".to_string(),
            }),
            &Err("skip".to_string()),
            &Err("skip".to_string()),
            &Err("skip".to_string()),
            &Err("skip".to_string()),
            0,
            12,
            1300,
        );
        assert_eq!(snap.el.syncing, Some(true));
        assert_eq!(snap.el.sync_distance, Some(16));
    }

    #[test]
    fn snapshot_omits_cl_slot_age_when_genesis_disabled() {
        let snap = build_snapshot(
            &Ok(ElSyncingResult::Synced(false)),
            &Ok("0x10".to_string()),
            &Ok(ElBlockResult {
                number: "0x5".to_string(),
                timestamp: "0x3e2".to_string(),
            }),
            &Ok(ClStatus {
                head_slot: 10,
                sync_distance: 0,
                is_syncing: false,
            }),
            &Ok(64u64),
            0,
            12,
            1300,
        );
        assert_eq!(snap.cl.head_slot, Some(10));
        assert_eq!(snap.cl.slot_age_secs, None);
    }
}
