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

/// The five upstream probe results captured by one poll, shared with the request
/// handlers via a `watch` channel. `/healthz` and `/readyz` read the latest one
/// instead of querying upstream per request, so upstream load is decoupled from
/// probe rate. Stored as raw results so block / slot ages can be recomputed live.
#[derive(Clone)]
pub(crate) struct Probe {
    el_syncing: Result<Value, String>,
    el_peers: Result<Value, String>,
    el_block: Result<Value, String>,
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

const SYNCING_REQ: &[u8] = br#"{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}"#;
const PEERS_REQ: &[u8] = br#"{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":1}"#;
const BLOCK_REQ: &[u8] =
    br#"{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":1}"#;

/// `/healthz` — verdict-free EL + CL snapshot, **always 200**. Serves the latest
/// background poll (see [`poll_loop`]); block / slot ages are recomputed live per
/// request. Intended for dashboards / alerting; use [`ready`] as the LB gate.
pub fn report(state: &AppState) -> Response<ResBody> {
    let probe = state.probe.borrow().clone();
    let snapshot = build_snapshot(
        &probe.el_syncing,
        &probe.el_peers,
        &probe.el_block,
        &probe.cl_status,
        &probe.cl_peers,
        state.cl_genesis_time,
        state.cl_seconds_per_slot,
        now_unix(),
    );
    json_response(StatusCode::OK, &snapshot)
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
    let probe = state.probe.borrow().clone();
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
fn evaluate_ready(state: &AppState, probe: &Probe) -> ReadyReport {
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
    // eth_syncing runs first and doubles as EL transport detection: it tries the
    // preferred client and, on a transport failure, the other — flipping
    // `el_use_h2` so the rest of this cycle and the data-plane follow.
    let el_syncing = el_syncing_detect(state).await;
    let el = el_client(state);
    let (el_peers, el_block, cl_status, cl_peers) = tokio::join!(
        el_rpc(state, Bytes::from_static(PEERS_REQ), el),
        el_rpc(state, Bytes::from_static(BLOCK_REQ), el),
        cl_syncing_status(state),
        cl_peer_count(state),
    );
    Probe {
        el_syncing,
        el_peers,
        el_block,
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

/// `eth_syncing` probe that also maintains the EL transport verdict. It sends over
/// the preferred client; on a transport-layer failure it retries over the other
/// and — only if that succeeds — flips `el_use_h2`. So an h2c↔h1 change is picked
/// up within one poll, while a dead upstream (both fail) leaves the verdict as is.
async fn el_syncing_detect(state: &AppState) -> Result<Value, String> {
    let prefer_h2 = state.el_use_h2.load(Ordering::Relaxed);
    let payload = Bytes::from_static(SYNCING_REQ);
    let primary = el_rpc(state, payload.clone(), el_client(state)).await;
    if !matches!(&primary, Err(e) if is_transport(e)) {
        // The preferred transport reached the upstream (Ok, or an HTTP/decode
        // error — both mean the connection itself worked); keep the verdict.
        return primary;
    }
    // The preferred transport failed at the connection level — try the other one.
    // If *it* reaches the upstream, the upstream changed protocols, so switch. If
    // it too fails at the transport level the upstream is simply down, so keep the
    // verdict and report the original error.
    let other = if prefer_h2 {
        &state.client
    } else {
        &state.el_h2_client
    };
    let alt = el_rpc(state, payload, other).await;
    if matches!(&alt, Err(e) if is_transport(e)) {
        return primary; // both transports failed → upstream down
    }
    state.el_use_h2.store(!prefer_h2, Ordering::Relaxed);
    if prefer_h2 {
        warn!("EL JSON-RPC upstream stopped speaking h2c — using HTTP/1.1");
    } else {
        info!("EL JSON-RPC upstream speaks h2c — using HTTP/2");
    }
    alt
}

/// A hard transport/connection failure (vs. a timeout, HTTP status, or decode
/// error) — the only signal that the chosen HTTP version is wrong for this
/// upstream. A timeout is deliberately excluded: it's ambiguous (a slow upstream),
/// and because the verdict is sticky, letting a transient timeout trigger a switch
/// would strand us on the wrong protocol until restart.
fn is_transport(e: &str) -> bool {
    e.starts_with("transport:")
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

fn check_el_syncing(r: &Result<Value, String>) -> Check {
    match r {
        Ok(Value::Bool(false)) => Check {
            ok: true,
            detail: "synced".into(),
        },
        Ok(v) => {
            let current = v
                .get("currentBlock")
                .and_then(Value::as_str)
                .and_then(hex_to_u64);
            let highest = v
                .get("highestBlock")
                .and_then(Value::as_str)
                .and_then(hex_to_u64);
            let detail = match (current, highest) {
                (Some(c), Some(h)) => {
                    let distance = h.saturating_sub(c);
                    format!("syncing (block {c}, distance {distance})")
                }
                _ => format!("syncing: {v}"),
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

fn check_el_block_fresh(r: &Result<Value, String>, max_age: u64, now: u64) -> Check {
    match r {
        Ok(block) => match block_number_and_age(block, now) {
            Some((n, age)) if age <= max_age => Check {
                ok: true,
                detail: format!("block {n}, age {age}s"),
            },
            Some((n, age)) => Check {
                ok: false,
                detail: format!("block {n} stale: {age}s (max {max_age})"),
            },
            None => Check {
                ok: false,
                detail: "block missing fields".into(),
            },
        },
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
    sync_r: &Result<Value, String>,
    peers_r: &Result<Value, String>,
    block_r: &Result<Value, String>,
    cl_status_r: &Result<ClStatus, String>,
    cl_peers_r: &Result<u64, String>,
    genesis: u64,
    seconds_per_slot: u64,
    now: u64,
) -> HealthSnapshot {
    let mut el = ElHealth::default();
    match sync_r {
        Ok(Value::Bool(false)) => el.syncing = Some(false),
        Ok(v) => {
            el.syncing = Some(true);
            el.sync_distance = el_sync_distance(v);
        }
        Err(e) => el.errors.push(format!("eth_syncing: {e}")),
    }
    match peers_r {
        Ok(Value::String(hex)) => match hex_to_u64(hex) {
            Some(n) => el.peers = Some(n),
            None => el.errors.push(format!("net_peerCount invalid hex: {hex}")),
        },
        Ok(v) => el.errors.push(format!("net_peerCount unexpected: {v}")),
        Err(e) => el.errors.push(format!("net_peerCount: {e}")),
    }
    match block_r {
        Ok(block) => match block_number_and_age(block, now) {
            Some((num, age)) => {
                el.block_number = Some(num);
                el.block_age_secs = Some(age);
            }
            None => el
                .errors
                .push("eth_getBlockByNumber: missing fields".into()),
        },
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

/// `highestBlock - currentBlock` from an `eth_syncing` object, if both parse.
fn el_sync_distance(v: &Value) -> Option<u64> {
    let current = v
        .get("currentBlock")
        .and_then(Value::as_str)
        .and_then(hex_to_u64)?;
    let highest = v
        .get("highestBlock")
        .and_then(Value::as_str)
        .and_then(hex_to_u64)?;
    Some(highest.saturating_sub(current))
}

/// `(number, age_secs)` from an `eth_getBlockByNumber` result, or `None` if the
/// `number` / `timestamp` fields are missing or unparseable.
fn block_number_and_age(block: &Value, now: u64) -> Option<(u64, u64)> {
    let ts = block
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(hex_to_u64)?;
    let num = block
        .get("number")
        .and_then(Value::as_str)
        .and_then(hex_to_u64)?;
    Some((num, now.saturating_sub(ts)))
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
) -> Result<Bytes, String> {
    let resp = tokio::time::timeout(state.cfg.health_timeout, client.request(req))
        .await
        .map_err(|_| "timeout".to_string())?
        .map_err(|e| format!("transport: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("http {}", resp.status()));
    }
    resp.into_body()
        .collect()
        .await
        .map(|c| c.to_bytes())
        .map_err(|e| format!("read: {e}"))
}

async fn el_rpc(state: &AppState, payload: Bytes, client: &ProxyClient) -> Result<Value, String> {
    let req = Request::builder()
        .method(Method::POST)
        .uri(state.el_http_uri.clone())
        .header("content-type", "application/json")
        .body(box_full(Full::new(payload)))
        .map_err(|e| format!("build: {e}"))?;
    let body = fetch_bytes(state, req, client).await?;
    let v: Value = serde_json::from_slice(&body).map_err(|e| format!("decode: {e}"))?;
    if let Some(err) = v.get("error") {
        return Err(format!("rpc error: {err}"));
    }
    v.get("result")
        .cloned()
        .ok_or_else(|| "missing result".into())
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
    let body = fetch_bytes(state, req, &state.client).await?;
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
        // 32-bit-ish hex timestamp value (uppercase to ensure radix=16 accepts both cases)
        assert_eq!(hex_to_u64("0x671E0000"), Some(0x671E_0000));
    }

    #[test]
    fn decimal_str_parses_beacon_format() {
        // Beacon API always quotes integers
        let v = json!("9412341");
        assert_eq!(parse_decimal_str(Some(&v)), Some(9_412_341));
    }

    #[test]
    fn decimal_str_rejects_numeric_json() {
        // If upstream ever returns unquoted (off-spec), refuse
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

    // ---- readiness verdicts ----

    #[test]
    fn el_syncing_false_is_synced() {
        let c = check_el_syncing(&Ok(json!(false)));
        assert!(c.ok);
        assert_eq!(c.detail, "synced");
    }

    #[test]
    fn el_syncing_object_reports_block_progress() {
        let c = check_el_syncing(&Ok(json!({"currentBlock": "0x10", "highestBlock": "0x20"})));
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
        // now = 1000, ts = 0x3e2 (994) -> age 6 <= 60
        let block = json!({"number": "0x5", "timestamp": "0x3e2"});
        let c = check_el_block_fresh(&Ok(block), 60, 1000);
        assert!(c.ok);
        assert_eq!(c.detail, "block 5, age 6s");
    }

    #[test]
    fn el_block_fresh_past_threshold_is_stale() {
        // now = 1000, ts = 0x384 (900) -> age 100 > 60
        let block = json!({"number": "0x5", "timestamp": "0x384"});
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
        // genesis 1000, slot 10, 12s/slot -> expected 1120; now 1300 -> age 180 > 60
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
        // genesis 1000, slot 10, 12s/slot -> expected 1120; now 1300 -> age 180
        assert_eq!(slot_age(10, 1000, 12, 1300), 180);
    }

    #[test]
    fn slot_age_saturates_on_garbage_head_slot() {
        // A bogus head_slot from upstream must not overflow-panic or wrap.
        assert_eq!(slot_age(u64::MAX, 1_606_824_023, 12, 2_000_000_000), 0);
    }

    #[test]
    fn readiness_transitions_log_only_on_change() {
        use Transition::*;
        // First poll: degraded warns, already-healthy stays quiet.
        assert!(matches!(readiness_transition(None, false), BecameNotReady));
        assert!(matches!(readiness_transition(None, true), Silent));
        // Steady state: no repeat logs.
        assert!(matches!(readiness_transition(Some(true), true), Silent));
        assert!(matches!(readiness_transition(Some(false), false), Silent));
        // Edges: log once each way.
        assert!(matches!(
            readiness_transition(Some(true), false),
            BecameNotReady
        ));
        assert!(matches!(readiness_transition(Some(false), true), Recovered));
    }

    // ---- /healthz numeric snapshot ----

    #[test]
    fn snapshot_reports_numeric_values_when_healthy() {
        let snap = build_snapshot(
            &Ok(json!(false)),
            &Ok(json!("0x10")),
            &Ok(json!({"number": "0x5", "timestamp": "0x3e2"})), // ts 994
            &Ok(ClStatus {
                head_slot: 10,
                sync_distance: 0,
                is_syncing: false,
            }),
            &Ok(64u64),
            1000, // genesis
            12,   // seconds_per_slot
            1300, // now
        );
        assert_eq!(snap.el.syncing, Some(false));
        assert_eq!(snap.el.peers, Some(16));
        assert_eq!(snap.el.block_number, Some(5));
        assert_eq!(snap.el.block_age_secs, Some(306)); // 1300 - 994
        assert!(snap.el.errors.is_empty());
        assert_eq!(snap.cl.syncing, Some(false));
        assert_eq!(snap.cl.peers, Some(64));
        assert_eq!(snap.cl.head_slot, Some(10));
        assert_eq!(snap.cl.slot_age_secs, Some(180)); // 1300 - (1000 + 10*12)
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
        assert_eq!(snap.el.errors.len(), 3); // syncing, peers, block
        assert_eq!(snap.cl.head_slot, None);
        assert_eq!(snap.cl.errors.len(), 2); // syncing, peers
    }

    #[test]
    fn snapshot_reports_el_sync_distance_while_syncing() {
        let snap = build_snapshot(
            &Ok(json!({"currentBlock": "0x10", "highestBlock": "0x20"})),
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
            &Ok(json!(false)),
            &Ok(json!("0x10")),
            &Ok(json!({"number": "0x5", "timestamp": "0x3e2"})),
            &Ok(ClStatus {
                head_slot: 10,
                sync_distance: 0,
                is_syncing: false,
            }),
            &Ok(64u64),
            0, // genesis disabled -> no slot age
            12,
            1300,
        );
        assert_eq!(snap.cl.head_slot, Some(10));
        assert_eq!(snap.cl.slot_age_secs, None);
    }
}
