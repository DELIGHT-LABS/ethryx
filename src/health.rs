use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Full};
use serde::Serialize;
use serde_json::Value;
use tracing::warn;

use crate::proxy::{ResBody, box_full};
use crate::state::AppState;

#[derive(Serialize)]
pub struct Check {
    pub ok: bool,
    pub detail: String,
}

#[derive(Serialize)]
pub struct Report {
    pub status: &'static str,
    pub el_syncing: Check,
    pub el_peers: Check,
    pub el_block_fresh: Check,
    pub cl_syncing: Check,
    pub cl_peers: Check,
    pub cl_slot_fresh: Check,
}

struct ClStatus {
    head_slot: u64,
    sync_distance: u64,
    is_syncing: bool,
}

const SYNCING_REQ: &[u8] = br#"{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}"#;
const PEERS_REQ: &[u8] = br#"{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":1}"#;
const BLOCK_REQ: &[u8] =
    br#"{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":1}"#;

pub async fn report(state: &AppState) -> Response<ResBody> {
    let (sync_r, peers_r, block_r, cl_status_r, cl_peers_r) = tokio::join!(
        el_rpc(state, Bytes::from_static(SYNCING_REQ)),
        el_rpc(state, Bytes::from_static(PEERS_REQ)),
        el_rpc(state, Bytes::from_static(BLOCK_REQ)),
        cl_syncing_status(state),
        cl_peer_count(state),
    );

    let el_syncing = match sync_r {
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
    };
    let el_peers = match peers_r {
        Ok(Value::String(hex)) => match hex_to_u64(&hex) {
            Some(n) if n >= state.cfg.el_min_peers => Check {
                ok: true,
                detail: format!("{n} peers"),
            },
            Some(n) => Check {
                ok: false,
                detail: format!("{n} peers (min {})", state.cfg.el_min_peers),
            },
            None => Check {
                ok: false,
                detail: format!("invalid hex: {hex}"),
            },
        },
        Ok(v) => Check {
            ok: false,
            detail: format!("unexpected: {v}"),
        },
        Err(e) => Check {
            ok: false,
            detail: format!("net_peerCount: {e}"),
        },
    };
    let el_block_fresh = match block_r {
        Ok(block) => {
            let ts = block
                .get("timestamp")
                .and_then(Value::as_str)
                .and_then(hex_to_u64);
            let num = block
                .get("number")
                .and_then(Value::as_str)
                .and_then(hex_to_u64);
            match (ts, num) {
                (Some(t), Some(n)) => {
                    let age = now_unix().saturating_sub(t);
                    if age <= state.cfg.el_max_block_age_secs {
                        Check {
                            ok: true,
                            detail: format!("block {n}, age {age}s"),
                        }
                    } else {
                        Check {
                            ok: false,
                            detail: format!(
                                "block {n} stale: {age}s (max {})",
                                state.cfg.el_max_block_age_secs
                            ),
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
    };

    let cl_syncing = match &cl_status_r {
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
    };
    let cl_peers = match cl_peers_r {
        Ok(n) if n >= state.cfg.cl_min_peers => Check {
            ok: true,
            detail: format!("{n} peers"),
        },
        Ok(n) => Check {
            ok: false,
            detail: format!("{n} peers (min {})", state.cfg.cl_min_peers),
        },
        Err(e) => Check {
            ok: false,
            detail: format!("node/peer_count: {e}"),
        },
    };
    let cl_slot_fresh = match &cl_status_r {
        Ok(s) if state.cl_genesis_time == 0 => Check {
            ok: true,
            detail: format!("slot {} (age check disabled)", s.head_slot),
        },
        Ok(s) => {
            let expected = state.cl_genesis_time + s.head_slot * state.cl_seconds_per_slot;
            let age = now_unix().saturating_sub(expected);
            if age <= state.cfg.cl_max_slot_age_secs {
                Check {
                    ok: true,
                    detail: format!("slot {}, age {age}s", s.head_slot),
                }
            } else {
                Check {
                    ok: false,
                    detail: format!(
                        "slot {} stale: {age}s (max {})",
                        s.head_slot, state.cfg.cl_max_slot_age_secs
                    ),
                }
            }
        }
        Err(e) => Check {
            ok: false,
            detail: format!("node/syncing: {e}"),
        },
    };

    let all_ok = el_syncing.ok
        && el_peers.ok
        && el_block_fresh.ok
        && cl_syncing.ok
        && cl_peers.ok
        && cl_slot_fresh.ok;
    if !all_ok {
        warn!(
            el_syncing = %el_syncing.detail,
            el_peers = %el_peers.detail,
            el_block = %el_block_fresh.detail,
            cl_syncing = %cl_syncing.detail,
            cl_peers = %cl_peers.detail,
            cl_slot = %cl_slot_fresh.detail,
            "unhealthy"
        );
    }

    let report = Report {
        status: if all_ok { "healthy" } else { "unhealthy" },
        el_syncing,
        el_peers,
        el_block_fresh,
        cl_syncing,
        cl_peers,
        cl_slot_fresh,
    };
    let body_bytes = serde_json::to_vec(&report).unwrap_or_else(|_| b"{}".to_vec());
    let code = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    Response::builder()
        .status(code)
        .header("content-type", "application/json")
        .body(box_full(Full::new(Bytes::from(body_bytes))))
        .expect("response builder")
}

async fn el_rpc(state: &AppState, payload: Bytes) -> Result<Value, String> {
    let req = Request::builder()
        .method(Method::POST)
        .uri(state.el_http_uri.clone())
        .header("content-type", "application/json")
        .body(box_full(Full::new(payload)))
        .map_err(|e| format!("build: {e}"))?;

    let resp = tokio::time::timeout(state.cfg.health_timeout, state.client.request(req))
        .await
        .map_err(|_| "timeout".to_string())?
        .map_err(|e| format!("transport: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("http {}", resp.status()));
    }

    let body = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("read: {e}"))?
        .to_bytes();
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

    let resp = tokio::time::timeout(state.cfg.health_timeout, state.client.request(req))
        .await
        .map_err(|_| "timeout".to_string())?
        .map_err(|e| format!("transport: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("http {}", resp.status()));
    }

    let body = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("read: {e}"))?
        .to_bytes();
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
}
