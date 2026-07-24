use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{Method, Request, Response, StatusCode, Uri, Version};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::body::Incoming;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio_tungstenite::tungstenite::protocol::Role;
use tracing::{debug, trace};

use crate::headers::strip_hop_by_hop;
use crate::health;
use crate::state::AppState;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ResBody = UnsyncBoxBody<Bytes, BoxError>;

type Https = hyper_rustls::HttpsConnector<HttpConnector>;
pub type ProxyClient = Client<Https, ResBody>;

pub fn build_client(force_h2: bool) -> ProxyClient {
    let mut http = HttpConnector::new();
    http.set_nodelay(true);
    http.enforce_http(false);
    http.set_keepalive(Some(Duration::from_secs(60)));
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        // ALPN advertises ["h2","http/1.1"] for https upstreams → auto-negotiate
        // h2 when the upstream offers it; cleartext upstreams stay h1.
        .enable_all_versions()
        .wrap_connector(http);
    let mut builder = Client::builder(TokioExecutor::new());
    builder.pool_idle_timeout(Duration::from_secs(60));
    // Cleartext h2 can't be auto-negotiated, so an opt-in h2c upstream forces
    // HTTP/2 (prior-knowledge); an https upstream then uses h2 directly.
    if force_h2 {
        builder.http2_only(true);
    }
    builder.build(https)
}

pub async fn dispatch(
    req: Request<Incoming>,
    state: Arc<AppState>,
) -> Result<Response<ResBody>, Infallible> {
    Ok(route(req, &state).await.unwrap_or_else(|e| {
        // Routine for a sidecar (upstream hiccup, client cancel); the client
        // still gets a 502. Surface at debug, not error.
        debug!(error = %e, "proxy error");
        text_response(StatusCode::BAD_GATEWAY, e.to_string())
    }))
}

pub(crate) fn is_cl_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    if bytes.first() != Some(&b'/') {
        return false;
    }
    let remainder = &bytes[1..];
    let len = remainder
        .iter()
        .position(|&b| b == b'/')
        .unwrap_or(remainder.len());
    let segment = &remainder[..len];
    match segment.len() {
        3 => segment == b"eth",
        4 => segment == b"teku",
        5 => segment == b"prysm",
        6 => segment == b"nimbus",
        8 => segment == b"lodestar",
        10 => segment == b"lighthouse",
        _ => false,
    }
}

fn to_head_response(res: Response<ResBody>) -> Response<ResBody> {
    let (parts, _) = res.into_parts();
    Response::from_parts(parts, box_full(http_body_util::Full::new(Bytes::new())))
}

async fn route(req: Request<Incoming>, state: &AppState) -> Result<Response<ResBody>, BoxError> {
    trace!(method = %req.method(), path = req.uri().path(), "routing request");
    let is_head = req.method() == Method::HEAD;
    if req.method() == Method::GET || is_head {
        let path = req.uri().path();
        if path == "/healthz" {
            let res = health::report(state);
            return Ok(if is_head { to_head_response(res) } else { res });
        }
        if path == "/readyz" {
            let res = health::ready(state);
            return Ok(if is_head { to_head_response(res) } else { res });
        }
        if path == "/livez" {
            let res = text_response(StatusCode::OK, Bytes::from_static(b"ok"));
            return Ok(if is_head { to_head_response(res) } else { res });
        }
        if path == "/metrics" {
            let buffer = crate::PROMETHEUS_HANDLE
                .get()
                .map(|h| h.render())
                .unwrap_or_default();

            let res = Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                .body(crate::proxy::box_full(http_body_util::Full::new(
                    Bytes::from(buffer),
                )))
                .unwrap();
            return Ok(if is_head { to_head_response(res) } else { res });
        }
    }
    if (req.method() == Method::GET || req.method() == Method::HEAD)
        && req.uri().path() == "/eth/v1/node/health"
    {
        return Ok(health::node_health(state));
    }
    if req.method() == Method::POST
        && req.uri().path() == "/eth/v1/validator/prepare_beacon_proposer"
        && state.cfg.mock_prepare_beacon_proposer
    {
        let res = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(box_full(Full::new(Bytes::from_static(
                b"{\"code\":200,\"message\":\"success\"}",
            ))))
            .unwrap();
        return Ok(res);
    }
    if req.method() == Method::POST
        && req.uri().path() == "/eth/v1/validator/beacon_committee_subscriptions"
        && state.cfg.mock_beacon_committee_subscriptions
    {
        let res = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(box_full(Full::new(Bytes::from_static(
                b"{\"code\":200,\"message\":\"success\"}",
            ))))
            .unwrap();
        return Ok(res);
    }
    if req.method() == Method::POST
        && req.uri().path() == "/eth/v1/validator/sync_committee_subscriptions"
        && state.cfg.mock_sync_committee_subscriptions
    {
        let res = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(box_full(Full::new(Bytes::from_static(
                b"{\"code\":200,\"message\":\"success\"}",
            ))))
            .unwrap();
        return Ok(res);
    }
    if req.method() == Method::POST
        && req.uri().path() == "/eth/v1/validator/register_validator"
        && let Some(ref mev_url) = state.cfg.cl_mev_relay_url
    {
        return http_proxy_with_path(
            req,
            state,
            mev_url,
            Some("/eth/v1/builder/validators"),
            &state.client,
        )
        .await;
    }
    if is_cl_path(req.uri().path()) {
        return http_proxy(req, state, &state.cfg.cl_beacon_url, &state.client).await;
    }
    let mock_ws = state.cfg.mock_eth_syncing || state.cfg.trust_upstream;
    // HTTP/2 Extended CONNECT WebSocket (RFC 8441): :method=CONNECT, :protocol=websocket.
    if req.method() == Method::CONNECT
        && req
            .extensions()
            .get::<hyper::ext::Protocol>()
            .is_some_and(|p| p.as_str() == "websocket")
    {
        return ws_upgrade_h2(
            req,
            state.cfg.el_ws_url.clone(),
            state.cfg.proxy_timeout,
            mock_ws,
        )
        .await;
    }
    // HTTP/1.1 Upgrade WebSocket.
    if hyper_tungstenite::is_upgrade_request(&req) {
        return ws_upgrade(
            req,
            state.cfg.el_ws_url.clone(),
            state.cfg.proxy_timeout,
            mock_ws,
        )
        .await;
    }
    // The health poller decides the EL transport (h2c vs h1); the data-plane just
    // follows its verdict.
    let el_client = if state.el_use_h2.load(Ordering::Relaxed) {
        &state.el_h2_client
    } else {
        &state.client
    };

    if req.method() == Method::POST
        && !is_cl_path(req.uri().path())
        && (state.cfg.mock_eth_syncing || state.cfg.trust_upstream)
    {
        let (parts, body) = req.into_parts();
        match http_body_util::BodyExt::collect(body).await {
            Ok(collected) => {
                let bytes = collected.to_bytes();
                if let Some(mock_res) = try_mock_eth_syncing(&bytes) {
                    return Ok(mock_res);
                }
                let req = Request::from_parts(parts, box_full(Full::new(bytes)));
                return http_proxy(req, state, &state.cfg.el_http_url, el_client).await;
            }
            Err(e) => {
                return Ok(text_response(
                    StatusCode::BAD_REQUEST,
                    format!("failed to read request body: {e}"),
                ));
            }
        }
    }

    http_proxy(req, state, &state.cfg.el_http_url, el_client).await
}

fn try_mock_eth_syncing(bytes: &[u8]) -> Option<Response<ResBody>> {
    if !bytes.windows(11).any(|w| w == b"eth_syncing") {
        return None;
    }

    #[derive(serde::Deserialize)]
    struct RpcReq<'a> {
        method: Option<&'a str>,
        #[serde(borrow)]
        id: Option<&'a serde_json::value::RawValue>,
    }

    if let Ok(req) = serde_json::from_slice::<RpcReq>(bytes) {
        if req.method == Some("eth_syncing") {
            let id_str = req.id.map(|v| v.get()).unwrap_or("null");
            let body = format!(r#"{{"jsonrpc":"2.0","id":{},"result":false}}"#, id_str);
            let res = Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(box_full(Full::new(Bytes::from(body))))
                .unwrap();
            return Some(res);
        }
    } else if let Ok(reqs) = serde_json::from_slice::<Vec<RpcReq>>(bytes)
        && !reqs.is_empty()
        && reqs.iter().all(|r| r.method == Some("eth_syncing"))
    {
        let mut items = Vec::with_capacity(reqs.len());
        for r in reqs {
            let id_str = r.id.map(|v| v.get()).unwrap_or("null");
            items.push(format!(
                r#"{{"jsonrpc":"2.0","id":{},"result":false}}"#,
                id_str
            ));
        }
        let body = format!("[{}]", items.join(","));
        let res = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(box_full(Full::new(Bytes::from(body))))
            .unwrap();
        return Some(res);
    }
    None
}

fn try_mock_ws_eth_syncing(text: &str) -> Option<String> {
    if !text.contains("eth_syncing") {
        return None;
    }

    #[derive(serde::Deserialize)]
    struct RpcReq<'a> {
        method: Option<&'a str>,
        #[serde(borrow)]
        id: Option<&'a serde_json::value::RawValue>,
    }

    if let Ok(req) = serde_json::from_str::<RpcReq>(text)
        && req.method == Some("eth_syncing")
    {
        let id_str = req.id.map(|v| v.get()).unwrap_or("null");
        return Some(format!(
            r#"{{"jsonrpc":"2.0","id":{},"result":false}}"#,
            id_str
        ));
    }
    None
}

async fn http_proxy_with_path<B>(
    req: Request<B>,
    state: &AppState,
    upstream_base: &str,
    override_path: Option<&str>,
    client: &ProxyClient,
) -> Result<Response<ResBody>, BoxError>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let (mut parts, body) = req.into_parts();
    #[cfg(feature = "otel")]
    crate::otel::propagate_context(&mut parts.headers);
    let upstream_uri: Uri = match override_path {
        Some(new_path) => {
            let base = upstream_base.trim_end_matches('/');
            if let Some(query) = parts.uri.query() {
                format!("{base}{new_path}?{query}").parse()?
            } else {
                format!("{base}{new_path}").parse()?
            }
        }
        None => {
            let pq = parts.uri.path_and_query().map_or("/", |p| p.as_str());
            if pq == "/" {
                upstream_base.parse()?
            } else {
                let base = upstream_base.trim_end_matches('/');
                format!("{base}{pq}").parse()?
            }
        }
    };
    parts.uri = upstream_uri;
    strip_hop_by_hop(&mut parts.headers);
    parts.extensions.clear();
    // The inbound version (e.g. HTTP/2) must not be forced onto the upstream
    // connection — normalize so the client uses whatever the upstream negotiates
    // (h1 for cleartext, h2 via ALPN for an https upstream).
    parts.version = Version::HTTP_11;
    let upstream_req = Request::from_parts(parts, box_incoming(body));

    let resp = tokio::time::timeout(state.cfg.proxy_timeout, client.request(upstream_req))
        .await
        .map_err(|_| -> BoxError { "upstream timeout".into() })??;

    let (mut resp_parts, resp_body) = resp.into_parts();
    strip_hop_by_hop(&mut resp_parts.headers);
    Ok(Response::from_parts(resp_parts, box_incoming(resp_body)))
}

async fn http_proxy<B>(
    req: Request<B>,
    state: &AppState,
    upstream_base: &str,
    client: &ProxyClient,
) -> Result<Response<ResBody>, BoxError>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    http_proxy_with_path(req, state, upstream_base, None, client).await
}

fn build_ws_request(req: &Request<Incoming>, upstream_url: &str) -> Result<Request<()>, BoxError> {
    let req_pq = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let target_uri = if req_pq == "/" {
        upstream_url.to_string()
    } else {
        let base = upstream_url.trim_end_matches('/');
        format!("{base}{req_pq}")
    };
    let parsed_uri: http::Uri = target_uri.parse()?;

    let mut upstream_req = Request::builder()
        .method(Method::GET)
        .uri(&target_uri)
        .body(())?;

    // Copy headers from client request
    *upstream_req.headers_mut() = req.headers().clone();

    // Strip host and content-length, but keep Connection and Upgrade
    upstream_req.headers_mut().remove(http::header::HOST);
    upstream_req
        .headers_mut()
        .remove(http::header::CONTENT_LENGTH);

    // Re-insert correct Host header for upstream
    if let Some(auth) = parsed_uri.authority() {
        upstream_req.headers_mut().insert(
            http::header::HOST,
            http::HeaderValue::from_str(auth.as_str())?,
        );
    }

    // Ensure Sec-WebSocket-Key and Sec-WebSocket-Version are present for HTTP/1.1 upstream handshake
    if !upstream_req
        .headers()
        .contains_key(http::header::SEC_WEBSOCKET_KEY)
    {
        let key = tokio_tungstenite::tungstenite::handshake::client::generate_key();
        upstream_req.headers_mut().insert(
            http::header::SEC_WEBSOCKET_KEY,
            http::HeaderValue::from_str(&key)?,
        );
    }
    if !upstream_req
        .headers()
        .contains_key(http::header::SEC_WEBSOCKET_VERSION)
    {
        upstream_req.headers_mut().insert(
            http::header::SEC_WEBSOCKET_VERSION,
            http::HeaderValue::from_static("13"),
        );
    }

    // Ensure Connection and Upgrade are correct
    upstream_req.headers_mut().insert(
        http::header::CONNECTION,
        http::HeaderValue::from_static("Upgrade"),
    );
    upstream_req.headers_mut().insert(
        http::header::UPGRADE,
        http::HeaderValue::from_static("websocket"),
    );

    Ok(upstream_req)
}

fn get_ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    let mut config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    config.max_message_size = Some(64 * 1024 * 1024); // 64MB
    config.max_frame_size = Some(16 * 1024 * 1024); // 16MB
    config
}

async fn ws_upgrade(
    mut req: Request<Incoming>,
    upstream_url: String,
    connect_timeout: Duration,
    trust_upstream: bool,
) -> Result<Response<ResBody>, BoxError> {
    // Dial the upstream WebSocket *before* completing the client upgrade. If we
    // returned 101 first and the upstream were down, the client would see a
    // successful handshake immediately followed by an abnormal close with no
    // reason. Connecting first lets a dead or slow upstream surface as a 502 on
    // the handshake instead — and bounds the dial so a hung upstream can't leak
    // a half-open client connection.
    let ws_req = build_ws_request(&req, &upstream_url)?;
    let ws_config = get_ws_config();
    let upstream_ws = match tokio::time::timeout(
        connect_timeout,
        tokio_tungstenite::connect_async_with_config(ws_req, Some(ws_config), false),
    )
    .await
    {
        Ok(Ok((ws, _))) => {
            configure_ws_keepalive(&ws);
            ws
        }
        Ok(Err(e)) => {
            debug!(error = %e, upstream = %upstream_url, "upstream ws connect failed");
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                format!("upstream ws connect failed: {e}"),
            ));
        }
        Err(_) => {
            debug!(upstream = %upstream_url, "upstream ws connect timed out");
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                "upstream ws connect timed out",
            ));
        }
    };

    let (response, websocket) = hyper_tungstenite::upgrade(&mut req, Some(ws_config))?;
    tokio::spawn(async move {
        let client_ws = match websocket.await {
            Ok(ws) => ws,
            Err(e) => {
                debug!(error = %e, "client ws upgrade failed");
                return;
            }
        };
        debug!(upstream = %upstream_url, "ws bridge established");
        bridge_ws(client_ws, upstream_ws, trust_upstream).await;
    });
    Ok(response.map(box_full))
}

/// HTTP/2 Extended CONNECT (RFC 8441) WebSocket. The handshake differs from h1
/// (200, not 101; no `Upgrade` header), but the tunnel carries the same RFC 6455
/// frames — so we terminate the h2 stream, wrap it (server role), and reuse
/// `bridge_ws` to relay to the upstream h1 WebSocket.
async fn ws_upgrade_h2(
    req: Request<Incoming>,
    upstream_url: String,
    connect_timeout: Duration,
    trust_upstream: bool,
) -> Result<Response<ResBody>, BoxError> {
    // Dial the upstream WebSocket first, same as the h1 path, so a dead upstream
    // is a 502 rather than an accepted-then-dropped tunnel.
    let ws_req = build_ws_request(&req, &upstream_url)?;
    let ws_config = get_ws_config();
    let upstream_ws = match tokio::time::timeout(
        connect_timeout,
        tokio_tungstenite::connect_async_with_config(ws_req, Some(ws_config), false),
    )
    .await
    {
        Ok(Ok((ws, _))) => {
            configure_ws_keepalive(&ws);
            ws
        }
        Ok(Err(e)) => {
            debug!(error = %e, upstream = %upstream_url, "upstream ws connect failed");
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                format!("upstream ws connect failed: {e}"),
            ));
        }
        Err(_) => {
            debug!(upstream = %upstream_url, "upstream ws connect timed out");
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                "upstream ws connect timed out",
            ));
        }
    };

    let on_upgrade = hyper::upgrade::on(req);
    tokio::spawn(async move {
        match on_upgrade.await {
            Ok(upgraded) => {
                let client_ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
                    TokioIo::new(upgraded),
                    Role::Server,
                    Some(ws_config),
                )
                .await;
                debug!(upstream = %upstream_url, "h2 ws bridge established");
                bridge_ws(client_ws, upstream_ws, trust_upstream).await;
            }
            Err(e) => debug!(error = %e, "h2 ws upgrade failed"),
        }
    });

    // 200 accepts the Extended CONNECT; the stream then becomes the WS tunnel.
    Ok(text_response(StatusCode::OK, Bytes::new()))
}

async fn bridge_ws<C, U>(
    client_ws: tokio_tungstenite::WebSocketStream<C>,
    upstream_ws: tokio_tungstenite::WebSocketStream<U>,
    trust_upstream: bool,
) where
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    U: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let _guard = crate::metrics::ActiveConnectionGuard::new("ws");
    let (c_tx, mut c_rx) = client_ws.split();
    let (mut u_tx, mut u_rx) = upstream_ws.split();
    let c_tx = Arc::new(tokio::sync::Mutex::new(c_tx));
    let c_tx_clone = c_tx.clone();

    let c2u = async move {
        while let Some(Ok(m)) = c_rx.next().await {
            if trust_upstream
                && let Ok(text) = m.to_text()
                && let Some(resp) = try_mock_ws_eth_syncing(text)
            {
                if c_tx_clone
                    .lock()
                    .await
                    .send(tokio_tungstenite::tungstenite::Message::Text(resp.into()))
                    .await
                    .is_err()
                {
                    break;
                }
                continue;
            }
            if u_tx.send(m).await.is_err() {
                break;
            }
        }
    };
    let u2c = async move {
        while let Some(Ok(m)) = u_rx.next().await {
            if c_tx.lock().await.send(m).await.is_err() {
                break;
            }
        }
    };
    tokio::pin!(c2u);
    tokio::pin!(u2c);
    tokio::select! {
        _ = &mut c2u => {},
        _ = &mut u2c => {},
    }
}

pub(crate) fn classify_request<B>(req: &Request<B>) -> (&'static str, &'static str) {
    let upstream_type = if is_cl_path(req.uri().path()) {
        "CL"
    } else {
        "EL"
    };
    let upstream_proto = if req.method() == Method::CONNECT
        && req
            .extensions()
            .get::<hyper::ext::Protocol>()
            .is_some_and(|p| p.as_str() == "websocket")
        || hyper_tungstenite::is_upgrade_request(req)
    {
        "WS"
    } else {
        "HTTP"
    };
    (upstream_type, upstream_proto)
}

pub fn box_incoming<B>(body: B) -> ResBody
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    body.map_err(Into::into).boxed_unsync()
}

pub fn box_full(body: Full<Bytes>) -> ResBody {
    body.map_err(|e: Infallible| match e {}).boxed_unsync()
}

pub fn text_response(code: StatusCode, msg: impl Into<Bytes>) -> Response<ResBody> {
    Response::builder()
        .status(code)
        .header("content-type", "text/plain; charset=utf-8")
        .body(box_full(Full::new(msg.into())))
        .unwrap()
}

fn configure_ws_keepalive(
    ws: &tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let inner = ws.get_ref();
    let tcp_stream = match inner {
        tokio_tungstenite::MaybeTlsStream::Plain(s) => Some(s),
        tokio_tungstenite::MaybeTlsStream::Rustls(s) => Some(s.get_ref().0),
        _ => None,
    };
    if let Some(stream) = tcp_stream {
        let sock_ref = socket2::SockRef::from(stream);
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(10));
        if let Err(e) = sock_ref.set_tcp_keepalive(&keepalive) {
            debug!(error = %e, "failed to set upstream WS TCP keepalive");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_cl_path() {
        // Positive cases: Standard and client-specific CL prefixes
        assert!(is_cl_path("/eth"));
        assert!(is_cl_path("/eth/"));
        assert!(is_cl_path("/eth/v1/node/syncing"));

        assert!(is_cl_path("/teku"));
        assert!(is_cl_path("/teku/"));
        assert!(is_cl_path("/teku/v1/node/syncing"));

        assert!(is_cl_path("/prysm"));
        assert!(is_cl_path("/prysm/"));
        assert!(is_cl_path("/prysm/v1/node/syncing"));

        assert!(is_cl_path("/nimbus"));
        assert!(is_cl_path("/nimbus/"));
        assert!(is_cl_path("/nimbus/v1/node/syncing"));

        assert!(is_cl_path("/lodestar"));
        assert!(is_cl_path("/lodestar/"));
        assert!(is_cl_path("/lodestar/v1/node/syncing"));

        assert!(is_cl_path("/lighthouse"));
        assert!(is_cl_path("/lighthouse/"));
        assert!(is_cl_path("/lighthouse/v1/node/syncing"));

        // Negative cases
        assert!(!is_cl_path("/"));
        assert!(!is_cl_path(""));
        assert!(!is_cl_path("/healthz"));
        assert!(!is_cl_path("/readyz"));
        assert!(!is_cl_path("/livez"));
        assert!(!is_cl_path("/lighthousestuff"));
        assert!(!is_cl_path("/prys"));
        assert!(!is_cl_path("eth"));
    }

    #[test]
    fn test_classify_request() {
        // 1. CL Path HTTP request
        let req = Request::builder()
            .uri("/eth/v1/node/syncing")
            .body(())
            .unwrap();
        assert_eq!(classify_request(&req), ("CL", "HTTP"));

        // 2. EL Path HTTP request
        let req = Request::builder().uri("/").body(()).unwrap();
        assert_eq!(classify_request(&req), ("EL", "HTTP"));

        // 3. WS HTTP/1.1 Upgrade request
        let req = Request::builder()
            .uri("/")
            .header("connection", "upgrade")
            .header("upgrade", "websocket")
            .body(())
            .unwrap();
        assert_eq!(classify_request(&req), ("EL", "WS"));

        // 4. WS HTTP/2 Extended CONNECT request
        let mut req = Request::builder()
            .method(Method::CONNECT)
            .uri("/")
            .body(())
            .unwrap();
        req.extensions_mut()
            .insert(hyper::ext::Protocol::from_static("websocket"));
        assert_eq!(classify_request(&req), ("EL", "WS"));
    }

    #[tokio::test]
    async fn test_try_mock_eth_syncing() {
        // Single eth_syncing request
        let req = r#"{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":100}"#;
        let res = try_mock_eth_syncing(req.as_bytes()).unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = http_body_util::BodyExt::collect(res.into_body())
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(body, r#"{"jsonrpc":"2.0","id":100,"result":false}"#);

        // Non-eth_syncing request should return None
        let req = r#"{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":101}"#;
        assert!(try_mock_eth_syncing(req.as_bytes()).is_none());

        // Batch eth_syncing request
        let batch = r#"[{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}]"#;
        let res = try_mock_eth_syncing(batch.as_bytes()).unwrap();
        let body = http_body_util::BodyExt::collect(res.into_body())
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(body, r#"[{"jsonrpc":"2.0","id":1,"result":false}]"#);
    }

    #[test]
    fn test_try_mock_ws_eth_syncing() {
        let req = r#"{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":42}"#;
        assert_eq!(
            try_mock_ws_eth_syncing(req).unwrap(),
            r#"{"jsonrpc":"2.0","id":42,"result":false}"#
        );

        let non_syncing = r#"{"jsonrpc":"2.0","method":"eth_call","params":[],"id":43}"#;
        assert!(try_mock_ws_eth_syncing(non_syncing).is_none());
    }
}
