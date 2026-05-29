use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{Method, Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::body::Incoming;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use tokio_tungstenite::connect_async;
use tracing::{debug, trace};

use crate::headers::strip_hop_by_hop;
use crate::health;
use crate::state::AppState;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ResBody = UnsyncBoxBody<Bytes, BoxError>;

type Https = hyper_rustls::HttpsConnector<HttpConnector>;
pub type ProxyClient = Client<Https, ResBody>;

pub fn build_client() -> ProxyClient {
    let mut http = HttpConnector::new();
    http.set_nodelay(true);
    http.enforce_http(false);
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .wrap_connector(http);
    Client::builder(TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(60))
        .build(https)
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

async fn route(req: Request<Incoming>, state: &AppState) -> Result<Response<ResBody>, BoxError> {
    trace!(method = %req.method(), path = req.uri().path(), "routing request");
    if req.method() == Method::GET {
        let path = req.uri().path();
        if path == "/healthz" {
            return Ok(health::report(state));
        }
        if path == "/readyz" {
            return Ok(health::ready(state));
        }
        if path == "/livez" {
            return Ok(text_response(StatusCode::OK, Bytes::from_static(b"ok")));
        }
    }
    if req.uri().path().starts_with("/eth/") {
        return http_proxy(req, state, &state.cfg.cl_beacon_url).await;
    }
    if hyper_tungstenite::is_upgrade_request(&req) {
        return ws_upgrade(req, state.cfg.el_ws_url.clone(), state.cfg.proxy_timeout).await;
    }
    http_proxy(req, state, &state.cfg.el_http_url).await
}

async fn http_proxy(
    req: Request<Incoming>,
    state: &AppState,
    upstream_base: &str,
) -> Result<Response<ResBody>, BoxError> {
    let (mut parts, body) = req.into_parts();
    let upstream_uri: Uri = {
        let pq = parts.uri.path_and_query().map_or("/", |p| p.as_str());
        format!("{}{}", upstream_base.trim_end_matches('/'), pq).parse()?
    };
    parts.uri = upstream_uri;
    strip_hop_by_hop(&mut parts.headers);
    parts.extensions.clear();
    let upstream_req = Request::from_parts(parts, box_incoming(body));

    let resp = tokio::time::timeout(state.cfg.proxy_timeout, state.client.request(upstream_req))
        .await
        .map_err(|_| -> BoxError { "upstream timeout".into() })??;

    let (mut resp_parts, resp_body) = resp.into_parts();
    strip_hop_by_hop(&mut resp_parts.headers);
    Ok(Response::from_parts(resp_parts, box_incoming(resp_body)))
}

async fn ws_upgrade(
    mut req: Request<Incoming>,
    upstream_url: String,
    connect_timeout: Duration,
) -> Result<Response<ResBody>, BoxError> {
    // Dial the upstream WebSocket *before* completing the client upgrade. If we
    // returned 101 first and the upstream were down, the client would see a
    // successful handshake immediately followed by an abnormal close with no
    // reason. Connecting first lets a dead or slow upstream surface as a 502 on
    // the handshake instead — and bounds the dial so a hung upstream can't leak
    // a half-open client connection.
    let upstream_ws =
        match tokio::time::timeout(connect_timeout, connect_async(&upstream_url)).await {
            Ok(Ok((ws, _))) => ws,
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

    let (response, websocket) = hyper_tungstenite::upgrade(&mut req, None)?;
    tokio::spawn(async move {
        let client_ws = match websocket.await {
            Ok(ws) => ws,
            Err(e) => {
                debug!(error = %e, "client ws upgrade failed");
                return;
            }
        };
        debug!(upstream = %upstream_url, "ws bridge established");
        bridge_ws(client_ws, upstream_ws).await;
    });
    Ok(response.map(box_full))
}

async fn bridge_ws<C, U>(
    client_ws: tokio_tungstenite::WebSocketStream<C>,
    upstream_ws: tokio_tungstenite::WebSocketStream<U>,
) where
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    U: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut c_tx, mut c_rx) = client_ws.split();
    let (mut u_tx, mut u_rx) = upstream_ws.split();

    let c2u = async {
        while let Some(Ok(m)) = c_rx.next().await {
            if u_tx.send(m).await.is_err() {
                break;
            }
        }
    };
    let u2c = async {
        while let Some(Ok(m)) = u_rx.next().await {
            if c_tx.send(m).await.is_err() {
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
