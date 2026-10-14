//! WebSocket handshake and bounded relay. HTTP/1 and HTTP/2 share the same dial path.
use crate::proxy::{BoxError, ResBody, box_full, text_response};
use crate::rpc::{MockResult, mock_syncing};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use http::{Method, Request, Response, StatusCode};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::{Error, Message, protocol::Role};
use tracing::debug;

pub(crate) fn is_upgrade_request<B>(req: &Request<B>) -> bool {
    fn contains(headers: &http::HeaderMap, name: http::HeaderName, token: &str) -> bool {
        headers
            .get_all(name)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|v| v.trim().eq_ignore_ascii_case(token))
    }
    contains(req.headers(), http::header::CONNECTION, "upgrade")
        && contains(req.headers(), http::header::UPGRADE, "websocket")
}

fn build_ws_request<B>(req: &Request<B>, upstream_url: &str) -> Result<Request<()>, BoxError> {
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
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .read_buffer_size(8 * 1024)
        .write_buffer_size(16 * 1024)
        .max_message_size(Some(64 * 1024 * 1024))
        .max_frame_size(Some(16 * 1024 * 1024))
}

pub(crate) async fn upgrade(
    req: Request<Incoming>,
    upstream_url: String,
    connect_timeout: Duration,
    mock_enabled: bool,
) -> Result<Response<ResBody>, BoxError> {
    let (parts, _body) = req.into_parts();
    let mut req = Request::from_parts(parts, ());
    let extended = req.method() == Method::CONNECT;
    // Delegate RFC 6455 validation to Tungstenite before dialing upstream.
    let mut response = if extended {
        Response::builder().status(StatusCode::OK).body(())?
    } else {
        match tokio_tungstenite::tungstenite::handshake::server::create_response(&req) {
            Ok(response) => response,
            Err(_) => {
                return Ok(text_response(
                    StatusCode::BAD_REQUEST,
                    "invalid websocket upgrade",
                ));
            }
        }
    };
    let ws_req = build_ws_request(&req, &upstream_url)?;
    let (upstream_ws, upstream_response) = match tokio::time::timeout(
        connect_timeout,
        tokio_tungstenite::connect_async_with_config(ws_req, Some(get_ws_config()), true),
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            debug!(error = %e, "upstream ws connect failed");
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                "upstream ws connect failed",
            ));
        }
        Err(_) => {
            return Ok(text_response(
                StatusCode::BAD_GATEWAY,
                "upstream ws connect timed out",
            ));
        }
    };
    configure_ws_keepalive(&upstream_ws);
    // Tungstenite checks that the selected protocol was offered by the client.
    if let Some(protocol) = upstream_response
        .headers()
        .get(http::header::SEC_WEBSOCKET_PROTOCOL)
    {
        response
            .headers_mut()
            .insert(http::header::SEC_WEBSOCKET_PROTOCOL, protocol.clone());
    }
    let on_upgrade = hyper::upgrade::on(&mut req);
    tokio::spawn(async move {
        match on_upgrade.await {
            Ok(upgraded) => {
                let client_ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
                    TokioIo::new(upgraded),
                    Role::Server,
                    Some(get_ws_config()),
                )
                .await;
                bridge_ws(client_ws, upstream_ws, mock_enabled).await;
            }
            Err(e) => debug!(error = %e, "client ws upgrade failed"),
        }
    });
    Ok(response.map(|()| box_full(http_body_util::Full::new(bytes::Bytes::new()))))
}

async fn forward<R, W>(mut reader: R, mut writer: W)
where
    R: Stream<Item = Result<Message, Error>> + Unpin,
    W: Sink<Message, Error = Error> + Unpin,
{
    while let Some(Ok(message)) = reader.next().await {
        let closing = message.is_close();
        let sent = writer.send(message).await;
        if closing {
            // Reading once more flushes Tungstenite's queued close acknowledgement
            // on this reader's socket without adding a shared sink mutex.
            let _ = tokio::time::timeout(Duration::from_secs(1), reader.next()).await;
            break;
        }
        if sent.is_err() {
            break;
        }
    }
}

async fn bridge_ws<C, U>(
    client_ws: tokio_tungstenite::WebSocketStream<C>,
    upstream_ws: tokio_tungstenite::WebSocketStream<U>,
    mock_enabled: bool,
) where
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    U: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let _guard = crate::metrics::ActiveConnectionGuard::new("ws");
    let (c_tx, mut c_rx) = client_ws.split();
    let (mut u_tx, mut u_rx) = upstream_ws.split();
    if !mock_enabled {
        drain_pair(forward(c_rx, u_tx), forward(u_rx, c_tx)).await;
        return;
    }
    let c_tx = Arc::new(tokio::sync::Mutex::new(c_tx));
    let c_tx_clone = c_tx.clone();

    let c2u = async move {
        while let Some(Ok(m)) = c_rx.next().await {
            if let Message::Text(text) = &m {
                match mock_syncing(text.as_bytes()) {
                    MockResult::Silent => continue,
                    MockResult::Reply(reply) => {
                        if c_tx_clone
                            .lock()
                            .await
                            .send(Message::Text(reply.into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    MockResult::Forward => {}
                }
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
    drain_pair(c2u, u2c).await;
}

async fn drain_pair(
    a: impl std::future::Future<Output = ()>,
    b: impl std::future::Future<Output = ()>,
) {
    tokio::pin!(a, b);
    // Give the opposite direction time to flush its close reply; a broken peer
    // must not keep the other half of a tunnel alive indefinitely.
    let a_finished = tokio::select! { _ = &mut a => true, _ = &mut b => false };
    let grace = Duration::from_secs(1);
    if a_finished {
        let _ = tokio::time::timeout(grace, b).await;
    } else {
        let _ = tokio::time::timeout(grace, a).await;
    }
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
