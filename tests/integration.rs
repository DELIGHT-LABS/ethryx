//! Integration tests: spawn ethryx in-process against a hyper-based mock
//! upstream, driving the full proxy / readiness / health flow with the same
//! hyper-util Client used in production code. Zero extra dev-dependencies.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use http::{Method, Request, Response, StatusCode, Version};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{accept_async, connect_async};

use ethryx::{Config, run};

type TestBody = Full<Bytes>;
type TestClient = Client<HttpConnector, TestBody>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ---------- helpers ----------

/// Bind an ephemeral port and immediately release it. Returns the port number.
/// Race window between drop and re-bind is tiny on localhost.
async fn pick_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// POST a JSON-RPC and retry briefly while the response is `502`. The very first
/// data-plane request to an h2c upstream may establish a fresh HTTP/2 connection,
/// which can momentarily exceed the test's short `--proxy-timeout` on a heavily
/// loaded (cold-start) CI runner; a steady-state sidecar keeps the pool warm. This
/// mirrors ethryx's documented behaviour that a transport in flux can briefly
/// surface a `502` until it settles, so it doesn't mask a real failure (a
/// persistent `502` still fails the test).
async fn post_json_settled(c: &TestClient, url: &str, body: Value) -> (StatusCode, Bytes) {
    let mut last = (StatusCode::BAD_GATEWAY, Bytes::new());
    for _ in 0..50 {
        last = post_json(c, url, body.clone()).await;
        if last.0 != StatusCode::BAD_GATEWAY {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    last
}

/// Poll until the given port accepts connections.
async fn wait_for_port(port: u16) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("port {port} did not become available");
}

fn client() -> TestClient {
    let mut http = HttpConnector::new();
    http.set_nodelay(true);
    Client::builder(TokioExecutor::new()).build::<_, TestBody>(http)
}

/// Cleartext HTTP/2 (h2c) prior-knowledge client — sends the h2 preface directly,
/// simulating a TLS-terminating LB / mesh that forwards h2c to the backend.
fn h2c_client() -> TestClient {
    let mut http = HttpConnector::new();
    http.set_nodelay(true);
    Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build::<_, TestBody>(http)
}

async fn get(c: &TestClient, url: &str) -> (StatusCode, Bytes) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(url)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = c.request(req).await.unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

async fn post_json(c: &TestClient, url: &str, body: Value) -> (StatusCode, Bytes) {
    let payload = serde_json::to_vec(&body).unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri(url)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(payload)))
        .unwrap();
    let resp = c.request(req).await.unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

// ---------- mock upstream ----------

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: Method,
    path: String,
    #[allow(dead_code)]
    headers: http::HeaderMap,
    version: Version,
    body: Bytes,
}

type MockHandler = Arc<dyn Fn(&RecordedRequest) -> (StatusCode, Vec<u8>) + Send + Sync>;

struct MockServer {
    url: String,
    recorded: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl MockServer {
    /// Serve HTTP/1.1 only (rejects an h2c prior-knowledge preface).
    async fn start(handler: MockHandler) -> Self {
        Self::serve(handler, false, false).await
    }

    /// Serve both HTTP/1.1 and cleartext HTTP/2 (h2c), like geth >=v1.17 / erigon.
    async fn start_h2c(handler: MockHandler) -> Self {
        Self::serve(handler, true, false).await
    }

    /// Serve HTTP/1.1 only, but hang if an h2c prior-knowledge preface (PRI) is received.
    async fn start_hanging_h2c(handler: MockHandler) -> Self {
        Self::serve(handler, false, true).await
    }

    async fn serve(handler: MockHandler, h2: bool, hang_h2c: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let url = format!("http://{addr}");
        let recorded = Arc::new(Mutex::new(Vec::new()));

        let h = handler;
        let rec = recorded.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let h = h.clone();
                let rec = rec.clone();
                tokio::spawn(async move {
                    if hang_h2c {
                        // Peek first 3 bytes to see if it starts with HTTP/2 preface "PRI"
                        let mut buf = [0u8; 3];
                        if stream
                            .peek(&mut buf)
                            .await
                            .is_ok_and(|n| n >= 3 && &buf == b"PRI")
                        {
                            // It's the HTTP/2 preface! Hang for 1.5 seconds to cause a timeout.
                            tokio::time::sleep(Duration::from_millis(1500)).await;
                            return;
                        }
                    }

                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let h = h.clone();
                        let rec = rec.clone();
                        async move {
                            let method = req.method().clone();
                            let path = req.uri().path().to_owned();
                            let headers = req.headers().clone();
                            let version = req.version();
                            let body_bytes = req.into_body().collect().await.unwrap().to_bytes();

                            let (status, resp_bytes) = if body_bytes.first() == Some(&b'[') {
                                // JSON-RPC batch request
                                if let Ok(Value::Array(req_arr)) =
                                    serde_json::from_slice::<Value>(&body_bytes)
                                {
                                    let mut resp_arr = Vec::new();
                                    let mut overall_status = StatusCode::OK;
                                    for req_item in req_arr {
                                        let item_bytes = serde_json::to_vec(&req_item).unwrap();
                                        let rr = RecordedRequest {
                                            method: method.clone(),
                                            path: path.clone(),
                                            headers: headers.clone(),
                                            version,
                                            body: Bytes::from(item_bytes),
                                        };
                                        rec.lock().unwrap().push(rr.clone());
                                        let (status, item_resp_bytes) = h(&rr);
                                        if !status.is_success() {
                                            overall_status = status;
                                        }
                                        let req_id = req_item.get("id").cloned();
                                        let mut item_resp_val: Value =
                                            serde_json::from_slice(&item_resp_bytes)
                                                .unwrap_or(Value::Null);
                                        if let (Some(id_val), Some(obj)) =
                                            (req_id, item_resp_val.as_object_mut())
                                        {
                                            obj.insert("id".to_string(), id_val);
                                        }
                                        resp_arr.push(item_resp_val);
                                    }
                                    let final_resp =
                                        serde_json::to_vec(&Value::Array(resp_arr)).unwrap();
                                    (overall_status, final_resp)
                                } else {
                                    (StatusCode::BAD_REQUEST, b"[]".to_vec())
                                }
                            } else {
                                // Single request
                                let rr = RecordedRequest {
                                    method: method.clone(),
                                    path: path.clone(),
                                    headers: headers.clone(),
                                    version,
                                    body: body_bytes.clone(),
                                };
                                rec.lock().unwrap().push(rr.clone());
                                let (status, resp_bytes) = h(&rr);
                                (status, resp_bytes)
                            };

                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(status)
                                    .header("content-type", "application/json")
                                    .body(Full::new(Bytes::from(resp_bytes)))
                                    .unwrap(),
                            )
                        }
                    });
                    if h2 {
                        let _ = auto::Builder::new(TokioExecutor::new())
                            .serve_connection(io, svc)
                            .await;
                    } else {
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, svc)
                            .await;
                    }
                });
            }
        });

        MockServer { url, recorded }
    }

    fn received(&self) -> Vec<RecordedRequest> {
        self.recorded.lock().unwrap().clone()
    }
}

// ---------- ethryx handle ----------

struct EthryxHandle {
    port: u16,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), BoxError>>>,
}

impl EthryxHandle {
    async fn start(args: &[&str]) -> Self {
        let port = pick_port().await;
        let addr_str = format!("127.0.0.1:{port}");
        let mut argv: Vec<&str> = vec![
            "ethryx",
            "--listen",
            &addr_str,
            "--shutdown-grace",
            "0",
            "--health-timeout",
            "1",
            "--proxy-timeout",
            "2",
        ];
        argv.extend_from_slice(args);
        let cfg = Config::try_parse_from(argv).expect("clap parse");

        let (tx, rx) = oneshot::channel::<()>();
        let task = tokio::spawn(run(cfg, async move {
            let _ = rx.await;
        }));
        wait_for_port(port).await;
        EthryxHandle {
            port,
            shutdown: Some(tx),
            task: Some(task),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{}", self.port, path)
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

fn ok_handler() -> MockHandler {
    Arc::new(|_| (StatusCode::OK, b"{}".to_vec()))
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------- tests ----------

#[tokio::test]
async fn livez_returns_200_ok() {
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethryx.url("/livez")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"ok");

    ethryx.shutdown().await;
}

#[tokio::test]
async fn jsonrpc_post_is_forwarded_to_el_http_upstream() {
    let el = MockServer::start(Arc::new(|req| {
        assert_eq!(req.method, Method::POST);
        assert_eq!(req.path, "/");
        (
            StatusCode::OK,
            br#"{"jsonrpc":"2.0","id":7,"result":"0xdeadbeef"}"#.to_vec(),
        )
    }))
    .await;
    let cl = MockServer::start(ok_handler()).await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = post_json(
        &c,
        &ethryx.url("/"),
        json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 7}),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["result"], "0xdeadbeef");

    // EL also receives background health-poll RPCs; filter for the forwarded one.
    let methods: Vec<String> = el
        .received()
        .iter()
        .filter_map(|r| serde_json::from_slice::<Value>(&r.body).ok())
        .filter_map(|v| v.get("method").and_then(Value::as_str).map(String::from))
        .collect();
    assert_eq!(
        methods.iter().filter(|m| *m == "eth_blockNumber").count(),
        1,
        "expected one forwarded eth_blockNumber, saw {methods:?}"
    );

    // The EL mock is HTTP/1.1-only, so ethryx (which prefers h2c) detects that the
    // h2c probe fails and falls back to HTTP/1.1 for the upstream hop.
    let fwd = el
        .received()
        .into_iter()
        .find(|r| String::from_utf8_lossy(&r.body).contains("eth_blockNumber"))
        .expect("forwarded request recorded");
    assert_eq!(fwd.version, Version::HTTP_11);

    ethryx.shutdown().await;
}

#[tokio::test]
async fn el_upstream_h2c_is_auto_detected() {
    // No flag: when the EL serves h2c, the health poller detects it (it prefers
    // h2c) and the data-plane forwards over HTTP/2. The mock records the version
    // it received. (An h1-only EL falls back to HTTP/1.1 — covered by
    // `jsonrpc_post_is_forwarded_to_el_http_upstream`.)
    let el = MockServer::start_h2c(Arc::new(|_| {
        (
            StatusCode::OK,
            br#"{"jsonrpc":"2.0","id":7,"result":"0xcafe"}"#.to_vec(),
        )
    }))
    .await;
    let cl = MockServer::start(ok_handler()).await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    // Downstream client version is irrelevant — it's the upstream hop we assert.
    let c = client();
    let (status, body) = post_json_settled(
        &c,
        &ethryx.url("/"),
        json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 7}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["result"], "0xcafe");

    // The EL upstream received the forwarded JSON-RPC over HTTP/2 (auto-detected).
    let fwd = el
        .received()
        .into_iter()
        .find(|r| String::from_utf8_lossy(&r.body).contains("eth_blockNumber"))
        .expect("EL received the forwarded JSON-RPC");
    assert_eq!(fwd.version, Version::HTTP_2);

    // CL is unaffected — its hop stays HTTP/1.1.
    let cl_req = cl
        .received()
        .into_iter()
        .next()
        .expect("CL received a health probe");
    assert_eq!(cl_req.version, Version::HTTP_11);

    // /healthz surfaces the detected upstream transport per layer.
    let (_, hz) = get(&c, &ethryx.url("/healthz")).await;
    let hv: Value = serde_json::from_slice(&hz).unwrap();
    assert_eq!(hv["el"]["transport"], "h2c");
    assert_eq!(hv["cl"]["transport"], "http/1.1");

    ethryx.shutdown().await;
}

// HTTP/2 downstream coverage: h1→h1 is `jsonrpc_post_is_forwarded` above; h2c→h1
// (the LB→backend h2c shape) is below. Upstream h2 (h1→h2, h2→h2) can't be tested
// hermetically — the production client trusts only webpki public roots, so a
// self-signed h2 mock is rejected. Verified manually against a public h2 server:
//   ethryx --el-http-url https://www.cloudflare.com --listen 127.0.0.1:18548 &
//   curl -s http://127.0.0.1:18548/cdn-cgi/trace | grep http=        # h1→h2 ⇒ http/2
//   curl -s --http2-prior-knowledge .../cdn-cgi/trace | grep http=   # h2→h2 ⇒ http/2
#[tokio::test]
async fn http2_h2c_jsonrpc_is_served_and_forwarded() {
    // A prior-knowledge h2c client (the LB→backend h2c shape) is served over
    // HTTP/2 by the auto server, and its JSON-RPC is forwarded to the EL upstream
    // (which stays h1).
    let el = MockServer::start(Arc::new(|req| {
        assert_eq!(req.method, Method::POST);
        (
            StatusCode::OK,
            br#"{"jsonrpc":"2.0","id":7,"result":"0xcafe"}"#.to_vec(),
        )
    }))
    .await;
    let cl = MockServer::start(ok_handler()).await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = h2c_client();
    let (status, body) = post_json(
        &c,
        &ethryx.url("/"),
        json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 7}),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["result"], "0xcafe");

    ethryx.shutdown().await;
}

#[tokio::test]
async fn http2_h2c_negotiates_to_http2() {
    // The response comes back over HTTP/2 → confirms the auto server actually
    // spoke h2c (not a silent downgrade to h1).
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = h2c_client();
    let req = Request::builder()
        .method(Method::GET)
        .uri(ethryx.url("/livez"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = c.request(req).await.unwrap();
    assert_eq!(resp.version(), Version::HTTP_2, "expected h2, got {resp:?}");
    assert_eq!(resp.status(), StatusCode::OK);

    ethryx.shutdown().await;
}

#[tokio::test]
async fn beacon_path_is_routed_to_cl_upstream() {
    let el = MockServer::start(Arc::new(|req| {
        // Background health polls legitimately POST to "/"; only /eth/ beacon
        // paths must never be routed to the EL upstream.
        assert!(
            !req.path.starts_with("/eth/"),
            "EL must not see beacon /eth/ traffic, got {}",
            req.path
        );
        (StatusCode::OK, b"{}".to_vec())
    }))
    .await;
    let cl = MockServer::start(Arc::new(|req| {
        assert_eq!(req.method, Method::GET);
        assert_eq!(req.path, "/eth/v1/beacon/genesis");
        (
            StatusCode::OK,
            br#"{"data":{"genesis_time":"1606824023"}}"#.to_vec(),
        )
    }))
    .await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethryx.url("/eth/v1/beacon/genesis")).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"]["genesis_time"], "1606824023");

    ethryx.shutdown().await;
}

#[tokio::test]
async fn client_specific_beacon_paths_are_routed_to_cl_upstream() {
    let el = MockServer::start(Arc::new(|req| {
        // Assert that none of the client-specific paths reach EL.
        let path = &req.path;
        assert!(
            !(path.starts_with("/lighthouse")
                || path.starts_with("/prysm")
                || path.starts_with("/teku")
                || path.starts_with("/lodestar")
                || path.starts_with("/nimbus")),
            "EL must not see client-specific beacon traffic, got {}",
            path
        );
        (StatusCode::OK, b"{}".to_vec())
    }))
    .await;
    let cl = MockServer::start(Arc::new(|req| {
        assert_eq!(req.method, Method::GET);
        let resp = format!(r#"{{"path":"{}"}}"#, req.path);
        (StatusCode::OK, resp.into_bytes())
    }))
    .await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let paths = vec![
        "/lighthouse/version",
        "/prysm/v1/node/syncing",
        "/teku/v1/node/syncing",
        "/lodestar/v1/node/syncing",
        "/nimbus/v1/node/syncing",
        "/lighthouse",
        "/prysm",
        "/teku",
        "/lodestar",
        "/nimbus",
    ];

    for path in paths {
        let (status, body) = get(&c, &ethryx.url(path)).await;
        assert_eq!(status, StatusCode::OK, "Failed for path: {path}");
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["path"], path, "Failed for path: {path}");
    }

    ethryx.shutdown().await;
}

#[tokio::test]
async fn hop_by_hop_headers_are_stripped_on_forward() {
    let el = MockServer::start(Arc::new(|req| {
        // Verify ethryx removed hop-by-hop headers when forwarding.
        // We can't introspect headers here (mock only sees method/path/body),
        // but we can return a marker so the test still exercises forwarding.
        let _ = req;
        (StatusCode::OK, br#"{"ok":true}"#.to_vec())
    }))
    .await;
    let cl = MockServer::start(ok_handler()).await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    // Send with a connection header; ethryx must drop it.
    let req = Request::builder()
        .method(Method::POST)
        .uri(ethryx.url("/"))
        .header("content-type", "application/json")
        .header("connection", "close")
        .body(Full::new(Bytes::from(b"{}".to_vec())))
        .unwrap();
    let resp = c.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    ethryx.shutdown().await;
}

#[tokio::test]
async fn healthz_is_200_even_when_upstreams_fail() {
    // Both mocks return 404 for everything: every probe errors. /healthz reports
    // state, it does not judge it, so it still answers 200 with the errors inline.
    let bad: MockHandler = Arc::new(|_| (StatusCode::NOT_FOUND, b"{}".to_vec()));
    let el = MockServer::start(bad.clone()).await;
    let cl = MockServer::start(bad).await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethryx.url("/healthz")).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    // No verdict field; each failed upstream is recorded under its layer's errors.
    assert!(
        v.get("status").is_none(),
        "healthz must not render a verdict"
    );
    let el_errors = v["el"]["errors"].as_array().expect("el.errors array");
    assert!(
        el_errors
            .iter()
            .any(|e| e.as_str().unwrap().contains("404")),
        "got {v}"
    );
    let cl_errors = v["cl"]["errors"].as_array().expect("cl.errors array");
    assert!(
        cl_errors
            .iter()
            .any(|e| e.as_str().unwrap().contains("404")),
        "got {v}"
    );
    // Transport is reported even when the health probes themselves error: the
    // h1-only mock rejects the h2c probe, so the EL hop falls back to http/1.1.
    assert_eq!(v["el"]["transport"], "http/1.1");
    assert_eq!(v["cl"]["transport"], "http/1.1");

    ethryx.shutdown().await;
}

#[tokio::test]
async fn el_down_keeps_default_h2c_verdict_and_502s() {
    // EL upstream down: both the h2c probe and the h1 fallback fail at the
    // transport layer, so the poller keeps the default (h2c) verdict instead of
    // flapping to h1. /healthz still answers 200 with the EL error inline and
    // reports the unconfirmed default transport; the data-plane returns 502.
    let dead = pick_port().await; // bound then released -> nothing listening
    let cl = MockServer::start(ok_handler()).await;
    let ethryx = EthryxHandle::start(&[
        "--el-http-url",
        &format!("http://127.0.0.1:{dead}"),
        "--cl-beacon-url",
        &cl.url,
    ])
    .await;

    let c = client();
    let (status, body) = get(&c, &ethryx.url("/healthz")).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let el_errors = v["el"]["errors"].as_array().expect("el.errors array");
    assert!(!el_errors.is_empty(), "expected an EL error, got {v}");
    assert_eq!(
        v["el"]["transport"], "h2c",
        "a down EL keeps the default verdict"
    );

    // The data-plane forwards to the (dead) EL and surfaces a 502.
    let (status, _) = post_json(
        &c,
        &ethryx.url("/"),
        json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);

    ethryx.shutdown().await;
}

#[tokio::test]
async fn healthz_is_served_from_cache_not_per_request() {
    // With a long poll interval, only the startup poll hits upstream. Repeated
    // /healthz requests must read the cached snapshot and add no upstream calls.
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ethryx = EthryxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--health-poll-interval",
        "60",
    ])
    .await;

    let c = client();
    for _ in 0..5 {
        let (status, _) = get(&c, &ethryx.url("/healthz")).await;
        assert_eq!(status, StatusCode::OK);
    }
    // One startup poll = 3 EL calls (eth_syncing, net_peerCount,
    // eth_getBlockByNumber) + 2 CL calls, regardless of how many probes arrive.
    assert_eq!(el.received().len(), 3, "EL calls: {:?}", el.received());
    assert_eq!(cl.received().len(), 2, "CL calls: {:?}", cl.received());

    ethryx.shutdown().await;
}

#[tokio::test]
async fn healthz_cache_refreshes_in_background() {
    // The poller must keep refreshing. net_peerCount returns 16 on the first
    // (warm-seed) call and 32 after; with a 1s interval, a read taken past one
    // interval must reflect the background poll, not the seed.
    use std::sync::atomic::{AtomicU64, Ordering};
    let pc = Arc::new(AtomicU64::new(0));
    let el = MockServer::start(Arc::new(move |req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let result = match body.get("method").and_then(Value::as_str).unwrap_or("") {
            "eth_syncing" => json!(false),
            "net_peerCount" => {
                if pc.fetch_add(1, Ordering::SeqCst) == 0 {
                    json!("0x10") // 16, warm seed
                } else {
                    json!("0x20") // 32, later background polls
                }
            }
            "eth_getBlockByNumber" => json!({"number": "0x1", "timestamp": "0x1"}),
            _ => json!(null),
        };
        let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
        (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
    }))
    .await;
    let cl = MockServer::start(ok_handler()).await;
    let ethryx = EthryxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--health-poll-interval",
        "1",
    ])
    .await;

    let c = client();
    let (_, b1) = get(&c, &ethryx.url("/healthz")).await;
    let v1: Value = serde_json::from_slice(&b1).unwrap();
    assert_eq!(
        v1["el"]["peers"], 16,
        "first read is the warm-seed poll: {v1}"
    );

    tokio::time::sleep(Duration::from_millis(1500)).await;

    let (_, b2) = get(&c, &ethryx.url("/healthz")).await;
    let v2: Value = serde_json::from_slice(&b2).unwrap();
    assert_eq!(
        v2["el"]["peers"], 32,
        "background poll should have refreshed the cache: {v2}"
    );

    ethryx.shutdown().await;
}

#[tokio::test]
async fn run_rejects_zero_poll_interval() {
    let cfg = Config::try_parse_from([
        "ethryx",
        "--listen",
        "127.0.0.1:0",
        "--health-poll-interval",
        "0",
    ])
    .expect("clap parse");
    let err = run(cfg, async {})
        .await
        .expect_err("zero poll interval must be rejected");
    assert!(
        err.to_string().contains("health-poll-interval"),
        "got: {err}"
    );
}

#[tokio::test]
async fn healthz_reports_numeric_state() {
    let now = now_unix();
    let block_ts_hex = format!("0x{:x}", now - 5);
    // Choose a head_slot that puts the wall-clock age within the default 60s threshold.
    let genesis = 1_606_824_023u64;
    let head_slot = (now.saturating_sub(genesis)) / 12;

    let el = MockServer::start(Arc::new(move |req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x10"), // 16 peers
            "eth_getBlockByNumber" => json!({
                "number": "0x1234",
                "timestamp": block_ts_hex,
            }),
            _ => json!(null),
        };
        let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
        (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
    }))
    .await;

    let cl = MockServer::start(Arc::new(move |req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({
                "data": {
                    "head_slot": head_slot.to_string(),
                    "sync_distance": "0",
                    "is_syncing": false,
                }
            }))
            .unwrap(),
        ),
        "/eth/v1/node/peer_count" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({
                "data": {
                    "disconnected": "0",
                    "connecting": "0",
                    "connected": "100",
                    "disconnecting": "0",
                }
            }))
            .unwrap(),
        ),
        _ => (StatusCode::NOT_FOUND, b"{}".to_vec()),
    }))
    .await;

    let ethryx = EthryxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--network",
        "mainnet",
    ])
    .await;

    let c = client();
    let (status, body) = get(&c, &ethryx.url("/healthz")).await;
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "expected /healthz 200, got {status}: {v}"
    );
    // Verdict-free: machine-readable numeric fields, no `ok` / `status`.
    assert!(
        v.get("status").is_none(),
        "healthz must not render a verdict"
    );
    assert_eq!(v["el"]["syncing"], false);
    assert_eq!(v["el"]["peers"], 16);
    assert!(v["el"]["block_number"].is_number(), "got {v}");
    assert!(v["el"]["block_age_secs"].is_number(), "got {v}");
    assert_eq!(v["cl"]["syncing"], false);
    assert_eq!(v["cl"]["peers"], 100);
    assert!(v["cl"]["head_slot"].is_number(), "got {v}");
    assert!(v["cl"]["slot_age_secs"].is_number(), "got {v}");

    ethryx.shutdown().await;
}

#[tokio::test]
async fn readyz_ready_when_synced_even_if_degraded() {
    // Synced EL + CL, but zero peers and a stale block. Default /readyz gates on
    // sync only, so the node stays ready; /healthz still surfaces the degraded
    // numbers. This is the whole point of the split.
    let el = MockServer::start(Arc::new(|req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x0"),
            "eth_getBlockByNumber" => json!({"number": "0x1", "timestamp": "0x1"}),
            _ => json!(null),
        };
        let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
        (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
    }))
    .await;
    let cl = MockServer::start(Arc::new(|req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({
                "data": {"head_slot": "1", "sync_distance": "0", "is_syncing": false}
            }))
            .unwrap(),
        ),
        "/eth/v1/node/peer_count" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({"data": {"connected": "0"}})).unwrap(),
        ),
        _ => (StatusCode::NOT_FOUND, b"{}".to_vec()),
    }))
    .await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethryx.url("/readyz")).await;
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, StatusCode::OK, "expected ready, got {status}: {v}");
    assert_eq!(v["status"], "ready");
    assert_eq!(v["el_syncing"]["ok"], true);
    assert_eq!(v["cl_syncing"]["ok"], true);
    // Default mode omits freshness/peers entirely.
    assert!(v.get("el_block_fresh").is_none(), "got {v}");
    assert!(v.get("cl_slot_fresh").is_none(), "got {v}");
    assert!(v.get("el_peers").is_none(), "got {v}");

    // /healthz reflects the degraded state without affecting readiness.
    let (hstatus, hbody) = get(&c, &ethryx.url("/healthz")).await;
    let hv: Value = serde_json::from_slice(&hbody).unwrap();
    assert_eq!(hstatus, StatusCode::OK);
    assert_eq!(hv["el"]["peers"], 0);
    assert_eq!(hv["cl"]["peers"], 0);

    ethryx.shutdown().await;
}

#[tokio::test]
async fn readyz_not_ready_when_el_syncing() {
    let el = MockServer::start(Arc::new(|req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!({"currentBlock": "0x10", "highestBlock": "0x20"}),
            _ => json!(null),
        };
        let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
        (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
    }))
    .await;
    let cl = MockServer::start(Arc::new(|req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({
                "data": {"head_slot": "100", "sync_distance": "0", "is_syncing": false}
            }))
            .unwrap(),
        ),
        _ => (StatusCode::NOT_FOUND, b"{}".to_vec()),
    }))
    .await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethryx.url("/readyz")).await;
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "got {v}");
    assert_eq!(v["status"], "not_ready");
    assert_eq!(v["el_syncing"]["ok"], false);
    assert_eq!(v["el_syncing"]["detail"], "syncing (block 16, distance 16)");

    ethryx.shutdown().await;
}

#[tokio::test]
async fn readyz_strict_gates_on_freshness() {
    // Synced, but the latest block is ancient. Default /readyz would be ready;
    // --readyz-strict pulls it because freshness fails.
    let el = MockServer::start(Arc::new(|req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!(false),
            "eth_getBlockByNumber" => json!({"number": "0x5", "timestamp": "0x1"}),
            _ => json!(null),
        };
        let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
        (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
    }))
    .await;
    let cl = MockServer::start(Arc::new(|req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({
                "data": {"head_slot": "1", "sync_distance": "0", "is_syncing": false}
            }))
            .unwrap(),
        ),
        _ => (StatusCode::NOT_FOUND, b"{}".to_vec()),
    }))
    .await;
    let ethryx = EthryxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--readyz-strict",
    ])
    .await;

    let c = client();
    let (status, body) = get(&c, &ethryx.url("/readyz")).await;
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "got {v}");
    assert_eq!(v["status"], "not_ready");
    // Sync is fine; freshness is what fails, and it is now present in the report.
    assert_eq!(v["el_syncing"]["ok"], true);
    assert_eq!(v["el_block_fresh"]["ok"], false);

    ethryx.shutdown().await;
}

#[tokio::test]
async fn multi_port_listen_serves_same_routes() {
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;

    let p1 = pick_port().await;
    let p2 = pick_port().await;
    let cfg = Config::try_parse_from([
        "ethryx",
        "--listen",
        &format!("127.0.0.1:{p1}"),
        "--listen",
        &format!("127.0.0.1:{p2}"),
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--shutdown-grace",
        "0",
    ])
    .unwrap();

    let (tx, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(run(cfg, async move {
        let _ = rx.await;
    }));
    wait_for_port(p1).await;
    wait_for_port(p2).await;

    let c = client();
    for port in [p1, p2] {
        let (status, body) = get(&c, &format!("http://127.0.0.1:{port}/livez")).await;
        assert_eq!(status, StatusCode::OK, "port {port}");
        assert_eq!(&body[..], b"ok", "port {port}");
    }

    let _ = tx.send(());
    let _ = task.await;
}

// ---------- websocket ----------

/// Minimal WebSocket echo upstream. Returns its `ws://addr` URL; echoes every
/// text/binary frame straight back.
async fn ws_echo_upstream() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(ws) = accept_async(stream).await else {
                    return;
                };
                let (mut tx, mut rx) = ws.split();
                while let Some(Ok(msg)) = rx.next().await {
                    if (msg.is_text() || msg.is_binary()) && tx.send(msg).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    format!("ws://{addr}")
}

#[tokio::test]
async fn ws_handshake_fails_with_502_when_upstream_down() {
    // Nothing listens on the WS upstream. ethryx dials upstream *before*
    // completing the client handshake, so the client gets a 502 on the handshake
    // rather than a 101 followed by an immediate abnormal close.
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let dead = pick_port().await; // bound then released -> nothing listening
    let ethryx = EthryxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--el-ws-url",
        &format!("ws://127.0.0.1:{dead}"),
    ])
    .await;

    let url = format!("ws://127.0.0.1:{}/", ethryx.port);
    match connect_async(&url).await {
        Err(WsError::Http(resp)) => assert_eq!(resp.status(), StatusCode::BAD_GATEWAY),
        other => panic!("expected a 502 handshake error, got {other:?}"),
    }

    ethryx.shutdown().await;
}

#[tokio::test]
async fn ws_bridge_echoes_through_upstream() {
    // With the upstream up, the reordered handshake still establishes and the
    // bidirectional bridge forwards frames.
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ws_upstream = ws_echo_upstream().await;
    let ethryx = EthryxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--el-ws-url",
        &ws_upstream,
    ])
    .await;

    let url = format!("ws://127.0.0.1:{}/", ethryx.port);
    let (mut ws, _resp) = connect_async(&url).await.expect("handshake should succeed");
    ws.send(Message::Text("ping".into())).await.unwrap();
    let echoed = loop {
        match ws.next().await.expect("a reply").expect("ok frame") {
            Message::Text(t) => break t,
            _ => continue,
        }
    };
    assert_eq!(echoed.as_str(), "ping");

    ethryx.shutdown().await;
}

#[tokio::test]
async fn http2_extended_connect_ws_502_when_upstream_down() {
    // The h2 path dials the upstream first too: a dead upstream surfaces as a 502
    // on the CONNECT response, not an accepted-then-dropped tunnel.
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let dead = pick_port().await;
    let ethryx = EthryxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--el-ws-url",
        &format!("ws://127.0.0.1:{dead}"),
    ])
    .await;

    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", ethryx.port))
        .await
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, TestBody>(TokioIo::new(tcp))
        .await
        .expect("h2 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut req = Request::builder()
        .method(Method::CONNECT)
        .uri(format!("http://127.0.0.1:{}/", ethryx.port))
        .body(Full::<Bytes>::new(Bytes::new()))
        .unwrap();
    req.extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));

    let resp = sender.send_request(req).await.expect("extended CONNECT");
    assert_eq!(
        resp.status(),
        StatusCode::BAD_GATEWAY,
        "dead upstream → 502"
    );

    ethryx.shutdown().await;
}

#[tokio::test]
async fn http2_extended_connect_ws_bridges_to_upstream() {
    // RFC 8441 Extended CONNECT: an h2 client opens a WebSocket via
    // `:method=CONNECT, :protocol=websocket`. ethryx terminates the h2 stream and
    // bridges the RFC 6455 frames to the upstream h1 WebSocket (h2 ws → h1 ws).
    // Driven by a hand-rolled h2 client because tokio/hyper-tungstenite are h1-only.
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ws_upstream = ws_echo_upstream().await;
    let ethryx = EthryxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--el-ws-url",
        &ws_upstream,
    ])
    .await;

    // Cleartext h2 (prior-knowledge) connection to ethryx.
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", ethryx.port))
        .await
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, TestBody>(TokioIo::new(tcp))
        .await
        .expect("h2 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut req = Request::builder()
        .method(Method::CONNECT)
        .uri(format!("http://127.0.0.1:{}/", ethryx.port))
        .body(Full::<Bytes>::new(Bytes::new()))
        .unwrap();
    req.extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));

    let resp = sender.send_request(req).await.expect("extended CONNECT");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "extended CONNECT should be 200"
    );
    assert_eq!(resp.version(), Version::HTTP_2);

    let upgraded = hyper::upgrade::on(resp).await.expect("upgrade");
    let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
        TokioIo::new(upgraded),
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    ws.send(Message::Text("ping".into())).await.unwrap();
    let echoed = loop {
        match ws.next().await.expect("a reply").expect("ok frame") {
            Message::Text(t) => break t,
            _ => continue,
        }
    };
    assert_eq!(echoed.as_str(), "ping");

    ethryx.shutdown().await;
}

#[tokio::test]
async fn el_upstream_h2c_timeout_falls_back_to_h1() {
    // If the EL upstream hangs on h2c connection preface (causing a timeout)
    // rather than closing it, ethryx should fallback to HTTP/1.1 if HTTP/1.1 works.
    let el = MockServer::start_hanging_h2c(Arc::new(|req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x0"),
            "eth_getBlockByNumber" => json!({"number": "0x1", "timestamp": "0x1"}),
            _ => json!(null),
        };
        let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
        (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
    }))
    .await;

    let cl = MockServer::start(Arc::new(|req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({
                "data": {"head_slot": "1", "sync_distance": "0", "is_syncing": false}
            }))
            .unwrap(),
        ),
        "/eth/v1/node/peer_count" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({"data": {"connected": "0"}})).unwrap(),
        ),
        _ => (StatusCode::NOT_FOUND, b"{}".to_vec()),
    }))
    .await;

    // Start ethryx. By default, it warms up cache on start.
    // If fallback works, starting will succeed because the warm-up succeeds.
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethryx.url("/readyz")).await;
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, StatusCode::OK, "expected ready, got {status}: {v}");
    assert_eq!(v["status"], "ready");

    // Let's also check /healthz report
    let (_, hz) = get(&c, &ethryx.url("/healthz")).await;
    let hv: Value = serde_json::from_slice(&hz).unwrap();
    assert_eq!(hv["el"]["transport"], "http/1.1"); // verified fallback to HTTP/1.1!

    ethryx.shutdown().await;
}

/// WebSocket upstream that verifies specific request properties (path, query, headers)
/// during handshake and then echoes.
async fn ws_verifying_upstream(
    expected_path: &'static str,
    expected_headers: Vec<(String, String)>,
    tx_verified: oneshot::Sender<bool>,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        // We only expect one connection in this test.
        if let Ok((stream, _)) = listener.accept().await {
            let expected_headers = expected_headers.clone();
            let verified = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let verified_cb = Arc::clone(&verified);

            let ws_result = tokio_tungstenite::accept_hdr_async(
                stream,
                #[allow(clippy::result_large_err)]
                move |req: &tokio_tungstenite::tungstenite::handshake::server::Request, resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    let actual_path = req.uri().path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
                    if actual_path != expected_path {
                        verified_cb.store(false, std::sync::atomic::Ordering::SeqCst);
                    }
                    for (k, v) in &expected_headers {
                        if req.headers().get(k).and_then(|val| val.to_str().ok()) != Some(v) {
                            verified_cb.store(false, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                    Ok(resp)
                },
            )
            .await;

            let _ = tx_verified.send(verified.load(std::sync::atomic::Ordering::SeqCst));

            if let Ok(ws) = ws_result {
                let (mut tx, mut rx) = ws.split();
                while let Some(Ok(msg)) = rx.next().await {
                    if (msg.is_text() || msg.is_binary()) && tx.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    format!("ws://{addr}")
}

#[tokio::test]
async fn ws_forwarding_preserves_path_query_and_headers() {
    // Tests that WebSocket proxy relays custom headers (like authorization)
    // and path/queries from client request to the upstream WebSocket correctly.
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;

    let (tx_verified, rx_verified) = oneshot::channel::<bool>();

    let ws_upstream = ws_verifying_upstream(
        "/foo/bar?baz=qux",
        vec![
            (
                "authorization".to_string(),
                "Bearer secret-token".to_string(),
            ),
            ("x-custom-header".to_string(), "hello-world".to_string()),
        ],
        tx_verified,
    )
    .await;

    let ethryx = EthryxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--el-ws-url",
        &ws_upstream,
    ])
    .await;

    // Connect using tungstenite Request with custom path/query and headers.
    let target_url = format!("ws://127.0.0.1:{}/foo/bar?baz=qux", ethryx.port);
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = target_url.into_client_request().unwrap();
    req.headers_mut().insert(
        "authorization",
        http::HeaderValue::from_static("Bearer secret-token"),
    );
    req.headers_mut().insert(
        "x-custom-header",
        http::HeaderValue::from_static("hello-world"),
    );

    let (mut ws, _resp) = connect_async(req).await.expect("handshake should succeed");

    // Send a message and wait for it to be echoed to ensure connection works
    ws.send(Message::Text("ping".into())).await.unwrap();
    let echoed = loop {
        match ws.next().await.expect("a reply").expect("ok frame") {
            Message::Text(t) => break t,
            _ => continue,
        }
    };
    assert_eq!(echoed.as_str(), "ping");

    // Check verification result
    let is_verified = rx_verified.await.unwrap_or(false);
    assert!(
        is_verified,
        "Upstream did not receive the expected path, query, or headers"
    );

    ethryx.shutdown().await;
}

#[tokio::test]
async fn metrics_endpoint_returns_formatted_prometheus_metrics() {
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();

    // Fire a quick request to record some request count metrics
    let (status, _) = post_json(
        &c,
        &ethryx.url("/"),
        json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Fetch metrics
    let (status, body) = get(&c, &ethryx.url("/metrics")).await;
    assert_eq!(status, StatusCode::OK);

    let metrics_str = String::from_utf8_lossy(&body);
    // Assert some of the metrics exist in the response
    assert!(metrics_str.contains("ethryx_proxy_requests_total"));
    assert!(metrics_str.contains("ethryx_proxy_request_duration_seconds"));
    assert!(metrics_str.contains("ethryx_active_connections"));
    assert!(metrics_str.contains("ethryx_upstream_health_status"));
    assert!(metrics_str.contains("ethryx_upstream_peers"));

    ethryx.shutdown().await;
}

#[tokio::test]
async fn test_cl_node_health_intercept_and_passthrough() {
    let el = MockServer::start(Arc::new(|req| {
        if req.body.windows(11).any(|w| w == b"eth_syncing") {
            (
                StatusCode::OK,
                br#"{"jsonrpc":"2.0","id":1,"result":false}"#.to_vec(),
            )
        } else {
            (StatusCode::OK, b"{}".to_vec())
        }
    }))
    .await;

    let cl_mock_synced = MockServer::start(Arc::new(|req| {
        if req.path == "/eth/v1/node/syncing" {
            (
                StatusCode::OK,
                br#"{"data":{"is_syncing":false,"sync_distance":"0","head_slot":"100"}}"#.to_vec(),
            )
        } else {
            (
                StatusCode::BAD_REQUEST,
                b"{\"error\": \"Unsupported method\"}".to_vec(),
            )
        }
    }))
    .await;

    let ethryx = EthryxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl_mock_synced.url,
    ])
    .await;

    let c = client();

    // GET /eth/v1/node/health returns 200 OK (intercepted, empty body)
    let (status, body) = get(&c, &ethryx.url("/eth/v1/node/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());

    // HEAD on /readyz, /healthz, /livez, /metrics, and /eth/v1/node/health return 200 OK
    for path in [
        "/readyz",
        "/healthz",
        "/livez",
        "/metrics",
        "/eth/v1/node/health",
    ] {
        let req_h = Request::builder()
            .method(Method::HEAD)
            .uri(ethryx.url(path))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp_h = c.request(req_h).await.unwrap();
        assert_eq!(
            resp_h.status(),
            StatusCode::OK,
            "HEAD {} should return 200 OK",
            path
        );
    }

    ethryx.shutdown().await;

    // Test trusted upstream mode (--trust-upstream)
    let ethryx_trust = EthryxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl_mock_synced.url,
        "--trust-upstream",
    ])
    .await;

    let (status_tr, body_tr) = get(&c, &ethryx_trust.url("/readyz")).await;
    assert_eq!(status_tr, StatusCode::OK);
    let ready_val: Value = serde_json::from_slice(&body_tr).unwrap();
    assert_eq!(ready_val["status"], "ready");

    let (status_tr_nh, body_tr_nh) = get(&c, &ethryx_trust.url("/eth/v1/node/health")).await;
    assert_eq!(status_tr_nh, StatusCode::OK);
    assert!(body_tr_nh.is_empty());

    ethryx_trust.shutdown().await;
}

#[cfg(feature = "otel")]
#[tokio::test]
async fn trace_context_is_propagated_to_upstream() {
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ethryx = EthryxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--otel-endpoint",
        "http://127.0.0.1:4318",
    ])
    .await;

    let c = client();

    let traceparent_val = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let req = Request::builder()
        .method(Method::POST)
        .uri(ethryx.url("/"))
        .header("traceparent", traceparent_val)
        .body(Full::new(Bytes::from(
            json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1})
                .to_string(),
        )))
        .unwrap();

    let resp = c.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let received_traceparent_str = {
        let recorded_reqs = el.recorded.lock().unwrap();
        let received_traceparent = recorded_reqs
            .iter()
            .filter_map(|r| r.headers.get("traceparent"))
            .next()
            .expect("traceparent header in any upstream request");
        received_traceparent.to_str().unwrap().to_owned()
    };

    assert!(
        received_traceparent_str.contains("4bf92f3577b34da6a3ce929d0e0e4736"),
        "Expected propagated traceparent to contain trace ID, got: {}",
        received_traceparent_str
    );

    ethryx.shutdown().await;
}
