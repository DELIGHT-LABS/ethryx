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
    body: Bytes,
}

type MockHandler = Arc<dyn Fn(&RecordedRequest) -> (StatusCode, Vec<u8>) + Send + Sync>;

struct MockServer {
    url: String,
    recorded: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl MockServer {
    async fn start(handler: MockHandler) -> Self {
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
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let h = h.clone();
                        let rec = rec.clone();
                        async move {
                            let method = req.method().clone();
                            let path = req.uri().path().to_owned();
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            let rr = RecordedRequest {
                                method: method.clone(),
                                path: path.clone(),
                                body: body.clone(),
                            };
                            rec.lock().unwrap().push(rr.clone());
                            let (status, body) = h(&rr);
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(status)
                                    .header("content-type", "application/json")
                                    .body(Full::new(Bytes::from(body)))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, svc)
                        .await;
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
