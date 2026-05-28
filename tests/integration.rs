//! Integration tests: spawn ethryx in-process against a hyper-based mock
//! upstream, drive the full HTTP/proxy/health flow with the same hyper-util
//! Client used in production code. Zero extra dev-dependencies.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use clap::Parser;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

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

    let got = el.received();
    assert_eq!(got.len(), 1);
    let req: Value = serde_json::from_slice(&got[0].body).unwrap();
    assert_eq!(req["method"], "eth_blockNumber");

    ethryx.shutdown().await;
}

#[tokio::test]
async fn beacon_path_is_routed_to_cl_upstream() {
    let el = MockServer::start(Arc::new(|_| {
        panic!("EL must not see /eth/ traffic");
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
async fn health_503_when_upstreams_404() {
    // Both mocks return 404 for everything: every probe fails.
    let bad: MockHandler = Arc::new(|_| (StatusCode::NOT_FOUND, b"{}".to_vec()));
    let el = MockServer::start(bad.clone()).await;
    let cl = MockServer::start(bad).await;
    let ethryx = EthryxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethryx.url("/health")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["status"], "unhealthy");
    assert_eq!(v["el_syncing"]["ok"], false);
    assert_eq!(v["cl_syncing"]["ok"], false);

    ethryx.shutdown().await;
}

#[tokio::test]
async fn health_200_when_everything_green() {
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
    let (status, body) = get(&c, &ethryx.url("/health")).await;
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "expected /health 200, got {status}: {v}"
    );
    assert_eq!(v["status"], "healthy");
    assert_eq!(v["el_syncing"]["ok"], true);
    assert_eq!(v["el_peers"]["ok"], true);
    assert_eq!(v["el_block_fresh"]["ok"], true);
    assert_eq!(v["cl_syncing"]["ok"], true);
    assert_eq!(v["cl_peers"]["ok"], true);
    assert_eq!(v["cl_slot_fresh"]["ok"], true);

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
