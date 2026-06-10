use prometheus::{
    HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};
use std::sync::OnceLock;

pub struct Metrics {
    pub registry: Registry,
    pub proxy_requests_total: IntCounterVec,
    pub proxy_request_duration_seconds: HistogramVec,
    pub active_connections: IntGaugeVec,
    pub upstream_peers: IntGaugeVec,
    pub upstream_sync_distance: IntGaugeVec,
    pub upstream_block_number: IntGauge,
    pub upstream_slot_number: IntGauge,
    pub upstream_health_status: IntGaugeVec,
}

pub fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let registry = Registry::new();

        let proxy_requests_total = IntCounterVec::new(
            Opts::new(
                "ethryx_proxy_requests_total",
                "Total number of proxied HTTP/WS requests",
            ),
            &["upstream", "method", "status"],
        )
        .unwrap();
        registry
            .register(Box::new(proxy_requests_total.clone()))
            .unwrap();

        let proxy_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "ethryx_proxy_request_duration_seconds",
                "Latency of upstream proxied requests in seconds",
            ),
            &["upstream"],
        )
        .unwrap();
        registry
            .register(Box::new(proxy_request_duration_seconds.clone()))
            .unwrap();

        let active_connections = IntGaugeVec::new(
            Opts::new(
                "ethryx_active_connections",
                "Number of currently active client connections",
            ),
            &["protocol"],
        )
        .unwrap();
        registry
            .register(Box::new(active_connections.clone()))
            .unwrap();

        let upstream_peers = IntGaugeVec::new(
            Opts::new(
                "ethryx_upstream_peers",
                "Number of peers reported by upstream nodes",
            ),
            &["layer"],
        )
        .unwrap();
        registry.register(Box::new(upstream_peers.clone())).unwrap();

        let upstream_sync_distance = IntGaugeVec::new(
            Opts::new(
                "ethryx_upstream_sync_distance",
                "Remaining sync distance in blocks or slots",
            ),
            &["layer"],
        )
        .unwrap();
        registry
            .register(Box::new(upstream_sync_distance.clone()))
            .unwrap();

        let upstream_block_number = IntGauge::new(
            "ethryx_upstream_block_number",
            "Latest execution layer block number",
        )
        .unwrap();
        registry
            .register(Box::new(upstream_block_number.clone()))
            .unwrap();

        let upstream_slot_number = IntGauge::new(
            "ethryx_upstream_slot_number",
            "Latest consensus layer head slot number",
        )
        .unwrap();
        registry
            .register(Box::new(upstream_slot_number.clone()))
            .unwrap();

        let upstream_health_status = IntGaugeVec::new(
            Opts::new(
                "ethryx_upstream_health_status",
                "Upstream health status (1 = healthy/synced, 0 = degraded/down)",
            ),
            &["layer"],
        )
        .unwrap();
        registry
            .register(Box::new(upstream_health_status.clone()))
            .unwrap();

        Metrics {
            registry,
            proxy_requests_total,
            proxy_request_duration_seconds,
            active_connections,
            upstream_peers,
            upstream_sync_distance,
            upstream_block_number,
            upstream_slot_number,
            upstream_health_status,
        }
    })
}

pub struct ActiveConnectionGuard(&'static str);

impl ActiveConnectionGuard {
    pub fn new(protocol: &'static str) -> Self {
        metrics()
            .active_connections
            .with_label_values(&[protocol])
            .inc();
        Self(protocol)
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        metrics()
            .active_connections
            .with_label_values(&[self.0])
            .dec();
    }
}
