use std::time::Duration;

use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Network {
    Mainnet,
    Hoodi,
    Sepolia,
    Holesky,
    /// Private / custom beacon chain. Requires explicit --cl-genesis-time and --cl-seconds-per-slot.
    Custom,
}

impl Network {
    pub fn preset_genesis_time(self) -> Option<u64> {
        match self {
            Network::Mainnet => Some(1_606_824_023),
            Network::Hoodi => Some(1_742_213_400),
            Network::Sepolia => Some(1_655_733_600),
            Network::Holesky => Some(1_695_902_400),
            Network::Custom => None,
        }
    }

    pub fn preset_seconds_per_slot(self) -> Option<u64> {
        match self {
            Network::Custom => None,
            _ => Some(12),
        }
    }
}

#[derive(Debug, Clone, Parser)]
#[command(
    name = "ethryx",
    version,
    about = "Ethryx — Ethereum EL/CL sidecar (HTTP + WS multiplexing, health)"
)]
pub struct Config {
    /// Listen address (host:port). Repeat the flag or comma-separate for multi-port.
    /// All ports serve the same routes; intended for direct IP:port access plus LB.
    #[arg(
        long,
        env = "ETHRYX_LISTEN",
        value_delimiter = ',',
        default_value = "0.0.0.0:8547"
    )]
    pub listen: Vec<String>,

    /// Execution-layer JSON-RPC over HTTP upstream URL.
    #[arg(
        long,
        env = "ETHRYX_EL_HTTP_URL",
        default_value = "http://127.0.0.1:8545"
    )]
    pub el_http_url: String,

    /// Execution-layer JSON-RPC over WebSocket upstream URL.
    #[arg(long, env = "ETHRYX_EL_WS_URL", default_value = "ws://127.0.0.1:8546")]
    pub el_ws_url: String,

    /// Consensus-layer Beacon API upstream URL (REST, /eth/v1/...).
    #[arg(
        long,
        env = "ETHRYX_CL_BEACON_URL",
        default_value = "http://127.0.0.1:5052"
    )]
    pub cl_beacon_url: String,

    /// Beacon network. Selects defaults for cl-genesis-time / cl-seconds-per-slot.
    /// Use `custom` for private chains and pass both values explicitly.
    #[arg(long, env = "ETHRYX_NETWORK", value_enum, default_value_t = Network::Mainnet)]
    pub network: Network,

    /// Minimum acceptable EL peer count (net_peerCount) for /health.
    #[arg(long, env = "ETHRYX_EL_MIN_PEERS", default_value_t = 3)]
    pub el_min_peers: u64,

    /// Maximum EL block age in seconds before /health reports stale.
    #[arg(long, env = "ETHRYX_EL_MAX_BLOCK_AGE", default_value_t = 60)]
    pub el_max_block_age_secs: u64,

    /// Minimum acceptable CL peer count (Beacon /eth/v1/node/peer_count `connected`).
    #[arg(long, env = "ETHRYX_CL_MIN_PEERS", default_value_t = 8)]
    pub cl_min_peers: u64,

    /// Maximum CL head_slot age in seconds (wall-clock).
    #[arg(long, env = "ETHRYX_CL_MAX_SLOT_AGE", default_value_t = 60)]
    pub cl_max_slot_age_secs: u64,

    /// Beacon-chain genesis Unix timestamp. Overrides --network preset. Set 0 to disable slot-age check.
    #[arg(long, env = "ETHRYX_CL_GENESIS_TIME")]
    pub cl_genesis_time: Option<u64>,

    /// Seconds per slot. Overrides --network preset.
    #[arg(long, env = "ETHRYX_CL_SECONDS_PER_SLOT")]
    pub cl_seconds_per_slot: Option<u64>,

    /// Upstream timeout for health-check RPCs (seconds).
    #[arg(long, env = "ETHRYX_HEALTH_TIMEOUT", default_value = "3", value_parser = parse_secs)]
    pub health_timeout: Duration,

    /// Upstream timeout for proxied requests (seconds).
    #[arg(long, env = "ETHRYX_PROXY_TIMEOUT", default_value = "60", value_parser = parse_secs)]
    pub proxy_timeout: Duration,

    /// Shutdown grace period (seconds) for draining in-flight connections.
    #[arg(long, env = "ETHRYX_SHUTDOWN_GRACE", default_value = "10", value_parser = parse_secs)]
    pub shutdown_grace: Duration,
}

impl Config {
    pub fn resolve_cl_genesis_time(&self) -> Result<u64, &'static str> {
        match self.cl_genesis_time {
            Some(v) => Ok(v),
            None => self.network.preset_genesis_time().ok_or(
                "--network custom requires --cl-genesis-time (use 0 to disable slot-age check)",
            ),
        }
    }

    pub fn resolve_cl_seconds_per_slot(&self) -> Result<u64, &'static str> {
        match self.cl_seconds_per_slot {
            Some(v) => Ok(v),
            None => self
                .network
                .preset_seconds_per_slot()
                .ok_or("--network custom requires --cl-seconds-per-slot"),
        }
    }
}

fn parse_secs(s: &str) -> Result<Duration, String> {
    s.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|e| format!("invalid seconds value: {e}"))
}
