use std::sync::Arc;

use http::Uri;
use tokio::sync::watch;

use crate::config::Config;
use crate::health::Probe;
use crate::proxy::ProxyClient;

pub struct AppState {
    pub cfg: Config,
    pub client: ProxyClient,
    pub el_http_uri: Uri,
    pub cl_syncing_uri: Uri,
    pub cl_peer_count_uri: Uri,
    pub cl_genesis_time: u64,
    pub cl_seconds_per_slot: u64,
    /// Latest background health poll, read by `/healthz` and `/readyz`.
    pub probe: watch::Receiver<Arc<Probe>>,
}
