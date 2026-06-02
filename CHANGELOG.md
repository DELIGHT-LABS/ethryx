# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->
## [Unreleased] - ReleaseDate

### Added

- The boot log line now includes the ethryx `version` (e.g. `v0.1.2`) and a `git`
  field from build-time `git describe`, so a tagged release (`git` = `v0.1.2`) is
  distinguishable from an ad-hoc build off a later commit
  (`v0.1.2-5-g20537f9`, `-dirty` when the tree was modified). No new dependency —
  a small `build.rs` captures it; `git` is `unknown` without a git checkout.
- `--access-log` (`ETHRYX_ACCESS_LOG`): an opt-in per-connection access log on a
  dedicated `access_log` target — peer, the negotiated `HTTP/1.1` vs `HTTP/2`, and
  the first request's method and path. The target sits outside the `ethryx`
  hierarchy, so raising the app log doesn't pull it in. Kept separate from the
  application log so `info` stays quiet by default, and health-probe paths
  (`/livez`, `/readyz`, `/healthz`) are excluded so frequent k8s / LB checks don't
  bury real traffic.

### Changed

- `info` logging stays reserved for lifecycle and state-change events; per-request
  proxy / WS outcomes and connection errors are `debug`, and connection accept /
  close are `trace`. A healthy sidecar is near-silent at `info` between changes.

## [0.1.2] - 2026-05-31

### Added

- HTTP/2 support: the listener serves HTTP/1.1 and HTTP/2 on the same port,
  auto-detected per connection — including cleartext **h2c** via prior-knowledge,
  which covers the common "TLS-terminating LB / mesh forwards h2c to the backend"
  shape (Envoy, Istio, HAProxy `proto h2`). h1 and h1 WebSocket are unchanged. The
  upstream client now auto-negotiates h2 for `https://` upstreams via ALPN
  (cleartext upstreams stay h1). No new flags; plaintext stays the default (TLS
  termination remains the LB/mesh's job).
- HTTP/2 WebSocket via RFC 8441 Extended CONNECT: an h2 client (or h2 mesh) can
  open a WebSocket with `:protocol=websocket`, which ethryx bridges to the upstream
  h1 WebSocket. The HTTP/1.1 `Upgrade` WebSocket path is unchanged.
- EL JSON-RPC upstream h2c auto-detection: the health poller probes the EL hop
  (preferring cleartext **h2c**) and forwards over HTTP/2 when the upstream serves
  it (geth ≥v1.17, erigon, reth), falling back to HTTP/1.1 otherwise. No flag. If a
  running upstream stops serving h2c, the poller falls back within one poll; a
  cleartext upstream that newly adds h2c is picked up on restart. The CL Beacon hop
  stays HTTP/1.1, and `https://` EL upstreams continue to negotiate h2 via ALPN.
- `/healthz` now reports the upstream HTTP `transport` per layer (`h2c` / `h2` /
  `http/1.1`), so the auto-detected EL transport is observable.
- `/readyz` readiness probe and `/healthz` state snapshot, joining `/livez` as a
  three-tier probe model (liveness / readiness / monitoring), following the
  Kubernetes `livez` / `readyz` / `healthz` convention.
  - `/readyz` is the load-balancer traffic gate. It gates on EL + CL **sync
    status** only, so a network-wide stall (or a fleet-wide peer dip) does not
    drain every backend out of rotation at once. `--readyz-strict`
    (`ETHRYX_READYZ_STRICT`) additionally gates on EL block / CL slot freshness.
  - `/healthz` is verdict-free: it always returns `200` and reports each live
    EL/CL value as a machine-readable numeric field (peer counts, block / slot
    age, sync status) under `el` / `cl`, with any upstream failure recorded in a
    per-layer `errors` array — leaving thresholding and alerting to the consumer.
  - Both endpoints serve a snapshot refreshed by a background poller that waits
    `--health-poll-interval` / `ETHRYX_HEALTH_POLL_INTERVAL` (default 5s) between
    polls, so upstream load is constant regardless of probe rate and a probe
    never blocks on upstream. Block / slot ages are recomputed live per request.
    Readiness transitions are logged once by the poller (bounded to the poll
    rate, and visible even if nothing probes `/readyz`), not per probe.
- `--log-level` / `ETHRYX_LOG_LEVEL` (default `info`) to set the log level when
  `RUST_LOG` is unset; `RUST_LOG` still overrides it and allows per-target
  directives.

### Changed

- Logging follows a sidecar discipline: routine upstream / client failures
  (proxy 502s, WebSocket drops) and each health poll are now `debug` (were
  `error`); readiness changes are logged once at `warn` (not-ready) / `info`
  (recovered); listener `accept()` failures stay `warn`; request routing is
  `trace`.

### Removed

- `--el-min-peers` / `--cl-min-peers` (`ETHRYX_EL_MIN_PEERS` /
  `ETHRYX_CL_MIN_PEERS`): peer count no longer gates any endpoint. `/readyz`
  gates on sync status; `/healthz` reports the raw peer count for the monitoring
  layer to threshold. The `--*-max-*-age-secs` flags are retained but now gate
  `/readyz` under `--readyz-strict` rather than a health verdict.

### Fixed

- The accept loop now backs off briefly after a failed `accept()` instead of
  retrying immediately, so a persistent error (e.g. file-descriptor exhaustion)
  no longer busy-spins a core.

## [0.1.1] - 2026-05-28

### Changed

- `/health` top-level `status` value is now `"healthy"` / `"unhealthy"` (was
  `"ok"` / `"unhealthy"`, asymmetric).
- `/health` EL syncing detail parses `currentBlock` / `highestBlock` from
  `eth_syncing` and renders as `"syncing (block X, distance Y)"`, mirroring
  the CL `"syncing (slot S, distance D)"` format.
- `/health` CL check errors now prefix with the Beacon API endpoint path
  (`node/syncing: ...`, `node/peer_count: ...`) to disambiguate against the
  literal `syncing` status word.
- Env vars for the two `*-age-secs` flags gained the missing `_SECS` suffix
  (`ETHRYX_EL_MAX_BLOCK_AGE` → `ETHRYX_EL_MAX_BLOCK_AGE_SECS`,
  `ETHRYX_CL_MAX_SLOT_AGE` → `ETHRYX_CL_MAX_SLOT_AGE_SECS`) so the env name
  matches its CLI flag.

## [0.1.0] - 2026-05-28

### Added

- Initial sidecar implementation: hyper 1 reverse proxy for Ethereum EL JSON-RPC
  (HTTP / WebSocket) and CL Beacon REST API, aggregated `/health` with EL+CL
  sync / peer / freshness checks, multi-port listen, `--network` presets for CL
  genesis (mainnet / hoodi / sepolia / holesky / custom), CLI + env config via
  clap.
