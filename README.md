# ethryx

[![CI](https://github.com/DELIGHT-LABS/ethryx/actions/workflows/ci.yml/badge.svg)](https://github.com/DELIGHT-LABS/ethryx/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/DELIGHT-LABS/ethryx?include_prereleases&sort=semver)](https://github.com/DELIGHT-LABS/ethryx/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust 1.95+](https://img.shields.io/badge/rust-1.95%2B-blue.svg)](rust-toolchain.toml)

Lightweight Rust sidecar that multiplexes HTTP / WebSocket traffic and surfaces
health for an Ethereum **Execution Layer** (EL) and **Consensus Layer** (CL) pair.

## Routing

| Request                              | Forwarded to        |
|--------------------------------------|---------------------|
| `GET /livez`                         | 200 OK (process liveness) |
| `GET /readyz`                        | EL + CL readiness gate (200 / 503) |
| `GET /healthz`                       | EL + CL state snapshot (always 200) |
| `/eth/...` (Beacon API)              | `--cl-beacon-url`   |
| `Upgrade: websocket`                 | `--el-ws-url`       |
| everything else (JSON-RPC `POST /`)  | `--el-http-url`     |

## Probes

Three endpoints, split by purpose (Kubernetes `z` convention):

| Endpoint       | Returns                       | Wire to                     | Gates on                                              |
|----------------|-------------------------------|-----------------------------|-------------------------------------------------------|
| `GET /livez`   | always `200`, body `ok`       | liveness probe (restart)    | nothing — only that the process is up; no upstream call |
| `GET /readyz`  | `200` ready / `503` not ready | readiness probe (LB gate)   | EL + CL **sync status** (plus freshness with `--readyz-strict`) |
| `GET /healthz` | always `200` + JSON snapshot  | monitoring / alerting / curl | nothing — reports state, never judges                 |

`/livez` and `/readyz` differ exactly when the node is up but not serving yet
(startup, mid-sync, or an upstream is down): `/livez` stays `200` (don't restart
me) while `/readyz` returns `503` (don't route to me). Once synced, both are
`200`. So **default `/readyz` is `/livez` plus an EL+CL sync check.**

### `/readyz` — the traffic gate

Gates **only on EL + CL sync status** by default. A caught-up node reports
`eth_syncing == false` even when the chain stalls network-wide, so a chain
incident (or a fleet-wide peer dip) won't drop every backend out of the load
balancer at once — which would turn a chain incident into a total RPC outage.
Sync status is the node-local signal that tells "this node can serve" apart from
"this node is behind its peers".

```jsonc
// 200 — synced. el_block_fresh / cl_slot_fresh appear only under --readyz-strict.
{ "status": "ready",
  "el_syncing": { "ok": true, "detail": "synced" },
  "cl_syncing": { "ok": true, "detail": "synced (slot 9412341, distance 0)" } }
```

`--readyz-strict` additionally gates on EL block age (`--el-max-block-age-secs`)
and CL slot age (`--cl-max-slot-age-secs`) — choose it when serving strictly
at-head data matters more than fleet availability during a stall.

### `/healthz` — state for monitoring

Always `200`. Reports each live EL/CL value as a machine-readable field for your
monitoring stack to threshold; it applies no thresholds and renders no verdict.
A signal whose upstream call failed is omitted and the error is recorded under
that layer's `errors` array.

| Field                                              | Source                                |
|----------------------------------------------------|---------------------------------------|
| `el.transport` / `cl.transport` (`h2c`/`h2`/`http/1.1`) | upstream HTTP version ethryx uses |
| `el.syncing` (`false` = synced)                    | EL `eth_syncing`                      |
| `el.sync_distance` (while syncing)                 | EL `eth_syncing` highest − current    |
| `el.peers`                                         | EL `net_peerCount`                    |
| `el.block_number` / `el.block_age_secs`            | EL `eth_getBlockByNumber("latest")`   |
| `cl.syncing` / `cl.sync_distance` / `cl.head_slot` | Beacon `/eth/v1/node/syncing`         |
| `cl.slot_age_secs`                                 | Beacon `head_slot` vs. wall-clock     |
| `cl.peers`                                         | Beacon `/eth/v1/node/peer_count`      |
| `el.errors` / `cl.errors`                          | any upstream call that failed         |

```json
{ "el": { "transport": "h2c", "syncing": false, "peers": 23, "block_number": 21000000, "block_age_secs": 5 },
  "cl": { "transport": "http/1.1", "syncing": false, "sync_distance": 0, "peers": 78, "head_slot": 9412341, "slot_age_secs": 3 } }
```

CL slot age is derived from `head_slot * --cl-seconds-per-slot +
--cl-genesis-time`. Use `--network <name>` for a preset (defaults to mainnet)
instead of typing both; `--cl-genesis-time 0` omits `cl.slot_age_secs`.

### Polling

`/readyz` and `/healthz` don't query upstream per request. A background task
polls all signals, then waits `--health-poll-interval` (default 5s; each call
bounded by `--health-timeout`) before the next poll, and the endpoints return
the latest snapshot instantly — so upstream load is constant regardless of probe
rate, a slow upstream never blocks a probe, and the poller pauses a full interval
between polls rather than hammering a struggling node. Block / slot ages are
recomputed live per request, so they stay accurate between polls. The cache is
warmed by one poll before the listener accepts, so the process serves a real
snapshot from its first probe.

Readiness is logged by the poller on transition (becoming not-ready / recovering)
— once per change, bounded to the poll rate and emitted even if nothing probes
`/readyz`, rather than warning on every probe.

| `--network` | genesis_time   | seconds_per_slot |
|-------------|----------------|------------------|
| `mainnet`   | `1606824023`   | `12`             |
| `hoodi`     | `1742213400`   | `12`             |
| `sepolia`   | `1655733600`   | `12`             |
| `holesky`   | `1695902400`   | `12`             |
| `custom`    | *(required)*   | *(required)*     |

For a **private / custom beacon chain**, pass `--network custom` together with
explicit `--cl-genesis-time <unix>` and `--cl-seconds-per-slot <secs>`. The
sidecar refuses to start if either is missing under `custom`.

## HTTP/2

ethryx serves HTTP/1.1 and HTTP/2 on the same `--listen` port — the protocol is
auto-detected per connection. Cleartext HTTP/2 (**h2c**, prior-knowledge) is
supported, covering the common "TLS-terminating LB / mesh forwards h2c to the
backend" shape (Envoy, Istio, HAProxy `proto h2`); plain HTTP/1.1 and the
HTTP/1.1 WebSocket upgrade are unchanged.

WebSocket works over both transports: the HTTP/1.1 `Upgrade` handshake and
HTTP/2 Extended CONNECT (RFC 8441, `:protocol=websocket`). Either is bridged to
the upstream's HTTP/1.1 WebSocket (`--el-ws-url`).

The upstream client auto-negotiates h2 for `https://` upstreams via ALPN. A
cleartext EL JSON-RPC upstream can't be auto-negotiated, so the health poller
probes it: it prefers cleartext **h2c** and forwards over HTTP/2 when the upstream
serves it (geth ≥v1.17, erigon, reth), falling back to HTTP/1.1 otherwise — no
flag. The verdict starts at h2c and is confirmed by the first poll before traffic
is served. A running upstream that drops h2c is detected within one poll; one that
newly adds h2c is picked up on restart (while HTTP/1.1 works it isn't re-probed).
The data-plane follows the verdict and never retries across protocols (to avoid
double-sending a non-idempotent call like `eth_sendRawTransaction`), so while a
running upstream is switching away from h2c the data-plane can briefly return
`502`s — up to one poll interval — until the verdict updates. The CL Beacon hop
stays HTTP/1.1. (The gain is mainly under high request concurrency; for a
localhost sidecar hop it's modest.)

ethryx does **not** terminate TLS — it serves plaintext and leaves TLS to the
LB / service mesh in front.

## Usage

```sh
ethryx \
  --listen 0.0.0.0:8547 \
  --el-http-url   http://127.0.0.1:8545 \
  --el-ws-url     ws://127.0.0.1:8546 \
  --cl-beacon-url http://127.0.0.1:5052
```

Every flag also accepts an `ETHRYX_*` env var (see `ethryx --help`).

### Multi-port listen

`--listen` is repeatable (or comma-separated). All ports serve identical
routes — useful when some traffic hits the box via LB on one port while
operators / scrapers reach it directly on another:

```sh
ethryx \
  --listen 0.0.0.0:8547 \
  --listen 127.0.0.1:9547 \
  ...
# or
ETHRYX_LISTEN=0.0.0.0:8547,127.0.0.1:9547 ethryx ...
```

Each listener runs an independent accept loop on the tokio runtime, so cores
saturate naturally without cross-listener locks.

## Logging

Structured JSON to stdout. Levels follow a sidecar-appropriate discipline:

| Level   | What                                                                |
|---------|---------------------------------------------------------------------|
| `error` | genuine internal faults                                             |
| `warn`  | readiness became **not-ready** (LB will deroute); `accept()` failed |
| `info`  | lifecycle (start w/ version / listen / shutdown), readiness **recovered**, the EL upstream h2c↔h1 switch |
| `debug` | routine activity: per-request proxy / WS outcomes, each health poll, connection errors |
| `trace` | fine-grained internal flow (request routing, connection accept / close) |

`info` is reserved for notable, low-frequency events, so a healthy sidecar is
nearly silent between state changes — request rates and latencies belong in
metrics, not one log line per request. Routine upstream / client failures (a 502,
a dropped WebSocket) are `debug`, not `error` — for a sidecar they are everyday.
Readiness *changes* are logged once by the poller (not per probe).

The startup line carries two version fields: `version` (the crate version, e.g.
`v0.1.2`) and `git` (the build's `git describe`). For a tagged release the two
match (`git` is `v0.1.2`); an ad-hoc build off a later commit shows
`v0.1.2-5-g20537f9`, with a `-dirty` suffix when the tree had uncommitted
changes — so a binary's exact provenance is always visible in its logs. (`git`
is `unknown` when built without a git checkout, e.g. from a source tarball.)

Set the level with `--log-level <trace|debug|info|warn|error>` (default `info`).
`RUST_LOG` overrides it and allows per-target directives:

```sh
ethryx --log-level debug ...
RUST_LOG=ethryx=debug,hyper=warn ethryx ...
```

### Access log

For a per-connection trail — peer, the negotiated `HTTP/1.1` vs `HTTP/2`, and the
first request's method and path — enable `--access-log` (`ETHRYX_ACCESS_LOG`). It
emits one line per connection on a dedicated `access_log` target, kept separate
from the application log (the nginx / Envoy / Caddy split) so the `info` stream
stays quiet by default. Health-probe paths (`/livez`, `/readyz`, `/healthz`) are
**excluded** even when it's on, so frequent k8s / LB checks don't bury real
traffic.

The `access_log` target is deliberately *not* under `ethryx`, so raising the app
log (`--log-level debug`, or `RUST_LOG=ethryx=debug`) does **not** turn it on —
the access log is controlled only by `--access-log` or by naming its target
directly. When `RUST_LOG` is set it takes over the whole filter (the
`--access-log` flag is then ignored), so name the target there if you want it:

```sh
ethryx --access-log ...
RUST_LOG=ethryx=debug,access_log=info ethryx ...
```

## systemd

```ini
[Unit]
Description=Ethryx EL/CL sidecar
After=network-online.target geth.service lighthouse.service
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/ethryx \
  --network mainnet \
  --listen 0.0.0.0:8547 \
  --el-http-url   http://127.0.0.1:8545 \
  --el-ws-url     ws://127.0.0.1:8546 \
  --cl-beacon-url http://127.0.0.1:5052
Restart=on-failure
RestartSec=2
User=ethryx
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

For testnets, set `--network hoodi` (or `sepolia` / `holesky`). For a private
chain: `--network custom --cl-genesis-time <unix> --cl-seconds-per-slot 12`.

### Readiness tuning

`/readyz` is sync-only by default and needs no tuning — it tracks whether the
node is caught up to its peers. Add `--readyz-strict` if you also want it to
fail when the node stops advancing at head; freshness then gates on:

| Flag                      | Default | Gates `/readyz` on            |
|---------------------------|---------|-------------------------------|
| `--el-max-block-age-secs` | `60`    | EL latest-block wall-clock age |
| `--cl-max-slot-age-secs`  | `60`    | CL head-slot wall-clock age    |

These two flags are inert without `--readyz-strict`; `/healthz` always reports
the raw age regardless. On young testnets or low-traffic private chains, widen
them (e.g. `120`) so normal slot gaps don't flap readiness. Peer counts no
longer gate anything — `/healthz` simply reports the live count for your
monitoring stack to threshold.

## Development

Git hooks live in `.githooks/`. One-time setup per clone:

```sh
git config core.hooksPath .githooks
cargo install --locked \
    cargo-audit \
    cargo-deny \
    cargo-release \
    cargo-llvm-cov
# optional: `just` (https://github.com/casey/just) for the shortcuts in justfile
```

- `pre-commit` runs `cargo fmt --all -- --check` and `cargo clippy
  --all-targets --locked -- -D warnings` (skipped when the commit touches no
  Rust / `Cargo.*` / `rust-toolchain*` files).
- `pre-push` runs `cargo test --locked` plus `cargo audit -D warnings`
  (RustSec advisory DB). The audit step is soft-skipped if `cargo-audit` is
  not installed.
- Bypass with `--no-verify` only for emergencies.

Common tasks (via `just`):

| Recipe          | Action                                        |
|-----------------|-----------------------------------------------|
| `just check`    | fmt + clippy + test + audit (full local gate) |
| `just fmt`      | `cargo fmt --all`                             |
| `just deny`     | `cargo deny check` (supply-chain audit)       |
| `just coverage` | HTML coverage report under `target/llvm-cov/` |
| `just release`  | `cargo release patch --execute`               |

Open in a devcontainer (VSCode / Codespaces) and `.devcontainer/devcontainer.json`
installs all of the above plus the cross-compile targets automatically.

## Release

Releases are cut with [`cargo-release`](https://github.com/crate-ci/cargo-release):

```sh
cargo install cargo-release --locked   # one-time

# Dry-run first to preview
cargo release patch

# Apply
cargo release patch --execute   # 0.1.0 → 0.1.1
# or: cargo release minor --execute     0.1.0 → 0.2.0
# or: cargo release 0.5.0  --execute    explicit
```

This runs `cargo test --locked` as a gate, then bumps `Cargo.toml` + `Cargo.lock`,
commits as `chore: release vX.Y.Z`, tags `vX.Y.Z`, and pushes both. The `v*` tag
push triggers `.github/workflows/release.yml`, which:

1. Verifies `Cargo.toml` version matches the tag
2. Creates a GitHub Release with auto-generated notes
3. Builds static `musl` binaries and attaches them with `.sha256` checksums:
   - `ethryx-vX.Y.Z-x86_64-unknown-linux-musl.tar.gz`
   - `ethryx-vX.Y.Z-aarch64-unknown-linux-musl.tar.gz`

`cargo-release` is configured under `[package.metadata.release]` in `Cargo.toml`
(main-branch only, no crates.io publish, `cargo test --locked` as pre-hook).
