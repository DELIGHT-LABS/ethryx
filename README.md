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
| `GET /health`                        | EL + CL health aggregate |
| `GET /livez`                         | 200 OK (process liveness) |
| `/eth/...` (Beacon API)              | `--cl-beacon-url`   |
| `Upgrade: websocket`                 | `--el-ws-url`       |
| everything else (JSON-RPC `POST /`)  | `--el-http-url`     |

## Health checks

`/health` returns 200 + JSON when all are green, else 503:

| Field            | Source                                  | Threshold flag                |
|------------------|-----------------------------------------|-------------------------------|
| `el_syncing`     | EL `eth_syncing`                        | —                             |
| `el_peers`       | EL `net_peerCount`                      | `--el-min-peers`              |
| `el_block_fresh` | EL `eth_getBlockByNumber("latest")`     | `--el-max-block-age-secs`     |
| `cl_syncing`     | Beacon `/eth/v1/node/syncing`           | —                             |
| `cl_peers`       | Beacon `/eth/v1/node/peer_count`        | `--cl-min-peers`              |
| `cl_slot_fresh`  | Beacon head_slot vs. wall-clock         | `--cl-max-slot-age-secs`      |

CL slot freshness is derived from `head_slot * --cl-seconds-per-slot +
--cl-genesis-time`. Use `--network <name>` to pick a preset (defaults to
mainnet) instead of typing both. Set `--cl-genesis-time 0` to skip the
slot-age check entirely.

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
  --cl-beacon-url http://127.0.0.1:5052 \
  --el-min-peers 8 \
  --el-max-block-age-secs 60 \
  --cl-min-peers 8 \
  --cl-max-slot-age-secs 60
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

### Threshold tuning

Defaults are chosen to fire only on **clearly degraded** state (lower
false-positive on k8s probes); not as tight as production alerting tooling.
Recommended adjustments:

| Scenario                              | `--el-min-peers` | `--cl-min-peers` | `--*-age-secs` |
|---------------------------------------|------------------|------------------|----------------|
| Default (balanced, both layers ≈ 8 %) | `8`              | `8`              | `60`           |
| Mainnet, production-tight             | `16` – `32`      | `16` – `32`      | `60`           |
| Hoodi / Sepolia / Holesky, early days | `4`              | `4`              | `120`          |
| Private / low-peer chain              | `2`              | `2`              | tune to slot   |

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
