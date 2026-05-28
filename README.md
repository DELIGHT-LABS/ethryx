# ethryx

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
  --listen 0.0.0.0:8547 \
  --el-http-url   http://127.0.0.1:8545 \
  --el-ws-url     ws://127.0.0.1:8546 \
  --cl-beacon-url http://127.0.0.1:5052 \
  --el-min-peers 3 \
  --el-max-block-age-secs 60 \
  --cl-min-peers 16 \
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
