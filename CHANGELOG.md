# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->
## [Unreleased] - ReleaseDate

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
