# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->
## [Unreleased] - ReleaseDate

## [0.1.0] - 2026-05-28

### Added

- Initial sidecar implementation: hyper 1 reverse proxy for Ethereum EL JSON-RPC
  (HTTP / WebSocket) and CL Beacon REST API, aggregated `/health` with EL+CL
  sync / peer / freshness checks, multi-port listen, `--network` presets for CL
  genesis (mainnet / hoodi / sepolia / holesky / custom), CLI + env config via
  clap.
