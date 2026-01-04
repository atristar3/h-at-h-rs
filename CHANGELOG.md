# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-01-03

### Added

- **Full H@H Protocol Support**
  - Client login and authentication with correct hash format
  - Server time synchronization (`serverTimeDelta`)
  - RPC server failover with multiple IPs
  - Dynamic RPC path support
  - Heartbeat and keep-alive mechanism
  - Server command handling (stats, speed tests, refresh)
  - Keystamp validation with correct time windows

- **Cache Management**
  - SQLite-backed file tracking
  - LRU cache eviction
  - Configurable cache size limits
  - Cache statistics and metrics

- **Static Range Assignments**
  - Proactive file downloads for assigned hash ranges
  - Background workers for range population
  - Range refresh from server

- **Bandwidth Management**
  - Upload/download speed throttling
  - Hourly bandwidth quotas
  - Server-assigned throttle limits

- **Flood Control**
  - Per-IP rate limiting
  - Automatic blocking of abusive clients
  - Local network bypass

- **File Integrity**
  - SHA-1 verification of served files
  - Inline integrity checking

- **Gallery Downloading**
  - Page-by-page gallery scraping
  - Multiple concurrent download workers
  - Progress tracking and status API

- **Archive Downloading**
  - ZIP file downloads via official API
  - Multiple resolution support (Original, 2400px, 1600px, 1280px, 980px, 780px)
  - GP cost estimation

- **H@H Download Queue**
  - Official API integration (`fetchqueue`, `dlfetch`, `dlfails`)
  - Server-managed gallery downloads
  - Automatic failure reporting

- **ExHentai Support**
  - Cookie-based authentication
  - Full access to ExHentai content

- **Terminal UI (TUI)**
  - Real-time dashboard with ratatui
  - Multiple tabs (Dashboard, Downloads, Cache, Logs)
  - Keyboard navigation

- **REST API**
  - Health and status endpoints
  - Gallery/archive download management
  - Cache statistics
  - Bandwidth monitoring
  - Metrics endpoint

- **Infrastructure**
  - Docker and docker-compose support
  - Multi-stage Docker builds
  - GitHub Actions CI/CD
  - Cross-platform builds (Linux, macOS, Windows)

### Performance

- HashSet-based static range lookup (4-13x faster than Vec)
- Zero-copy keystamp parsing
- Pre-allocated SHA-1 buffers
- Inline hints on hot paths
- DashMap for concurrent access

### Security

- Non-root Docker container
- Secure credential handling via environment variables
- Proper request validation

[unreleased]: https://github.com/atristar3/h-at-h-rs/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/atristar3/h-at-h-rs/releases/tag/v0.1.0
