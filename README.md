<p align="center">
  <h1 align="center">🦀 H@H-rs</h1>
  <p align="center">
    <strong>A high-performance Hentai@Home client written in Rust</strong>
  </p>
  <p align="center">
    <a href="https://github.com/atristar3/h-at-h-rs/actions/workflows/ci.yml">
      <img src="https://github.com/atristar3/h-at-h-rs/actions/workflows/ci.yml/badge.svg" alt="CI Status">
    </a>
    <a href="https://github.com/atristar3/h-at-h-rs/releases">
      <img src="https://img.shields.io/github/v/release/atristar3/h-at-h-rs?color=green" alt="Release">
    </a>
    <a href="https://github.com/atristar3/h-at-h-rs/blob/main/LICENSE-MIT">
      <img src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue" alt="License">
    </a>
    <a href="https://github.com/atristar3/h-at-h-rs">
      <img src="https://img.shields.io/badge/rust-1.85+-orange.svg" alt="Rust Version">
    </a>
    <a href="https://ghcr.io/atristar3/h-at-h-rs">
      <img src="https://img.shields.io/badge/docker-ghcr.io-blue" alt="Docker">
    </a>
  </p>
</p>

---

Full-featured H@H client implementation with 100% protocol compatibility, gallery downloading, and modern infrastructure support. Drop-in replacement for the official Java client.

## ✨ Features

<table>
<tr>
<td width="50%">

### 🔌 Core H@H Protocol
- Full server authentication & handshake
- Static range assignments
- Bandwidth throttling (server & client)
- Trust & quality metrics
- Heartbeat & keep-alive
- Server command handling
- Proxy mode for uncached files

</td>
<td width="50%">

### 📥 Downloads
- Gallery downloading (page by page)
- Archive downloads (ZIP files)
- H@H Download Queue integration
- Multiple resolutions support
- ExHentai access with cookies

</td>
</tr>
<tr>
<td>

### 🛡️ Security & Performance
- Request flood control
- File integrity verification
- Optimized HashSet lookups
- Zero-copy parsing
- Concurrent request handling

</td>
<td>

### 🚀 Infrastructure
- Docker & docker-compose ready
- SQLite cache tracking
- REST API for monitoring
- Terminal UI (TUI) dashboard
- Cross-platform support

</td>
</tr>
</table>

## 📦 Installation

### Using Pre-built Binaries

Download from the [releases page](https://github.com/atristar3/h-at-h-rs/releases):

```bash
# Linux (amd64)
curl -LO https://github.com/atristar3/h-at-h-rs/releases/latest/download/h-at-h-rs-linux-amd64.tar.gz
tar xzf h-at-h-rs-linux-amd64.tar.gz
./h-at-h-rs --help

# macOS (Apple Silicon)
curl -LO https://github.com/atristar3/h-at-h-rs/releases/latest/download/h-at-h-rs-macos-arm64.tar.gz
tar xzf h-at-h-rs-macos-arm64.tar.gz
./h-at-h-rs --help
```

### Using Docker (Recommended)

```bash
# Pull from GitHub Container Registry
docker pull ghcr.io/atristar3/h-at-h-rs:latest

# Or use docker-compose
git clone https://github.com/atristar3/h-at-h-rs.git
cd h-at-h-rs
cp .env.example .env
# Edit .env with your credentials
docker-compose up -d
```

### Building from Source

```bash
# Requires Rust 1.85+
cargo install --git https://github.com/atristar3/h-at-h-rs

# Or build locally
git clone https://github.com/atristar3/h-at-h-rs.git
cd h-at-h-rs
cargo build --release
```

## 🚀 Quick Start

### 1. Get Your Credentials

Register a H@H client at the [E-Hentai forums](https://forums.e-hentai.org/) and obtain your:
- **Client ID** (numeric)
- **Client Key** (alphanumeric)

### 2. Configure

```bash
# Create configuration
cat > .env << EOF
HAH_CLIENT_ID=your_client_id
HAH_CLIENT_KEY=your_client_key
HAH_PORT=8080
HAH_CACHE_SIZE_GB=100
EOF
```

### 3. Run

```bash
# Direct execution
./h-at-h-rs

# With TUI dashboard
./h-at-h-rs --tui

# Using Docker
docker-compose up -d
```

## ⚙️ Configuration

All settings via environment variables or CLI flags:

### Required

| Variable | CLI | Description |
|----------|-----|-------------|
| `HAH_CLIENT_ID` | `--client-id` | Your H@H client ID |
| `HAH_CLIENT_KEY` | `--client-key` | Your H@H client key |

### Network & Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `HAH_PORT` | `8080` | Port to listen on |
| `HAH_BIND_ADDRESS` | `0.0.0.0` | Bind address |
| `HAH_CACHE_DIR` | `./cache` | Cache directory |
| `HAH_CACHE_SIZE_GB` | `100` | Max cache size (GB) |
| `HAH_MIN_FREE_SPACE_GB` | `5` | Minimum free disk space |

### Bandwidth

| Variable | Default | Description |
|----------|---------|-------------|
| `HAH_MAX_UPLOAD_SPEED` | `-1` | Max upload KB/s (-1 = unlimited) |
| `HAH_MAX_DOWNLOAD_SPEED` | `-1` | Max download KB/s (-1 = unlimited) |
| `HAH_MAX_HOURLY_BANDWIDTH` | `-1` | Hourly limit MB (-1 = unlimited) |

### Features

| Variable | Default | Description |
|----------|---------|-------------|
| `HAH_STATIC_RANGES` | `false` | Enable static range proactive caching |
| `HAH_PROXY_MODE` | `false` | Proxy uncached file requests |
| `HAH_GALLERY_DOWNLOAD` | `true` | Enable gallery downloads |
| `HAH_TUI` | `false` | Enable terminal UI |

### ExHentai Access

| Variable | Description |
|----------|-------------|
| `HAH_EXHENTAI_MEMBER_ID` | `ipb_member_id` cookie |
| `HAH_EXHENTAI_PASS_HASH` | `ipb_pass_hash` cookie |
| `HAH_EXHENTAI_IGNEOUS` | `igneous` cookie (optional) |

<details>
<summary>📋 Full Configuration Reference</summary>

See [env.example](./env.example) for all available options with documentation.

</details>

## 🖥️ Terminal UI

Launch with `--tui` flag for a real-time dashboard:

```bash
./h-at-h-rs --tui
```

| Key | Action |
|-----|--------|
| `q` / `Esc` | Quit |
| `Tab` / `→` | Next tab |
| `1-4` | Jump to tab |
| `↑↓` / `jk` | Scroll |
| `?` | Help |

## 📡 REST API

### Status & Metrics

```bash
# Health check
curl http://localhost:8080/api/health

# Full status
curl http://localhost:8080/api/status

# Detailed metrics
curl http://localhost:8080/api/metrics

# Bandwidth stats
curl http://localhost:8080/api/bandwidth

# Cache stats
curl http://localhost:8080/api/cache/stats
```

### Gallery Downloads

```bash
# Queue by URL
curl -X POST http://localhost:8080/api/gallery/download \
  -H "Content-Type: application/json" \
  -d '{"url": "https://e-hentai.org/g/1234567/abcdef1234/"}'

# Check status
curl http://localhost:8080/api/gallery/status/1234567

# List all
curl http://localhost:8080/api/gallery/list
```

### Archive Downloads

```bash
# Queue archive (original quality)
curl -X POST http://localhost:8080/api/archive/download \
  -H "Content-Type: application/json" \
  -d '{"url": "https://e-hentai.org/g/1234567/abcdef1234/"}'

# Queue with resolution (org/2400/1600/1280/980/780)
curl -X POST http://localhost:8080/api/archive/download \
  -H "Content-Type: application/json" \
  -d '{"gallery_id": "1234567", "gallery_token": "abc123", "resolution": "1280"}'

# Download completed archive
curl -O -J http://localhost:8080/api/archive/file/1234567
```

## 🐳 Docker

### Quick Start

```bash
docker run -d \
  --name hah-rs \
  -p 8080:8080 \
  -v ./cache:/app/cache \
  -e HAH_CLIENT_ID=your_id \
  -e HAH_CLIENT_KEY=your_key \
  ghcr.io/atristar3/h-at-h-rs:latest
```

### docker-compose

```yaml
version: '3.8'
services:
  hah:
    image: ghcr.io/atristar3/h-at-h-rs:latest
    container_name: hah-rs
    restart: unless-stopped
    ports:
      - "8080:8080"
    volumes:
      - ./cache:/app/cache
      - ./db:/app/db
    environment:
      - HAH_CLIENT_ID=${HAH_CLIENT_ID}
      - HAH_CLIENT_KEY=${HAH_CLIENT_KEY}
      - HAH_CACHE_SIZE_GB=100
```

## 📊 Performance

Benchmarked on typical hardware:

| Operation | Time |
|-----------|------|
| Static range lookup | ~8 ns |
| Keystamp validation | ~260 ns |
| SHA-1 hash (1KB) | ~475 ns |
| SHA-1 hash (1MB) | ~375 µs |
| Flood control check | ~13 ns |

## 🔧 Development

```bash
# Run tests
cargo test

# Run benchmarks
cargo bench

# Check formatting
cargo fmt --check

# Run clippy
cargo clippy -- -D warnings

# Build docs
cargo doc --open
```

## 🤝 Contributing

Contributions welcome! Please see [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## 📄 License

Dual licensed under [MIT](./LICENSE-MIT) or [Apache-2.0](./LICENSE-APACHE) at your option.

## ⚠️ Disclaimer

This is an unofficial implementation. Use at your own risk and in accordance with the H@H network's terms of service.

---

<p align="center">
  <strong>Made with ❤️ by <a href="https://github.com/atristar3">atristar3</a></strong>
</p>
