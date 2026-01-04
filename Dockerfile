# =============================================================================
# H@H-rs Dockerfile
# Multi-stage build for a minimal production image
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Build the Rust application
# -----------------------------------------------------------------------------
FROM rust:1.85-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a new directory for the app
WORKDIR /app

# Copy dependency files first (for better layer caching)
COPY Cargo.toml Cargo.lock* ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies only (this layer will be cached)
RUN cargo build --release && rm -rf src target/release/deps/h_at_h_rs*

# Copy the actual source code
COPY src ./src

# Build the final binary
RUN cargo build --release

# -----------------------------------------------------------------------------
# Stage 2: Create the runtime image
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    libssl3 \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -r -s /bin/false -m -d /app hah

# Create necessary directories
RUN mkdir -p /app/cache /app/temp /app/data && \
    chown -R hah:hah /app

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/target/release/h-at-h-rs /app/h-at-h-rs

# Switch to non-root user
USER hah

# Expose the default port
EXPOSE 8080

# Set default environment variables
ENV HAH_PORT=8080 \
    HAH_CACHE_DIR=/app/cache \
    HAH_TEMP_DIR=/app/temp \
    HAH_DATABASE_PATH=/app/data/hah.db \
    HAH_LOG_LEVEL=info \
    HAH_LOG_JSON=true \
    HAH_BIND_ADDRESS=0.0.0.0 \
    HAH_CACHE_SIZE_GB=100 \
    HAH_DOWNLOAD_WORKERS=4 \
    HAH_GALLERY_DOWNLOAD=true

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${HAH_PORT}/api/health || exit 1

# Use tini as init system for proper signal handling
ENTRYPOINT ["/usr/bin/tini", "--"]

# Run the application
CMD ["/app/h-at-h-rs"]
