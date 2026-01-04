//! HTTP server module
//!
//! Handles serving cached files and API endpoints for the H@H client.

use crate::api::HahApiClient;
use crate::archive::{ArchiveDownloader, ArchiveRequest, ArchiveResolution};
use crate::cache::CacheManager;
use crate::config::Config;
use crate::gallery::{DownloadRequest, GalleryDownloader};
use crate::hath_downloader::HathDownloader;
use crate::metrics::MetricsCollector;
use crate::throttle::BandwidthThrottler;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use dashmap::DashMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, warn};

/// Flood control entry for rate limiting per IP
struct FloodControlEntry {
    /// Connection count (decays over time)
    connect_count: AtomicI64,
    /// Last connection timestamp (millis)
    last_connect: AtomicI64,
    /// Block until timestamp (millis)
    block_until: AtomicI64,
}

impl FloodControlEntry {
    fn new() -> Self {
        Self {
            connect_count: AtomicI64::new(0),
            last_connect: AtomicI64::new(0),
            block_until: AtomicI64::new(0),
        }
    }

    /// Check if IP is currently blocked
    fn is_blocked(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        self.block_until.load(Ordering::Relaxed) > now
    }

    /// Check if entry is stale (no activity for 60 seconds)
    fn is_stale(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        self.last_connect.load(Ordering::Relaxed) < now - 60000
    }

    /// Record a connection attempt. Returns false if flood control triggered.
    fn hit(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let last = self.last_connect.load(Ordering::Relaxed);
        let elapsed = (now - last) / 1000; // seconds since last connect

        // Decay connection count based on time elapsed
        let old_count = self.connect_count.load(Ordering::Relaxed);
        let decayed = (old_count - elapsed).max(0) + 1;
        self.connect_count.store(decayed, Ordering::Relaxed);
        self.last_connect.store(now, Ordering::Relaxed);

        // If more than 10 connections in ~5 seconds, block for 60 seconds
        if decayed > 10 {
            self.block_until.store(now + 60000, Ordering::Relaxed);
            return false;
        }
        true
    }
}

/// Flood control table for rate limiting
pub struct FloodControl {
    entries: DashMap<String, FloodControlEntry>,
    /// Pattern for detecting local network addresses
    local_network_pattern: Regex,
}

impl FloodControl {
    pub fn new() -> Self {
        // Pattern for local networks: localhost, 127.x, 10.x, 192.168.x, 172.16-31.x, 169.254.x, IPv6 loopback
        let pattern = Regex::new(
            r"^((localhost)|(127\.)|(10\.)|(192\.168\.)|(172\.((1[6-9])|(2[0-9])|(3[0-1]))\.)|(169\.254\.)|(::1)|(0:0:0:0:0:0:0:1)|(fc)|(fd)).*$"
        ).unwrap();

        Self {
            entries: DashMap::new(),
            local_network_pattern: pattern,
        }
    }

    /// Check if address is from local network
    pub fn is_local_network(&self, addr: &str) -> bool {
        self.local_network_pattern.is_match(addr)
    }

    /// Check if IP should be allowed to connect. Returns false if blocked.
    pub fn check_and_record(&self, ip: &str) -> bool {
        // Local network always allowed
        if self.is_local_network(ip) {
            return true;
        }

        let entry = self
            .entries
            .entry(ip.to_string())
            .or_insert_with(FloodControlEntry::new);

        if entry.is_blocked() {
            return false;
        }

        entry.hit()
    }

    /// Prune stale entries from the table
    pub fn prune_stale(&self) {
        self.entries.retain(|_, v| !v.is_stale());
    }
}

/// File integrity verification tracker
pub struct FileVerificationTracker {
    /// Last time any file was verified (millis since epoch)
    last_verification: AtomicI64,
    /// Map of file hash -> last verification time (millis since epoch)
    verified_files: DashMap<String, i64>,
}

impl FileVerificationTracker {
    pub fn new() -> Self {
        Self {
            last_verification: AtomicI64::new(0),
            verified_files: DashMap::new(),
        }
    }

    /// Check if we can verify a file now (rate limited to once every 2 seconds)
    pub fn can_verify_now(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Only allow one file to be verified every 2 seconds
        let last = self.last_verification.load(Ordering::Relaxed);
        if now - last < 2000 {
            return false;
        }

        // Try to atomically claim this verification slot
        self.last_verification
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    /// Check if a file should be verified (not verified in the last week)
    pub fn should_verify(&self, hash: &str) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let week_ago = now - (7 * 24 * 60 * 60 * 1000);

        if let Some(last_verified) = self.verified_files.get(hash) {
            *last_verified < week_ago
        } else {
            true
        }
    }

    /// Record that a file was verified
    pub fn record_verification(&self, hash: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        self.verified_files.insert(hash.to_string(), now);
    }
}

/// Application state shared across handlers
pub struct AppState {
    pub config: Arc<Config>,
    pub cache: Arc<CacheManager>,
    pub api: Arc<HahApiClient>,
    pub gallery_downloader: Arc<GalleryDownloader>,
    pub archive_downloader: Arc<ArchiveDownloader>,
    pub hath_downloader: Arc<HathDownloader>,
    pub metrics: Arc<MetricsCollector>,
    pub upload_throttler: Arc<BandwidthThrottler>,
    pub download_throttler: Arc<BandwidthThrottler>,
    pub flood_control: Arc<FloodControl>,
    pub file_verifier: Arc<FileVerificationTracker>,
}

#[derive(Debug, Deserialize)]
pub struct FileRequest {
    pub keystamp: Option<String>,
    pub fileindex: Option<String>,
    pub xres: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(message: &str) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message.to_string()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub version: String,
    pub client_id: String,
    pub cache_size_bytes: u64,
    pub cache_files: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub bytes_served: u64,
    pub uptime_seconds: u64,
    pub gallery_download_enabled: bool,
    pub trust: f64,
    pub quality: f64,
    pub successful_serves: u64,
    pub failed_serves: u64,
    pub current_connections: u64,
    pub peak_connections: u64,
    pub bytes_per_second: u64,
    pub bytes_per_hour: u64,
    pub is_throttled: bool,
    pub static_ranges_enabled: bool,
    pub proxy_mode_enabled: bool,
}

#[derive(Debug, Serialize)]
pub struct GalleryStatusResponse {
    pub gallery_id: String,
    pub status: String,
    pub downloaded: i64,
    pub total: i64,
}

#[derive(Debug, Deserialize)]
pub struct GalleryDownloadParams {
    pub url: Option<String>,
    pub gallery_id: Option<String>,
    pub gallery_token: Option<String>,
    pub start_page: Option<u32>,
    pub end_page: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ArchiveDownloadParams {
    pub url: Option<String>,
    pub gallery_id: Option<String>,
    pub gallery_token: Option<String>,
    pub resolution: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ArchiveStatusResponse {
    pub gallery_id: String,
    pub gallery_token: String,
    pub title: Option<String>,
    pub status: String,
    pub resolution: String,
    pub file_size: i64,
    pub downloaded_bytes: i64,
    pub file_path: Option<String>,
    pub error: Option<String>,
    pub progress_percent: f64,
}

/// Build the application router
pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        // H@H file serving endpoints
        .route("/h/{file_id}/{additional}/{filename}", get(serve_file))
        .route("/h/{file_id}/{additional}", get(serve_file_no_name))
        // Health and status endpoints
        .route("/api/health", get(health_check))
        .route("/api/status", get(get_status))
        .route("/api/cache/stats", get(get_cache_stats))
        .route("/api/metrics", get(get_metrics))
        .route("/api/bandwidth", get(get_bandwidth_stats))
        // Gallery download endpoints
        .route("/api/gallery/download", post(queue_gallery_download))
        .route("/api/gallery/status/{gallery_id}", get(get_gallery_status))
        .route("/api/gallery/list", get(list_gallery_downloads))
        .route("/api/gallery/info", get(get_gallery_info))
        // Archive download endpoints
        .route("/api/archive/download", post(queue_archive_download))
        .route("/api/archive/status/{gallery_id}", get(get_archive_status))
        .route("/api/archive/list", get(list_archive_downloads))
        .route("/api/archive/file/{gallery_id}", get(serve_archive_file))
        // Internal endpoints
        .route("/servercmd", get(server_command))
        // H@H protocol test endpoints
        .route("/t/{test_size}", get(speed_test))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Get detailed metrics
async fn get_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let metrics = state.metrics.get_metrics();
    Json(ApiResponse::success(metrics))
}

/// Get bandwidth statistics
async fn get_bandwidth_stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let upload = state.upload_throttler.get_stats();
    let download = state.download_throttler.get_stats();

    Json(ApiResponse::success(serde_json::json!({
        "upload": upload,
        "download": download,
        "is_throttled": state.upload_throttler.is_throttled(),
        "hourly_remaining": state.upload_throttler.hourly_remaining(),
    })))
}

/// Speed test endpoint (H@H protocol)
async fn speed_test(
    State(state): State<Arc<AppState>>,
    Path(test_size): Path<String>,
) -> impl IntoResponse {
    let size: usize = test_size
        .trim_end_matches(|c: char| !c.is_numeric())
        .parse()
        .unwrap_or(1000);

    // Cap at 10MB
    let capped_size = size.min(10_000_000);
    let data = vec![0u8; capped_size];

    state.upload_throttler.record_transfer(capped_size as u64);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, capped_size)
        .body(Body::from(data))
        .unwrap()
}

/// Health check endpoint
async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

/// Get client status
async fn get_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cache_stats = state.cache.get_stats();
    let metrics = state.metrics.get_metrics();
    let bw_stats = state.upload_throttler.get_stats();

    Json(ApiResponse::success(StatusResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        client_id: state.config.client_id.clone(),
        cache_size_bytes: cache_stats.total_size,
        cache_files: state.cache.get_file_count(),
        cache_hits: cache_stats.hits,
        cache_misses: cache_stats.misses,
        bytes_served: cache_stats.bytes_served,
        uptime_seconds: metrics.uptime_seconds,
        gallery_download_enabled: state.config.gallery_download_enabled,
        trust: metrics.trust,
        quality: metrics.quality,
        successful_serves: metrics.successful_serves,
        failed_serves: metrics.failed_serves,
        current_connections: metrics.current_connections,
        peak_connections: metrics.peak_connections,
        bytes_per_second: bw_stats.bytes_per_second,
        bytes_per_hour: bw_stats.bytes_per_hour,
        is_throttled: state.upload_throttler.is_throttled(),
        static_ranges_enabled: state.config.static_ranges,
        proxy_mode_enabled: state.config.proxy_mode,
    }))
}

/// Get cache statistics
async fn get_cache_stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let stats = state.cache.get_stats();
    Json(ApiResponse::success(stats))
}

/// Serve a cached file
async fn serve_file(
    State(state): State<Arc<AppState>>,
    Path((file_id, additional, _filename)): Path<(String, String, String)>,
    Query(params): Query<FileRequest>,
    headers: HeaderMap,
) -> impl IntoResponse {
    serve_file_internal(state, &file_id, &additional, params, headers).await
}

/// Serve a cached file (without filename in path)
async fn serve_file_no_name(
    State(state): State<Arc<AppState>>,
    Path((file_id, additional)): Path<(String, String)>,
    Query(params): Query<FileRequest>,
    headers: HeaderMap,
) -> impl IntoResponse {
    serve_file_internal(state, &file_id, &additional, params, headers).await
}

async fn serve_file_internal(
    state: Arc<AppState>,
    file_id: &str,
    additional: &str,
    params: FileRequest,
    _headers: HeaderMap,
) -> Response {
    let request_start = Instant::now();

    // Record request
    state.metrics.record_request();
    state.metrics.record_connection();

    // Parse additional parameters (format: keystamp={time}-{hash};fileindex={idx};xres={res})
    let mut parsed_keystamp = params.keystamp.clone();
    let mut _fileindex = params.fileindex.clone();
    let mut _xres = params.xres.clone();

    for pair in additional.split(';') {
        if let Some((key, value)) = pair.split_once('=') {
            match key {
                "keystamp" => parsed_keystamp = Some(value.to_string()),
                "fileindex" => _fileindex = Some(value.to_string()),
                "xres" => _xres = Some(value.to_string()),
                _ => {}
            }
        }
    }

    // Parse the file ID to get hash
    // Format: {hash}_{size}_{width}x{height}_{type}
    // Optimized: Use split_once() instead of collect() to Vec for zero-copy parsing
    let hash = file_id.split('_').next().unwrap_or(file_id);
    if hash.is_empty() {
        state.metrics.record_connection_closed();
        state.metrics.record_failed_serve();
        return (StatusCode::BAD_REQUEST, "Invalid file ID").into_response();
    }

    // Verify request if keystamp provided (required for valid H@H requests)
    // Optimized: Use split_once() for zero-copy parsing of keystamp
    if let Some(keystamp) = &parsed_keystamp {
        if let Some((timestamp_str, key)) = keystamp.split_once('-') {
            let timestamp: i64 = timestamp_str.parse().unwrap_or(0);

            if !state.api.verify_request(file_id, key, timestamp) {
                warn!("Invalid request signature for file {}", file_id);
                state.metrics.record_connection_closed();
                state.metrics.record_failed_serve();
                return (StatusCode::FORBIDDEN, "Invalid request").into_response();
            }
        } else {
            warn!("Malformed keystamp for file {}", file_id);
            state.metrics.record_connection_closed();
            state.metrics.record_failed_serve();
            return (StatusCode::FORBIDDEN, "Invalid keystamp format").into_response();
        }
    } else {
        // Keystamp is required for H@H requests (but optional for internal API)
        // For now, we'll allow requests without keystamp for testing
        debug!("No keystamp provided for file {}", file_id);
    }

    // Try to get file from cache
    match state.cache.get_file(hash).await {
        Ok(Some(cached)) => {
            debug!("Serving cached file: {} ({})", hash, cached.size);
            state.metrics.record_cache_hit();

            // Read entire file into memory (fine for image sizes)
            let data = match fs::read(&cached.path).await {
                Ok(d) => d,
                Err(e) => {
                    error!("Failed to read cached file: {}", e);
                    state.metrics.record_connection_closed();
                    state.metrics.record_failed_serve();
                    state.metrics.record_error();
                    return (StatusCode::INTERNAL_SERVER_ERROR, "File read error").into_response();
                }
            };

            // Check file size matches expected
            if data.len() as u64 != cached.size {
                warn!(
                    "File size mismatch for {}: expected {}, got {}",
                    hash,
                    cached.size,
                    data.len()
                );
                state.metrics.record_connection_closed();
                state.metrics.record_failed_serve();
                state.metrics.record_error();
                return (StatusCode::NOT_FOUND, "File corrupted").into_response();
            }

            // Inline file integrity verification (rate-limited)
            // Only verify if: not recently verified AND verification cooldown elapsed
            let should_verify =
                state.file_verifier.should_verify(hash) && state.file_verifier.can_verify_now();

            if should_verify {
                // Verify SHA-1 hash matches filename
                let computed_hash = {
                    use sha1::{Digest, Sha1};
                    let mut hasher = Sha1::new();
                    hasher.update(&data);
                    hex::encode(hasher.finalize())
                };

                if computed_hash != hash {
                    error!(
                        "File integrity check failed for {}: expected {}, got {}",
                        cached.path.display(),
                        hash,
                        computed_hash
                    );
                    state.metrics.record_connection_closed();
                    state.metrics.record_failed_serve();
                    state.metrics.record_error();
                    // TODO: Mark file for re-download or deletion
                    return (StatusCode::NOT_FOUND, "File corrupted").into_response();
                }

                debug!("File integrity verified for {}", hash);
                state.file_verifier.record_verification(hash);
            }

            let data_len = data.len() as u64;

            // Apply bandwidth throttling if configured
            state.upload_throttler.throttle_and_record(data_len).await;

            // Record metrics
            state.metrics.record_successful_serve(data_len);
            state.metrics.record_response_time(request_start.elapsed());
            state.metrics.record_connection_closed();

            // Determine content type
            let content_type = match cached.file_type.as_str() {
                "jpg" | "jpeg" => "image/jpeg",
                "png" => "image/png",
                "gif" => "image/gif",
                "webp" => "image/webp",
                _ => "application/octet-stream",
            };

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, content_type)
                .header(header::CONTENT_LENGTH, data.len())
                .header(header::CACHE_CONTROL, "public, max-age=31536000")
                .body(Body::from(data))
                .unwrap()
        }
        Ok(None) => {
            debug!("Cache miss for file: {}", hash);
            state.metrics.record_cache_miss();
            state.metrics.record_connection_closed();

            // If proxy mode is enabled, try to fetch from server
            if state.config.proxy_mode {
                // Try to proxy the request
                match state.api.proxy_request(hash).await {
                    Ok(Some(proxy_url)) => {
                        debug!("Proxying request for {} via {}", hash, proxy_url);
                        // Proxy implementation would go here
                        // For now, just return not found
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!("Proxy request failed: {}", e);
                    }
                }
            }

            state.metrics.record_failed_serve();
            (StatusCode::NOT_FOUND, "File not in cache").into_response()
        }
        Err(e) => {
            error!("Cache error: {}", e);
            state.metrics.record_connection_closed();
            state.metrics.record_failed_serve();
            state.metrics.record_error();
            (StatusCode::INTERNAL_SERVER_ERROR, "Cache error").into_response()
        }
    }
}

/// Handle server commands (from H@H server)
async fn server_command(
    State(state): State<Arc<AppState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let cmd = params.get("cmd").map(|s| s.as_str()).unwrap_or("");

    match cmd {
        "speed_test" => {
            // Return test data for speed measurement
            let size: usize = params
                .get("size")
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000);

            let data = vec![0u8; size.min(10_000_000)]; // Max 10MB
            state.upload_throttler.record_transfer(data.len() as u64);
            (StatusCode::OK, data).into_response()
        }
        "still_alive" => {
            // Heartbeat response
            "OK".into_response()
        }
        "cache_stats" => {
            let stats = state.cache.get_stats();
            Json(stats).into_response()
        }
        "server_stat" => {
            // Return server statistics
            let metrics = state.metrics.get_metrics();
            let bw = state.upload_throttler.get_stats();
            Json(serde_json::json!({
                "uptime": metrics.uptime_seconds,
                "bytes_served": metrics.bytes_served,
                "files_served": metrics.successful_serves,
                "cache_hits": metrics.cache_hits,
                "cache_misses": metrics.cache_misses,
                "connections": metrics.connections_handled,
                "current_bps": bw.bytes_per_second,
                "trust": metrics.trust,
                "quality": metrics.quality,
            }))
            .into_response()
        }
        "refresh_settings" => {
            // Acknowledge settings refresh request
            info!("Received refresh_settings command");
            "OK".into_response()
        }
        "start_downloader" => {
            // Start H@H download queue processor
            info!("Received start_downloader command");
            state.hath_downloader.start();
            "OK".into_response()
        }
        "stop_downloader" => {
            // Stop H@H download queue processor
            info!("Received stop_downloader command");
            state.hath_downloader.stop();
            "OK".into_response()
        }
        "throttle" => {
            // Set throttle limit from server
            if let Some(limit_str) = params.get("bytes") {
                if let Ok(limit) = limit_str.parse::<i64>() {
                    state.upload_throttler.set_server_throttle(limit);
                    info!("Throttle set to {} bytes/sec", limit);
                }
            }
            "OK".into_response()
        }
        "proxy_test" => {
            // Proxy mode test
            if state.config.proxy_mode {
                "PROXY_OK".into_response()
            } else {
                "PROXY_DISABLED".into_response()
            }
        }
        "get_info" => {
            // Return client info
            Json(serde_json::json!({
                "version": env!("CARGO_PKG_VERSION"),
                "client_id": state.config.client_id,
                "cache_size": state.cache.get_total_size(),
                "cache_files": state.cache.get_file_count(),
                "static_ranges": state.config.static_ranges,
                "proxy_mode": state.config.proxy_mode,
            }))
            .into_response()
        }
        _ => {
            warn!("Unknown server command: {}", cmd);
            (StatusCode::BAD_REQUEST, "Unknown command").into_response()
        }
    }
}

/// Queue a gallery for download
async fn queue_gallery_download(
    State(state): State<Arc<AppState>>,
    Json(params): Json<GalleryDownloadParams>,
) -> impl IntoResponse {
    if !state.config.gallery_download_enabled {
        return Json(ApiResponse::<serde_json::Value>::error(
            "Gallery download is disabled",
        ));
    }

    // Parse gallery ID and token
    let (gallery_id, gallery_token) = if let Some(url) = &params.url {
        match GalleryDownloader::parse_gallery_url(url) {
            Some((id, token)) => (id, token),
            None => {
                return Json(ApiResponse::<serde_json::Value>::error(
                    "Invalid gallery URL",
                ))
            }
        }
    } else {
        match (&params.gallery_id, &params.gallery_token) {
            (Some(id), Some(token)) => (id.clone(), token.clone()),
            _ => {
                return Json(ApiResponse::<serde_json::Value>::error(
                    "Missing gallery_id or gallery_token",
                ))
            }
        }
    };

    let request = DownloadRequest {
        gallery_id: gallery_id.clone(),
        gallery_token: gallery_token.clone(),
        start_page: params.start_page,
        end_page: params.end_page,
        priority: 5,
    };

    match state.gallery_downloader.queue_gallery(request).await {
        Ok(_) => {
            info!(
                "Queued gallery for download: {}/{}",
                gallery_id, gallery_token
            );
            Json(ApiResponse::success(serde_json::json!({
                "message": "Gallery queued for download",
                "gallery_id": gallery_id,
                "gallery_token": gallery_token
            })))
        }
        Err(e) => {
            error!("Failed to queue gallery: {}", e);
            Json(ApiResponse::<serde_json::Value>::error(&e.to_string()))
        }
    }
}

/// Get gallery download status
async fn get_gallery_status(
    State(state): State<Arc<AppState>>,
    Path(gallery_id): Path<String>,
) -> impl IntoResponse {
    match state
        .gallery_downloader
        .get_download_status(&gallery_id)
        .await
    {
        Ok(Some((status, downloaded, total))) => {
            Json(ApiResponse::success(GalleryStatusResponse {
                gallery_id,
                status,
                downloaded,
                total,
            }))
        }
        Ok(None) => Json(ApiResponse::<GalleryStatusResponse>::error(
            "Gallery not found",
        )),
        Err(e) => Json(ApiResponse::<GalleryStatusResponse>::error(&e.to_string())),
    }
}

/// List all gallery downloads
async fn list_gallery_downloads(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.gallery_downloader.list_downloads().await {
        Ok(downloads) => {
            let list: Vec<GalleryStatusResponse> = downloads
                .into_iter()
                .map(
                    |(id, _token, status, downloaded, total)| GalleryStatusResponse {
                        gallery_id: id,
                        status,
                        downloaded,
                        total,
                    },
                )
                .collect();
            Json(ApiResponse::success(list))
        }
        Err(e) => Json(ApiResponse::<Vec<GalleryStatusResponse>>::error(
            &e.to_string(),
        )),
    }
}

/// Get gallery info
async fn get_gallery_info(
    State(state): State<Arc<AppState>>,
    Query(params): Query<GalleryDownloadParams>,
) -> impl IntoResponse {
    let (gallery_id, gallery_token) = if let Some(url) = &params.url {
        match GalleryDownloader::parse_gallery_url(url) {
            Some((id, token)) => (id, token),
            None => {
                return Json(ApiResponse::<serde_json::Value>::error(
                    "Invalid gallery URL",
                ))
            }
        }
    } else {
        match (&params.gallery_id, &params.gallery_token) {
            (Some(id), Some(token)) => (id.clone(), token.clone()),
            _ => {
                return Json(ApiResponse::<serde_json::Value>::error(
                    "Missing parameters",
                ))
            }
        }
    };

    match state
        .gallery_downloader
        .fetch_gallery_info(&gallery_id, &gallery_token)
        .await
    {
        Ok(info) => Json(ApiResponse::success(serde_json::to_value(info).unwrap())),
        Err(e) => Json(ApiResponse::<serde_json::Value>::error(&e.to_string())),
    }
}

// =============================================================================
// Archive Download Handlers
// =============================================================================

/// Queue an archive for download
async fn queue_archive_download(
    State(state): State<Arc<AppState>>,
    Json(params): Json<ArchiveDownloadParams>,
) -> impl IntoResponse {
    // Parse gallery ID and token
    let (gallery_id, gallery_token) = if let Some(url) = &params.url {
        match ArchiveDownloader::parse_gallery_url(url) {
            Some((id, token)) => (id, token),
            None => {
                return Json(ApiResponse::<serde_json::Value>::error(
                    "Invalid gallery URL",
                ))
            }
        }
    } else {
        match (&params.gallery_id, &params.gallery_token) {
            (Some(id), Some(token)) => (id.clone(), token.clone()),
            _ => {
                return Json(ApiResponse::<serde_json::Value>::error(
                    "Missing gallery_id or gallery_token",
                ))
            }
        }
    };

    // Parse resolution
    let resolution = params
        .resolution
        .as_deref()
        .and_then(ArchiveResolution::from_str)
        .unwrap_or(ArchiveResolution::Original);

    let request = ArchiveRequest {
        gallery_id: gallery_id.clone(),
        gallery_token: gallery_token.clone(),
        resolution,
        or_token: None,
    };

    match state.archive_downloader.queue_archive(request).await {
        Ok(_) => {
            info!(
                "Queued archive for download: {}/{} ({})",
                gallery_id,
                gallery_token,
                resolution.as_str()
            );
            Json(ApiResponse::success(serde_json::json!({
                "message": "Archive queued for download",
                "gallery_id": gallery_id,
                "gallery_token": gallery_token,
                "resolution": resolution.as_str()
            })))
        }
        Err(e) => {
            error!("Failed to queue archive: {}", e);
            Json(ApiResponse::<serde_json::Value>::error(&e.to_string()))
        }
    }
}

/// Get archive download status
async fn get_archive_status(
    State(state): State<Arc<AppState>>,
    Path(gallery_id): Path<String>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let resolution = params.get("resolution").map(|s| s.as_str());

    match state
        .archive_downloader
        .get_archive_status(&gallery_id, resolution)
        .await
    {
        Ok(statuses) if !statuses.is_empty() => {
            let responses: Vec<ArchiveStatusResponse> = statuses
                .into_iter()
                .map(|s| ArchiveStatusResponse {
                    gallery_id: s.gallery_id,
                    gallery_token: s.gallery_token,
                    title: s.title,
                    status: s.status,
                    resolution: s.resolution,
                    file_size: s.file_size,
                    downloaded_bytes: s.downloaded_bytes,
                    file_path: s.file_path,
                    error: s.error,
                    progress_percent: if s.file_size > 0 {
                        (s.downloaded_bytes as f64 / s.file_size as f64) * 100.0
                    } else {
                        0.0
                    },
                })
                .collect();
            Json(ApiResponse::success(responses))
        }
        Ok(_) => Json(ApiResponse::<Vec<ArchiveStatusResponse>>::error(
            "Archive not found",
        )),
        Err(e) => Json(ApiResponse::<Vec<ArchiveStatusResponse>>::error(
            &e.to_string(),
        )),
    }
}

/// List all archive downloads
async fn list_archive_downloads(
    State(state): State<Arc<AppState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let limit: i64 = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    match state.archive_downloader.list_archives(limit).await {
        Ok(archives) => {
            let responses: Vec<ArchiveStatusResponse> = archives
                .into_iter()
                .map(|s| ArchiveStatusResponse {
                    gallery_id: s.gallery_id,
                    gallery_token: s.gallery_token,
                    title: s.title,
                    status: s.status,
                    resolution: s.resolution,
                    file_size: s.file_size,
                    downloaded_bytes: s.downloaded_bytes,
                    file_path: s.file_path,
                    error: s.error,
                    progress_percent: if s.file_size > 0 {
                        (s.downloaded_bytes as f64 / s.file_size as f64) * 100.0
                    } else {
                        0.0
                    },
                })
                .collect();
            Json(ApiResponse::success(responses))
        }
        Err(e) => Json(ApiResponse::<Vec<ArchiveStatusResponse>>::error(
            &e.to_string(),
        )),
    }
}

/// Serve a downloaded archive file
async fn serve_archive_file(
    State(state): State<Arc<AppState>>,
    Path(gallery_id): Path<String>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let resolution = params.get("resolution").map(|s| s.as_str());

    match state
        .archive_downloader
        .get_archive_status(&gallery_id, resolution)
        .await
    {
        Ok(statuses) => {
            // Find a completed download
            let completed = statuses
                .into_iter()
                .find(|s| s.status == "completed" && s.file_path.is_some());

            if let Some(archive) = completed {
                if let Some(file_path) = archive.file_path {
                    // Read and serve the file
                    match fs::read(&file_path).await {
                        Ok(data) => {
                            let filename = std::path::Path::new(&file_path)
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("archive.zip");

                            Response::builder()
                                .status(StatusCode::OK)
                                .header(header::CONTENT_TYPE, "application/zip")
                                .header(header::CONTENT_LENGTH, data.len())
                                .header(
                                    header::CONTENT_DISPOSITION,
                                    format!("attachment; filename=\"{}\"", filename),
                                )
                                .body(Body::from(data))
                                .unwrap()
                        }
                        Err(e) => {
                            error!("Failed to read archive file: {}", e);
                            (StatusCode::INTERNAL_SERVER_ERROR, "File read error").into_response()
                        }
                    }
                } else {
                    (StatusCode::NOT_FOUND, "Archive file path not found").into_response()
                }
            } else {
                (StatusCode::NOT_FOUND, "No completed archive found").into_response()
            }
        }
        Err(e) => {
            error!("Failed to get archive status: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// Start the HTTP server
pub async fn start_server(state: Arc<AppState>) -> anyhow::Result<()> {
    let addr = state.config.bind_addr();
    let router = build_router(state);

    info!("Starting H@H server on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, router).await?;

    Ok(())
}
