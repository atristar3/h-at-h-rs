//! Metrics and statistics module
//!
//! Tracks trust, quality, uptime, and performance metrics for the H@H client.
//! These metrics mirror those tracked by the original Java client.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// H@H performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HahMetrics {
    /// Trust score (0.0 - 1.0)
    /// Increases with successful operations, decreases with failures
    pub trust: f64,

    /// Quality score (0.0 - 1.0)
    /// Based on long-term success rate vs failure rate
    pub quality: f64,

    /// Uptime in seconds since client started
    pub uptime_seconds: u64,

    /// Total file requests received
    pub total_requests: u64,

    /// Successful file serves
    pub successful_serves: u64,

    /// Failed file serves
    pub failed_serves: u64,

    /// Cache hits
    pub cache_hits: u64,

    /// Cache misses
    pub cache_misses: u64,

    /// Total bytes served
    pub bytes_served: u64,

    /// Total bytes received (downloads)
    pub bytes_received: u64,

    /// Number of connections handled
    pub connections_handled: u64,

    /// Average response time in milliseconds
    pub avg_response_time_ms: f64,

    /// Number of heartbeats sent
    pub heartbeats_sent: u64,

    /// Successful heartbeats
    pub heartbeats_success: u64,

    /// Static range files served
    pub static_range_serves: u64,

    /// Files downloaded for static ranges
    pub static_range_downloads: u64,

    /// Current connection count
    pub current_connections: u64,

    /// Peak concurrent connections
    pub peak_connections: u64,

    /// Start timestamp (Unix epoch seconds)
    pub start_time: i64,

    /// Last activity timestamp
    pub last_activity: i64,

    /// Number of errors encountered
    pub error_count: u64,

    /// Server-reported trust (if available)
    pub server_trust: Option<f64>,

    /// Server-reported quality (if available)
    pub server_quality: Option<f64>,
}

impl Default for HahMetrics {
    fn default() -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            trust: 1.0,
            quality: 1.0,
            uptime_seconds: 0,
            total_requests: 0,
            successful_serves: 0,
            failed_serves: 0,
            cache_hits: 0,
            cache_misses: 0,
            bytes_served: 0,
            bytes_received: 0,
            connections_handled: 0,
            avg_response_time_ms: 0.0,
            heartbeats_sent: 0,
            heartbeats_success: 0,
            static_range_serves: 0,
            static_range_downloads: 0,
            current_connections: 0,
            peak_connections: 0,
            start_time: now,
            last_activity: now,
            error_count: 0,
            server_trust: None,
            server_quality: None,
        }
    }
}

/// Thread-safe metrics collector
pub struct MetricsCollector {
    /// Start instant for uptime calculation
    start_instant: Instant,
    /// Start timestamp
    start_time: i64,

    /// Atomic counters for high-frequency updates
    total_requests: AtomicU64,
    successful_serves: AtomicU64,
    failed_serves: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    bytes_served: AtomicU64,
    bytes_received: AtomicU64,
    connections_handled: AtomicU64,
    current_connections: AtomicU64,
    peak_connections: AtomicU64,
    heartbeats_sent: AtomicU64,
    heartbeats_success: AtomicU64,
    static_range_serves: AtomicU64,
    static_range_downloads: AtomicU64,
    error_count: AtomicU64,

    /// Response time tracking (protected by lock)
    response_times: RwLock<ResponseTimeTracker>,

    /// Server-reported metrics
    server_metrics: RwLock<ServerMetrics>,
}

#[derive(Debug, Default)]
struct ResponseTimeTracker {
    total_time_ms: u64,
    count: u64,
}

#[derive(Debug, Default)]
struct ServerMetrics {
    trust: Option<f64>,
    quality: Option<f64>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            start_instant: Instant::now(),
            start_time: chrono::Utc::now().timestamp(),
            total_requests: AtomicU64::new(0),
            successful_serves: AtomicU64::new(0),
            failed_serves: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            bytes_served: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            connections_handled: AtomicU64::new(0),
            current_connections: AtomicU64::new(0),
            peak_connections: AtomicU64::new(0),
            heartbeats_sent: AtomicU64::new(0),
            heartbeats_success: AtomicU64::new(0),
            static_range_serves: AtomicU64::new(0),
            static_range_downloads: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            response_times: RwLock::new(ResponseTimeTracker::default()),
            server_metrics: RwLock::new(ServerMetrics::default()),
        }
    }

    /// Record a file request
    pub fn record_request(&self) {
        self.total_requests.fetch_add(1, Ordering::SeqCst);
    }

    /// Record a successful serve
    pub fn record_successful_serve(&self, bytes: u64) {
        self.successful_serves.fetch_add(1, Ordering::SeqCst);
        self.bytes_served.fetch_add(bytes, Ordering::SeqCst);
    }

    /// Record a failed serve
    pub fn record_failed_serve(&self) {
        self.failed_serves.fetch_add(1, Ordering::SeqCst);
    }

    /// Record a cache hit
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::SeqCst);
    }

    /// Record a cache miss
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::SeqCst);
    }

    /// Record bytes received (downloaded)
    pub fn record_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::SeqCst);
    }

    /// Record a connection
    pub fn record_connection(&self) {
        self.connections_handled.fetch_add(1, Ordering::SeqCst);
        let current = self.current_connections.fetch_add(1, Ordering::SeqCst) + 1;

        // Update peak
        let mut peak = self.peak_connections.load(Ordering::SeqCst);
        while current > peak {
            match self.peak_connections.compare_exchange(
                peak,
                current,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }

    /// Record connection closed
    pub fn record_connection_closed(&self) {
        self.current_connections.fetch_sub(1, Ordering::SeqCst);
    }

    /// Record response time
    pub fn record_response_time(&self, duration: Duration) {
        let ms = duration.as_millis() as u64;
        let mut tracker = self.response_times.write();
        tracker.total_time_ms += ms;
        tracker.count += 1;
    }

    /// Record a heartbeat
    pub fn record_heartbeat(&self, success: bool) {
        self.heartbeats_sent.fetch_add(1, Ordering::SeqCst);
        if success {
            self.heartbeats_success.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Record static range serve
    pub fn record_static_range_serve(&self) {
        self.static_range_serves.fetch_add(1, Ordering::SeqCst);
    }

    /// Record static range download
    pub fn record_static_range_download(&self) {
        self.static_range_downloads.fetch_add(1, Ordering::SeqCst);
    }

    /// Record an error
    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Update server-reported trust
    pub fn set_server_trust(&self, trust: f64) {
        self.server_metrics.write().trust = Some(trust);
    }

    /// Update server-reported quality
    pub fn set_server_quality(&self, quality: f64) {
        self.server_metrics.write().quality = Some(quality);
    }

    /// Get uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        self.start_instant.elapsed().as_secs()
    }

    /// Calculate local trust score based on success rate
    fn calculate_trust(&self) -> f64 {
        let successful = self.successful_serves.load(Ordering::SeqCst);
        let failed = self.failed_serves.load(Ordering::SeqCst);
        let total = successful + failed;

        if total == 0 {
            return 1.0;
        }

        // Trust is success rate, weighted towards recent performance
        let success_rate = successful as f64 / total as f64;

        // Apply a minimum based on total requests to give new clients benefit of doubt
        let min_trust = if total < 100 {
            0.9
        } else if total < 1000 {
            0.5
        } else {
            0.0
        };

        success_rate.max(min_trust)
    }

    /// Calculate local quality score based on long-term performance
    fn calculate_quality(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::SeqCst);
        let misses = self.cache_misses.load(Ordering::SeqCst);
        let total = hits + misses;

        if total == 0 {
            return 1.0;
        }

        let hit_rate = hits as f64 / total as f64;

        // Quality is based on hit rate and success rate
        let trust = self.calculate_trust();
        (hit_rate + trust) / 2.0
    }

    /// Get current metrics
    pub fn get_metrics(&self) -> HahMetrics {
        let server = self.server_metrics.read();
        let response_tracker = self.response_times.read();

        let avg_response = if response_tracker.count > 0 {
            response_tracker.total_time_ms as f64 / response_tracker.count as f64
        } else {
            0.0
        };

        HahMetrics {
            trust: self.calculate_trust(),
            quality: self.calculate_quality(),
            uptime_seconds: self.uptime_seconds(),
            total_requests: self.total_requests.load(Ordering::SeqCst),
            successful_serves: self.successful_serves.load(Ordering::SeqCst),
            failed_serves: self.failed_serves.load(Ordering::SeqCst),
            cache_hits: self.cache_hits.load(Ordering::SeqCst),
            cache_misses: self.cache_misses.load(Ordering::SeqCst),
            bytes_served: self.bytes_served.load(Ordering::SeqCst),
            bytes_received: self.bytes_received.load(Ordering::SeqCst),
            connections_handled: self.connections_handled.load(Ordering::SeqCst),
            avg_response_time_ms: avg_response,
            heartbeats_sent: self.heartbeats_sent.load(Ordering::SeqCst),
            heartbeats_success: self.heartbeats_success.load(Ordering::SeqCst),
            static_range_serves: self.static_range_serves.load(Ordering::SeqCst),
            static_range_downloads: self.static_range_downloads.load(Ordering::SeqCst),
            current_connections: self.current_connections.load(Ordering::SeqCst),
            peak_connections: self.peak_connections.load(Ordering::SeqCst),
            start_time: self.start_time,
            last_activity: chrono::Utc::now().timestamp(),
            error_count: self.error_count.load(Ordering::SeqCst),
            server_trust: server.trust,
            server_quality: server.quality,
        }
    }

    /// Get success rate (0.0 - 1.0)
    pub fn success_rate(&self) -> f64 {
        let successful = self.successful_serves.load(Ordering::SeqCst);
        let failed = self.failed_serves.load(Ordering::SeqCst);
        let total = successful + failed;

        if total == 0 {
            1.0
        } else {
            successful as f64 / total as f64
        }
    }

    /// Get cache hit rate (0.0 - 1.0)
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::SeqCst);
        let misses = self.cache_misses.load(Ordering::SeqCst);
        let total = hits + misses;

        if total == 0 {
            1.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Reset all counters (for testing or restart)
    pub fn reset(&self) {
        self.total_requests.store(0, Ordering::SeqCst);
        self.successful_serves.store(0, Ordering::SeqCst);
        self.failed_serves.store(0, Ordering::SeqCst);
        self.cache_hits.store(0, Ordering::SeqCst);
        self.cache_misses.store(0, Ordering::SeqCst);
        self.bytes_served.store(0, Ordering::SeqCst);
        self.bytes_received.store(0, Ordering::SeqCst);
        self.connections_handled.store(0, Ordering::SeqCst);
        self.current_connections.store(0, Ordering::SeqCst);
        self.heartbeats_sent.store(0, Ordering::SeqCst);
        self.heartbeats_success.store(0, Ordering::SeqCst);
        self.static_range_serves.store(0, Ordering::SeqCst);
        self.static_range_downloads.store(0, Ordering::SeqCst);
        self.error_count.store(0, Ordering::SeqCst);
        // Note: peak_connections intentionally not reset
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Format bytes as human-readable string
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format duration as human-readable string
pub fn format_duration(seconds: u64) -> String {
    let days = seconds / 86400;
    let hours = (seconds % 86400) / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    if days > 0 {
        format!("{}d {}h {}m", days, hours, minutes)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, secs)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}
