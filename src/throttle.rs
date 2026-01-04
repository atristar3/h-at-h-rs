//! Bandwidth throttling module
//!
//! Provides rate limiting for upload and download bandwidth to comply with
//! H@H network requirements and user-configured limits.

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;

/// Bandwidth statistics for a time window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthStats {
    /// Bytes transferred in the current second
    pub bytes_per_second: u64,
    /// Bytes transferred in the current minute
    pub bytes_per_minute: u64,
    /// Bytes transferred in the current hour
    pub bytes_per_hour: u64,
    /// Total bytes transferred since start
    pub total_bytes: u64,
    /// Average speed in bytes per second
    pub average_speed: f64,
    /// Peak speed in bytes per second
    pub peak_speed: u64,
    /// Number of requests served
    pub requests_served: u64,
}

impl Default for BandwidthStats {
    fn default() -> Self {
        Self {
            bytes_per_second: 0,
            bytes_per_minute: 0,
            bytes_per_hour: 0,
            total_bytes: 0,
            average_speed: 0.0,
            peak_speed: 0,
            requests_served: 0,
        }
    }
}

/// A time-windowed byte counter
struct WindowedCounter {
    /// Circular buffer of (timestamp, bytes) pairs
    samples: VecDeque<(Instant, u64)>,
    /// Window duration
    window: Duration,
    /// Total bytes in the window
    total: u64,
}

impl WindowedCounter {
    fn new(window: Duration) -> Self {
        Self {
            samples: VecDeque::with_capacity(1000),
            window,
            total: 0,
        }
    }

    fn add(&mut self, bytes: u64) {
        let now = Instant::now();
        self.samples.push_back((now, bytes));
        self.total += bytes;
        self.prune(now);
    }

    fn prune(&mut self, now: Instant) {
        while let Some(&(time, bytes)) = self.samples.front() {
            if now.duration_since(time) > self.window {
                self.samples.pop_front();
                self.total = self.total.saturating_sub(bytes);
            } else {
                break;
            }
        }
    }

    fn get_total(&mut self) -> u64 {
        self.prune(Instant::now());
        self.total
    }
}

/// Bandwidth throttler with configurable limits
pub struct BandwidthThrottler {
    /// Maximum bytes per second (-1 = unlimited)
    max_bytes_per_second: AtomicI64,
    /// Maximum bytes per hour (-1 = unlimited)
    max_bytes_per_hour: AtomicI64,
    /// Server-assigned throttle limit
    server_throttle: AtomicI64,
    /// Counters protected by mutex
    counters: Mutex<ThrottleCounters>,
    /// Start time for average calculation
    start_time: Instant,
    /// Total bytes transferred
    total_bytes: AtomicU64,
    /// Peak speed
    peak_speed: AtomicU64,
    /// Requests served
    requests_served: AtomicU64,
}

struct ThrottleCounters {
    second_counter: WindowedCounter,
    minute_counter: WindowedCounter,
    hour_counter: WindowedCounter,
}

impl BandwidthThrottler {
    /// Create a new throttler with the given limits
    /// Use -1 for unlimited
    pub fn new(max_bytes_per_second: i64, max_bytes_per_hour: i64) -> Self {
        Self {
            max_bytes_per_second: AtomicI64::new(max_bytes_per_second),
            max_bytes_per_hour: AtomicI64::new(max_bytes_per_hour),
            server_throttle: AtomicI64::new(-1),
            counters: Mutex::new(ThrottleCounters {
                second_counter: WindowedCounter::new(Duration::from_secs(1)),
                minute_counter: WindowedCounter::new(Duration::from_secs(60)),
                hour_counter: WindowedCounter::new(Duration::from_secs(3600)),
            }),
            start_time: Instant::now(),
            total_bytes: AtomicU64::new(0),
            peak_speed: AtomicU64::new(0),
            requests_served: AtomicU64::new(0),
        }
    }

    /// Create an unlimited throttler
    pub fn unlimited() -> Self {
        Self::new(-1, -1)
    }

    /// Set the server-assigned throttle limit
    pub fn set_server_throttle(&self, bytes_per_second: i64) {
        self.server_throttle
            .store(bytes_per_second, Ordering::SeqCst);
        if bytes_per_second > 0 {
            info!("Server throttle set to {} KB/s", bytes_per_second / 1024);
        }
    }

    /// Set maximum bytes per second
    pub fn set_max_bps(&self, bytes: i64) {
        self.max_bytes_per_second.store(bytes, Ordering::SeqCst);
    }

    /// Set maximum bytes per hour
    pub fn set_max_bph(&self, bytes: i64) {
        self.max_bytes_per_hour.store(bytes, Ordering::SeqCst);
    }

    /// Check if we can send `bytes` right now without exceeding limits
    pub fn can_send(&self, bytes: u64) -> bool {
        let mut counters = self.counters.lock();

        // Check server throttle (most restrictive)
        let server_limit = self.server_throttle.load(Ordering::SeqCst);
        if server_limit > 0 {
            let current_second = counters.second_counter.get_total();
            if current_second + bytes > server_limit as u64 {
                return false;
            }
        }

        // Check per-second limit
        let max_bps = self.max_bytes_per_second.load(Ordering::SeqCst);
        if max_bps > 0 {
            let current_second = counters.second_counter.get_total();
            if current_second + bytes > max_bps as u64 {
                return false;
            }
        }

        // Check hourly limit
        let max_bph = self.max_bytes_per_hour.load(Ordering::SeqCst);
        if max_bph > 0 {
            let current_hour = counters.hour_counter.get_total();
            if current_hour + bytes > max_bph as u64 {
                return false;
            }
        }

        true
    }

    /// Wait until we can send `bytes`, then record the transfer
    pub async fn throttle_and_record(&self, bytes: u64) {
        // Wait until we can send
        while !self.can_send(bytes) {
            sleep(Duration::from_millis(10)).await;
        }

        self.record_transfer(bytes);
    }

    /// Record a transfer of `bytes`
    pub fn record_transfer(&self, bytes: u64) {
        {
            let mut counters = self.counters.lock();
            counters.second_counter.add(bytes);
            counters.minute_counter.add(bytes);
            counters.hour_counter.add(bytes);
        }

        self.total_bytes.fetch_add(bytes, Ordering::SeqCst);
        self.requests_served.fetch_add(1, Ordering::SeqCst);

        // Update peak speed (approximate)
        let current_second = {
            let mut counters = self.counters.lock();
            counters.second_counter.get_total()
        };
        let current_peak = self.peak_speed.load(Ordering::SeqCst);
        if current_second > current_peak {
            self.peak_speed.store(current_second, Ordering::SeqCst);
        }
    }

    /// Get current bandwidth statistics
    pub fn get_stats(&self) -> BandwidthStats {
        let mut counters = self.counters.lock();
        let total = self.total_bytes.load(Ordering::SeqCst);
        let elapsed = self.start_time.elapsed().as_secs_f64();

        BandwidthStats {
            bytes_per_second: counters.second_counter.get_total(),
            bytes_per_minute: counters.minute_counter.get_total(),
            bytes_per_hour: counters.hour_counter.get_total(),
            total_bytes: total,
            average_speed: if elapsed > 0.0 {
                total as f64 / elapsed
            } else {
                0.0
            },
            peak_speed: self.peak_speed.load(Ordering::SeqCst),
            requests_served: self.requests_served.load(Ordering::SeqCst),
        }
    }

    /// Get current bytes per second
    pub fn current_bps(&self) -> u64 {
        let mut counters = self.counters.lock();
        counters.second_counter.get_total()
    }

    /// Get bytes remaining in hourly quota
    pub fn hourly_remaining(&self) -> Option<u64> {
        let max_bph = self.max_bytes_per_hour.load(Ordering::SeqCst);
        if max_bph < 0 {
            return None;
        }

        let mut counters = self.counters.lock();
        let current = counters.hour_counter.get_total();
        Some((max_bph as u64).saturating_sub(current))
    }

    /// Check if throttled (at or near limits)
    pub fn is_throttled(&self) -> bool {
        let server_limit = self.server_throttle.load(Ordering::SeqCst);
        let max_bps = self.max_bytes_per_second.load(Ordering::SeqCst);
        let max_bph = self.max_bytes_per_hour.load(Ordering::SeqCst);

        let mut counters = self.counters.lock();

        // Check if we're at >90% of any limit
        if server_limit > 0 {
            let current = counters.second_counter.get_total();
            if current as f64 > server_limit as f64 * 0.9 {
                return true;
            }
        }

        if max_bps > 0 {
            let current = counters.second_counter.get_total();
            if current as f64 > max_bps as f64 * 0.9 {
                return true;
            }
        }

        if max_bph > 0 {
            let current = counters.hour_counter.get_total();
            if current as f64 > max_bph as f64 * 0.9 {
                return true;
            }
        }

        false
    }

    /// Reset all counters
    pub fn reset(&self) {
        let mut counters = self.counters.lock();
        counters.second_counter = WindowedCounter::new(Duration::from_secs(1));
        counters.minute_counter = WindowedCounter::new(Duration::from_secs(60));
        counters.hour_counter = WindowedCounter::new(Duration::from_secs(3600));
        self.total_bytes.store(0, Ordering::SeqCst);
        self.peak_speed.store(0, Ordering::SeqCst);
        self.requests_served.store(0, Ordering::SeqCst);
    }
}

impl Default for BandwidthThrottler {
    fn default() -> Self {
        Self::unlimited()
    }
}

/// Rate limiter for API requests (prevents hitting server too fast)
pub struct RequestRateLimiter {
    /// Minimum delay between requests
    min_delay: Duration,
    /// Last request time
    last_request: Mutex<Option<Instant>>,
    /// Request counter
    request_count: AtomicU64,
}

impl RequestRateLimiter {
    pub fn new(requests_per_second: f64) -> Self {
        let min_delay = if requests_per_second > 0.0 {
            Duration::from_secs_f64(1.0 / requests_per_second)
        } else {
            Duration::ZERO
        };

        Self {
            min_delay,
            last_request: Mutex::new(None),
            request_count: AtomicU64::new(0),
        }
    }

    /// Wait if necessary and acquire a request slot
    pub async fn acquire(&self) {
        let wait_time = {
            let mut last = self.last_request.lock();
            if let Some(last_time) = *last {
                let elapsed = last_time.elapsed();
                if elapsed < self.min_delay {
                    Some(self.min_delay - elapsed)
                } else {
                    *last = Some(Instant::now());
                    None
                }
            } else {
                *last = Some(Instant::now());
                None
            }
        };

        if let Some(wait) = wait_time {
            sleep(wait).await;
            *self.last_request.lock() = Some(Instant::now());
        }

        self.request_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Get total request count
    pub fn request_count(&self) -> u64 {
        self.request_count.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_throttler_unlimited() {
        let throttler = BandwidthThrottler::unlimited();
        assert!(throttler.can_send(1_000_000));
        throttler.record_transfer(1_000_000);
        assert_eq!(throttler.total_bytes.load(Ordering::SeqCst), 1_000_000);
    }

    #[test]
    fn test_throttler_limited() {
        let throttler = BandwidthThrottler::new(1000, -1); // 1000 bytes/sec
        assert!(throttler.can_send(500));
        throttler.record_transfer(500);
        assert!(throttler.can_send(500));
        throttler.record_transfer(500);
        // Now at limit, should not allow more
        assert!(!throttler.can_send(100));
    }
}
