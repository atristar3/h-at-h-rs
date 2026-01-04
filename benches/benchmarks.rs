//! Benchmarks for H@H-rs components
//!
//! Run with: cargo bench

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// SHA-1 Hashing Benchmarks (critical for file verification)
// ============================================================================

fn bench_sha1_hashing(c: &mut Criterion) {
    let mut group = c.benchmark_group("sha1_hashing");

    // Test different file sizes
    let sizes = [
        (1024, "1KB"),
        (10 * 1024, "10KB"),
        (100 * 1024, "100KB"),
        (1024 * 1024, "1MB"),
        (10 * 1024 * 1024, "10MB"),
    ];

    for (size, name) in sizes.iter() {
        let data: Vec<u8> = (0..*size).map(|i| (i % 256) as u8).collect();
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::new("sha1", name), &data, |b, data| {
            b.iter(|| {
                let mut hasher = Sha1::new();
                hasher.update(black_box(data));
                let result = hasher.finalize();
                black_box(hex::encode(result))
            });
        });
    }

    group.finish();
}

// ============================================================================
// Keystamp Verification Benchmarks
// ============================================================================

fn generate_keystamp(timestamp: i64, file_id: &str, client_key: &str) -> String {
    let input = format!("{}-{}-{}-hotlinkthis", timestamp, file_id, client_key);
    let mut hasher = Sha1::new();
    hasher.update(input.as_bytes());
    let hash = hex::encode(hasher.finalize());
    format!("{}-{}", timestamp, &hash[..10])
}

fn verify_keystamp(keystamp: &str, file_id: &str, client_key: &str, server_time: i64) -> bool {
    let parts: Vec<&str> = keystamp.split('-').collect();
    if parts.len() < 2 {
        return false;
    }

    let timestamp: i64 = parts[0].parse().unwrap_or(0);
    let key = parts[1];

    // Check time window (15 minutes)
    if (server_time - timestamp).abs() > 900 {
        return false;
    }

    // Verify hash
    let expected_input = format!("{}-{}-{}-hotlinkthis", timestamp, file_id, client_key);
    let mut hasher = Sha1::new();
    hasher.update(expected_input.as_bytes());
    let full_hash = hex::encode(hasher.finalize());
    let expected_key = &full_hash[..10];

    key.eq_ignore_ascii_case(expected_key)
}

fn bench_keystamp_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("keystamp");

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let file_id = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
    let client_key = "test_client_key_12345";

    group.bench_function("generate", |b| {
        b.iter(|| {
            generate_keystamp(
                black_box(timestamp),
                black_box(file_id),
                black_box(client_key),
            )
        });
    });

    let keystamp = generate_keystamp(timestamp, file_id, client_key);

    group.bench_function("verify_valid", |b| {
        b.iter(|| {
            verify_keystamp(
                black_box(&keystamp),
                black_box(file_id),
                black_box(client_key),
                black_box(timestamp),
            )
        });
    });

    group.bench_function("verify_invalid", |b| {
        b.iter(|| {
            verify_keystamp(
                black_box("0000000000-invalidkey"),
                black_box(file_id),
                black_box(client_key),
                black_box(timestamp),
            )
        });
    });

    group.finish();
}

// ============================================================================
// Flood Control Benchmarks
// ============================================================================

struct FloodControlEntry {
    connect_count: AtomicI64,
    last_connect: AtomicI64,
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

    fn hit(&self, now: i64) -> bool {
        let last = self.last_connect.swap(now, Ordering::SeqCst);
        let elapsed = now - last;

        // Decay count based on elapsed time
        if elapsed > 1000 {
            self.connect_count.store(1, Ordering::SeqCst);
        } else {
            self.connect_count.fetch_add(1, Ordering::SeqCst);
        }

        // Check if blocked
        if self.connect_count.load(Ordering::SeqCst) > 10 {
            self.block_until.store(now + 60000, Ordering::SeqCst);
            return false;
        }

        true
    }

    fn is_blocked(&self, now: i64) -> bool {
        self.block_until.load(Ordering::SeqCst) > now
    }
}

fn bench_flood_control(c: &mut Criterion) {
    let mut group = c.benchmark_group("flood_control");

    let entry = FloodControlEntry::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    group.bench_function("hit", |b| {
        b.iter(|| entry.hit(black_box(now)));
    });

    group.bench_function("is_blocked", |b| {
        b.iter(|| entry.is_blocked(black_box(now)));
    });

    // Benchmark with HashMap lookup (simulating real flood control)
    let mut flood_map: HashMap<String, FloodControlEntry> = HashMap::new();
    for i in 0..1000 {
        flood_map.insert(format!("192.168.1.{}", i % 256), FloodControlEntry::new());
    }

    group.bench_function("hashmap_lookup_and_hit", |b| {
        let ip = "192.168.1.100";
        b.iter(|| {
            if let Some(entry) = flood_map.get(black_box(ip)) {
                entry.hit(black_box(now))
            } else {
                true
            }
        });
    });

    group.finish();
}

// ============================================================================
// File ID Parsing Benchmarks
// ============================================================================

fn parse_file_id(file_id: &str) -> Option<(&str, u64, &str)> {
    // Format: {hash}-{size}-{type}
    // Example: a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2-12345-jpg
    let parts: Vec<&str> = file_id.split('-').collect();
    if parts.len() != 3 {
        return None;
    }

    let hash = parts[0];
    let size: u64 = parts[1].parse().ok()?;
    let file_type = parts[2];

    // Validate hash (40 hex chars for SHA-1)
    if hash.len() != 40 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }

    Some((hash, size, file_type))
}

fn bench_file_id_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_id_parsing");

    let valid_id = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2-12345-jpg";
    let invalid_id = "not-a-valid-id";

    group.bench_function("parse_valid", |b| {
        b.iter(|| parse_file_id(black_box(valid_id)));
    });

    group.bench_function("parse_invalid", |b| {
        b.iter(|| parse_file_id(black_box(invalid_id)));
    });

    group.finish();
}

// ============================================================================
// Static Range Lookup Benchmarks
// ============================================================================

fn is_in_static_range(hash: &str, ranges: &[String]) -> bool {
    if hash.len() < 4 {
        return false;
    }
    let prefix = &hash[..4];
    ranges.iter().any(|r| r == prefix)
}

fn is_in_static_range_hashset(hash: &str, ranges: &std::collections::HashSet<String>) -> bool {
    if hash.len() < 4 {
        return false;
    }
    let prefix = &hash[..4];
    ranges.contains(prefix)
}

fn bench_static_range_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("static_range_lookup");

    // Generate sample ranges (typical client might have 100-500 ranges)
    let ranges_vec: Vec<String> = (0..256).map(|i| format!("{:04x}", i * 16)).collect();

    let ranges_set: std::collections::HashSet<String> = ranges_vec.iter().cloned().collect();

    let hash_in_range = "00f0a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6";
    let hash_not_in_range = "ffffa1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6";

    group.bench_function("vec_lookup_hit", |b| {
        b.iter(|| is_in_static_range(black_box(hash_in_range), black_box(&ranges_vec)));
    });

    group.bench_function("vec_lookup_miss", |b| {
        b.iter(|| is_in_static_range(black_box(hash_not_in_range), black_box(&ranges_vec)));
    });

    group.bench_function("hashset_lookup_hit", |b| {
        b.iter(|| is_in_static_range_hashset(black_box(hash_in_range), black_box(&ranges_set)));
    });

    group.bench_function("hashset_lookup_miss", |b| {
        b.iter(|| is_in_static_range_hashset(black_box(hash_not_in_range), black_box(&ranges_set)));
    });

    group.finish();
}

// ============================================================================
// Authentication Hash Benchmarks
// ============================================================================

fn generate_auth_hash(
    action: &str,
    additional: &str,
    client_id: i32,
    client_key: &str,
    time: i64,
) -> String {
    let hash_input = format!(
        "hentai@home-{}-{}-{}-{}-{}",
        client_key, action, additional, client_id, time
    );

    let mut hasher = Sha1::new();
    hasher.update(hash_input.as_bytes());
    hex::encode(hasher.finalize())
}

fn bench_auth_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("auth_hash");

    let client_id = 12345;
    let client_key = "my_secret_client_key_abc123";
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    group.bench_function("client_login", |b| {
        b.iter(|| {
            generate_auth_hash(
                black_box("client_login"),
                black_box(""),
                black_box(client_id),
                black_box(client_key),
                black_box(time),
            )
        });
    });

    group.bench_function("still_alive", |b| {
        b.iter(|| {
            generate_auth_hash(
                black_box("still_alive"),
                black_box(""),
                black_box(client_id),
                black_box(client_key),
                black_box(time),
            )
        });
    });

    group.bench_function("with_additional", |b| {
        b.iter(|| {
            generate_auth_hash(
                black_box("srfetch"),
                black_box("1234;5;0;org;1"),
                black_box(client_id),
                black_box(client_key),
                black_box(time),
            )
        });
    });

    group.finish();
}

// ============================================================================
// Bandwidth Throttle Calculation Benchmarks
// ============================================================================

struct BandwidthTracker {
    bytes_this_second: AtomicU64,
    bytes_this_hour: AtomicU64,
    last_second: AtomicI64,
    last_hour: AtomicI64,
    max_bps: i64,
    max_bph: i64,
}

impl BandwidthTracker {
    fn new(max_bps: i64, max_bph: i64) -> Self {
        Self {
            bytes_this_second: AtomicU64::new(0),
            bytes_this_hour: AtomicU64::new(0),
            last_second: AtomicI64::new(0),
            last_hour: AtomicI64::new(0),
            max_bps,
            max_bph,
        }
    }

    fn can_send(&self, bytes: u64, now: i64) -> bool {
        let current_second = now;
        let current_hour = now / 3600;

        // Reset counters if needed
        if self.last_second.load(Ordering::SeqCst) != current_second {
            self.bytes_this_second.store(0, Ordering::SeqCst);
            self.last_second.store(current_second, Ordering::SeqCst);
        }

        if self.last_hour.load(Ordering::SeqCst) != current_hour {
            self.bytes_this_hour.store(0, Ordering::SeqCst);
            self.last_hour.store(current_hour, Ordering::SeqCst);
        }

        // Check limits
        if self.max_bps > 0
            && (self.bytes_this_second.load(Ordering::SeqCst) + bytes) as i64 > self.max_bps
        {
            return false;
        }

        if self.max_bph > 0
            && (self.bytes_this_hour.load(Ordering::SeqCst) + bytes) as i64 > self.max_bph
        {
            return false;
        }

        true
    }

    fn record(&self, bytes: u64) {
        self.bytes_this_second.fetch_add(bytes, Ordering::SeqCst);
        self.bytes_this_hour.fetch_add(bytes, Ordering::SeqCst);
    }
}

fn bench_bandwidth_throttle(c: &mut Criterion) {
    let mut group = c.benchmark_group("bandwidth_throttle");

    let tracker = BandwidthTracker::new(10_000_000, 100_000_000_000); // 10MB/s, 100GB/h
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    group.bench_function("can_send_check", |b| {
        b.iter(|| tracker.can_send(black_box(65536), black_box(now)));
    });

    group.bench_function("record_bytes", |b| {
        b.iter(|| tracker.record(black_box(65536)));
    });

    group.bench_function("full_throttle_cycle", |b| {
        b.iter(|| {
            if tracker.can_send(black_box(65536), black_box(now)) {
                tracker.record(black_box(65536));
                true
            } else {
                false
            }
        });
    });

    group.finish();
}

// ============================================================================
// Main Benchmark Groups
// ============================================================================

criterion_group!(
    benches,
    bench_sha1_hashing,
    bench_keystamp_operations,
    bench_flood_control,
    bench_file_id_parsing,
    bench_static_range_lookup,
    bench_auth_hash,
    bench_bandwidth_throttle,
);

criterion_main!(benches);
