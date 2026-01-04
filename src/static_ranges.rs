//! Static range management module
//!
//! Handles static range assignments from the H@H server. Static ranges are
//! portions of the image hash space that this client is responsible for caching
//! and serving. This is a core H@H protocol feature.

use crate::api::HahApiClient;
use crate::cache::CacheManager;
use crate::config::Config;
use anyhow::Result;
use parking_lot::RwLock;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

#[derive(Error, Debug)]
pub enum StaticRangeError {
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("API error: {0}")]
    Api(String),
    #[error("Invalid range format: {0}")]
    InvalidFormat(String),
    #[error("Download failed: {0}")]
    DownloadFailed(String),
}

/// Represents a static range assignment
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StaticRange {
    /// The range prefix (e.g., "ab", "abc")
    pub prefix: String,
    /// Whether this is a high-capacity range (for larger files)
    pub high_capacity: bool,
    /// Number of files expected in this range
    pub expected_files: u32,
    /// Priority level (higher = more important)
    pub priority: u8,
}

/// File to be downloaded for a static range
#[derive(Debug, Clone)]
pub struct StaticRangeFile {
    pub hash: String,
    pub size: u64,
    pub file_type: String,
    pub url: String,
}

/// Statistics for static range operations
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StaticRangeStats {
    pub total_ranges: usize,
    pub total_files: u64,
    pub files_downloaded: u64,
    pub files_pending: u64,
    pub bytes_downloaded: u64,
    pub download_errors: u64,
    pub last_update: i64,
}

pub struct StaticRangeManager {
    config: Arc<Config>,
    cache: Arc<CacheManager>,
    api: Arc<HahApiClient>,
    client: Client,
    /// Currently assigned static ranges
    assigned_ranges: RwLock<HashSet<StaticRange>>,
    /// Files pending download for static ranges
    pending_files: RwLock<Vec<StaticRangeFile>>,
    /// Statistics
    stats: RwLock<StaticRangeStats>,
    /// Whether downloads are enabled
    downloads_enabled: AtomicBool,
    /// Number of concurrent download workers
    download_workers: usize,
    /// Channel for queuing file downloads
    download_tx: Option<mpsc::Sender<StaticRangeFile>>,
}

impl StaticRangeManager {
    pub async fn new(
        config: Arc<Config>,
        cache: Arc<CacheManager>,
        api: Arc<HahApiClient>,
    ) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.request_timeout))
            .user_agent(format!("H@H-rs/{}", env!("CARGO_PKG_VERSION")))
            .build()?;

        let download_workers = config.download_workers.max(1);

        let manager = Self {
            config,
            cache,
            api,
            client,
            assigned_ranges: RwLock::new(HashSet::new()),
            pending_files: RwLock::new(Vec::new()),
            stats: RwLock::new(StaticRangeStats::default()),
            downloads_enabled: AtomicBool::new(true),
            download_workers,
            download_tx: None,
        };

        Ok(manager)
    }

    /// Start the static range download workers
    pub fn start_workers(self: &Arc<Self>) -> mpsc::Sender<StaticRangeFile> {
        let (tx, rx) = mpsc::channel::<StaticRangeFile>(1000);

        let manager = Arc::clone(self);
        tokio::spawn(async move {
            manager.run_download_workers(rx).await;
        });

        tx
    }

    /// Fetch static range assignments from the server
    pub async fn fetch_ranges(&self) -> Result<Vec<StaticRange>, StaticRangeError> {
        info!("Fetching static range assignments from server...");

        let settings = self.api.get_settings();
        if settings.request_server.is_empty() {
            return Ok(Vec::new());
        }

        // The server returns a list of hash prefixes this client should cache
        // Format: "prefix;priority;hc" where hc=1 means high-capacity
        let ranges_data = self
            .api
            .get_static_range()
            .await
            .map_err(|e| StaticRangeError::Api(e.to_string()))?;

        let mut ranges = Vec::new();
        for line in ranges_data {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = line.split(';').collect();
            if parts.is_empty() {
                continue;
            }

            let prefix = parts[0].to_lowercase();
            let priority = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(5);
            let high_capacity = parts.get(2).map(|hc| *hc == "1").unwrap_or(false);

            ranges.push(StaticRange {
                prefix,
                high_capacity,
                expected_files: 0,
                priority,
            });
        }

        // Update assigned ranges
        {
            let mut assigned = self.assigned_ranges.write();
            assigned.clear();
            for range in &ranges {
                assigned.insert(range.clone());
            }
        }

        {
            let mut stats = self.stats.write();
            stats.total_ranges = ranges.len();
            stats.last_update = chrono::Utc::now().timestamp();
        }

        info!("Received {} static range assignments", ranges.len());
        Ok(ranges)
    }

    /// Check if a file hash belongs to any of our assigned ranges
    pub fn is_in_assigned_range(&self, hash: &str) -> bool {
        let ranges = self.assigned_ranges.read();
        let hash_lower = hash.to_lowercase();

        for range in ranges.iter() {
            if hash_lower.starts_with(&range.prefix) {
                return true;
            }
        }

        false
    }

    /// Fetch list of files to download for static ranges
    pub async fn fetch_files_to_download(&self) -> Result<Vec<StaticRangeFile>, StaticRangeError> {
        let settings = self.api.get_settings();
        if settings.request_server.is_empty() {
            return Ok(Vec::new());
        }

        // Request file list from server
        // The server returns files that need to be downloaded for our assigned ranges
        let url = format!(
            "{}/servercmd?cmd=srfetch&cid={}",
            settings.request_server, self.config.client_id
        );

        let response = self.client.get(&url).send().await?;

        let text = response.text().await?;
        let mut files = Vec::new();

        for line in text.lines() {
            if line.is_empty() || line.starts_with('#') || line == "OK" {
                continue;
            }

            // Format: hash;size;type;url
            let parts: Vec<&str> = line.split(';').collect();
            if parts.len() >= 4 {
                files.push(StaticRangeFile {
                    hash: parts[0].to_string(),
                    size: parts[1].parse().unwrap_or(0),
                    file_type: parts[2].to_string(),
                    url: parts[3].to_string(),
                });
            }
        }

        // Update pending files
        {
            let mut pending = self.pending_files.write();
            *pending = files.clone();
        }

        {
            let mut stats = self.stats.write();
            stats.files_pending = files.len() as u64;
        }

        info!(
            "Received {} files to download for static ranges",
            files.len()
        );
        Ok(files)
    }

    /// Queue files for download
    pub async fn queue_downloads(
        &self,
        files: Vec<StaticRangeFile>,
        tx: &mpsc::Sender<StaticRangeFile>,
    ) {
        for file in files {
            // Skip if already in cache
            if self.cache.has_file(&file.hash) {
                debug!("File {} already in cache, skipping", file.hash);
                continue;
            }

            if let Err(e) = tx.send(file.clone()).await {
                error!("Failed to queue file for download: {}", e);
            }
        }
    }

    /// Background download workers
    async fn run_download_workers(self: Arc<Self>, mut rx: mpsc::Receiver<StaticRangeFile>) {
        info!(
            "Starting {} static range download workers",
            self.download_workers
        );

        while let Some(file) = rx.recv().await {
            if !self.downloads_enabled.load(Ordering::SeqCst) {
                debug!("Downloads disabled, skipping file {}", file.hash);
                continue;
            }

            let manager = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = manager.download_file(&file).await {
                    error!("Failed to download static range file {}: {}", file.hash, e);
                    let mut stats = manager.stats.write();
                    stats.download_errors += 1;
                }
            });
        }
    }

    /// Download a single file for static range
    async fn download_file(&self, file: &StaticRangeFile) -> Result<(), StaticRangeError> {
        debug!(
            "Downloading static range file: {} ({})",
            file.hash, file.size
        );

        let response = self.client.get(&file.url).send().await?;

        if !response.status().is_success() {
            return Err(StaticRangeError::DownloadFailed(format!(
                "HTTP {}",
                response.status()
            )));
        }

        let bytes = response.bytes().await?.to_vec();

        // Verify hash
        let actual_hash = {
            let mut hasher = Sha1::new();
            hasher.update(&bytes);
            hex::encode(hasher.finalize())
        };

        if actual_hash != file.hash {
            return Err(StaticRangeError::DownloadFailed(format!(
                "Hash mismatch: expected {}, got {}",
                file.hash, actual_hash
            )));
        }

        // Store in cache
        self.cache
            .store_file(&file.hash, &bytes, &file.file_type)
            .await
            .map_err(|e| StaticRangeError::DownloadFailed(e.to_string()))?;

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.files_downloaded += 1;
            stats.bytes_downloaded += bytes.len() as u64;
            if stats.files_pending > 0 {
                stats.files_pending -= 1;
            }
        }

        // Report download to server
        let _ = self
            .api
            .downloaded_files(std::slice::from_ref(&file.hash))
            .await;

        debug!("Successfully downloaded static range file: {}", file.hash);
        Ok(())
    }

    /// Enable/disable static range downloads
    pub fn set_downloads_enabled(&self, enabled: bool) {
        self.downloads_enabled.store(enabled, Ordering::SeqCst);
        info!(
            "Static range downloads {}",
            if enabled { "enabled" } else { "disabled" }
        );
    }

    /// Get current statistics
    pub fn get_stats(&self) -> StaticRangeStats {
        self.stats.read().clone()
    }

    /// Get assigned ranges
    pub fn get_assigned_ranges(&self) -> Vec<StaticRange> {
        self.assigned_ranges.read().iter().cloned().collect()
    }

    /// Clear all assigned ranges (for shutdown)
    pub fn clear_ranges(&self) {
        self.assigned_ranges.write().clear();
        self.pending_files.write().clear();
    }
}

/// Background task to periodically refresh static ranges
pub async fn run_static_range_refresh(
    manager: Arc<StaticRangeManager>,
    download_tx: mpsc::Sender<StaticRangeFile>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    // Initial delay before first fetch
    tokio::time::sleep(Duration::from_secs(30)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Refresh every hour

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Fetch assigned ranges
                match manager.fetch_ranges().await {
                    Ok(ranges) => {
                        if !ranges.is_empty() {
                            // Fetch files to download
                            match manager.fetch_files_to_download().await {
                                Ok(files) => {
                                    manager.queue_downloads(files, &download_tx).await;
                                }
                                Err(e) => {
                                    warn!("Failed to fetch static range files: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to fetch static ranges: {}", e);
                    }
                }
            }
            _ = shutdown.recv() => {
                info!("Stopping static range refresh task");
                break;
            }
        }
    }
}
