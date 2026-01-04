//! H@H Download Queue module
//!
//! This module implements the official H@H download queue functionality.
//! When users add galleries to their download queue on the E-Hentai website,
//! this module fetches and downloads them using the H@H network.
//!
//! This is different from the gallery.rs web scraper - this uses the official
//! H@H API and integrates with the website's download queue feature.

use crate::api::{GalleryFileMeta, GalleryMeta, HahApiClient};
use crate::cache::CacheManager;
use crate::config::Config;
use anyhow::{Context, Result};
use reqwest::Client;
use sha1::{Digest, Sha1};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Client version string (matching Java client)
const CLIENT_VERSION: &str = "1.6.4";

/// H@H Download Queue Manager
/// Handles server-managed gallery downloads from the website queue
pub struct HathDownloader {
    config: Arc<Config>,
    api: Arc<HahApiClient>,
    cache: Arc<CacheManager>,
    client: Client,
    download_dir: PathBuf,
    running: AtomicBool,
    downloads_available: AtomicBool,
    files_downloaded: AtomicU64,
    bytes_downloaded: AtomicU64,
}

impl HathDownloader {
    pub fn new(
        config: Arc<Config>,
        api: Arc<HahApiClient>,
        cache: Arc<CacheManager>,
    ) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(300))
            .user_agent(format!("Hentai@Home {}", CLIENT_VERSION))
            .build()?;

        // Create download directory (parallel to cache directory)
        let download_dir = config
            .cache_dir
            .parent()
            .map(|p| p.join("download"))
            .unwrap_or_else(|| config.cache_dir.join("../download"));
        std::fs::create_dir_all(&download_dir)?;

        Ok(Self {
            config,
            api,
            cache,
            client,
            download_dir,
            running: AtomicBool::new(false),
            downloads_available: AtomicBool::new(true),
            files_downloaded: AtomicU64::new(0),
            bytes_downloaded: AtomicU64::new(0),
        })
    }

    /// Get the download directory path
    pub fn get_download_dir(&self) -> &Path {
        &self.download_dir
    }

    /// Check if the downloader is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Start the download loop
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
        self.downloads_available.store(true, Ordering::SeqCst);
        info!("H@H Downloader started");
    }

    /// Stop the download loop
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        info!("H@H Downloader stopped");
    }

    /// Get download statistics
    pub fn get_stats(&self) -> (u64, u64) {
        (
            self.files_downloaded.load(Ordering::SeqCst),
            self.bytes_downloaded.load(Ordering::SeqCst),
        )
    }

    /// Main download loop - processes galleries from the queue
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) {
        info!("H@H Download queue processor started");

        let mut mark_previous: Option<(i32, String)> = None;
        let mut failures: Vec<String> = Vec::new();

        while self.running.load(Ordering::SeqCst) && self.downloads_available.load(Ordering::SeqCst)
        {
            // Check for shutdown
            if shutdown.try_recv().is_ok() {
                info!("H@H Downloader received shutdown signal");
                break;
            }

            // Report any failures from previous gallery
            if !failures.is_empty() {
                let _ = self.api.report_download_failures(&failures).await;
                failures.clear();
            }

            // Fetch next gallery from queue
            let mark_ref = mark_previous
                .as_ref()
                .map(|(gid, xres)| (*gid, xres.as_str()));
            let gallery = match self.api.fetch_download_queue(mark_ref).await {
                Ok(Some(g)) => g,
                Ok(None) => {
                    info!("No pending downloads in queue");
                    self.downloads_available.store(false, Ordering::SeqCst);
                    break;
                }
                Err(e) => {
                    error!("Failed to fetch download queue: {}", e);
                    sleep(Duration::from_secs(60)).await;
                    continue;
                }
            };

            info!(
                "Starting download of gallery: {} (gid={})",
                gallery.title, gallery.gid
            );

            // Process the gallery
            let success = self.download_gallery(&gallery, &mut failures).await;

            if success {
                info!("Finished download of gallery: {}", gallery.title);

                // Write gallery info file
                if let Err(e) = self.write_gallery_info(&gallery).await {
                    warn!("Failed to write gallery info: {}", e);
                }
            } else {
                warn!("Failed to download gallery: {}", gallery.title);
            }

            // Mark for completion on next iteration
            mark_previous = Some((gallery.gid, gallery.minxres.clone()));
        }

        // Final failure report
        if !failures.is_empty() {
            let _ = self.api.report_download_failures(&failures).await;
        }

        self.running.store(false, Ordering::SeqCst);
        info!("H@H Download queue processor finished");
    }

    /// Download a single gallery
    async fn download_gallery(&self, gallery: &GalleryMeta, failures: &mut Vec<String>) -> bool {
        let gallery_dir = self.create_gallery_dir(gallery);

        if let Err(e) = fs::create_dir_all(&gallery_dir).await {
            error!("Failed to create gallery directory: {}", e);
            return false;
        }

        let mut successful_files: i32 = 0;
        let mut total_failed: i32 = 0;
        let max_retries: i32 = 10;
        let max_total_failures = gallery.filecount * 2;

        for retry in 0..max_retries {
            if total_failed >= max_total_failures {
                break;
            }

            for file in &gallery.files {
                if !self.running.load(Ordering::SeqCst) {
                    return false;
                }

                // Check disk space
                if self.is_low_disk_space() {
                    warn!("Low disk space, pausing downloads");
                    sleep(Duration::from_secs(300)).await;
                    continue;
                }

                let result = self
                    .download_gallery_file(gallery, file, &gallery_dir, retry + 1)
                    .await;

                match result {
                    DownloadResult::Success => {
                        successful_files += 1;
                        self.files_downloaded.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_secs(1)).await;
                    }
                    DownloadResult::AlreadyExists => {
                        successful_files += 1;
                    }
                    DownloadResult::Failed(failure_info) => {
                        total_failed += 1;
                        if let Some(info) = failure_info {
                            if !failures.contains(&info) {
                                failures.push(info);
                            }
                        }
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }

            if successful_files >= gallery.filecount {
                return true;
            }
        }

        successful_files >= gallery.filecount
    }

    /// Download a single file from the gallery
    async fn download_gallery_file(
        &self,
        gallery: &GalleryMeta,
        file: &GalleryFileMeta,
        gallery_dir: &Path,
        retry: i32,
    ) -> DownloadResult {
        let file_path = gallery_dir.join(format!("{}.{}", file.filename, file.filetype));

        // Check if file already exists and is valid
        if file_path.exists() {
            if let Some(expected_hash) = &file.sha1hash {
                match self.verify_file_hash(&file_path, expected_hash).await {
                    Ok(true) => {
                        debug!("File already exists and verified: {}", file.filename);
                        return DownloadResult::AlreadyExists;
                    }
                    Ok(false) => {
                        // Hash mismatch, delete and re-download
                        let _ = fs::remove_file(&file_path).await;
                    }
                    Err(_) => {
                        // Can't verify, try to re-download
                        let _ = fs::remove_file(&file_path).await;
                    }
                }
            } else {
                // No hash to verify, assume it's good
                if fs::metadata(&file_path)
                    .await
                    .map(|m| m.len() > 0)
                    .unwrap_or(false)
                {
                    return DownloadResult::AlreadyExists;
                }
            }
        }

        // Get download URL from server
        let url = match self
            .api
            .get_downloader_fetch_url(gallery.gid, file.page, file.fileindex, &file.xres, retry)
            .await
        {
            Ok(Some(u)) => u,
            Ok(None) => {
                warn!("No download URL for file: {}", file.filename);
                return DownloadResult::Failed(None);
            }
            Err(e) => {
                error!("Failed to get download URL: {}", e);
                return DownloadResult::Failed(None);
            }
        };

        // Download the file
        debug!("Downloading: {} from {}", file.filename, url);

        let response = match self
            .client
            .get(&url)
            .header(
                "Hath-Request",
                format!(
                    "{}-{}",
                    self.config.client_id,
                    self.generate_hath_request_header(&format!(
                        "{}-{}-{}-{}",
                        gallery.gid, file.page, file.fileindex, file.xres
                    ))
                ),
            )
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                error!("Download request failed: {}", e);
                return DownloadResult::Failed(self.extract_failure_info(&url, file));
            }
        };

        if !response.status().is_success() {
            return DownloadResult::Failed(self.extract_failure_info(&url, file));
        }

        let bytes = match response.bytes().await {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to read response: {}", e);
                return DownloadResult::Failed(self.extract_failure_info(&url, file));
            }
        };

        // Verify hash if available
        if let Some(expected_hash) = &file.sha1hash {
            let mut hasher = Sha1::new();
            hasher.update(&bytes);
            let actual_hash = hex::encode(hasher.finalize());

            if !actual_hash.eq_ignore_ascii_case(expected_hash) {
                warn!(
                    "Hash mismatch for {}: expected {}, got {}",
                    file.filename, expected_hash, actual_hash
                );
                return DownloadResult::Failed(self.extract_failure_info(&url, file));
            }
            debug!(
                "Verified SHA-1 hash for {}: {}",
                file.filename, expected_hash
            );
        }

        // Write to file
        let mut output = match fs::File::create(&file_path).await {
            Ok(f) => f,
            Err(e) => {
                error!("Failed to create file {}: {}", file_path.display(), e);
                return DownloadResult::Failed(None);
            }
        };

        if let Err(e) = output.write_all(&bytes).await {
            error!("Failed to write file {}: {}", file_path.display(), e);
            let _ = fs::remove_file(&file_path).await;
            return DownloadResult::Failed(None);
        }

        self.bytes_downloaded
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        info!(
            "Downloaded: gid={} page={}: {}.{}",
            gallery.gid, file.page, file.filename, file.filetype
        );

        // Also store in cache if it's in our static range
        let hash = if let Some(h) = &file.sha1hash {
            h.clone()
        } else {
            let mut hasher = Sha1::new();
            hasher.update(&bytes);
            hex::encode(hasher.finalize())
        };

        if self.api.is_in_static_range(&hash) {
            if let Err(e) = self.cache.store_file(&hash, &bytes, &file.filetype).await {
                debug!("Failed to cache file: {}", e);
            }
        }

        DownloadResult::Success
    }

    /// Create the gallery directory with proper naming
    fn create_gallery_dir(&self, gallery: &GalleryMeta) -> PathBuf {
        let xres_suffix = if gallery.minxres == "org" {
            String::new()
        } else {
            format!("-{}x", gallery.minxres)
        };

        let postfix = format!(" [{}{}]", gallery.gid, xres_suffix);
        let max_title_len = 125 - postfix.len() - 3; // Reserve space for "..."

        let title = if gallery.title.len() > max_title_len {
            format!("{}...{}", &gallery.title[..max_title_len], postfix)
        } else {
            format!("{}{}", gallery.title, postfix)
        };

        self.download_dir.join(title)
    }

    /// Write gallery info file
    async fn write_gallery_info(&self, gallery: &GalleryMeta) -> Result<()> {
        let gallery_dir = self.create_gallery_dir(gallery);
        let info_path = gallery_dir.join("galleryinfo.txt");

        let mut content = format!(
            "Title: {}\nGID: {}\nFiles: {}\nResolution: {}\n\n",
            gallery.title, gallery.gid, gallery.filecount, gallery.minxres
        );
        content.push_str(&gallery.information);

        fs::write(&info_path, content)
            .await
            .context("Failed to write galleryinfo.txt")?;
        Ok(())
    }

    /// Verify file hash
    async fn verify_file_hash(&self, path: &Path, expected: &str) -> Result<bool> {
        let data = fs::read(path).await?;
        let mut hasher = Sha1::new();
        hasher.update(&data);
        let actual = hex::encode(hasher.finalize());
        Ok(actual.eq_ignore_ascii_case(expected))
    }

    /// Check if disk space is low
    fn is_low_disk_space(&self) -> bool {
        // Use fs2 crate functionality via std
        // Just check if we can write a small test file as a simple heuristic
        // For more accurate checking, would need platform-specific APIs
        let min_bytes = self.config.min_free_space_bytes + 1024 * 1024 * 1024; // +1GB buffer

        // Try to get available space using available_space from std (nightly) or estimate
        // For now, just return false as a simple implementation
        // Real implementation would use platform APIs
        let _ = min_bytes; // Suppress warning
        false
    }

    /// Generate Hath-Request header value
    fn generate_hath_request_header(&self, data: &str) -> String {
        let mut hasher = Sha1::new();
        hasher.update(format!("{}{}", self.config.client_key, data).as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Extract failure info for reporting
    fn extract_failure_info(&self, url: &str, file: &GalleryFileMeta) -> Option<String> {
        // Format: host-fileindex-xres
        if let Ok(parsed) = url::Url::parse(url) {
            if let Some(host) = parsed.host_str() {
                return Some(format!("{}-{}-{}", host, file.fileindex, file.xres));
            }
        }
        None
    }
}

#[derive(Debug)]
enum DownloadResult {
    Success,
    AlreadyExists,
    Failed(Option<String>),
}

/// Run the H@H download queue processor
pub async fn run_hath_downloader(
    downloader: Arc<HathDownloader>,
    mut shutdown: broadcast::Receiver<()>,
) {
    // Wait for explicit start command or auto-start based on config
    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                info!("H@H Downloader shutdown requested");
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                if downloader.is_running() {
                    // Create a new receiver for the inner run loop
                    let inner_shutdown = shutdown.resubscribe();
                    downloader.run(inner_shutdown).await;
                }
            }
        }
    }
}
