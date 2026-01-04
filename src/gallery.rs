//! Gallery downloader module
//!
//! Handles downloading gallery images while browsing the site.
//! This feature allows pre-fetching images from galleries you're viewing.

use crate::cache::CacheManager;
use crate::config::Config;
use anyhow::{Context, Result};
use regex::Regex;
use reqwest::{Client, Url, cookie::Jar};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

#[derive(Error, Debug)]
pub enum GalleryError {
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Gallery not found: {0}")]
    NotFound(String),
    #[error("Access denied - login required")]
    AccessDenied,
    #[error("Rate limited")]
    RateLimited,
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Gallery {
    pub gid: String,
    pub token: String,
    pub title: String,
    pub title_jpn: Option<String>,
    pub category: String,
    pub thumb: String,
    pub uploader: Option<String>,
    pub posted: String,
    pub filecount: u32,
    pub filesize: u64,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GalleryImage {
    pub page_num: u32,
    pub url: String,
    pub file_hash: Option<String>,
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadRequest {
    pub gallery_id: String,
    pub gallery_token: String,
    pub start_page: Option<u32>,
    pub end_page: Option<u32>,
    pub priority: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DownloadStatus {
    Pending,
    Downloading,
    Completed,
    Failed,
    Paused,
}

impl std::fmt::Display for DownloadStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadStatus::Pending => write!(f, "pending"),
            DownloadStatus::Downloading => write!(f, "downloading"),
            DownloadStatus::Completed => write!(f, "completed"),
            DownloadStatus::Failed => write!(f, "failed"),
            DownloadStatus::Paused => write!(f, "paused"),
        }
    }
}

pub struct GalleryDownloader {
    config: Arc<Config>,
    cache: Arc<CacheManager>,
    client: Client,
    download_tx: mpsc::Sender<DownloadRequest>,
    active_downloads: parking_lot::RwLock<HashSet<String>>,
}

impl GalleryDownloader {
    pub async fn new(config: Arc<Config>, cache: Arc<CacheManager>) -> Result<Self> {
        // Build client with cookies if ExHentai access is configured
        let client = Self::build_client(&config)?;

        // Create download queue channel
        let (tx, rx) = mpsc::channel::<DownloadRequest>(100);

        let downloader = Self {
            config: config.clone(),
            cache: cache.clone(),
            client,
            download_tx: tx,
            active_downloads: parking_lot::RwLock::new(HashSet::new()),
        };

        // Start background download workers
        let workers = config.download_workers;
        let cache_for_workers = cache.clone();
        let config_for_workers = config.clone();

        tokio::spawn(async move {
            Self::run_download_workers(rx, cache_for_workers, config_for_workers, workers).await;
        });

        Ok(downloader)
    }

    fn build_client(config: &Config) -> Result<Client> {
        let mut builder = Client::builder()
            .timeout(Duration::from_secs(config.request_timeout))
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .cookie_store(true);

        if let Some(cookies) = &config.exhentai_cookies {
            let jar = Jar::default();
            let eh_url = "https://exhentai.org".parse::<Url>().unwrap();
            let e_url = "https://e-hentai.org".parse::<Url>().unwrap();

            // Set cookies for both domains
            jar.add_cookie_str(&format!("ipb_member_id={}", cookies.member_id), &eh_url);
            jar.add_cookie_str(&format!("ipb_pass_hash={}", cookies.pass_hash), &eh_url);
            if let Some(igneous) = &cookies.igneous {
                jar.add_cookie_str(&format!("igneous={}", igneous), &eh_url);
            }

            jar.add_cookie_str(&format!("ipb_member_id={}", cookies.member_id), &e_url);
            jar.add_cookie_str(&format!("ipb_pass_hash={}", cookies.pass_hash), &e_url);

            builder = builder.cookie_provider(Arc::new(jar));
        }

        Ok(builder.build()?)
    }

    /// Queue a gallery for download
    pub async fn queue_gallery(&self, request: DownloadRequest) -> Result<(), GalleryError> {
        let gallery_key = format!("{}_{}", request.gallery_id, request.gallery_token);

        // Check if already downloading
        {
            let active = self.active_downloads.read();
            if active.contains(&gallery_key) {
                debug!("Gallery {} already in download queue", gallery_key);
                return Ok(());
            }
        }

        // Add to database
        let db = self.cache.get_db();
        let now = chrono::Utc::now().timestamp();

        sqlx::query(
            r#"
            INSERT OR IGNORE INTO gallery_downloads
            (gallery_id, gallery_token, status, created_at, updated_at)
            VALUES (?, ?, 'pending', ?, ?)
            "#,
        )
        .bind(&request.gallery_id)
        .bind(&request.gallery_token)
        .bind(now)
        .bind(now)
        .execute(&db)
        .await?;

        // Send to download queue
        self.download_tx
            .send(request)
            .await
            .map_err(|e| GalleryError::Parse(e.to_string()))?;

        Ok(())
    }

    /// Fetch gallery metadata from E-Hentai/ExHentai API
    pub async fn fetch_gallery_info(
        &self,
        gallery_id: &str,
        gallery_token: &str,
    ) -> Result<Gallery, GalleryError> {
        let api_url = format!("{}/api.php", self.config.gallery_api);

        let request_body = serde_json::json!({
            "method": "gdata",
            "gidlist": [[gallery_id.parse::<i64>().unwrap_or(0), gallery_token]],
            "namespace": 1
        });

        let response = self
            .client
            .post(&api_url)
            .json(&request_body)
            .send()
            .await?;

        if response.status() == 403 {
            return Err(GalleryError::AccessDenied);
        }

        let data: serde_json::Value = response.json().await?;

        // Parse response
        let gmetadata = data
            .get("gmetadata")
            .and_then(|g| g.as_array())
            .and_then(|arr| arr.first())
            .ok_or_else(|| GalleryError::NotFound(gallery_id.to_string()))?;

        Ok(Gallery {
            gid: gmetadata
                .get("gid")
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
                .to_string(),
            token: gmetadata
                .get("token")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            title: gmetadata
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            title_jpn: gmetadata
                .get("title_jpn")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            category: gmetadata
                .get("category")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            thumb: gmetadata
                .get("thumb")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            uploader: gmetadata
                .get("uploader")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            posted: gmetadata
                .get("posted")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            filecount: gmetadata
                .get("filecount")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            filesize: gmetadata
                .get("filesize")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            tags: gmetadata
                .get("tags")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|t| t.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default(),
        })
    }

    /// Parse gallery page to get image URLs
    pub async fn fetch_gallery_pages(
        &self,
        gallery_id: &str,
        gallery_token: &str,
        page: u32,
    ) -> Result<Vec<GalleryImage>, GalleryError> {
        let base_url = if self.config.has_exhentai_access() {
            "https://exhentai.org"
        } else {
            "https://e-hentai.org"
        };

        let url = format!(
            "{}/g/{}/{}/?p={}",
            base_url, gallery_id, gallery_token, page
        );

        let response = self.client.get(&url).send().await?;

        if response.status() == 403 || response.status() == 404 {
            return Err(GalleryError::NotFound(gallery_id.to_string()));
        }

        let html = response.text().await?;
        let document = Html::parse_document(&html);

        // Check for sad panda (access denied)
        if html.contains("Your IP address has been") || html.len() < 1000 {
            return Err(GalleryError::AccessDenied);
        }

        let mut images = Vec::new();

        // Parse image links
        let link_selector = Selector::parse("div.gdtm a, div.gdtl a")
            .map_err(|e| GalleryError::Parse(e.to_string()))?;

        let page_num_regex = Regex::new(r"/s/([a-f0-9]+)/(\d+)-(\d+)").unwrap();

        for (idx, element) in document.select(&link_selector).enumerate() {
            if let Some(href) = element.value().attr("href") {
                if let Some(caps) = page_num_regex.captures(href) {
                    let page_token = caps.get(1).map(|m| m.as_str()).unwrap_or("");
                    let _gid = caps.get(2).map(|m| m.as_str()).unwrap_or("");
                    let page_num: u32 = caps
                        .get(3)
                        .and_then(|m| m.as_str().parse().ok())
                        .unwrap_or(idx as u32 + 1);

                    images.push(GalleryImage {
                        page_num,
                        url: href.to_string(),
                        file_hash: Some(page_token.to_string()),
                        width: 0,
                        height: 0,
                    });
                }
            }
        }

        Ok(images)
    }

    /// Fetch actual image URL from page viewer
    pub async fn fetch_image_url(&self, page_url: &str) -> Result<String, GalleryError> {
        // Add rate limiting delay
        sleep(Duration::from_millis(500)).await;

        let response = self.client.get(page_url).send().await?;

        if response.status() == 509 {
            return Err(GalleryError::RateLimited);
        }

        let html = response.text().await?;
        let document = Html::parse_document(&html);

        // Find the actual image URL
        let img_selector =
            Selector::parse("img#img").map_err(|e| GalleryError::Parse(e.to_string()))?;

        if let Some(img) = document.select(&img_selector).next() {
            if let Some(src) = img.value().attr("src") {
                return Ok(src.to_string());
            }
        }

        // Try to find in script tags (for original images)
        let nl_regex = Regex::new(r#"nl\('([^']+)'\)"#).unwrap();
        if let Some(caps) = nl_regex.captures(&html) {
            if let Some(nl_param) = caps.get(1) {
                // Construct original image URL
                let orig_url = format!("{}?nl={}", page_url, nl_param.as_str());
                return Ok(orig_url);
            }
        }

        Err(GalleryError::Parse("Could not find image URL".to_string()))
    }

    /// Download and cache an image
    pub async fn download_image(&self, image_url: &str) -> Result<(String, Vec<u8>), GalleryError> {
        let response = self.client.get(image_url).send().await?;

        if !response.status().is_success() {
            return Err(GalleryError::Network(
                response.error_for_status().unwrap_err(),
            ));
        }

        let bytes = response.bytes().await?.to_vec();
        let hash = self.cache.compute_hash(&bytes);

        Ok((hash, bytes))
    }

    /// Background worker for processing download queue
    async fn run_download_workers(
        mut rx: mpsc::Receiver<DownloadRequest>,
        cache: Arc<CacheManager>,
        config: Arc<Config>,
        worker_count: usize,
    ) {
        info!("Starting {} download workers", worker_count);

        let client = Self::build_client(&config).expect("Failed to build client for workers");
        let client = Arc::new(client);

        while let Some(request) = rx.recv().await {
            let cache = cache.clone();
            let config = config.clone();
            let client = client.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::process_gallery_download(request, cache, config, client).await
                {
                    error!("Gallery download failed: {}", e);
                }
            });
        }
    }

    async fn process_gallery_download(
        request: DownloadRequest,
        cache: Arc<CacheManager>,
        config: Arc<Config>,
        client: Arc<Client>,
    ) -> Result<()> {
        let db = cache.get_db();
        let gallery_id = &request.gallery_id;
        let gallery_token = &request.gallery_token;

        info!(
            "Processing gallery download: {}/{}",
            gallery_id, gallery_token
        );

        // Update status to downloading
        sqlx::query("UPDATE gallery_downloads SET status = 'downloading', updated_at = ? WHERE gallery_id = ?")
            .bind(chrono::Utc::now().timestamp())
            .bind(gallery_id)
            .execute(&db)
            .await?;

        // Fetch gallery info
        let api_url = format!("{}/api.php", config.gallery_api);
        let request_body = serde_json::json!({
            "method": "gdata",
            "gidlist": [[gallery_id.parse::<i64>().unwrap_or(0), gallery_token]],
            "namespace": 1
        });

        let response = client.post(&api_url).json(&request_body).send().await?;
        let data: serde_json::Value = response.json().await?;

        let gmetadata = data
            .get("gmetadata")
            .and_then(|g| g.as_array())
            .and_then(|arr| arr.first())
            .context("Gallery metadata not found")?;

        let filecount: u32 = gmetadata
            .get("filecount")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let title = gmetadata
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown");

        // Update gallery info in database
        sqlx::query("UPDATE gallery_downloads SET title = ?, page_count = ?, updated_at = ? WHERE gallery_id = ?")
            .bind(title)
            .bind(filecount as i64)
            .bind(chrono::Utc::now().timestamp())
            .bind(gallery_id)
            .execute(&db)
            .await?;

        info!("Downloading gallery '{}' with {} pages", title, filecount);

        let base_url = if config.has_exhentai_access() {
            "https://exhentai.org"
        } else {
            "https://e-hentai.org"
        };

        let start_page = request.start_page.unwrap_or(1);
        let end_page = request.end_page.unwrap_or(filecount);

        let mut downloaded = 0u32;

        // Process pages (40 images per gallery page)
        let pages_to_fetch = ((end_page - start_page) / 40) + 1;

        // Pre-compile regex outside loop for performance
        let page_num_regex = Regex::new(r"/s/([a-f0-9]+)/(\d+)-(\d+)").unwrap();

        for page_idx in 0..pages_to_fetch {
            let gallery_page_url = format!(
                "{}/g/{}/{}/?p={}",
                base_url, gallery_id, gallery_token, page_idx
            );

            // Rate limiting
            sleep(Duration::from_secs(1)).await;

            let response = match client.get(&gallery_page_url).send().await {
                Ok(r) => r,
                Err(e) => {
                    error!("Failed to fetch gallery page: {}", e);
                    continue;
                }
            };

            let html = response.text().await?;

            // Parse image links and collect URLs (before any await)
            let image_pages: Vec<(u32, String)> = {
                let document = Html::parse_document(&html);
                let link_selector = Selector::parse("div.gdtm a, div.gdtl a").unwrap();

                document
                    .select(&link_selector)
                    .filter_map(|element| {
                        let href = element.value().attr("href")?;
                        let caps = page_num_regex.captures(href)?;
                        let page_num: u32 = caps.get(3)?.as_str().parse().ok()?;

                        if page_num >= start_page && page_num <= end_page {
                            Some((page_num, href.to_string()))
                        } else {
                            None
                        }
                    })
                    .collect()
            };

            // Now process each image page (with awaits)
            for (page_num, href) in image_pages {
                // Rate limiting between image pages
                sleep(Duration::from_millis(500)).await;

                // Fetch the image page to get actual image URL
                let img_page_response = match client.get(&href).send().await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("Failed to fetch image page {}: {}", page_num, e);
                        continue;
                    }
                };

                let img_html = img_page_response.text().await?;

                // Extract image URL (before any await)
                let img_src: Option<String> = {
                    let img_document = Html::parse_document(&img_html);
                    let img_selector = Selector::parse("img#img").unwrap();
                    img_document
                        .select(&img_selector)
                        .next()
                        .and_then(|img| img.value().attr("src"))
                        .map(|s| s.to_string())
                };

                if let Some(src) = img_src {
                    // Download the image
                    match client.get(&src).send().await {
                        Ok(img_response) => {
                            if let Ok(bytes) = img_response.bytes().await {
                                let hash = cache.compute_hash(&bytes);

                                // Determine file type from URL
                                let file_type = if src.contains(".png") {
                                    "png"
                                } else if src.contains(".gif") {
                                    "gif"
                                } else {
                                    "jpg"
                                };

                                // Store in cache
                                match cache.store_file(&hash, &bytes, file_type).await {
                                    Ok(_) => {
                                        downloaded += 1;
                                        debug!(
                                            "Downloaded page {}/{}: {}",
                                            page_num, filecount, hash
                                        );

                                        // Update progress
                                        let _ = sqlx::query(
                                            "UPDATE gallery_downloads SET downloaded_pages = ?, updated_at = ? WHERE gallery_id = ?"
                                        )
                                        .bind(downloaded as i64)
                                        .bind(chrono::Utc::now().timestamp())
                                        .bind(gallery_id)
                                        .execute(&db)
                                        .await;
                                    }
                                    Err(e) => {
                                        warn!("Failed to cache page {}: {}", page_num, e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to download image {}: {}", page_num, e);
                        }
                    }
                }
            }
        }

        // Update status to completed
        sqlx::query(
            "UPDATE gallery_downloads SET status = 'completed', updated_at = ? WHERE gallery_id = ?",
        )
        .bind(chrono::Utc::now().timestamp())
        .bind(gallery_id)
        .execute(&db)
        .await?;

        info!(
            "Completed gallery download: {}/{} ({}/{} pages)",
            gallery_id, gallery_token, downloaded, filecount
        );

        Ok(())
    }

    /// Get download status for a gallery
    pub async fn get_download_status(
        &self,
        gallery_id: &str,
    ) -> Result<Option<(String, i64, i64)>, GalleryError> {
        let db = self.cache.get_db();

        let result: Option<(String, i64, i64)> = sqlx::query_as(
            "SELECT status, downloaded_pages, page_count FROM gallery_downloads WHERE gallery_id = ?",
        )
        .bind(gallery_id)
        .fetch_optional(&db)
        .await?;

        Ok(result)
    }

    /// Get list of all gallery downloads
    pub async fn list_downloads(
        &self,
    ) -> Result<Vec<(String, String, String, i64, i64)>, GalleryError> {
        let db = self.cache.get_db();

        let results: Vec<(String, String, String, i64, i64)> = sqlx::query_as(
            "SELECT gallery_id, gallery_token, status, downloaded_pages, COALESCE(page_count, 0) FROM gallery_downloads ORDER BY updated_at DESC LIMIT 100",
        )
        .fetch_all(&db)
        .await?;

        Ok(results)
    }

    /// Parse gallery URL to extract ID and token
    pub fn parse_gallery_url(url: &str) -> Option<(String, String)> {
        let re = Regex::new(r"(?:e-hentai|exhentai)\.org/g/(\d+)/([a-f0-9]+)").ok()?;
        let caps = re.captures(url)?;

        let gid = caps.get(1)?.as_str().to_string();
        let token = caps.get(2)?.as_str().to_string();

        Some((gid, token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_gallery_url() {
        let url = "https://e-hentai.org/g/1234567/abcdef1234/";
        let result = GalleryDownloader::parse_gallery_url(url);
        assert_eq!(
            result,
            Some(("1234567".to_string(), "abcdef1234".to_string()))
        );

        let url2 = "https://exhentai.org/g/7654321/fedcba4321/?p=2";
        let result2 = GalleryDownloader::parse_gallery_url(url2);
        assert_eq!(
            result2,
            Some(("7654321".to_string(), "fedcba4321".to_string()))
        );
    }
}
