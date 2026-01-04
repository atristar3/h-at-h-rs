//! Archive download module
//!
//! Handles downloading gallery archives (ZIP files) from E-Hentai/ExHentai.
//! This mirrors the archive download functionality of the main H@H client.

use crate::config::Config;
use anyhow::Result;
use regex::Regex;
use reqwest::{Client, Url, cookie::Jar};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, info};

#[derive(Error, Debug)]
pub enum ArchiveError {
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Gallery not found: {0}")]
    NotFound(String),
    #[error("Access denied - login required")]
    AccessDenied,
    #[error("Archive not available")]
    NotAvailable,
    #[error("GP/Credits required for archive download")]
    InsufficientCredits,
    #[error("Rate limited - please wait")]
    RateLimited,
    #[error("Download failed: {0}")]
    DownloadFailed(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Archive resolution/quality options
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
pub enum ArchiveResolution {
    /// Original resolution (highest quality)
    #[default]
    Original,
    /// 2400px max dimension
    Res2400,
    /// 1600px max dimension
    Res1600,
    /// 1280px max dimension
    Res1280,
    /// 980px max dimension
    Res980,
    /// 780px max dimension
    Res780,
}

impl ArchiveResolution {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArchiveResolution::Original => "org",
            ArchiveResolution::Res2400 => "2400",
            ArchiveResolution::Res1600 => "1600",
            ArchiveResolution::Res1280 => "1280",
            ArchiveResolution::Res980 => "980",
            ArchiveResolution::Res780 => "780",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "org" | "original" => Some(ArchiveResolution::Original),
            "2400" => Some(ArchiveResolution::Res2400),
            "1600" => Some(ArchiveResolution::Res1600),
            "1280" => Some(ArchiveResolution::Res1280),
            "980" => Some(ArchiveResolution::Res980),
            "780" => Some(ArchiveResolution::Res780),
            _ => None,
        }
    }
}

/// Archive download request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveRequest {
    pub gallery_id: String,
    pub gallery_token: String,
    pub resolution: ArchiveResolution,
    pub or_token: Option<String>, // Original/resample token if known
}

/// Archive download status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveStatus {
    pub gallery_id: String,
    pub gallery_token: String,
    pub title: Option<String>,
    pub status: String,
    pub resolution: String,
    pub file_size: i64,
    pub downloaded_bytes: i64,
    pub file_path: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Archive information from gallery page
#[derive(Debug, Clone)]
pub struct ArchiveInfo {
    pub archiver_key: String,
    pub or_token: Option<String>,
    pub available_resolutions: Vec<ArchiveResolution>,
    pub estimated_size: Option<u64>,
    pub gp_cost: Option<u32>,
}

pub struct ArchiveDownloader {
    config: Arc<Config>,
    client: Client,
    db: Pool<Sqlite>,
    download_tx: mpsc::Sender<ArchiveRequest>,
    downloads_dir: PathBuf,
}

impl ArchiveDownloader {
    pub async fn new(config: Arc<Config>, db: Pool<Sqlite>) -> Result<Self> {
        // Build client with cookies
        let client = Self::build_client(&config)?;

        // Create downloads directory
        let downloads_dir = config.cache_dir.join("archives");
        fs::create_dir_all(&downloads_dir).await?;

        // Run migrations for archive tables
        Self::run_migrations(&db).await?;

        // Create download queue channel
        let (tx, rx) = mpsc::channel::<ArchiveRequest>(50);

        let downloader = Self {
            config: config.clone(),
            client: client.clone(),
            db: db.clone(),
            download_tx: tx,
            downloads_dir: downloads_dir.clone(),
        };

        // Start background download worker
        let worker_config = config.clone();
        let worker_client = client;
        let worker_db = db;
        let worker_dir = downloads_dir;

        tokio::spawn(async move {
            Self::run_download_worker(rx, worker_config, worker_client, worker_db, worker_dir)
                .await;
        });

        Ok(downloader)
    }

    fn build_client(config: &Config) -> Result<Client> {
        let mut builder = Client::builder()
            .timeout(Duration::from_secs(config.request_timeout * 10)) // Longer timeout for archives
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .cookie_store(true);

        if let Some(cookies) = &config.exhentai_cookies {
            let jar = Jar::default();
            let eh_url = "https://exhentai.org".parse::<Url>().unwrap();
            let e_url = "https://e-hentai.org".parse::<Url>().unwrap();

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

    async fn run_migrations(db: &Pool<Sqlite>) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS archive_downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gallery_id TEXT NOT NULL,
                gallery_token TEXT NOT NULL,
                title TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                resolution TEXT NOT NULL DEFAULT 'org',
                file_size INTEGER DEFAULT 0,
                downloaded_bytes INTEGER DEFAULT 0,
                file_path TEXT,
                error TEXT,
                archiver_key TEXT,
                or_token TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(gallery_id, resolution)
            )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_archive_downloads_status
            ON archive_downloads(status)
            "#,
        )
        .execute(db)
        .await?;

        Ok(())
    }

    /// Queue an archive for download
    pub async fn queue_archive(&self, request: ArchiveRequest) -> Result<(), ArchiveError> {
        let now = chrono::Utc::now().timestamp();

        // Insert or update in database
        sqlx::query(
            r#"
            INSERT INTO archive_downloads
            (gallery_id, gallery_token, status, resolution, created_at, updated_at)
            VALUES (?, ?, 'pending', ?, ?, ?)
            ON CONFLICT(gallery_id, resolution) DO UPDATE SET
                status = CASE WHEN status IN ('completed', 'downloading') THEN status ELSE 'pending' END,
                updated_at = ?
            "#,
        )
        .bind(&request.gallery_id)
        .bind(&request.gallery_token)
        .bind(request.resolution.as_str())
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&self.db)
        .await?;

        // Log before sending (request will be moved)
        let gallery_id = request.gallery_id.clone();
        let gallery_token = request.gallery_token.clone();
        let resolution = request.resolution;

        // Send to download queue
        self.download_tx
            .send(request)
            .await
            .map_err(|e| ArchiveError::Parse(e.to_string()))?;

        info!(
            "Queued archive download: {}/{} ({})",
            gallery_id,
            gallery_token,
            resolution.as_str()
        );

        Ok(())
    }

    /// Get archive download page and extract archiver key
    pub async fn get_archive_info(
        &self,
        gallery_id: &str,
        gallery_token: &str,
    ) -> Result<ArchiveInfo, ArchiveError> {
        let base_url = if self.config.has_exhentai_access() {
            "https://exhentai.org"
        } else {
            "https://e-hentai.org"
        };

        // First, get the gallery page to find the archiver link
        let gallery_url = format!("{}/g/{}/{}/", base_url, gallery_id, gallery_token);
        let response = self.client.get(&gallery_url).send().await?;

        if response.status() == 404 {
            return Err(ArchiveError::NotFound(gallery_id.to_string()));
        }

        let html = response.text().await?;

        // Check for sad panda
        if html.len() < 1000 || html.contains("Content Warning") {
            return Err(ArchiveError::AccessDenied);
        }

        let document = Html::parse_document(&html);

        // Find the archiver link - it's usually in the popup menu or toolbar
        // Format: onclick="return popUp('https://e-hentai.org/archiver.php?gid=XXX&token=YYY&or=ZZZ',480,320)"
        let archiver_regex =
            Regex::new(r#"archiver\.php\?gid=(\d+)&token=([a-f0-9]+)(?:&or=([a-z0-9]+))?"#)
                .unwrap();

        let archiver_key;
        #[allow(unused_assignments)]
        let mut or_token = None;

        if let Some(caps) = archiver_regex.captures(&html) {
            archiver_key = format!(
                "gid={}&token={}",
                caps.get(1).map(|m| m.as_str()).unwrap_or(gallery_id),
                caps.get(2).map(|m| m.as_str()).unwrap_or(gallery_token)
            );
            or_token = caps.get(3).map(|m| m.as_str().to_string());
        } else {
            // Try alternate method - look for archiver link directly
            let link_selector = Selector::parse("a[onclick*='archiver.php']").ok();
            if let Some(selector) = link_selector {
                if let Some(element) = document.select(&selector).next() {
                    if let Some(onclick) = element.value().attr("onclick") {
                        if let Some(caps) = archiver_regex.captures(onclick) {
                            archiver_key = format!(
                                "gid={}&token={}",
                                caps.get(1).map(|m| m.as_str()).unwrap_or(gallery_id),
                                caps.get(2).map(|m| m.as_str()).unwrap_or(gallery_token)
                            );
                            or_token = caps.get(3).map(|m| m.as_str().to_string());
                        } else {
                            return Err(ArchiveError::NotAvailable);
                        }
                    } else {
                        return Err(ArchiveError::NotAvailable);
                    }
                } else {
                    return Err(ArchiveError::NotAvailable);
                }
            } else {
                return Err(ArchiveError::NotAvailable);
            }
        }

        // Available resolutions (usually all are available for logged-in users)
        let available_resolutions = vec![
            ArchiveResolution::Original,
            ArchiveResolution::Res2400,
            ArchiveResolution::Res1600,
            ArchiveResolution::Res1280,
            ArchiveResolution::Res980,
            ArchiveResolution::Res780,
        ];

        Ok(ArchiveInfo {
            archiver_key,
            or_token,
            available_resolutions,
            estimated_size: None,
            gp_cost: None,
        })
    }

    /// Request archive generation and get download URL
    pub async fn request_archive(
        &self,
        gallery_id: &str,
        gallery_token: &str,
        resolution: ArchiveResolution,
        or_token: Option<&str>,
    ) -> Result<String, ArchiveError> {
        let base_url = if self.config.has_exhentai_access() {
            "https://exhentai.org"
        } else {
            "https://e-hentai.org"
        };

        // Build archiver URL
        let mut archiver_url = format!(
            "{}/archiver.php?gid={}&token={}",
            base_url, gallery_id, gallery_token
        );
        if let Some(or) = or_token {
            archiver_url.push_str(&format!("&or={}", or));
        }

        // First request to get the archiver page
        let response = self.client.get(&archiver_url).send().await?;
        let html = response.text().await?;

        // Check for errors
        if html.contains("This gallery is unavailable") {
            return Err(ArchiveError::NotAvailable);
        }
        if html.contains("insufficient funds") || html.contains("You do not have enough") {
            return Err(ArchiveError::InsufficientCredits);
        }

        // Parse the page to get the download form
        let document = Html::parse_document(&html);

        // Look for the form action or direct download link
        // The form usually has dltype and dlcheck fields
        let form_selector = Selector::parse("form").ok();
        let mut form_action = None;

        if let Some(selector) = form_selector {
            for form in document.select(&selector) {
                if let Some(action) = form.value().attr("action") {
                    if action.contains("archiver.php") || action.contains("?start=") {
                        form_action = Some(action.to_string());
                        break;
                    }
                }
            }
        }

        // Post the download request
        let dltype = match resolution {
            ArchiveResolution::Original => "org",
            _ => "res",
        };

        let dlcheck = match resolution {
            ArchiveResolution::Original => "Download Original Archive",
            _ => "Download Resample Archive",
        };

        let form_data = [("dltype", dltype), ("dlcheck", dlcheck)];

        // If resolution is not original, we need to specify the resolution
        let post_url = form_action.unwrap_or(archiver_url.clone());
        let response = self.client.post(&post_url).form(&form_data).send().await?;

        let result_html = response.text().await?;

        // Check for "Preparing download" page or direct download link
        // Look for the continue link or download URL
        let download_regex =
            Regex::new(r#"(?:document\.location\s*=\s*["']|<a[^>]*href=["'])([^"']+\.zip[^"']*)"#)
                .unwrap();

        if let Some(caps) = download_regex.captures(&result_html) {
            let download_url = caps.get(1).unwrap().as_str().to_string();

            // Handle relative URLs
            if download_url.starts_with("//") {
                return Ok(format!("https:{}", download_url));
            } else if download_url.starts_with("/") {
                return Ok(format!("{}{}", base_url, download_url));
            } else if download_url.starts_with("http") {
                return Ok(download_url);
            }
        }

        // Look for the H@H download link pattern
        let hah_regex = Regex::new(r#"(https?://[^"'\s]+/archive/[^"'\s]+)"#).unwrap();
        if let Some(caps) = hah_regex.captures(&result_html) {
            return Ok(caps.get(1).unwrap().as_str().to_string());
        }

        // Check if we need to wait for preparation
        if result_html.contains("Preparing your archive") || result_html.contains("Please wait") {
            // Look for the continuation URL
            let continue_regex =
                Regex::new(r#"<a[^>]*href=["']([^"']+)["'][^>]*>Click here"#).unwrap();
            if let Some(caps) = continue_regex.captures(&result_html) {
                let continue_url = caps.get(1).unwrap().as_str();

                // Wait a bit and follow the link
                sleep(Duration::from_secs(3)).await;

                let continue_response = self.client.get(continue_url).send().await?;
                let continue_html = continue_response.text().await?;

                if let Some(caps) = download_regex.captures(&continue_html) {
                    let download_url = caps.get(1).unwrap().as_str().to_string();
                    if download_url.starts_with("http") {
                        return Ok(download_url);
                    }
                }
            }

            return Err(ArchiveError::Parse(
                "Archive is being prepared, try again later".to_string(),
            ));
        }

        Err(ArchiveError::Parse(
            "Could not find download URL".to_string(),
        ))
    }

    /// Download archive file
    pub async fn download_archive(
        &self,
        download_url: &str,
        gallery_id: &str,
        _gallery_token: &str,
        resolution: ArchiveResolution,
    ) -> Result<PathBuf, ArchiveError> {
        info!("Downloading archive from: {}", download_url);

        // Update status to downloading
        sqlx::query(
            "UPDATE archive_downloads SET status = 'downloading', updated_at = ? WHERE gallery_id = ? AND resolution = ?",
        )
        .bind(chrono::Utc::now().timestamp())
        .bind(gallery_id)
        .bind(resolution.as_str())
        .execute(&self.db)
        .await?;

        // Start download
        let response = self.client.get(download_url).send().await?;

        if !response.status().is_success() {
            return Err(ArchiveError::DownloadFailed(format!(
                "HTTP {}",
                response.status()
            )));
        }

        // Get content length if available
        let content_length = response.content_length().unwrap_or(0);

        if content_length > 0 {
            sqlx::query(
                "UPDATE archive_downloads SET file_size = ?, updated_at = ? WHERE gallery_id = ? AND resolution = ?",
            )
            .bind(content_length as i64)
            .bind(chrono::Utc::now().timestamp())
            .bind(gallery_id)
            .bind(resolution.as_str())
            .execute(&self.db)
            .await?;
        }

        // Get filename from Content-Disposition header or URL
        let filename = response
            .headers()
            .get("content-disposition")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| {
                let re = Regex::new(r#"filename[*]?=(?:UTF-8'')?["']?([^"';\n]+)"#).ok()?;
                re.captures(s)
                    .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
            })
            .unwrap_or_else(|| format!("{}_{}.zip", gallery_id, resolution.as_str()));

        // Sanitize filename
        let safe_filename: String = filename
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '.' || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();

        let file_path = self.downloads_dir.join(&safe_filename);

        // Create temp file for downloading
        let temp_path = self.downloads_dir.join(format!(".{}.tmp", safe_filename));
        let mut file = File::create(&temp_path).await?;

        // Download with progress tracking
        let mut downloaded: u64 = 0;
        let mut stream = response.bytes_stream();

        use futures::StreamExt;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;

            // Update progress periodically (every 1MB)
            if downloaded.is_multiple_of(1024 * 1024) || downloaded == content_length {
                let _ = sqlx::query(
                    "UPDATE archive_downloads SET downloaded_bytes = ?, updated_at = ? WHERE gallery_id = ? AND resolution = ?",
                )
                .bind(downloaded as i64)
                .bind(chrono::Utc::now().timestamp())
                .bind(gallery_id)
                .bind(resolution.as_str())
                .execute(&self.db)
                .await;

                debug!(
                    "Archive download progress: {}/{} bytes",
                    downloaded, content_length
                );
            }
        }

        file.flush().await?;
        drop(file);

        // Rename temp file to final path
        fs::rename(&temp_path, &file_path).await?;

        // Update database with completion
        sqlx::query(
            r#"
            UPDATE archive_downloads
            SET status = 'completed',
                file_path = ?,
                downloaded_bytes = file_size,
                updated_at = ?
            WHERE gallery_id = ? AND resolution = ?
            "#,
        )
        .bind(file_path.to_string_lossy().to_string())
        .bind(chrono::Utc::now().timestamp())
        .bind(gallery_id)
        .bind(resolution.as_str())
        .execute(&self.db)
        .await?;

        info!(
            "Archive download completed: {} ({} bytes)",
            file_path.display(),
            downloaded
        );

        Ok(file_path)
    }

    /// Background worker for processing archive downloads
    async fn run_download_worker(
        mut rx: mpsc::Receiver<ArchiveRequest>,
        config: Arc<Config>,
        client: Client,
        db: Pool<Sqlite>,
        downloads_dir: PathBuf,
    ) {
        info!("Archive download worker started");

        while let Some(request) = rx.recv().await {
            let config = config.clone();
            let client = client.clone();
            let db = db.clone();
            let downloads_dir = downloads_dir.clone();

            tokio::spawn(async move {
                if let Err(e) =
                    Self::process_archive_download(request, config, client, db, downloads_dir).await
                {
                    error!("Archive download failed: {}", e);
                }
            });
        }
    }

    async fn process_archive_download(
        request: ArchiveRequest,
        config: Arc<Config>,
        client: Client,
        db: Pool<Sqlite>,
        downloads_dir: PathBuf,
    ) -> Result<()> {
        let gallery_id = &request.gallery_id;
        let gallery_token = &request.gallery_token;
        let resolution = request.resolution;

        info!(
            "Processing archive download: {}/{} ({})",
            gallery_id,
            gallery_token,
            resolution.as_str()
        );

        let base_url = if config.has_exhentai_access() {
            "https://exhentai.org"
        } else {
            "https://e-hentai.org"
        };

        // Update status
        sqlx::query(
            "UPDATE archive_downloads SET status = 'preparing', updated_at = ? WHERE gallery_id = ? AND resolution = ?",
        )
        .bind(chrono::Utc::now().timestamp())
        .bind(gallery_id)
        .bind(resolution.as_str())
        .execute(&db)
        .await?;

        // Get gallery page to find archiver link
        let gallery_url = format!("{}/g/{}/{}/", base_url, gallery_id, gallery_token);
        let response = client.get(&gallery_url).send().await?;
        let html = response.text().await?;

        // Extract archiver URL
        let archiver_regex =
            Regex::new(r#"archiver\.php\?gid=(\d+)&token=([a-f0-9]+)(?:&or=([a-z0-9]+))?"#)
                .unwrap();

        let (archiver_gid, archiver_token, or_token) =
            if let Some(caps) = archiver_regex.captures(&html) {
                (
                    caps.get(1)
                        .map(|m| m.as_str())
                        .unwrap_or(gallery_id)
                        .to_string(),
                    caps.get(2)
                        .map(|m| m.as_str())
                        .unwrap_or(gallery_token)
                        .to_string(),
                    caps.get(3).map(|m| m.as_str().to_string()),
                )
            } else {
                (gallery_id.to_string(), gallery_token.to_string(), None)
            };

        // Build archiver URL
        let mut archiver_url = format!(
            "{}/archiver.php?gid={}&token={}",
            base_url, archiver_gid, archiver_token
        );
        if let Some(ref or) = or_token {
            archiver_url.push_str(&format!("&or={}", or));
        }

        // Request the archiver page
        let response = client.get(&archiver_url).send().await?;
        let archiver_html = response.text().await?;

        // Check for errors
        if archiver_html.contains("insufficient funds") || archiver_html.contains("not have enough")
        {
            sqlx::query(
                "UPDATE archive_downloads SET status = 'failed', error = 'Insufficient GP/Credits', updated_at = ? WHERE gallery_id = ? AND resolution = ?",
            )
            .bind(chrono::Utc::now().timestamp())
            .bind(gallery_id)
            .bind(resolution.as_str())
            .execute(&db)
            .await?;
            return Err(anyhow::anyhow!("Insufficient GP/Credits"));
        }

        // Determine download type
        let (dltype, dlcheck) = match resolution {
            ArchiveResolution::Original => ("org", "Download Original Archive"),
            _ => ("res", "Download Resample Archive"),
        };

        // Post the form to initiate download
        let form_data = [("dltype", dltype), ("dlcheck", dlcheck)];

        let response = client.post(&archiver_url).form(&form_data).send().await?;

        let result_html = response.text().await?;

        // Try to find download URL in different formats
        let download_url = Self::extract_download_url(&result_html, base_url)?;

        // Download the archive
        sqlx::query(
            "UPDATE archive_downloads SET status = 'downloading', updated_at = ? WHERE gallery_id = ? AND resolution = ?",
        )
        .bind(chrono::Utc::now().timestamp())
        .bind(gallery_id)
        .bind(resolution.as_str())
        .execute(&db)
        .await?;

        let response = client.get(&download_url).send().await?;

        if !response.status().is_success() {
            sqlx::query(
                "UPDATE archive_downloads SET status = 'failed', error = ?, updated_at = ? WHERE gallery_id = ? AND resolution = ?",
            )
            .bind(format!("HTTP {}", response.status()))
            .bind(chrono::Utc::now().timestamp())
            .bind(gallery_id)
            .bind(resolution.as_str())
            .execute(&db)
            .await?;
            return Err(anyhow::anyhow!(
                "Download failed: HTTP {}",
                response.status()
            ));
        }

        let content_length = response.content_length().unwrap_or(0);

        // Get filename
        let filename = response
            .headers()
            .get("content-disposition")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| {
                let re = Regex::new(r#"filename[*]?=(?:UTF-8'')?["']?([^"';\n]+)"#).ok()?;
                re.captures(s)
                    .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
            })
            .unwrap_or_else(|| format!("{}_{}.zip", gallery_id, resolution.as_str()));

        // Sanitize filename
        let safe_filename: String = filename
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '.' || c == '_' || c == '-' || c == ' ' {
                    c
                } else {
                    '_'
                }
            })
            .collect();

        let file_path = downloads_dir.join(&safe_filename);
        let temp_path = downloads_dir.join(format!(".{}.tmp", safe_filename));

        // Update file size
        if content_length > 0 {
            sqlx::query(
                "UPDATE archive_downloads SET file_size = ?, updated_at = ? WHERE gallery_id = ? AND resolution = ?",
            )
            .bind(content_length as i64)
            .bind(chrono::Utc::now().timestamp())
            .bind(gallery_id)
            .bind(resolution.as_str())
            .execute(&db)
            .await?;
        }

        // Download with progress
        let mut file = File::create(&temp_path).await?;
        let mut downloaded: u64 = 0;
        let mut stream = response.bytes_stream();

        use futures::StreamExt;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;

            // Update progress every 1MB
            if downloaded.is_multiple_of(1024 * 1024) {
                let _ = sqlx::query(
                    "UPDATE archive_downloads SET downloaded_bytes = ?, updated_at = ? WHERE gallery_id = ? AND resolution = ?",
                )
                .bind(downloaded as i64)
                .bind(chrono::Utc::now().timestamp())
                .bind(gallery_id)
                .bind(resolution.as_str())
                .execute(&db)
                .await;
            }
        }

        file.flush().await?;
        drop(file);

        // Rename to final path
        fs::rename(&temp_path, &file_path).await?;

        // Mark as completed
        sqlx::query(
            r#"
            UPDATE archive_downloads
            SET status = 'completed',
                file_path = ?,
                downloaded_bytes = ?,
                file_size = ?,
                updated_at = ?
            WHERE gallery_id = ? AND resolution = ?
            "#,
        )
        .bind(file_path.to_string_lossy().to_string())
        .bind(downloaded as i64)
        .bind(downloaded as i64)
        .bind(chrono::Utc::now().timestamp())
        .bind(gallery_id)
        .bind(resolution.as_str())
        .execute(&db)
        .await?;

        info!(
            "Archive download completed: {} ({} bytes)",
            file_path.display(),
            downloaded
        );

        Ok(())
    }

    fn extract_download_url(html: &str, base_url: &str) -> Result<String> {
        // Try multiple patterns
        let patterns = [
            r#"document\.location\s*=\s*["']([^"']+\.zip[^"']*)"#,
            r#"<a[^>]*href=["']([^"']+\.zip[^"']*)"#,
            r#"(https?://[^\s"'<>]+/archive/[^\s"'<>]+)"#,
            r#"(https?://[^\s"'<>]+\.zip[^\s"'<>]*)"#,
        ];

        for pattern in patterns {
            if let Ok(re) = Regex::new(pattern) {
                if let Some(caps) = re.captures(html) {
                    let url = caps.get(1).unwrap().as_str().to_string();

                    // Handle relative URLs
                    if url.starts_with("//") {
                        return Ok(format!("https:{}", url));
                    } else if url.starts_with("/") {
                        return Ok(format!("{}{}", base_url, url));
                    } else if url.starts_with("http") {
                        return Ok(url);
                    }
                }
            }
        }

        // Check if we need to wait
        if html.contains("Preparing") || html.contains("Please wait") {
            return Err(anyhow::anyhow!("Archive is being prepared, retry later"));
        }

        Err(anyhow::anyhow!("Could not find download URL"))
    }

    /// Get status of an archive download
    pub async fn get_archive_status(
        &self,
        gallery_id: &str,
        resolution: Option<&str>,
    ) -> Result<Vec<ArchiveStatus>, ArchiveError> {
        let query = if let Some(res) = resolution {
            sqlx::query_as::<_, (String, String, Option<String>, String, String, i64, i64, Option<String>, Option<String>, i64, i64)>(
                r#"SELECT gallery_id, gallery_token, title, status, resolution, file_size, downloaded_bytes, file_path, error, created_at, updated_at
                   FROM archive_downloads WHERE gallery_id = ? AND resolution = ?"#
            )
            .bind(gallery_id)
            .bind(res)
            .fetch_all(&self.db)
            .await?
        } else {
            sqlx::query_as::<_, (String, String, Option<String>, String, String, i64, i64, Option<String>, Option<String>, i64, i64)>(
                r#"SELECT gallery_id, gallery_token, title, status, resolution, file_size, downloaded_bytes, file_path, error, created_at, updated_at
                   FROM archive_downloads WHERE gallery_id = ? ORDER BY created_at DESC"#
            )
            .bind(gallery_id)
            .fetch_all(&self.db)
            .await?
        };

        Ok(query
            .into_iter()
            .map(
                |(
                    gid,
                    token,
                    title,
                    status,
                    res,
                    size,
                    downloaded,
                    path,
                    error,
                    created,
                    updated,
                )| {
                    ArchiveStatus {
                        gallery_id: gid,
                        gallery_token: token,
                        title,
                        status,
                        resolution: res,
                        file_size: size,
                        downloaded_bytes: downloaded,
                        file_path: path,
                        error,
                        created_at: created,
                        updated_at: updated,
                    }
                },
            )
            .collect())
    }

    /// List all archive downloads
    pub async fn list_archives(&self, limit: i64) -> Result<Vec<ArchiveStatus>, ArchiveError> {
        let results = sqlx::query_as::<_, (String, String, Option<String>, String, String, i64, i64, Option<String>, Option<String>, i64, i64)>(
            r#"SELECT gallery_id, gallery_token, title, status, resolution, file_size, downloaded_bytes, file_path, error, created_at, updated_at
               FROM archive_downloads ORDER BY updated_at DESC LIMIT ?"#
        )
        .bind(limit)
        .fetch_all(&self.db)
        .await?;

        Ok(results
            .into_iter()
            .map(
                |(
                    gid,
                    token,
                    title,
                    status,
                    res,
                    size,
                    downloaded,
                    path,
                    error,
                    created,
                    updated,
                )| {
                    ArchiveStatus {
                        gallery_id: gid,
                        gallery_token: token,
                        title,
                        status,
                        resolution: res,
                        file_size: size,
                        downloaded_bytes: downloaded,
                        file_path: path,
                        error,
                        created_at: created,
                        updated_at: updated,
                    }
                },
            )
            .collect())
    }

    /// Get downloads directory path
    pub fn get_downloads_dir(&self) -> &Path {
        &self.downloads_dir
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
