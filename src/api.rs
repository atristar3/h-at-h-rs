//! H@H API client module
//!
//! Handles communication with the H@H server for registration, heartbeats,
//! and receiving file requests.

use crate::config::Config;
use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::{debug, error, info, warn};

/// Default RPC host (resolved via DNS for load balancing)
const HAH_RPC_HOST: &str = "rpc.hentaiathome.net";
const HAH_RPC_PROTOCOL: &str = "http://";
/// Client build number - must match Java client for compatibility
const CLIENT_BUILD: i32 = 176;
/// Maximum time drift allowed for request validation (5 minutes)
const MAX_KEY_TIME_DRIFT: i64 = 300;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("Server returned error: {0}")]
    ServerError(String),
    #[error("Invalid response: {0}")]
    InvalidResponse(String),
    #[error("Authentication failed: {0}")]
    AuthFailed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSettings {
    pub min_client_build: i32,
    pub cur_client_build: i32,
    pub max_files_xfer: i32,
    pub max_kb_xfer: i32,
    pub request_server: String,
    pub throttle_bytes: i64,
    pub disable_browse_load: bool,
    pub image_server: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub request_valid_base: i64,
    /// RPC server IPs for failover
    pub rpc_server_ips: Vec<String>,
    /// RPC server port
    pub rpc_server_port: u16,
    /// Dynamic RPC path from server
    pub rpc_path: String,
    /// Static range prefixes assigned to this client (4-char hex)
    /// Using HashSet for O(1) lookup instead of O(n) Vec iteration
    pub static_ranges: HashSet<String>,
    /// Number of static ranges assigned
    pub static_range_count: i32,
    /// Disk limit in bytes
    pub disk_limit_bytes: u64,
    /// Minimum remaining disk space in bytes
    pub disk_remaining_bytes: u64,
    /// Filesystem block size
    pub filesystem_blocksize: u64,
    /// Maximum allowed file size
    pub max_allowed_filesize: u64,
}

impl Default for ServerSettings {
    fn default() -> Self {
        ServerSettings {
            min_client_build: 0,
            cur_client_build: 0,
            max_files_xfer: 20,
            max_kb_xfer: 100000,
            request_server: String::new(),
            throttle_bytes: -1,
            disable_browse_load: false,
            image_server: String::new(),
            name: String::new(),
            host: String::new(),
            port: 8080,
            request_valid_base: 0,
            rpc_server_ips: Vec::new(),
            rpc_server_port: 80,
            rpc_path: "15/rpc?".to_string(),
            static_ranges: HashSet::new(),
            static_range_count: 0,
            disk_limit_bytes: 0,
            disk_remaining_bytes: 0,
            filesystem_blocksize: 4096,
            max_allowed_filesize: 1073741824, // 1GB default
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRequest {
    pub file_id: String,
    pub file_key: String,
    pub file_hash: String,
    pub file_size: u64,
}

pub struct HahApiClient {
    client: Client,
    config: Arc<Config>,
    server_settings: parking_lot::RwLock<ServerSettings>,
    /// Server time delta for clock synchronization
    server_time_delta: AtomicI32,
    /// Current RPC server IP (for failover)
    current_rpc_server: parking_lot::RwLock<Option<String>>,
    /// Last failed RPC server (to avoid immediately retrying)
    last_failed_rpc_server: parking_lot::RwLock<Option<String>>,
}

impl HahApiClient {
    pub fn new(config: Arc<Config>) -> Result<Self, anyhow::Error> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(config.request_timeout))
            .user_agent(format!("Hentai@Home {}", env!("CARGO_PKG_VERSION")))
            .build()?;

        Ok(Self {
            client,
            config,
            server_settings: parking_lot::RwLock::new(ServerSettings::default()),
            server_time_delta: AtomicI32::new(0),
            current_rpc_server: parking_lot::RwLock::new(None),
            last_failed_rpc_server: parking_lot::RwLock::new(None),
        })
    }

    /// Get the current server time (adjusted by delta)
    pub fn get_server_time(&self) -> i64 {
        let local_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        local_time + self.server_time_delta.load(Ordering::SeqCst) as i64
    }

    /// Set server time delta from server response
    fn set_server_time(&self, server_time: i64) {
        let local_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let delta = (server_time - local_time) as i32;
        self.server_time_delta.store(delta, Ordering::SeqCst);
        debug!("Server time delta set to: {} seconds", delta);
    }

    /// Get current RPC server host with failover support
    fn get_rpc_server_host(&self) -> String {
        let settings = self.server_settings.read();
        let mut current = self.current_rpc_server.write();
        let last_failed = self.last_failed_rpc_server.read();

        if let Some(ref server) = *current {
            // Check if we should add port
            let port = settings.rpc_server_port;
            if port == 80 {
                return server.clone();
            } else {
                return format!("{}:{}", server, port);
            }
        }

        // Select a new RPC server
        if settings.rpc_server_ips.is_empty() {
            return HAH_RPC_HOST.to_string();
        }

        // Try to select a server that's not the last failed one
        for ip in &settings.rpc_server_ips {
            if last_failed.as_ref() != Some(ip) {
                *current = Some(ip.clone());
                let port = settings.rpc_server_port;
                if port == 80 {
                    return ip.clone();
                } else {
                    return format!("{}:{}", ip, port);
                }
            }
        }

        // If all servers were tried, use the first one anyway
        let server = settings
            .rpc_server_ips
            .first()
            .cloned()
            .unwrap_or_else(|| HAH_RPC_HOST.to_string());
        *current = Some(server.clone());
        server
    }

    /// Mark current RPC server as failed (triggers failover)
    fn mark_rpc_server_failed(&self, host: &str) {
        let mut current = self.current_rpc_server.write();
        let mut last_failed = self.last_failed_rpc_server.write();

        if current.as_ref().map(|s| s.contains(host)).unwrap_or(false) {
            debug!("Marking RPC server {} as failed", host);
            *last_failed = current.take();
        }
    }

    /// Clear RPC server failure status
    pub fn clear_rpc_server_failure(&self) {
        let mut last_failed = self.last_failed_rpc_server.write();
        if last_failed.is_some() {
            debug!("Clearing RPC server failure status");
            *last_failed = None;
            *self.current_rpc_server.write() = None;
        }
    }

    /// Generate the authentication hash for API requests (matches Java client format)
    fn generate_auth(&self, action: &str, additional: &str) -> (i64, String) {
        let time = self.get_server_time();

        // Hash format: "hentai@home-{action}-{additional}-{clientID}-{time}-{clientKey}"
        let hash_input = format!(
            "hentai@home-{}-{}-{}-{}-{}",
            action, additional, self.config.client_id, time, self.config.client_key
        );

        let mut hasher = Sha1::new();
        hasher.update(hash_input.as_bytes());
        let hash = hex::encode(hasher.finalize());

        (time, hash)
    }

    /// Build the base URL for API requests (matches Java client format)
    fn build_url(&self, action: &str, additional: &str) -> String {
        let (time, hash) = self.generate_auth(action, additional);
        let rpc_host = self.get_rpc_server_host();
        let rpc_path = {
            let settings = self.server_settings.read();
            if settings.rpc_path.is_empty() {
                "/15/rpc?".to_string()
            } else {
                format!("/{}", settings.rpc_path)
            }
        };

        format!(
            "{}{}{}clientbuild={}&act={}&add={}&cid={}&acttime={}&actkey={}",
            HAH_RPC_PROTOCOL,
            rpc_host,
            rpc_path,
            CLIENT_BUILD,
            action,
            additional,
            self.config.client_id,
            time,
            hash
        )
    }

    /// Build URL for server_stat (doesn't require authentication)
    fn build_server_stat_url(&self) -> String {
        let rpc_host = self.get_rpc_server_host();
        let rpc_path = {
            let settings = self.server_settings.read();
            if settings.rpc_path.is_empty() {
                "/15/rpc?".to_string()
            } else {
                format!("/{}", settings.rpc_path)
            }
        };
        format!(
            "{}{}{}clientbuild={}&act=server_stat",
            HAH_RPC_PROTOCOL, rpc_host, rpc_path, CLIENT_BUILD,
        )
    }

    /// Get initial server stats (for time synchronization and minimum build check)
    pub async fn server_stat(&self) -> Result<(), ApiError> {
        let url = self.build_server_stat_url();
        debug!("Getting server stats from: {}", url);

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.starts_with("FAIL") {
            return Err(ApiError::ServerError(text));
        }

        // Parse settings from response
        for line in text.lines() {
            if let Some((key, value)) = line.split_once('=') {
                match key.to_lowercase().as_str() {
                    "server_time" => {
                        if let Ok(server_time) = value.parse::<i64>() {
                            self.set_server_time(server_time);
                        }
                    }
                    "min_client_build" => {
                        if let Ok(min_build) = value.parse::<i32>() {
                            if min_build > CLIENT_BUILD {
                                return Err(ApiError::ServerError(format!(
                                    "Client build {} is too old. Minimum required: {}",
                                    CLIENT_BUILD, min_build
                                )));
                            }
                        }
                    }
                    "rpc_server_ip" => {
                        let ips: Vec<String> = value
                            .split(';')
                            .map(|s| s.to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                        self.server_settings.write().rpc_server_ips = ips;
                    }
                    "rpc_server_port" => {
                        if let Ok(port) = value.parse::<u16>() {
                            self.server_settings.write().rpc_server_port = port;
                        }
                    }
                    "rpc_path" => {
                        self.server_settings.write().rpc_path = value.to_string();
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    /// Register client with the H@H server
    pub async fn client_login(&self) -> Result<ServerSettings, ApiError> {
        // First, get server stats for time synchronization
        self.server_stat().await?;

        let url = self.build_url("client_login", "");
        debug!("Logging in to H@H server: {}", url);

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.starts_with("FAIL") || text.starts_with("KEY_EXPIRED") {
            // If key expired, try to refresh server time and retry once
            if text.starts_with("KEY_EXPIRED") {
                warn!("Key expired, refreshing server time and retrying...");
                self.server_stat().await?;
                let url = self.build_url("client_login", "");
                let response = self.client.get(&url).send().await?;
                let text = response.text().await?;
                if text.starts_with("FAIL") {
                    return Err(ApiError::AuthFailed(text));
                }
                let settings = self.parse_server_settings(&text)?;
                *self.server_settings.write() = settings.clone();
                return Ok(settings);
            }
            return Err(ApiError::AuthFailed(text));
        }

        let settings = self.parse_server_settings(&text)?;
        *self.server_settings.write() = settings.clone();

        info!(
            "Successfully logged in as '{}' on {}:{}",
            settings.name, settings.host, settings.port
        );

        Ok(settings)
    }

    /// Send heartbeat to server
    pub async fn client_still_alive(&self) -> Result<bool, ApiError> {
        let url = self.build_url("still_alive", "");
        debug!("Sending heartbeat");

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.contains("OK") {
            debug!("Heartbeat acknowledged");
            Ok(true)
        } else if text.contains("FAIL") {
            warn!("Heartbeat failed: {}", text);
            Ok(false)
        } else {
            Err(ApiError::InvalidResponse(text))
        }
    }

    /// Notify server that client is suspending
    pub async fn client_suspend(&self) -> Result<(), ApiError> {
        let url = self.build_url("client_suspend", "");
        info!("Suspending client");

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.contains("OK") {
            info!("Client suspended successfully");
            Ok(())
        } else {
            Err(ApiError::ServerError(text))
        }
    }

    /// Notify server that client is resuming
    pub async fn client_resume(&self) -> Result<(), ApiError> {
        let url = self.build_url("client_resume", "");
        info!("Resuming client");

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.contains("OK") {
            info!("Client resumed successfully");
            Ok(())
        } else {
            Err(ApiError::ServerError(text))
        }
    }

    /// Notify server that client is shutting down
    pub async fn client_stop(&self) -> Result<(), ApiError> {
        let url = self.build_url("client_stop", "");
        info!("Stopping client");

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.contains("OK") {
            info!("Client stopped successfully");
            Ok(())
        } else {
            error!("Failed to stop client: {}", text);
            Err(ApiError::ServerError(text))
        }
    }

    /// Get list of files to download from server
    pub async fn get_static_range(&self) -> Result<Vec<String>, ApiError> {
        let url = self.build_url("get_blacklist", "");
        debug!("Getting static ranges");

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.starts_with("FAIL") {
            return Err(ApiError::ServerError(text));
        }

        let ranges: Vec<String> = text.lines().map(|s| s.to_string()).collect();
        Ok(ranges)
    }

    /// Report downloaded file to server
    pub async fn downloaded_files(&self, files: &[String]) -> Result<(), ApiError> {
        if files.is_empty() {
            return Ok(());
        }

        let files_str = files.join(";");
        let url = self.build_url("dl_finished", &files_str);
        debug!("Reporting {} downloaded files", files.len());

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.contains("OK") {
            debug!("Download report acknowledged");
            Ok(())
        } else {
            Err(ApiError::ServerError(text))
        }
    }

    /// Parse server settings from login response
    fn parse_server_settings(&self, response: &str) -> Result<ServerSettings, ApiError> {
        let mut settings = self.server_settings.read().clone();
        let mut params: HashMap<String, String> = HashMap::new();

        for line in response.lines() {
            if line == "OK" {
                continue;
            }
            if let Some((key, value)) = line.split_once('=') {
                params.insert(key.to_string(), value.to_string());
            }
        }

        // Parse time delta first
        if let Some(v) = params.get("server_time") {
            if let Ok(server_time) = v.parse::<i64>() {
                self.set_server_time(server_time);
            }
        }

        if let Some(v) = params.get("min_client_build") {
            settings.min_client_build = v.parse().unwrap_or(0);
            if settings.min_client_build > CLIENT_BUILD {
                return Err(ApiError::ServerError(format!(
                    "Client too old. Required build: {}, current: {}",
                    settings.min_client_build, CLIENT_BUILD
                )));
            }
        }
        if let Some(v) = params.get("cur_client_build") {
            settings.cur_client_build = v.parse().unwrap_or(0);
            if settings.cur_client_build > CLIENT_BUILD {
                warn!(
                    "A newer client version is available (build {})",
                    settings.cur_client_build
                );
            }
        }
        if let Some(v) = params.get("max_files_xfer") {
            settings.max_files_xfer = v.parse().unwrap_or(20);
        }
        if let Some(v) = params.get("max_kb_xfer") {
            settings.max_kb_xfer = v.parse().unwrap_or(100000);
        }
        if let Some(v) = params.get("rpc_server_ip") {
            settings.rpc_server_ips = v
                .split(';')
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
        if let Some(v) = params.get("rpc_server_port") {
            settings.rpc_server_port = v.parse().unwrap_or(80);
        }
        if let Some(v) = params.get("rpc_path") {
            settings.rpc_path = v.clone();
        }
        if let Some(v) = params.get("throttle_bytes") {
            settings.throttle_bytes = v.parse().unwrap_or(-1);
        }
        if let Some(v) = params.get("disklimit_bytes") {
            settings.disk_limit_bytes = v.parse().unwrap_or(0);
        }
        if let Some(v) = params.get("diskremaining_bytes") {
            settings.disk_remaining_bytes = v.parse().unwrap_or(0);
        }
        if let Some(v) = params.get("filesystem_blocksize") {
            settings.filesystem_blocksize = v.parse().unwrap_or(4096);
        }
        if let Some(v) = params.get("max_allowed_filesize") {
            settings.max_allowed_filesize = v.parse().unwrap_or(1073741824);
        }
        if let Some(v) = params.get("disableBrowseLoad") {
            settings.disable_browse_load = v == "1";
        }
        if let Some(v) = params.get("image_server") {
            settings.image_server = v.clone();
        }
        if let Some(v) = params.get("name") {
            settings.name = v.clone();
        }
        if let Some(v) = params.get("host") {
            settings.host = v.clone();
        }
        if let Some(v) = params.get("port") {
            settings.port = v.parse().unwrap_or(self.config.port);
        }
        if let Some(v) = params.get("request_valid_base") {
            settings.request_valid_base = v.parse().unwrap_or(0);
        }
        // Parse static ranges (format: "0000;0001;0002;...")
        if let Some(v) = params.get("static_ranges") {
            settings.static_ranges = v
                .split(';')
                .filter(|s| s.len() == 4)
                .map(|s| s.to_string())
                .collect();
            settings.static_range_count = settings.static_ranges.len() as i32;
        }
        if let Some(v) = params.get("static_range_count") {
            settings.static_range_count = v.parse().unwrap_or(settings.static_range_count);
        }

        Ok(settings)
    }

    /// Get current server settings
    pub fn get_settings(&self) -> ServerSettings {
        self.server_settings.read().clone()
    }

    /// Request files to uncache (remove from local cache)
    pub async fn get_uncache_list(&self) -> Result<Vec<String>, ApiError> {
        let url = self.build_url("get_uncache_list", "");
        debug!("Getting uncache list");

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.starts_with("FAIL") {
            return Err(ApiError::ServerError(text));
        }

        let hashes: Vec<String> = text
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#') && *line != "OK")
            .map(|s| s.to_string())
            .collect();

        Ok(hashes)
    }

    /// Report uncached files to server
    pub async fn report_uncached(&self, files: &[String]) -> Result<(), ApiError> {
        if files.is_empty() {
            return Ok(());
        }

        let files_str = files.join(";");
        let url = self.build_url("uncache_finished", &files_str);
        debug!("Reporting {} uncached files", files.len());

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.contains("OK") {
            debug!("Uncache report acknowledged");
            Ok(())
        } else {
            Err(ApiError::ServerError(text))
        }
    }

    /// Fetch more files for static ranges
    pub async fn fetch_static_range_files(&self) -> Result<Vec<(String, u64, String)>, ApiError> {
        let url = self.build_url("srfetch", "");
        debug!("Fetching static range files");

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.starts_with("FAIL") {
            return Err(ApiError::ServerError(text));
        }

        let mut files = Vec::new();
        for line in text.lines() {
            if line.is_empty() || line.starts_with('#') || line == "OK" {
                continue;
            }
            // Format: hash;size;type
            let parts: Vec<&str> = line.split(';').collect();
            if parts.len() >= 3 {
                let hash = parts[0].to_string();
                let size: u64 = parts[1].parse().unwrap_or(0);
                let file_type = parts[2].to_string();
                files.push((hash, size, file_type));
            }
        }

        Ok(files)
    }

    /// Report served file (for statistics)
    pub async fn report_served(&self, file_hash: &str, served_bytes: u64) -> Result<(), ApiError> {
        let additional = format!("{};{}", file_hash, served_bytes);
        let url = self.build_url("file_served", &additional);

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.contains("OK") || text.contains("ACCEPT") {
            Ok(())
        } else {
            // Non-critical, just log
            debug!("Server response to file_served: {}", text);
            Ok(())
        }
    }

    /// Request overload notification (when server is under heavy load)
    pub async fn notify_overload(&self, overloaded: bool) -> Result<(), ApiError> {
        let additional = if overloaded { "1" } else { "0" };
        let url = self.build_url("client_overload", additional);

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.contains("OK") {
            info!(
                "Overload status {} acknowledged",
                if overloaded { "ON" } else { "OFF" }
            );
            Ok(())
        } else {
            Err(ApiError::ServerError(text))
        }
    }

    /// Get proxy request (for proxy mode - forward uncached requests)
    pub async fn proxy_request(&self, file_hash: &str) -> Result<Option<String>, ApiError> {
        let url = self.build_url("proxy_request", file_hash);

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.starts_with("FAIL") || text.contains("NOT_FOUND") {
            return Ok(None);
        }

        // Server returns URL to fetch file from
        let download_url = text.lines().next().map(|s| s.to_string());
        Ok(download_url)
    }

    /// Update server with current statistics
    pub async fn report_statistics(
        &self,
        bytes_served: u64,
        files_served: u64,
        cache_size: u64,
        cache_files: u64,
    ) -> Result<(), ApiError> {
        let stats = format!(
            "{};{};{};{}",
            bytes_served, files_served, cache_size, cache_files
        );
        let url = self.build_url("client_stats", &stats);

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.contains("OK") {
            debug!("Statistics reported successfully");
            Ok(())
        } else {
            // Non-critical
            debug!("Statistics report response: {}", text);
            Ok(())
        }
    }

    /// Speed test endpoint - returns test data for bandwidth measurement
    pub async fn speed_test(&self, size: usize) -> Result<Vec<u8>, ApiError> {
        let url = {
            let settings = self.server_settings.read();
            format!("{}?cmd=speed_test&size={}", settings.request_server, size)
        }; // Lock released here before await

        let response = self.client.get(&url).send().await?;
        let bytes = response.bytes().await?.to_vec();
        Ok(bytes)
    }

    /// Verify a file request keystamp is valid
    /// Keystamp format: {time}-{sha1(time + "-" + fileid + "-" + clientKey + "-hotlinkthis").substring(0,10)}
    /// Verify a file request keystamp is valid
    /// Optimized with early returns and inline hints for hot path
    #[inline]
    pub fn verify_request(&self, file_id: &str, key: &str, timestamp: i64) -> bool {
        // Early return: Key must be exactly 10 characters
        if key.len() != 10 {
            return false;
        }

        // Early return: Key must be valid hex
        if !key.chars().all(|c| c.is_ascii_hexdigit()) {
            return false;
        }

        let server_time = self.get_server_time();

        // Early return: Check time window (15 minutes = 900 seconds)
        let time_diff = server_time - timestamp;
        if !(-900..=900).contains(&time_diff) {
            warn!(
                "Request timestamp {} out of valid window (server time: {})",
                timestamp, server_time
            );
            return false;
        }

        // Verify the key matches expected value
        // Format: sha1("{time}-{fileid}-{clientKey}-hotlinkthis").substring(0,10)
        // Use write! to a stack-allocated buffer to avoid heap allocation
        use std::fmt::Write;
        let mut expected_input =
            String::with_capacity(64 + file_id.len() + self.config.client_key.len());
        let _ = write!(
            expected_input,
            "{}-{}-{}-hotlinkthis",
            timestamp, file_id, self.config.client_key
        );

        let mut hasher = Sha1::new();
        hasher.update(expected_input.as_bytes());
        let hash_result = hasher.finalize();

        // Only encode the first 5 bytes (10 hex chars) we need
        let mut expected_key = [0u8; 10];
        hex::encode_to_slice(&hash_result[..5], &mut expected_key).unwrap();

        // Compare case-insensitively using ASCII bytes directly
        key.as_bytes().eq_ignore_ascii_case(&expected_key)
    }

    /// Check if a hash is in an assigned static range
    /// Optimized: Uses HashSet.contains() for O(1) lookup instead of O(n) iteration
    #[inline]
    pub fn is_in_static_range(&self, hash: &str) -> bool {
        if hash.len() < 4 {
            return false;
        }
        let prefix = &hash[..4];
        let settings = self.server_settings.read();
        settings.static_ranges.contains(prefix)
    }

    /// Get static range count
    pub fn get_static_range_count(&self) -> i32 {
        self.server_settings.read().static_range_count
    }

    /// Get throttle bytes per second from server
    pub fn get_throttle_bytes(&self) -> i64 {
        self.server_settings.read().throttle_bytes
    }

    // =========================================================================
    // H@H Download Queue API - For website-integrated gallery downloads
    // =========================================================================

    /// Fetch the next gallery from the H@H download queue
    /// This is the official website integration - users add galleries via the website
    /// Returns gallery metadata in a custom format
    pub async fn fetch_download_queue(
        &self,
        mark_previous: Option<(i32, &str)>,
    ) -> Result<Option<GalleryMeta>, ApiError> {
        // If we're marking a previous download as complete, include gid;minxres
        let additional = match mark_previous {
            Some((gid, minxres)) => format!("{};{}", gid, minxres),
            None => String::new(),
        };

        let url = self.build_download_url("fetchqueue", &additional);
        debug!("Fetching download queue: {}", url);

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.starts_with("FAIL") || text.starts_with("INVALID_REQUEST") {
            warn!("Download queue fetch failed: {}", text);
            return Err(ApiError::ServerError(text));
        }

        if text == "NO_PENDING_DOWNLOADS" {
            debug!("No pending downloads in queue");
            return Ok(None);
        }

        // Parse the gallery metadata
        self.parse_gallery_meta(&text)
    }

    /// Build URL for download queue API (uses /15/dl? endpoint)
    fn build_download_url(&self, action: &str, additional: &str) -> String {
        let (time, hash) = self.generate_auth(action, additional);
        let rpc_host = self.get_rpc_server_host();

        format!(
            "{}{}{}clientbuild={}&act={}&add={}&cid={}&acttime={}&actkey={}",
            HAH_RPC_PROTOCOL,
            rpc_host,
            "/15/dl?",
            CLIENT_BUILD,
            action,
            additional,
            self.config.client_id,
            time,
            hash
        )
    }

    /// Parse gallery metadata from server response
    fn parse_gallery_meta(&self, response: &str) -> Result<Option<GalleryMeta>, ApiError> {
        let mut meta = GalleryMeta::default();
        let mut parse_state = 0; // 0 = header, 1 = filelist, 2 = information

        for line in response.lines() {
            if line == "FILELIST" && parse_state == 0 {
                parse_state = 1;
                continue;
            }
            if line == "INFORMATION" && parse_state == 1 {
                parse_state = 2;
                continue;
            }
            if parse_state < 2 && line.is_empty() {
                continue;
            }

            if parse_state == 0 {
                // Header section: GID, FILECOUNT, MINXRES, TITLE
                if let Some((key, value)) = line.split_once(' ') {
                    match key {
                        "GID" => meta.gid = value.parse().unwrap_or(0),
                        "FILECOUNT" => meta.filecount = value.parse().unwrap_or(0),
                        "MINXRES" => meta.minxres = value.to_string(),
                        "TITLE" => {
                            // Sanitize title for filesystem
                            meta.title = value
                                .chars()
                                .filter(|c| {
                                    !matches!(c, '*' | '"' | '\\' | '<' | '>' | ':' | '|' | '?')
                                })
                                .collect::<String>()
                                .split_whitespace()
                                .collect::<Vec<_>>()
                                .join(" ");
                        }
                        _ => {}
                    }
                }
            } else if parse_state == 1 {
                // File list section: page fileindex xres sha1hash filetype filename
                let parts: Vec<&str> = line.splitn(6, ' ').collect();
                if parts.len() >= 6 {
                    let file = GalleryFileMeta {
                        page: parts[0].parse().unwrap_or(0),
                        fileindex: parts[1].parse().unwrap_or(0),
                        xres: parts[2].to_string(),
                        sha1hash: if parts[3] == "unknown" {
                            None
                        } else {
                            Some(parts[3].to_string())
                        },
                        filetype: parts[4].to_string(),
                        filename: parts[5].to_string(),
                    };
                    meta.files.push(file);
                }
            } else {
                // Information section
                meta.information.push_str(line);
                meta.information.push('\n');
            }
        }

        if meta.gid > 0 && meta.filecount > 0 && !meta.title.is_empty() {
            Ok(Some(meta))
        } else {
            warn!("Failed to parse gallery metadata");
            Ok(None)
        }
    }

    /// Get download URL for a specific gallery file
    /// Returns URL to fetch the file from (could be localhost if in cache, or remote)
    pub async fn get_downloader_fetch_url(
        &self,
        gid: i32,
        page: i32,
        fileindex: i32,
        xres: &str,
        retry: i32,
    ) -> Result<Option<String>, ApiError> {
        let additional = format!("{};{};{};{};{}", gid, page, fileindex, xres, retry);
        let url = self.build_url("dlfetch", &additional);
        debug!(
            "Fetching download URL: gid={} page={} fileindex={}",
            gid, page, fileindex
        );

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.starts_with("FAIL") || text.starts_with("INVALID") {
            warn!(
                "Failed to get download URL for fileindex={}: {}",
                fileindex, text
            );
            return Ok(None);
        }

        // First line is the download URL
        Ok(text.lines().next().map(|s| s.to_string()))
    }

    /// Report download failures to the server
    pub async fn report_download_failures(&self, failures: &[String]) -> Result<(), ApiError> {
        if failures.is_empty() || failures.len() > 50 {
            // If too many failures, it's probably a client problem
            return Ok(());
        }

        let failures_str = failures.join(";");
        let url = self.build_url("dlfails", &failures_str);
        debug!("Reporting {} download failures", failures.len());

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        if text.contains("OK") {
            debug!("Download failures reported");
        } else {
            debug!("Download failure report response: {}", text);
        }

        Ok(())
    }
}

/// Gallery metadata from the H@H download queue
#[derive(Debug, Clone, Default)]
pub struct GalleryMeta {
    pub gid: i32,
    pub filecount: i32,
    pub minxres: String,
    pub title: String,
    pub files: Vec<GalleryFileMeta>,
    pub information: String,
}

/// File metadata for a gallery file
#[derive(Debug, Clone)]
pub struct GalleryFileMeta {
    pub page: i32,
    pub fileindex: i32,
    pub xres: String,
    pub sha1hash: Option<String>,
    pub filetype: String,
    pub filename: String,
}
