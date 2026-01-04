//! Configuration module for H@H client
//!
//! Handles all configuration from environment variables and config files.

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Missing required configuration: {0}")]
    MissingRequired(String),
    #[error("Invalid configuration value: {0}")]
    InvalidValue(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Command line arguments and environment variables
#[derive(Parser, Debug, Clone)]
#[command(name = "h-at-h-rs")]
#[command(author = "Pegasus Heavy Industries")]
#[command(version = "0.1.0")]
#[command(about = "Hentai@Home client clone in Rust")]
pub struct Args {
    /// Client ID from H@H registration
    #[arg(long, env = "HAH_CLIENT_ID")]
    pub client_id: String,

    /// Client key from H@H registration
    #[arg(long, env = "HAH_CLIENT_KEY")]
    pub client_key: String,

    /// Port to listen on for serving files
    #[arg(long, env = "HAH_PORT", default_value = "8080")]
    pub port: u16,

    /// Cache directory path
    #[arg(long, env = "HAH_CACHE_DIR", default_value = "./cache")]
    pub cache_dir: PathBuf,

    /// Temporary directory for downloads
    #[arg(long, env = "HAH_TEMP_DIR", default_value = "./temp")]
    pub temp_dir: PathBuf,

    /// Maximum cache size in GB
    #[arg(long, env = "HAH_CACHE_SIZE_GB", default_value = "100")]
    pub cache_size_gb: u64,

    /// Maximum disk usage percentage (0-100)
    #[arg(long, env = "HAH_MAX_DISK_USAGE", default_value = "95")]
    pub max_disk_usage: u8,

    /// Enable gallery downloading while browsing
    #[arg(long, env = "HAH_GALLERY_DOWNLOAD", default_value = "true")]
    pub gallery_download_enabled: bool,

    /// API endpoint for gallery downloads
    #[arg(
        long,
        env = "HAH_GALLERY_API",
        default_value = "https://api.e-hentai.org"
    )]
    pub gallery_api: String,

    /// ExHentai member ID cookie (for ExHentai access)
    #[arg(long, env = "HAH_EXHENTAI_MEMBER_ID")]
    pub exhentai_member_id: Option<String>,

    /// ExHentai pass hash cookie (for ExHentai access)
    #[arg(long, env = "HAH_EXHENTAI_PASS_HASH")]
    pub exhentai_pass_hash: Option<String>,

    /// ExHentai igneous cookie (for ExHentai access)
    #[arg(long, env = "HAH_EXHENTAI_IGNEOUS")]
    pub exhentai_igneous: Option<String>,

    /// Database path for tracking cache and downloads
    #[arg(long, env = "HAH_DATABASE_PATH", default_value = "./data/hah.db")]
    pub database_path: PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "HAH_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Enable JSON logging format
    #[arg(long, env = "HAH_LOG_JSON", default_value = "false")]
    pub log_json: bool,

    /// Number of concurrent download workers
    #[arg(long, env = "HAH_DOWNLOAD_WORKERS", default_value = "4")]
    pub download_workers: usize,

    /// Request timeout in seconds
    #[arg(long, env = "HAH_REQUEST_TIMEOUT", default_value = "30")]
    pub request_timeout: u64,

    /// Enable static ranges (for bandwidth throttling)
    #[arg(long, env = "HAH_STATIC_RANGES", default_value = "false")]
    pub static_ranges: bool,

    /// Bind address
    #[arg(long, env = "HAH_BIND_ADDRESS", default_value = "0.0.0.0")]
    pub bind_address: String,

    /// Maximum upload speed in KB/s (-1 for unlimited)
    #[arg(long, env = "HAH_MAX_UPLOAD_SPEED", default_value = "-1")]
    pub max_upload_speed: i64,

    /// Maximum download speed in KB/s (-1 for unlimited)
    #[arg(long, env = "HAH_MAX_DOWNLOAD_SPEED", default_value = "-1")]
    pub max_download_speed: i64,

    /// Maximum hourly bandwidth in MB (-1 for unlimited)
    #[arg(long, env = "HAH_MAX_HOURLY_BANDWIDTH", default_value = "-1")]
    pub max_hourly_bandwidth: i64,

    /// Enable proxy mode (forward uncached requests)
    #[arg(long, env = "HAH_PROXY_MODE", default_value = "false")]
    pub proxy_mode: bool,

    /// Minimum disk space to keep free in GB
    #[arg(long, env = "HAH_MIN_FREE_SPACE_GB", default_value = "5")]
    pub min_free_space_gb: u64,

    /// Enable metrics collection
    #[arg(long, env = "HAH_ENABLE_METRICS", default_value = "true")]
    pub enable_metrics: bool,

    /// Enable TUI (Terminal User Interface) dashboard
    #[arg(long, env = "HAH_TUI", default_value = "false")]
    pub tui: bool,
}

/// Runtime configuration derived from args
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub client_id: String,
    pub client_key: String,
    pub port: u16,
    pub cache_dir: PathBuf,
    pub temp_dir: PathBuf,
    pub cache_size_bytes: u64,
    pub max_disk_usage: u8,
    pub gallery_download_enabled: bool,
    pub gallery_api: String,
    pub exhentai_cookies: Option<ExHentaiCookies>,
    pub database_path: PathBuf,
    pub log_level: String,
    pub log_json: bool,
    pub download_workers: usize,
    pub request_timeout: u64,
    pub static_ranges: bool,
    pub bind_address: String,
    /// Max upload speed in bytes per second (-1 = unlimited)
    pub max_upload_speed: i64,
    /// Max download speed in bytes per second (-1 = unlimited)
    pub max_download_speed: i64,
    /// Max hourly bandwidth in bytes (-1 = unlimited)
    pub max_hourly_bandwidth: i64,
    /// Enable proxy mode for uncached requests
    pub proxy_mode: bool,
    /// Minimum free space in bytes
    pub min_free_space_bytes: u64,
    /// Enable metrics collection
    pub enable_metrics: bool,
    /// Enable TUI dashboard
    pub tui: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExHentaiCookies {
    pub member_id: String,
    pub pass_hash: String,
    pub igneous: Option<String>,
}

impl Config {
    /// Create configuration from command line args
    pub fn from_args(args: Args) -> Result<Self, ConfigError> {
        let exhentai_cookies = match (&args.exhentai_member_id, &args.exhentai_pass_hash) {
            (Some(member_id), Some(pass_hash)) => Some(ExHentaiCookies {
                member_id: member_id.clone(),
                pass_hash: pass_hash.clone(),
                igneous: args.exhentai_igneous.clone(),
            }),
            _ => None,
        };

        // Convert KB/s to bytes/s for upload/download speeds
        let max_upload_speed = if args.max_upload_speed > 0 {
            args.max_upload_speed * 1024
        } else {
            -1
        };
        let max_download_speed = if args.max_download_speed > 0 {
            args.max_download_speed * 1024
        } else {
            -1
        };
        // Convert MB to bytes for hourly bandwidth
        let max_hourly_bandwidth = if args.max_hourly_bandwidth > 0 {
            args.max_hourly_bandwidth * 1024 * 1024
        } else {
            -1
        };

        Ok(Config {
            client_id: args.client_id,
            client_key: args.client_key,
            port: args.port,
            cache_dir: args.cache_dir,
            temp_dir: args.temp_dir,
            cache_size_bytes: args.cache_size_gb * 1024 * 1024 * 1024,
            max_disk_usage: args.max_disk_usage.min(99),
            gallery_download_enabled: args.gallery_download_enabled,
            gallery_api: args.gallery_api,
            exhentai_cookies,
            database_path: args.database_path,
            log_level: args.log_level,
            log_json: args.log_json,
            download_workers: args.download_workers,
            request_timeout: args.request_timeout,
            static_ranges: args.static_ranges,
            bind_address: args.bind_address,
            max_upload_speed,
            max_download_speed,
            max_hourly_bandwidth,
            proxy_mode: args.proxy_mode,
            min_free_space_bytes: args.min_free_space_gb * 1024 * 1024 * 1024,
            enable_metrics: args.enable_metrics,
            tui: args.tui,
        })
    }

    /// Get the full bind address string
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.bind_address, self.port)
    }

    /// Check if ExHentai access is configured
    pub fn has_exhentai_access(&self) -> bool {
        self.exhentai_cookies.is_some()
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            client_id: String::new(),
            client_key: String::new(),
            port: 8080,
            cache_dir: PathBuf::from("./cache"),
            temp_dir: PathBuf::from("./temp"),
            cache_size_bytes: 100 * 1024 * 1024 * 1024, // 100GB
            max_disk_usage: 95,
            gallery_download_enabled: true,
            gallery_api: "https://api.e-hentai.org".to_string(),
            exhentai_cookies: None,
            database_path: PathBuf::from("./data/hah.db"),
            log_level: "info".to_string(),
            log_json: false,
            download_workers: 4,
            request_timeout: 30,
            static_ranges: false,
            bind_address: "0.0.0.0".to_string(),
            max_upload_speed: -1,
            max_download_speed: -1,
            max_hourly_bandwidth: -1,
            proxy_mode: false,
            min_free_space_bytes: 5 * 1024 * 1024 * 1024, // 5GB
            enable_metrics: true,
            tui: false,
        }
    }
}
