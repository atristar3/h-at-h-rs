//! H@H-rs: A Hentai@Home client clone in Rust
//!
//! This is a Rust implementation of the Hentai@Home distributed caching network client.
//! It includes support for gallery downloading and archive downloads while browsing the site.
//!
//! ## Features
//! - Full H@H protocol support (login, heartbeat, file serving)
//! - Static range assignments and proactive caching
//! - Bandwidth throttling (per-second and hourly limits)
//! - Trust and quality metrics tracking
//! - Gallery downloading (page by page)
//! - Archive downloading (ZIP files)
//! - Proxy mode for uncached files
//! - ExHentai support with cookie authentication
//! - Terminal User Interface (TUI) dashboard

// Allow dead code for public API methods not used internally
#![allow(dead_code)]

mod api;
mod archive;
mod cache;
mod config;
mod gallery;
mod hath_downloader;
mod metrics;
mod server;
mod static_ranges;
mod throttle;
mod tui;

use crate::api::HahApiClient;
use crate::archive::ArchiveDownloader;
use crate::cache::CacheManager;
use crate::config::{Args, Config};
use crate::gallery::GalleryDownloader;
use crate::hath_downloader::HathDownloader;
use crate::metrics::MetricsCollector;
use crate::server::{AppState, FileVerificationTracker, FloodControl, start_server};
use crate::static_ranges::StaticRangeManager;
use crate::throttle::BandwidthThrottler;
use crate::tui::TuiData;
use anyhow::{Context, Result};
use clap::Parser;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::broadcast;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file if present
    let _ = dotenvy::dotenv();

    // Parse command line arguments
    let args = Args::parse();
    let tui_enabled = args.tui;

    // Initialize logging (skip if TUI is enabled to avoid conflicts)
    if !tui_enabled {
        init_logging(&args.log_level, args.log_json)?;
    }

    if !tui_enabled {
        info!(
            "Starting H@H-rs v{} (Hentai@Home Rust Client)",
            env!("CARGO_PKG_VERSION")
        );
    }

    // Create configuration
    let config = Arc::new(Config::from_args(args).context("Failed to create configuration")?);

    if !tui_enabled {
        info!("Client ID: {}", config.client_id);
        info!("Cache directory: {}", config.cache_dir.display());
        info!(
            "Cache size limit: {} GB",
            config.cache_size_bytes / 1024 / 1024 / 1024
        );
        info!(
            "Gallery download enabled: {}",
            config.gallery_download_enabled
        );
        info!("Static ranges enabled: {}", config.static_ranges);
        info!("Proxy mode enabled: {}", config.proxy_mode);

        // Log bandwidth limits if set
        if config.max_upload_speed > 0 {
            info!("Max upload speed: {} KB/s", config.max_upload_speed / 1024);
        }
        if config.max_hourly_bandwidth > 0 {
            info!(
                "Max hourly bandwidth: {} MB",
                config.max_hourly_bandwidth / 1024 / 1024
            );
        }
    }

    // Initialize metrics collector
    let metrics = Arc::new(MetricsCollector::new());

    // Initialize bandwidth throttlers
    let upload_throttler = Arc::new(BandwidthThrottler::new(
        config.max_upload_speed,
        config.max_hourly_bandwidth,
    ));
    let download_throttler = Arc::new(BandwidthThrottler::new(
        config.max_download_speed,
        -1, // No hourly limit on downloads
    ));

    // Initialize cache manager
    if !tui_enabled {
        info!("Initializing cache manager...");
    }
    let cache = Arc::new(
        CacheManager::new(config.clone())
            .await
            .context("Failed to initialize cache manager")?,
    );

    // Scan for unindexed files
    let recovered = cache.scan_and_recover().await?;
    if recovered > 0 && !tui_enabled {
        info!(
            "Recovered {} unindexed files from cache directory",
            recovered
        );
    }

    // Initialize API client
    let api = Arc::new(HahApiClient::new(config.clone()).context("Failed to create API client")?);

    // Login to H@H server
    if !tui_enabled {
        info!("Logging in to H@H server...");
    }

    let (login_success, client_name, server_host, server_port) = match api.client_login().await {
        Ok(settings) => {
            if !tui_enabled {
                info!(
                    "Successfully connected to H@H network as '{}'",
                    settings.name
                );
                info!("Server assigned host: {}:{}", settings.host, settings.port);
            }

            // Apply server-assigned throttle if present
            if settings.throttle_bytes > 0 {
                upload_throttler.set_server_throttle(settings.throttle_bytes);
            }

            (true, settings.name, settings.host, settings.port)
        }
        Err(e) => {
            if !tui_enabled {
                error!("Failed to login to H@H server: {}", e);
                warn!("Continuing in offline mode - only gallery download will be available");
            }
            (false, String::new(), String::new(), config.port)
        }
    };

    // Initialize static range manager (if enabled and logged in)
    let static_range_manager = if config.static_ranges && login_success {
        if !tui_enabled {
            info!("Initializing static range manager...");
        }
        match StaticRangeManager::new(config.clone(), cache.clone(), api.clone()).await {
            Ok(manager) => Some(Arc::new(manager)),
            Err(e) => {
                if !tui_enabled {
                    warn!("Failed to initialize static range manager: {}", e);
                }
                None
            }
        }
    } else {
        None
    };

    // Initialize gallery downloader
    let gallery_downloader = Arc::new(
        GalleryDownloader::new(config.clone(), cache.clone())
            .await
            .context("Failed to initialize gallery downloader")?,
    );

    // Initialize archive downloader
    if !tui_enabled {
        info!("Initializing archive downloader...");
    }
    let archive_downloader = Arc::new(
        ArchiveDownloader::new(config.clone(), cache.get_db())
            .await
            .context("Failed to initialize archive downloader")?,
    );
    if !tui_enabled {
        info!(
            "Archive downloads directory: {}",
            archive_downloader.get_downloads_dir().display()
        );
    }

    // Initialize H@H download queue manager (official website integration)
    if !tui_enabled {
        info!("Initializing H@H download queue manager...");
    }
    let hath_downloader = Arc::new(
        HathDownloader::new(config.clone(), api.clone(), cache.clone())
            .context("Failed to initialize H@H downloader")?,
    );
    if !tui_enabled {
        info!(
            "H@H downloads directory: {}",
            hath_downloader.get_download_dir().display()
        );
    }

    // Initialize flood control
    let flood_control = Arc::new(FloodControl::new());

    // Initialize file verification tracker
    let file_verifier = Arc::new(FileVerificationTracker::new());

    // Create application state
    let state = Arc::new(AppState {
        config: config.clone(),
        cache: cache.clone(),
        api: api.clone(),
        gallery_downloader: gallery_downloader.clone(),
        archive_downloader: archive_downloader.clone(),
        hath_downloader: hath_downloader.clone(),
        metrics: metrics.clone(),
        upload_throttler: upload_throttler.clone(),
        download_throttler: download_throttler.clone(),
        flood_control: flood_control.clone(),
        file_verifier,
    });

    // Create shutdown signal channel
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Create TUI data channel if TUI is enabled
    let tui_tx = if tui_enabled {
        let (tx, _) = tui::create_tui_channel();
        Some(tx)
    } else {
        None
    };

    // Start heartbeat task
    let heartbeat_api = api.clone();
    let heartbeat_metrics = metrics.clone();
    let heartbeat_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        run_heartbeat(heartbeat_api, heartbeat_metrics, heartbeat_shutdown).await;
    });

    // Start static range refresh task (if enabled)
    let static_ranges_count = if let Some(sr_manager) = &static_range_manager {
        let sr_manager_clone = Arc::clone(sr_manager);
        let sr_shutdown = shutdown_tx.subscribe();
        let sr_tx = sr_manager_clone.start_workers();
        tokio::spawn(async move {
            static_ranges::run_static_range_refresh(sr_manager_clone, sr_tx, sr_shutdown).await;
        });
        static_range_manager
            .as_ref()
            .map(|m| m.get_assigned_ranges().len())
            .unwrap_or(0)
    } else {
        0
    };

    // Start statistics reporting task
    let stats_api = api.clone();
    let stats_cache = cache.clone();
    let stats_metrics = metrics.clone();
    let stats_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        run_stats_reporter(stats_api, stats_cache, stats_metrics, stats_shutdown).await;
    });

    // Start flood control cleanup task
    let fc_flood_control = flood_control.clone();
    let fc_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        run_flood_control_cleanup(fc_flood_control, fc_shutdown).await;
    });

    // Start RPC server failure clear task (clears failure status every 4 hours as per Java client)
    let rpc_api = api.clone();
    let rpc_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        run_rpc_failure_clear(rpc_api, rpc_shutdown).await;
    });

    // Start H@H download queue processor
    let hath_dl = hath_downloader.clone();
    let hath_dl_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        hath_downloader::run_hath_downloader(hath_dl, hath_dl_shutdown).await;
    });

    // Start TUI data update task if TUI is enabled
    if let Some(tui_tx) = tui_tx.clone() {
        let tui_config = config.clone();
        let tui_metrics = metrics.clone();
        let tui_cache = cache.clone();
        let tui_upload_throttler = upload_throttler.clone();
        let tui_gallery = gallery_downloader.clone();
        let tui_archive = archive_downloader.clone();
        let tui_client_name = client_name.clone();
        let tui_host = server_host.clone();
        let tui_shutdown = shutdown_tx.subscribe();

        tokio::spawn(async move {
            run_tui_updater(
                tui_tx,
                tui_config,
                tui_metrics,
                tui_cache,
                tui_upload_throttler,
                tui_gallery,
                tui_archive,
                tui_client_name,
                tui_host,
                server_port,
                login_success,
                static_ranges_count,
                tui_shutdown,
            )
            .await;
        });
    }

    // Start HTTP server
    let server_state = state.clone();
    let server_handle = tokio::spawn(async move {
        if let Err(e) = start_server(server_state).await {
            error!("Server error: {}", e);
        }
    });

    // Run TUI or wait for shutdown signal
    if tui_enabled {
        if let Some(tui_tx) = tui_tx {
            let tui_rx = tui_tx.subscribe();
            let tui_shutdown_tx = shutdown_tx.clone();

            // Run TUI in a blocking task (it takes over the terminal)
            let tui_result = tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Handle::current();
                rt.block_on(async { tui::run_tui(tui_rx, tui_shutdown_tx).await })
            })
            .await;

            if let Err(e) = tui_result {
                eprintln!("TUI error: {}", e);
            }
        }
    } else {
        // Wait for shutdown signal
        info!("H@H-rs is running. Press Ctrl+C to stop.");
        wait_for_shutdown().await;
    }

    // Graceful shutdown
    if !tui_enabled {
        info!("Shutting down...");
    }
    let _ = shutdown_tx.send(());

    // Notify H@H server
    if let Err(e) = api.client_stop().await {
        if !tui_enabled {
            warn!("Failed to notify server of shutdown: {}", e);
        }
    }

    // Wait for server to stop
    let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

    // Log final statistics
    let final_metrics = metrics.get_metrics();
    if !tui_enabled {
        info!(
            "Final stats - Uptime: {}s, Served: {} files ({} bytes), Trust: {:.2}, Quality: {:.2}",
            final_metrics.uptime_seconds,
            final_metrics.successful_serves,
            final_metrics.bytes_served,
            final_metrics.trust,
            final_metrics.quality
        );
        info!("H@H-rs shutdown complete");
    } else {
        println!(
            "Final stats - Uptime: {}s, Served: {} files, Trust: {:.2}%, Quality: {:.2}%",
            final_metrics.uptime_seconds,
            final_metrics.successful_serves,
            final_metrics.trust * 100.0,
            final_metrics.quality * 100.0
        );
    }

    Ok(())
}

/// Initialize logging with the specified level
fn init_logging(level: &str, json_format: bool) -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    if json_format {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    Ok(())
}

/// Run periodic heartbeat to H@H server
async fn run_heartbeat(
    api: Arc<HahApiClient>,
    metrics: Arc<MetricsCollector>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                match api.client_still_alive().await {
                    Ok(true) => {
                        tracing::debug!("Heartbeat successful");
                        metrics.record_heartbeat(true);
                    }
                    Ok(false) => {
                        warn!("Heartbeat returned false - may need to re-login");
                        metrics.record_heartbeat(false);
                    }
                    Err(e) => {
                        warn!("Heartbeat failed: {}", e);
                        metrics.record_heartbeat(false);
                    }
                }
            }
            _ = shutdown.recv() => {
                info!("Stopping heartbeat task");
                break;
            }
        }
    }
}

/// Run periodic statistics reporter to H@H server
async fn run_stats_reporter(
    api: Arc<HahApiClient>,
    cache: Arc<CacheManager>,
    metrics: Arc<MetricsCollector>,
    mut shutdown: broadcast::Receiver<()>,
) {
    // Report stats every 5 minutes
    let mut interval = tokio::time::interval(Duration::from_secs(300));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let m = metrics.get_metrics();
                let cache_stats = cache.get_stats();

                // Report statistics to server
                let _ = api.report_statistics(
                    m.bytes_served,
                    m.successful_serves,
                    cache_stats.total_size,
                    cache_stats.total_files,
                ).await;

                tracing::debug!(
                    "Stats reported - served: {} files, {} bytes",
                    m.successful_serves,
                    m.bytes_served
                );
            }
            _ = shutdown.recv() => {
                info!("Stopping statistics reporter task");
                break;
            }
        }
    }
}

/// Run periodic flood control table cleanup
async fn run_flood_control_cleanup(
    flood_control: Arc<FloodControl>,
    mut shutdown: broadcast::Receiver<()>,
) {
    // Prune stale entries every minute
    let mut interval = tokio::time::interval(Duration::from_secs(60));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                flood_control.prune_stale();
                tracing::debug!("Flood control table pruned");
            }
            _ = shutdown.recv() => {
                tracing::debug!("Stopping flood control cleanup task");
                break;
            }
        }
    }
}

/// Run periodic RPC server failure status clear
async fn run_rpc_failure_clear(api: Arc<HahApiClient>, mut shutdown: broadcast::Receiver<()>) {
    // Clear RPC server failure status every 4 hours (like Java client: 1440 * 10 seconds)
    let mut interval = tokio::time::interval(Duration::from_secs(14400));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                api.clear_rpc_server_failure();
                tracing::debug!("RPC server failure status cleared");
            }
            _ = shutdown.recv() => {
                tracing::debug!("Stopping RPC failure clear task");
                break;
            }
        }
    }
}

/// Wait for shutdown signal (Ctrl+C or SIGTERM)
async fn wait_for_shutdown() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Run the TUI data updater task
#[allow(clippy::too_many_arguments)]
async fn run_tui_updater(
    tui_tx: tokio::sync::watch::Sender<TuiData>,
    config: Arc<Config>,
    metrics: Arc<MetricsCollector>,
    cache: Arc<CacheManager>,
    upload_throttler: Arc<BandwidthThrottler>,
    gallery_downloader: Arc<GalleryDownloader>,
    archive_downloader: Arc<ArchiveDownloader>,
    client_name: String,
    server_host: String,
    server_port: u16,
    connected: bool,
    static_ranges_count: usize,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(250));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Gather all data for TUI
                let metrics_data = metrics.get_metrics();
                let cache_stats = cache.get_stats();
                let bandwidth = upload_throttler.get_stats();

                // Get gallery downloads
                let gallery_downloads = match gallery_downloader.list_downloads().await {
                    Ok(downloads) => downloads
                        .into_iter()
                        .map(|(id, _token, status, downloaded, total)| {
                            (id, status, downloaded, total)
                        })
                        .collect(),
                    Err(_) => Vec::new(),
                };

                // Get archive downloads
                let archive_downloads = match archive_downloader.list_archives(20).await {
                    Ok(archives) => archives
                        .into_iter()
                        .map(|a| {
                            let progress = if a.file_size > 0 {
                                (a.downloaded_bytes as f64 / a.file_size as f64) * 100.0
                            } else {
                                0.0
                            };
                            (a.gallery_id, a.title.unwrap_or_default(), a.status, progress)
                        })
                        .collect(),
                    Err(_) => Vec::new(),
                };

                let data = TuiData {
                    client_id: config.client_id.clone(),
                    client_name: client_name.clone(),
                    connected,
                    host: server_host.clone(),
                    port: server_port,
                    metrics: metrics_data,
                    cache_stats,
                    bandwidth,
                    gallery_downloads,
                    archive_downloads,
                    log_messages: Vec::new(), // TODO: Implement log capture
                    static_ranges_count,
                    static_ranges_enabled: config.static_ranges,
                    proxy_mode_enabled: config.proxy_mode,
                };

                let _ = tui_tx.send(data);
            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
}
