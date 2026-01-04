//! Cache management module
//!
//! Handles storing, retrieving, and managing cached files.

use crate::config::Config;
use anyhow::{Context, Result};
use dashmap::DashMap;
use parking_lot::RwLock;
use sha1::{Digest, Sha1};
use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info};
use walkdir::WalkDir;

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("File not found: {0}")]
    NotFound(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Invalid hash: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },
    #[error("Cache full")]
    CacheFull,
}

#[derive(Debug, Clone)]
pub struct CachedFile {
    pub hash: String,
    pub size: u64,
    pub path: PathBuf,
    pub file_type: String,
    pub last_accessed: i64,
    pub hit_count: u64,
}

pub struct CacheManager {
    config: Arc<Config>,
    db: Pool<Sqlite>,
    file_index: DashMap<String, CachedFile>,
    total_size: AtomicU64,
    stats: RwLock<CacheStats>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct CacheStats {
    pub total_files: u64,
    pub total_size: u64,
    pub hits: u64,
    pub misses: u64,
    pub bytes_served: u64,
}

impl CacheManager {
    pub async fn new(config: Arc<Config>) -> Result<Self> {
        // Ensure directories exist
        fs::create_dir_all(&config.cache_dir).await?;
        fs::create_dir_all(&config.temp_dir).await?;

        // Ensure database directory exists
        if let Some(parent) = config.database_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Initialize database
        let db_url = format!("sqlite:{}?mode=rwc", config.database_path.display());
        let db = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await
            .context("Failed to connect to database")?;

        // Run migrations
        Self::run_migrations(&db).await?;

        let manager = Self {
            config,
            db,
            file_index: DashMap::new(),
            total_size: AtomicU64::new(0),
            stats: RwLock::new(CacheStats::default()),
        };

        // Load existing cache from disk
        manager.load_cache_index().await?;

        Ok(manager)
    }

    async fn run_migrations(db: &Pool<Sqlite>) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cached_files (
                hash TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                path TEXT NOT NULL,
                file_type TEXT NOT NULL,
                last_accessed INTEGER NOT NULL,
                hit_count INTEGER DEFAULT 0,
                created_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS gallery_downloads (
                gallery_id TEXT PRIMARY KEY,
                gallery_token TEXT NOT NULL,
                title TEXT,
                page_count INTEGER,
                downloaded_pages INTEGER DEFAULT 0,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS gallery_images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gallery_id TEXT NOT NULL,
                page_number INTEGER NOT NULL,
                file_hash TEXT,
                url TEXT,
                status TEXT NOT NULL,
                FOREIGN KEY (gallery_id) REFERENCES gallery_downloads(gallery_id),
                UNIQUE(gallery_id, page_number)
            )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cached_files_last_accessed
            ON cached_files(last_accessed)
            "#,
        )
        .execute(db)
        .await?;

        Ok(())
    }

    async fn load_cache_index(&self) -> Result<()> {
        info!("Loading cache index from disk...");

        let mut total_size = 0u64;
        let mut file_count = 0u64;

        // First, load from database
        let rows = sqlx::query_as::<_, (String, i64, String, String, i64, i64)>(
            "SELECT hash, size, path, file_type, last_accessed, hit_count FROM cached_files",
        )
        .fetch_all(&self.db)
        .await?;

        for (hash, size, path, file_type, last_accessed, hit_count) in rows {
            let path = PathBuf::from(path);

            // Verify file still exists
            if path.exists() {
                let cached_file = CachedFile {
                    hash: hash.clone(),
                    size: size as u64,
                    path,
                    file_type,
                    last_accessed,
                    hit_count: hit_count as u64,
                };

                total_size += cached_file.size;
                file_count += 1;
                self.file_index.insert(hash, cached_file);
            }
        }

        self.total_size.store(total_size, Ordering::SeqCst);
        {
            let mut stats = self.stats.write();
            stats.total_files = file_count;
            stats.total_size = total_size;
        }

        info!(
            "Loaded {} files ({:.2} GB) into cache index",
            file_count,
            total_size as f64 / (1024.0 * 1024.0 * 1024.0)
        );

        Ok(())
    }

    /// Get the path to store a file based on its hash
    fn get_file_path(&self, hash: &str) -> PathBuf {
        // Use first 2 characters of hash as directory to avoid too many files in one dir
        let dir = &hash[..2.min(hash.len())];
        self.config.cache_dir.join(dir).join(hash)
    }

    /// Check if a file exists in cache
    pub fn has_file(&self, hash: &str) -> bool {
        self.file_index.contains_key(hash)
    }

    /// Get a file from cache
    pub async fn get_file(&self, hash: &str) -> Result<Option<CachedFile>, CacheError> {
        if let Some(mut entry) = self.file_index.get_mut(hash) {
            let cached = entry.value_mut();

            // Update access stats
            cached.last_accessed = chrono::Utc::now().timestamp();
            cached.hit_count += 1;

            // Update stats
            {
                let mut stats = self.stats.write();
                stats.hits += 1;
                stats.bytes_served += cached.size;
            }

            // Update database asynchronously
            let hash_clone = hash.to_string();
            let db = self.db.clone();
            let last_accessed = cached.last_accessed;
            let hit_count = cached.hit_count as i64;

            tokio::spawn(async move {
                let _ = sqlx::query(
                    "UPDATE cached_files SET last_accessed = ?, hit_count = ? WHERE hash = ?",
                )
                .bind(last_accessed)
                .bind(hit_count)
                .bind(&hash_clone)
                .execute(&db)
                .await;
            });

            Ok(Some(cached.clone()))
        } else {
            {
                let mut stats = self.stats.write();
                stats.misses += 1;
            }
            Ok(None)
        }
    }

    /// Store a file in cache
    pub async fn store_file(
        &self,
        hash: &str,
        data: &[u8],
        file_type: &str,
    ) -> Result<CachedFile, CacheError> {
        // Verify hash
        let actual_hash = self.compute_hash(data);
        if actual_hash != hash {
            return Err(CacheError::HashMismatch {
                expected: hash.to_string(),
                actual: actual_hash,
            });
        }

        // Check if we have space
        let file_size = data.len() as u64;
        if self.total_size.load(Ordering::SeqCst) + file_size > self.config.cache_size_bytes {
            // Try to make space
            self.evict_files(file_size).await?;
        }

        // Get file path and ensure directory exists
        let path = self.get_file_path(hash);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Write file
        let mut file = fs::File::create(&path).await?;
        file.write_all(data).await?;
        file.flush().await?;

        let now = chrono::Utc::now().timestamp();
        let cached_file = CachedFile {
            hash: hash.to_string(),
            size: file_size,
            path: path.clone(),
            file_type: file_type.to_string(),
            last_accessed: now,
            hit_count: 0,
        };

        // Update database
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO cached_files
            (hash, size, path, file_type, last_accessed, hit_count, created_at)
            VALUES (?, ?, ?, ?, ?, 0, ?)
            "#,
        )
        .bind(hash)
        .bind(file_size as i64)
        .bind(path.to_string_lossy().to_string())
        .bind(file_type)
        .bind(now)
        .bind(now)
        .execute(&self.db)
        .await?;

        // Update index and stats
        self.file_index
            .insert(hash.to_string(), cached_file.clone());
        self.total_size.fetch_add(file_size, Ordering::SeqCst);

        {
            let mut stats = self.stats.write();
            stats.total_files += 1;
            stats.total_size += file_size;
        }

        debug!("Stored file {} ({} bytes)", hash, file_size);
        Ok(cached_file)
    }

    /// Store file from a stream/download
    pub async fn store_file_from_path(
        &self,
        hash: &str,
        source_path: &Path,
        file_type: &str,
    ) -> Result<CachedFile, CacheError> {
        let data = fs::read(source_path).await?;
        self.store_file(hash, &data, file_type).await
    }

    /// Evict files to make space
    async fn evict_files(&self, needed_bytes: u64) -> Result<(), CacheError> {
        info!("Evicting files to make {} bytes of space", needed_bytes);

        // Get files ordered by last accessed (oldest first)
        let files_to_evict: Vec<(String, i64)> = sqlx::query_as(
            "SELECT hash, size FROM cached_files ORDER BY last_accessed ASC LIMIT 100",
        )
        .fetch_all(&self.db)
        .await?;

        let mut freed = 0u64;
        for (hash, size) in files_to_evict {
            if freed >= needed_bytes {
                break;
            }

            if let Some((_, cached)) = self.file_index.remove(&hash) {
                // Delete file
                if cached.path.exists() {
                    let _ = fs::remove_file(&cached.path).await;
                }

                // Remove from database
                sqlx::query("DELETE FROM cached_files WHERE hash = ?")
                    .bind(&hash)
                    .execute(&self.db)
                    .await?;

                freed += size as u64;
                self.total_size.fetch_sub(size as u64, Ordering::SeqCst);

                debug!("Evicted file {}", hash);
            }
        }

        {
            let mut stats = self.stats.write();
            stats.total_size = self.total_size.load(Ordering::SeqCst);
        }

        info!("Freed {} bytes", freed);
        Ok(())
    }

    /// Compute SHA1 hash of data
    /// Compute SHA-1 hash of data
    /// Optimized with inline hint for hot path
    #[inline]
    pub fn compute_hash(&self, data: &[u8]) -> String {
        let mut hasher = Sha1::new();
        hasher.update(data);
        // Pre-allocate exact size needed (40 hex chars)
        let hash = hasher.finalize();
        let mut result = String::with_capacity(40);
        for byte in hash.iter() {
            use std::fmt::Write;
            let _ = write!(result, "{:02x}", byte);
        }
        result
    }

    /// Compute SHA-1 hash directly to a byte array (more efficient for comparisons)
    #[inline]
    pub fn compute_hash_bytes(&self, data: &[u8]) -> [u8; 20] {
        let mut hasher = Sha1::new();
        hasher.update(data);
        hasher.finalize().into()
    }

    /// Get current cache statistics
    pub fn get_stats(&self) -> CacheStats {
        self.stats.read().clone()
    }

    /// Get total cached size in bytes
    pub fn get_total_size(&self) -> u64 {
        self.total_size.load(Ordering::SeqCst)
    }

    /// Get number of cached files
    pub fn get_file_count(&self) -> usize {
        self.file_index.len()
    }

    /// Scan disk for files not in index (recovery)
    pub async fn scan_and_recover(&self) -> Result<u64> {
        info!("Scanning cache directory for unindexed files...");

        let mut recovered = 0u64;
        let cache_dir = self.config.cache_dir.clone();

        for entry in WalkDir::new(&cache_dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.path();
            let hash = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();

            if !self.file_index.contains_key(&hash) && hash.len() == 40 {
                // SHA1 hash is 40 hex chars
                if let Ok(metadata) = fs::metadata(path).await {
                    let now = chrono::Utc::now().timestamp();
                    let cached = CachedFile {
                        hash: hash.clone(),
                        size: metadata.len(),
                        path: path.to_path_buf(),
                        file_type: "unknown".to_string(),
                        last_accessed: now,
                        hit_count: 0,
                    };

                    // Add to database
                    let _ = sqlx::query(
                        r#"
                        INSERT OR IGNORE INTO cached_files
                        (hash, size, path, file_type, last_accessed, hit_count, created_at)
                        VALUES (?, ?, ?, ?, ?, 0, ?)
                        "#,
                    )
                    .bind(&hash)
                    .bind(metadata.len() as i64)
                    .bind(path.to_string_lossy().to_string())
                    .bind("unknown")
                    .bind(now)
                    .bind(now)
                    .execute(&self.db)
                    .await;

                    self.total_size.fetch_add(metadata.len(), Ordering::SeqCst);
                    self.file_index.insert(hash, cached);
                    recovered += 1;
                }
            }
        }

        info!("Recovered {} unindexed files", recovered);
        Ok(recovered)
    }

    /// Get database pool for gallery downloads
    pub fn get_db(&self) -> Pool<Sqlite> {
        self.db.clone()
    }
}
