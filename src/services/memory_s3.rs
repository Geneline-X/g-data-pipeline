use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::fs::{self, File};
use std::io::{Write, Read};
use std::path::Path;
use log::{info, error};

#[derive(Clone, Debug)]
pub struct MemoryS3Service {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    storage_dir: String,
}

impl MemoryS3Service {
    pub fn new() -> Self {
        // Create storage directory if it doesn't exist
        let storage_dir = "./storage";
        if !Path::new(storage_dir).exists() {
            fs::create_dir_all(storage_dir).unwrap_or_else(|e| {
                error!("Failed to create storage directory: {}", e);
            });
        }
        
        info!("🗄️ Memory S3 service initialized with storage directory: {}", storage_dir);
        
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            storage_dir: storage_dir.to_string(),
        }
    }

    /// Upload data to in-memory storage and save to disk
    pub async fn upload_file(&self, key: &str, data: Vec<u8>) -> Result<()> {
        info!("📤 Uploading file to key: {} (size: {} bytes)", key, data.len());
        
        // Store in memory
        let mut storage = self.data.lock().map_err(|e| {
            error!("Failed to lock storage: {}", e);
            anyhow!("Failed to lock storage")
        })?;
        storage.insert(key.to_string(), data.clone());
        
        // Also save to disk for debugging/verification
        let file_path = self.get_file_path(key);
        let dir_path = Path::new(&file_path).parent().unwrap();
        
        // Create directory if it doesn't exist
        if !dir_path.exists() {
            fs::create_dir_all(dir_path).map_err(|e| {
                error!("Failed to create directory {}: {}", dir_path.display(), e);
                anyhow!("Failed to create directory: {}", e)
            })?;
        }
        
        // Write file to disk
        let mut file = File::create(&file_path).map_err(|e| {
            error!("Failed to create file {}: {}", file_path, e);
            anyhow!("Failed to create file: {}", e)
        })?;
        
        file.write_all(&data).map_err(|e| {
            error!("Failed to write to file {}: {}", file_path, e);
            anyhow!("Failed to write to file: {}", e)
        })?;
        
        info!("✅ File saved to disk at: {}", file_path);
        Ok(())
    }

    /// Download data from in-memory storage
    pub async fn download_file(&self, key: &str) -> Result<Vec<u8>> {
        self.get_object("default-bucket", key).await
    }
    
    /// Get object from in-memory storage or disk if not in memory
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        info!("🔍 Retrieving object: {}/{}", bucket, key);
        
        // Try to get from memory first. Lock is scoped to release before any .await.
        let maybe_data_from_memory: Option<Vec<u8>> = {
            let storage = self.data.lock().map_err(|e| {
                error!("Failed to lock storage for memory check: {}", e);
                anyhow!("Failed to lock storage for memory check")
            })?;
            let full_key = format!("{}/{}", bucket, key);
            if let Some(data) = storage.get(&full_key) {
                info!("✅ Found data in memory at full key: {} (size: {} bytes)", full_key, data.len());
                Some(data.clone())
            } else if let Some(data) = storage.get(key) {
                info!("✅ Found data in memory at key: {} (size: {} bytes)", key, data.len());
                Some(data.clone())
            } else {
                None
            }
            // MutexGuard `storage` is dropped here
        };

        if let Some(data) = maybe_data_from_memory {
            return Ok(data); // Return if found in memory
        }

        // If not in memory, proceed to disk read. No MutexGuard held here.
        let file_path = self.get_file_path(key);
        let file_path_for_blocking = file_path.clone();
        if Path::new(&file_path).exists() {
            let data_from_disk = tokio::task::spawn_blocking(move || {
                info!("[BLOCKING_TASK] Attempting to open file: {}", file_path_for_blocking);
                let mut file = File::open(&file_path_for_blocking).map_err(|e| {
                    error!("[BLOCKING_TASK] Failed to open file {}: {}", file_path_for_blocking, e);
                    std::io::Error::new(e.kind(), format!("Failed to open file {}: {}", file_path_for_blocking, e))
                })?;
                info!("[BLOCKING_TASK] Successfully opened file. Attempting to read: {}", file_path_for_blocking);
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer).map_err(|e| {
                    error!("[BLOCKING_TASK] Failed to read file {}: {}", file_path_for_blocking, e);
                    std::io::Error::new(e.kind(), format!("Failed to read file {}: {}", file_path_for_blocking, e))
                })?;
                info!("[BLOCKING_TASK] Successfully read file (size: {} bytes): {}", buffer.len(), file_path_for_blocking);
                Ok::<_, std::io::Error>(buffer)
            })
            .await // Async thread awaits completion
            .inspect(|res| { // Log immediately after await returns
                match res {
                    Ok(_) => info!("[ASYNC_TASK] spawn_blocking for file read completed successfully."),
                    Err(join_error) => error!("[ASYNC_TASK] spawn_blocking for file read failed with JoinError: {}", join_error),
                }
            })
            .map_err(|e| anyhow!("Task join error during file reading: {}", e))? // Handle JoinError
            .map_err(|e| { // Handle std::io::Error from file operations
                error!("I/O error during spawned file read (spawn_blocking task): {}", e);
                anyhow!("I/O error during spawned file read (spawn_blocking task): {}", e)
            })?;
            let mut storage = self.data.lock().map_err(|e| {
                error!("Failed to lock storage: {}", e);
                anyhow!("Failed to lock storage")
            })?;
            storage.insert(key.to_string(), data_from_disk.clone());
            
            info!("✅ Read file from disk: {} (size: {} bytes)", file_path, data_from_disk.len());
            return Ok(data_from_disk);
        }
        
        // Not found anywhere
        error!("❌ Object not found: {}/{}", bucket, key);
        Err(anyhow!("Object not found: {}/{}", bucket, key))
    }
    
    // Helper method to get file path on disk
    fn get_file_path(&self, key: &str) -> String {
        format!("{}/{}", self.storage_dir, key)
    }
    
    // List all files in storage (both memory and disk)
    pub fn list_files(&self) -> Vec<String> {
        let mut files = Vec::new();
        
        // Get files from memory
        if let Ok(storage) = self.data.lock() {
            for key in storage.keys() {
                files.push(key.clone());
            }
        }
        
        info!("📋 Files in storage: {:?}", files);
        files
    }
}
