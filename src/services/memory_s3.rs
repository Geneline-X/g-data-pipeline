use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::fs::{self, File};
use std::io::{Write, Read};
use std::path::Path;
use log::{info, error, debug};

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
        
        // Try to get from memory first
        let storage = self.data.lock().map_err(|e| {
            error!("Failed to lock storage: {}", e);
            anyhow!("Failed to lock storage")
        })?;
        
        let full_key = format!("{}/{}", bucket, key);
        
        // Check memory cache first
        if let Some(data) = storage.get(&full_key) {
            info!("✅ Found data in memory at full key: {} (size: {} bytes)", full_key, data.len());
            return Ok(data.clone());
        } 
        
        if let Some(data) = storage.get(key) {
            info!("✅ Found data in memory at key: {} (size: {} bytes)", key, data.len());
            return Ok(data.clone());
        }
        
        // If not in memory, try to read from disk
        let file_path = self.get_file_path(key);
        if Path::new(&file_path).exists() {
            info!("🔍 Reading file from disk: {}", file_path);
            let mut file = File::open(&file_path).map_err(|e| {
                error!("Failed to open file {}: {}", file_path, e);
                anyhow!("Failed to open file: {}", e)
            })?;
            
            let mut data = Vec::new();
            file.read_to_end(&mut data).map_err(|e| {
                error!("Failed to read file {}: {}", file_path, e);
                anyhow!("Failed to read file: {}", e)
            })?;
            
            // Store in memory for next time
            let mut storage = self.data.lock().map_err(|e| {
                error!("Failed to lock storage: {}", e);
                anyhow!("Failed to lock storage")
            })?;
            storage.insert(key.to_string(), data.clone());
            
            info!("✅ Read file from disk: {} (size: {} bytes)", file_path, data.len());
            return Ok(data);
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
