use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct MemoryRedisService {
    data: Arc<Mutex<HashMap<String, (String, Option<Instant>)>>>,
}

impl MemoryRedisService {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get a connection (no-op in memory implementation)
    fn get_connection(&self) -> Result<()> {
        Ok(())
    }
    
    pub fn set_with_expiry(&self, key: &str, value: &str, expiry_secs: u64) -> Result<()> {
        let expiry = if expiry_secs > 0 {
            Some(Instant::now() + Duration::from_secs(expiry_secs))
        } else {
            None
        };
        
        let mut data = self.data.lock().map_err(|_| anyhow!("Failed to lock data"))?;
        data.insert(key.to_string(), (value.to_string(), expiry));
        Ok(())
    }
    
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let mut data = self.data.lock().map_err(|_| anyhow!("Failed to lock data"))?;
        
        // Check if key exists and is not expired
        if let Some((value, expiry)) = data.get(key) {
            if let Some(expiry_time) = expiry {
                if expiry_time < &Instant::now() {
                    // Key has expired, remove it
                    data.remove(key);
                    return Ok(None);
                }
            }
            return Ok(Some(value.clone()));
        }
        
        Ok(None)
    }
    
    pub fn delete(&self, key: &str) -> Result<()> {
        let mut data = self.data.lock().map_err(|_| anyhow!("Failed to lock data"))?;
        data.remove(key);
        Ok(())
    }

    /// Set a value with an optional expiry
    pub fn set_value(&self, key: &str, value: &str) -> Result<()> {
        let mut data = self.data.lock().map_err(|_| anyhow!("Failed to lock data"))?;
        data.insert(key.to_string(), (value.to_string(), None));
        Ok(())
    }
    
    /// Get a value
    pub fn get_value(&self, key: &str) -> Result<Option<String>> {
        self.get(key)
    }




}
