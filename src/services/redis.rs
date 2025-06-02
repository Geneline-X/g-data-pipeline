use anyhow::{Result, Context};
#[cfg(feature = "external-services")]
use redis::{Client, Commands, Connection};
use uuid::Uuid;
use crate::models::response::Insights;

#[cfg(feature = "external-services")]
#[derive(Clone, Debug)]
pub struct RedisService {
    client: Client,
}

#[cfg(feature = "external-services")]
impl RedisService {
    pub fn new(redis_url: &str) -> Result<Self> {
        let client = Client::open(redis_url)?;
        Ok(Self { client })
    }

    /// Get a connection to Redis
    fn get_connection(&self) -> Result<Connection> {
        let conn = self.client.get_connection()?;
        Ok(conn)
    }
    
    pub fn set_with_expiry(&self, key: &str, value: &str, expiry_secs: u64) -> Result<()> {
        let mut conn = self.get_connection()?;
        conn.set_ex::<_, _, ()>(key, value, expiry_secs as usize)?;
        Ok(())
    }
    
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.get_connection()?;
        let result: redis::RedisResult<String> = conn.get(key);
        match result {
            Ok(value) => Ok(Some(value)),
            Err(_) => Ok(None),
        }
    }
    
    pub fn get_value(&self, key: &str) -> Result<Option<String>> {
        self.get(key)
    }
    
    pub fn delete(&self, key: &str) -> Result<()> {
        let mut conn = self.get_connection()?;
        let _: () = conn.del(key)?;
        Ok(())
    }

    /// Cache insights for a job
    pub fn cache_insights(&self, job_id: Uuid, insights: &Insights) -> Result<()> {
        let key = format!("insights:{}", job_id);
        let serialized = serde_json::to_string(insights)
            .context("Failed to serialize insights")?;
        
        self.set_with_expiry(&key, &serialized, 3600 * 24)?;
        
        Ok(())
    }

    /// Get cached insights for a job
    pub fn get_insights(&self, job_id: Uuid) -> Result<Option<Insights>> {
        let key = format!("insights:{}", job_id);
        
        if let Some(serialized) = self.get(&key)? {
            let insights: Insights = serde_json::from_str(&serialized)
                .context("Failed to deserialize insights")?;
            Ok(Some(insights))
        } else {
            Ok(None)
        }
    }

    /// Cache chart URL for a job
    pub fn cache_chart_url(&self, job_id: Uuid, chart_url: &str) -> Result<()> {
        let key = format!("chart_url:{}", job_id);
        
        self.set_with_expiry(&key, chart_url, 3600 * 24)?;
        
        Ok(())
    }

    /// Get cached chart URL for a job
    pub fn get_chart_url(&self, job_id: Uuid) -> Result<Option<String>> {
        let key = format!("chart_url:{}", job_id);
        self.get(&key)
    }
}
