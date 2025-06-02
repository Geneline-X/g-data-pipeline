pub mod s3;
pub mod database;
pub mod redis;
pub mod processor;
pub mod memory_db;
pub mod memory_redis;
pub mod memory_s3;

use anyhow::Result;

// Define traits for service functionality
#[async_trait::async_trait]
pub trait S3ServiceTrait: Send + Sync + 'static {
    async fn upload_file(&self, key: &str, data: Vec<u8>) -> Result<()>;
    async fn download_file(&self, key: &str) -> Result<Vec<u8>>;
    async fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>>;
}

#[async_trait::async_trait]
pub trait DatabaseServiceTrait: Send + Sync + 'static {
    async fn create_job(&self, new_job: crate::models::job::NewJob) -> Result<uuid::Uuid>;
    async fn get_job(&self, job_id: uuid::Uuid) -> Result<Option<crate::models::job::Job>>;
    async fn update_job_status(&self, job_id: uuid::Uuid, status: crate::models::job::JobStatus) -> Result<()>;
}

#[async_trait::async_trait]
pub trait RedisServiceTrait: Send + Sync + 'static {
    fn get_insights(&self, job_id: uuid::Uuid) -> Result<Option<String>>;
    fn cache_insights(&self, job_id: uuid::Uuid, insights: &crate::models::response::Insights) -> Result<()>;
}

// Implement the traits for both real and memory services
#[cfg(feature = "external-services")]
#[async_trait::async_trait]
impl S3ServiceTrait for s3::S3Service {
    async fn upload_file(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.upload_file(key, data).await
    }
    
    async fn download_file(&self, key: &str) -> Result<Vec<u8>> {
        self.download_file(key).await
    }
    
    async fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        self.get_object(bucket, key).await
    }
}

#[async_trait::async_trait]
impl S3ServiceTrait for memory_s3::MemoryS3Service {
    async fn upload_file(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.upload_file(key, data).await
    }
    
    async fn download_file(&self, key: &str) -> Result<Vec<u8>> {
        self.download_file(key).await
    }
    
    async fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        self.get_object(bucket, key).await
    }
}

#[cfg(feature = "external-services")]
#[async_trait::async_trait]
impl DatabaseServiceTrait for database::DatabaseService {
    async fn create_job(&self, new_job: crate::models::job::NewJob) -> Result<uuid::Uuid> {
        self.create_job(new_job).await
    }
    
    async fn get_job(&self, job_id: uuid::Uuid) -> Result<Option<crate::models::job::Job>> {
        self.get_job(job_id).await
    }
    
    async fn update_job_status(&self, job_id: uuid::Uuid, status: crate::models::job::JobStatus) -> Result<()> {
        self.update_job_status(job_id, status).await
    }
}

#[async_trait::async_trait]
impl DatabaseServiceTrait for memory_db::MemoryDatabaseService {
    async fn create_job(&self, new_job: crate::models::job::NewJob) -> Result<uuid::Uuid> {
        self.create_job(new_job).await
    }
    
    async fn get_job(&self, job_id: uuid::Uuid) -> Result<Option<crate::models::job::Job>> {
        self.get_job(job_id).await
    }
    
    async fn update_job_status(&self, job_id: uuid::Uuid, status: crate::models::job::JobStatus) -> Result<()> {
        self.update_job_status(job_id, status).await
    }
}

#[cfg(feature = "external-services")]
#[async_trait::async_trait]
impl RedisServiceTrait for redis::RedisService {
    fn get_insights(&self, job_id: uuid::Uuid) -> Result<Option<String>> {
        self.get_value(&format!("insights:{}", job_id))
    }
    
    fn cache_insights(&self, job_id: uuid::Uuid, insights: &crate::models::response::Insights) -> Result<()> {
        let insights_json = serde_json::to_string(insights)?;
        self.set_with_expiry(&format!("insights:{}", job_id), &insights_json, 3600 * 24)
    }
}

#[async_trait::async_trait]
impl RedisServiceTrait for memory_redis::MemoryRedisService {
    fn get_insights(&self, job_id: uuid::Uuid) -> Result<Option<String>> {
        self.get_value(&format!("insights:{}", job_id))
    }
    
    fn cache_insights(&self, job_id: uuid::Uuid, insights: &crate::models::response::Insights) -> Result<()> {
        let insights_json = serde_json::to_string(insights)?;
        self.set_value(&format!("insights:{}", job_id), &insights_json)
    }
}

// Re-export the services
#[cfg(feature = "external-services")]
pub use database::DatabaseService;
#[cfg(feature = "external-services")]
pub use redis::RedisService;
#[cfg(feature = "external-services")]
pub use s3::S3Service;
pub use processor::DataProcessor;
