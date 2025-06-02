use anyhow::{Result, Context};
#[cfg(feature = "external-services")]
use rusoto_core::Region;
#[cfg(feature = "external-services")]
use rusoto_s3::{
    GetObjectRequest, PutObjectRequest, S3Client, S3,
};
#[cfg(feature = "external-services")]
use std::io::Read;
#[cfg(feature = "external-services")]
use std::str::FromStr;

#[cfg(feature = "external-services")]
#[derive(Clone)]
pub struct S3Service {
    client: S3Client,
    bucket: String,
}

// Manual Debug implementation since S3Client doesn't implement Debug
#[cfg(feature = "external-services")]
impl std::fmt::Debug for S3Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Service")
            .field("bucket", &self.bucket)
            .field("client", &"S3Client")
            .finish()
    }
}

#[cfg(feature = "external-services")]
impl S3Service {
    pub fn new(region: String, bucket: String) -> Self {
        let region = Region::from_str(&region).unwrap_or(Region::UsEast1);
        let client = S3Client::new(region);
        Self { client, bucket }
    }

    /// Upload data to S3 bucket
    pub async fn upload_file(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let req = PutObjectRequest {
            bucket: self.bucket.clone(),
            key: key.to_string(),
            body: Some(data.into()),
            ..Default::default()
        };

        self.client.put_object(req).await?;
        Ok(())
    }

    /// Download data from S3 bucket
    pub async fn download_file(&self, key: &str) -> Result<Vec<u8>> {
        self.get_object(&self.bucket, key).await
    }
    
    /// Get object from any S3 bucket
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let req = GetObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        };

        let result = self.client.get_object(req).await
            .context(format!("Failed to get object {}/{}", bucket, key))?;
            
        let mut body = result.body.unwrap().into_blocking_read();
        let mut data = Vec::new();
        body.read_to_end(&mut data)
            .context("Failed to read object body")?;
        
        Ok(data)
    }
}
