use serde::{Deserialize, Serialize};
#[cfg(feature = "external-services")]
use sqlx::FromRow;
use uuid::Uuid;
use std::time::SystemTime;

/// Represents the status of a data processing job
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobStatus {
    #[serde(rename = "queued")]
    Queued,
    #[serde(rename = "processing")]
    Processing,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "failed")]
    Failed,
}

impl ToString for JobStatus {
    fn to_string(&self) -> String {
        match self {
            JobStatus::Queued => "queued".to_string(),
            JobStatus::Processing => "processing".to_string(),
            JobStatus::Completed => "completed".to_string(),
            JobStatus::Failed => "failed".to_string(),
        }
    }
}

/// Represents a data processing job in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "external-services", derive(FromRow))]
pub struct Job {
    pub id: Uuid,
    pub user_id: String,
    pub file_key: String,
    pub status: String,
    pub created_at: Option<SystemTime>,
    pub updated_at: Option<SystemTime>,
}

/// Represents a new job to be created
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewJob {
    pub user_id: String,
    pub file_key: String,
}

/// Represents job metadata for client responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMetadata {
    pub id: Uuid,
    pub status: String,
    pub created_at: Option<SystemTime>,
}
