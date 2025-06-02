#[cfg(feature = "external-services")]
use anyhow::Result;
#[cfg(feature = "external-services")]
use sqlx::postgres::PgPool;
use uuid::Uuid;

use crate::models::job::{Job, JobStatus, NewJob};

#[cfg(feature = "external-services")]
#[derive(Clone, Debug)]
pub struct DatabaseService {
    pool: PgPool,
}

#[cfg(feature = "external-services")]
impl DatabaseService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    
    /// Create a new job in the database
    pub async fn create_job(&self, new_job: NewJob) -> Result<Uuid> {
        let job_id = Uuid::new_v4();
        let status = JobStatus::Queued.to_string();
        
        sqlx::query!("INSERT INTO jobs (id, user_id, file_key, status) VALUES ($1, $2, $3, $4) RETURNING id",
            job_id,
            new_job.user_id,
            new_job.file_key,
            status
        )
        .fetch_one(&self.pool)
        .await?;
        
        Ok(job_id)
    }
    
    /// Get a job by ID
    pub async fn get_job(&self, job_id: Uuid) -> Result<Option<Job>> {
        let job = sqlx::query_as!(Job,
            "SELECT id, user_id, file_key, status as \"status: JobStatus\", created_at, updated_at FROM jobs WHERE id = $1",
            job_id
        )
        .fetch_optional(&self.pool)
        .await?;
        
        Ok(job)
    }
    
    /// Update job status
    pub async fn update_job_status(&self, job_id: Uuid, status: JobStatus) -> Result<()> {
        sqlx::query!("UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2",
            status.to_string(),
            job_id
        )
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }
}
