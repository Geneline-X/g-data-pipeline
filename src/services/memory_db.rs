use anyhow::{Result, anyhow};
use uuid::Uuid;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::models::job::{Job, JobStatus, NewJob};

#[derive(Clone, Debug)]
pub struct MemoryDatabaseService {
    jobs: Arc<Mutex<HashMap<Uuid, Job>>>,
}

impl MemoryDatabaseService {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Create a new job in the in-memory database
    pub async fn create_job(&self, new_job: NewJob) -> Result<Uuid> {
        let job_id = Uuid::new_v4();
        let status = JobStatus::Queued.to_string();
        let now = Some(SystemTime::now());
        
        let job = Job {
            id: job_id,
            user_id: new_job.user_id,
            file_key: new_job.file_key,
            status,
            created_at: now,
            updated_at: now,
        };
        
        let mut jobs = self.jobs.lock().map_err(|_| anyhow!("Failed to lock jobs"))?;
        jobs.insert(job_id, job);
        
        Ok(job_id)
    }
    
    /// Get a job by ID
    pub async fn get_job(&self, job_id: Uuid) -> Result<Option<Job>> {
        let jobs = self.jobs.lock().map_err(|_| anyhow!("Failed to lock jobs"))?;
        Ok(jobs.get(&job_id).cloned())
    }
    
    /// Update job status
    pub async fn update_job_status(&self, job_id: Uuid, status: JobStatus) -> Result<()> {
        let mut jobs = self.jobs.lock().map_err(|_| anyhow!("Failed to lock jobs"))?;
        
        if let Some(job) = jobs.get_mut(&job_id) {
            job.status = status.to_string();
            job.updated_at = Some(SystemTime::now());
            Ok(())
        } else {
            Err(anyhow!("Job not found"))
        }
    }
}
