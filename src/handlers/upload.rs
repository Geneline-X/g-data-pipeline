use actix_web::{web, HttpResponse, Error};
use std::sync::Arc;
use actix_multipart::Multipart;
use futures::StreamExt;
use uuid::Uuid;
use std::io::Write;
use tokio::sync::mpsc;
use actix_web::HttpRequest;

use crate::models::response::{UploadResponse, ErrorResponse};
use crate::models::job::{NewJob, JobStatus};
use crate::services::{DatabaseServiceTrait, S3ServiceTrait};

/// Handle file upload, store in S3, and create a job
pub async fn upload_csv<S, D>(
    mut payload: Multipart,
    db_service: web::Data<D>,
    s3_service: web::Data<S>,
    req: HttpRequest,
) -> Result<HttpResponse, Error>
where
    S: S3ServiceTrait,
    D: DatabaseServiceTrait,
{
    // Default user ID (in a real app, this would come from authentication)
    let user_id = "user123".to_string();
    
    // Generate a unique job ID and file key
    let job_id = Uuid::new_v4();
    let file_key = format!("uploads/{}.csv", job_id);
    
    // Process the multipart form data
    let mut file_content = Vec::new();
    let mut filename = String::new();
    
    while let Some(item) = payload.next().await {
        let mut field = item?;
        let content_disposition = field.content_disposition();
        
        if let Some(name) = content_disposition.get_name() {
            if name == "file" {
                // Get the original filename
                if let Some(fname) = content_disposition.get_filename() {
                    filename = fname.to_string();
                }
                
                // Read the file data
                while let Some(chunk) = field.next().await {
                    let data = chunk?;
                    file_content.write_all(&data)?;
                }
            }
        }
    }
    
    // Validate the file
    if file_content.is_empty() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "No file uploaded".to_string(),
            status_code: 400,
        }));
    }
    
    if !filename.to_lowercase().ends_with(".csv") {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "File must be a CSV".to_string(),
            status_code: 400,
        }));
    }
    
    // Upload file to S3
    match s3_service.upload_file(&file_key, file_content).await {
        Ok(_) => {
            // Create job in database
            let new_job = NewJob {
                user_id: user_id.clone(),
                file_key: file_key.clone(),
            };
            
            match db_service.create_job(new_job).await {
                Ok(job_id) => {
                    // Get the job queue sender
                    log::info!("🔄 Attempting to queue job: {} for processing", job_id);
                    if let Some(tx) = req.app_data::<web::Data<Arc<mpsc::Sender<Uuid>>>>() {
                        // Send job to the worker
                        match tx.send(job_id).await {
                            Ok(_) => log::info!("✅ Successfully queued job: {} for processing", job_id),
                            Err(e) => {
                                log::error!("❌ Failed to queue job: {} - Error: {}", job_id, e);
                                return Ok(HttpResponse::InternalServerError().json(ErrorResponse {
                                    error: format!("Failed to queue job: {}", e),
                                    status_code: 500,
                                }));
                            }
                        }
                    } else {
                        log::error!("❌ Job queue sender not found in app_data");
                        return Ok(HttpResponse::InternalServerError().json(ErrorResponse {
                            error: "Job queue unavailable".to_string(),
                            status_code: 500,
                        }));
                    }
                    
                    // Return success response
                    let status = JobStatus::Queued.to_string();
                    Ok(HttpResponse::Ok().json(UploadResponse {
                        job_id,
                        status: status.clone(),
                        message: Some(format!("File uploaded and job queued for processing. Status: {}", status)),
                    }))
                },
                Err(e) => {
                    // Return database error
                    Ok(HttpResponse::InternalServerError().json(ErrorResponse {
                        error: format!("Failed to create job: {}", e),
                        status_code: 500,
                    }))
                }
            }
        },
        Err(e) => {
            // Return S3 upload error
            Ok(HttpResponse::InternalServerError().json(ErrorResponse {
                error: format!("Failed to upload file: {}", e),
                status_code: 500,
            }))
        }
    }
}
