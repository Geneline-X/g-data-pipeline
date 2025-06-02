use actix_web::{web, HttpResponse, Error};
use uuid::Uuid;

use crate::models::response::{InsightsResponse, UploadResponse, ErrorResponse};
use crate::models::job::JobStatus;
use crate::services::{DatabaseServiceTrait, RedisServiceTrait, DataProcessor, S3ServiceTrait};

/// Get insights for a job
pub async fn get_insights<S, D, R>(
    job_id: web::Path<Uuid>,
    db_service: web::Data<D>,
    redis_service: web::Data<R>,
    processor: web::Data<DataProcessor<S, D, R>>,
) -> Result<HttpResponse, Error>
where
    S: S3ServiceTrait + Clone + std::fmt::Debug + 'static,
    D: DatabaseServiceTrait + Clone + std::fmt::Debug + 'static,
    R: RedisServiceTrait + Clone + std::fmt::Debug + 'static,
{
    let job_id = job_id.into_inner();
    
    // Check if job exists
    let job = match db_service.get_job(job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => {
            return Ok(HttpResponse::NotFound().json(ErrorResponse {
                error: format!("Job with ID {} not found", job_id),
                status_code: 404,
            }));
        },
        Err(e) => {
            return Ok(HttpResponse::InternalServerError().json(ErrorResponse {
                error: format!("Database error: {}", e),
                status_code: 500,
            }));
        }
    };
    
    // If job is not completed, return status
    if job.status != JobStatus::Completed.to_string() {
        return Ok(HttpResponse::Accepted().json(UploadResponse {
            job_id,
            status: job.status.clone(),
            message: Some(format!("Job is {}", job.status.to_lowercase())),
        }));
    }
    
    // Try to get insights from Redis cache
    match redis_service.get_insights(job_id) {
        Ok(Some(insights)) => {
            // Return insights
            Ok(HttpResponse::Ok().json(InsightsResponse {
                job_id,
                status: "completed".to_string(),
                message: Some("Job completed successfully".to_string()),
                insights: match serde_json::from_str(&insights) {
                    Ok(parsed_insights) => Some(parsed_insights),
                    Err(_) => None,
                },
            }))
        },
        Ok(None) => {
            // If insights not in cache, trigger processing
            match processor.process_job(job_id).await {
                Ok(_) => {
                    // Try to get insights after processing
                    match redis_service.get_insights(job_id) {
                        Ok(Some(insights)) => {
                            Ok(HttpResponse::Ok().json(InsightsResponse {
                                job_id,
                                status: "completed".to_string(),
                                message: Some("Job completed successfully".to_string()),
                                insights: match serde_json::from_str(&insights) {
                                    Ok(parsed_insights) => Some(parsed_insights),
                                    Err(_) => None,
                                },
                            }))
                        },
                        _ => {
                            // If still no insights, return error
                            Ok(HttpResponse::InternalServerError().json(ErrorResponse {
                                error: "Failed to generate insights".to_string(),
                                status_code: 500,
                            }))
                        }
                    }
                },
                Err(e) => {
                    // Return processing error
                    Ok(HttpResponse::InternalServerError().json(ErrorResponse {
                        error: format!("Failed to process job: {}", e),
                        status_code: 500,
                    }))
                }
            }
        },
        Err(e) => {
            // Return Redis error
            Ok(HttpResponse::InternalServerError().json(ErrorResponse {
                error: format!("Cache error: {}", e),
                status_code: 500,
            }))
        }
    }
}
