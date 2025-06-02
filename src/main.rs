mod config;
mod models;
mod services;
mod handlers;

use actix_web::{web, App, HttpServer, middleware::Logger, HttpResponse};
use std::sync::Arc;
use tokio::sync::mpsc;

use config::Config;
use services::DataProcessor;
use services::memory_s3::MemoryS3Service;
use services::memory_db::MemoryDatabaseService;
use services::memory_redis::MemoryRedisService;
use handlers::{upload_csv, get_insights};
use uuid::Uuid;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    log::info!("🚀 Starting Data Processing API");
    
    // Load configuration from environment variables
    let config = Config::from_env();
    
    // Initialize in-memory services
    log::info!("💾 Using in-memory services for local development");
    let s3_service = MemoryS3Service::new();
    let db_service = MemoryDatabaseService::new();
    let redis_service = MemoryRedisService::new();
    
    // Initialize data processor
    let processor = DataProcessor::new(
        s3_service.clone(),
        db_service.clone(),
        redis_service.clone(),
        config.s3_bucket.clone(),
    );
    
    // Create a channel for job processing
    let (tx, mut rx) = mpsc::channel::<Uuid>(32);
    let tx = Arc::new(tx);
    
    // Start background worker
    let processor_clone = processor.clone();
    tokio::spawn(async move {
        log::info!("🔵 Background worker started and ready to process jobs");
        let mut job_count = 0;
        
        // Log channel status periodically
        let channel_capacity = rx.capacity();
        log::info!("📊 Job queue channel initialized with capacity: {}", channel_capacity);
        
        while let Some(job_id) = rx.recv().await {
            job_count += 1;
            log::info!("🔄 [Job-{}] Received job for processing (total processed: {})", job_id, job_count);
            log::info!("📋 [Job-{}] Current channel status: {} slots available", job_id, rx.capacity());
            
            let start_time = std::time::Instant::now();
            log::info!("🚀 [Job-{}] Starting processing at {:?}", job_id, std::time::SystemTime::now());
            
            match processor_clone.process_job(job_id).await {
                Ok(_) => {
                    let duration = start_time.elapsed();
                    log::info!("✅ [Job-{}] Completed successfully in {:.2?}", job_id, duration);
                    log::info!("📈 [Job-{}] Processing stats: Duration={:.2?}", job_id, duration);
                },
                Err(e) => {
                    let duration = start_time.elapsed();
                    log::error!("❌ [Job-{}] Failed after {:.2?}: {}", job_id, duration, e);
                    log::error!("🔍 [Job-{}] Error details: {:#?}", job_id, e);
                }
            }
        }
        log::warn!("🛑 Background worker shutting down (total jobs processed: {})", job_count);
    });
    
    // Start HTTP server
    let server_url = format!("http://127.0.0.1:{}", config.server_port);
    log::info!("🌐 Starting server at {}", server_url);
    
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(s3_service.clone()))
            .app_data(web::Data::new(db_service.clone()))
            .app_data(web::Data::new(redis_service.clone()))
            .app_data(web::Data::new(processor.clone()))
            .app_data(web::Data::new(tx.clone()))
            .service(
                web::resource("/upload")
                    .route(web::post().to(upload_csv::<MemoryS3Service, MemoryDatabaseService>))
            )
            .service(
                web::resource("/insights/{job_id}")
                    .route(web::get().to(get_insights::<MemoryS3Service, MemoryDatabaseService, MemoryRedisService>))
            )
            .service(
                web::resource("/debug/files")
                    .route(web::get().to(|s3: web::Data<MemoryS3Service>| async move {
                        let files = s3.list_files();
                        HttpResponse::Ok().json(files)
                    }))
            )
    })
    .bind(format!("127.0.0.1:{}", config.server_port))
    .map_err(|e| {
        log::error!("❌ Failed to bind to port {}: {}", config.server_port, e);
        e
    })?
    .run()
    .await
}