use actix_web::{web, HttpResponse, Error};
use log::{info, error};
use std::sync::Arc;

use crate::models::conversation::QueryRequest;
use crate::services::conversation::ConversationService;
use crate::services::{S3ServiceTrait, DatabaseServiceTrait, RedisServiceTrait};

/// Handle a natural language query about a dataset
pub async fn query_endpoint<S, D, R>(
    query_req: web::Json<QueryRequest>,
    conversation_service: web::Data<Arc<ConversationService<S, D, R>>>,
) -> Result<HttpResponse, Error>
where
    S: S3ServiceTrait + Clone + std::fmt::Debug,
    D: DatabaseServiceTrait + Clone + std::fmt::Debug,
    R: RedisServiceTrait + Clone + std::fmt::Debug,
{
    info!("Received query: {}", query_req.query);
    
    // Process the query
    match conversation_service.process_query(query_req.into_inner()).await {
        Ok(response) => {
            info!("Query processed successfully");
            Ok(HttpResponse::Ok().json(response))
        },
        Err(e) => {
            error!("Error processing query: {}", e);
            Ok(HttpResponse::InternalServerError().json(format!("Error processing query: {}", e)))
        }
    }
}
