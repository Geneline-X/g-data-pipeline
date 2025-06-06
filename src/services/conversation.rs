use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use anyhow::{Result, anyhow, Context};
use log::{info, warn, error};
use serde_json::{Value, json};
use uuid::Uuid;
use polars::prelude::*;
use polars::io::json::{JsonWriter, JsonFormat};

use crate::models::conversation::{
    ConversationContext, QueryRequest, QueryResponse, DatasetMetadata
};
use crate::services::ai::AIService;
use crate::services::processor::DataProcessor;
use crate::services::{S3ServiceTrait, DatabaseServiceTrait, RedisServiceTrait};
use crate::services::query_translator::{QueryTranslator, StructuredQuery};

/// In-memory store for conversation contexts
#[derive(Debug, Clone)]
pub struct InMemoryStore {
    conversations: Arc<Mutex<HashMap<String, ConversationContext>>>,
}

impl InMemoryStore {
    /// Create a new in-memory store
    pub fn new() -> Self {
        Self {
            conversations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Store a conversation context
    pub fn store(&self, context: ConversationContext) -> Result<()> {
        let mut conversations = self.conversations.lock()
            .map_err(|_| anyhow!("Failed to acquire lock on conversations"))?;
        
        conversations.insert(context.id.clone(), context);
        Ok(())
    }

    /// Get a conversation context by ID
    pub fn get(&self, id: &str) -> Result<Option<ConversationContext>> {
        let conversations = self.conversations.lock()
            .map_err(|_| anyhow!("Failed to acquire lock on conversations"))?;
        
        Ok(conversations.get(id).cloned())
    }
}

/// Service for managing conversational interactions with datasets
#[derive(Clone)]
pub struct ConversationService<S, D, R>
where
    S: S3ServiceTrait + Clone + std::fmt::Debug,
    D: DatabaseServiceTrait + Clone + std::fmt::Debug,
    R: RedisServiceTrait + Clone + std::fmt::Debug,
{
    store: InMemoryStore,
    ai_service: Option<AIService>,
    data_processor: DataProcessor<S, D, R>,
    query_translator: QueryTranslator,
}

impl<S, D, R> ConversationService<S, D, R>
where
    S: S3ServiceTrait + Clone + std::fmt::Debug,
    D: DatabaseServiceTrait + Clone + std::fmt::Debug,
    R: RedisServiceTrait + Clone + std::fmt::Debug,
{
    /// Create a new conversation service
    pub fn new(
        ai_service: Option<AIService>,
        data_processor: DataProcessor<S, D, R>,
    ) -> Self {
        // Create a new QueryTranslator with a clone of the AIService if available
        let query_translator = if let Some(ai) = &ai_service {
            QueryTranslator::new(Some(ai.clone()))
        } else {
            QueryTranslator::new(None)
        };
        
        Self {
            store: InMemoryStore::new(),
            ai_service,
            data_processor,
            query_translator,
        }
    }

    /// Process a natural language query
    pub async fn process_query(&self, request: QueryRequest) -> Result<QueryResponse> {
        info!("Processing query: {}", request.query);
        
        // Get or create conversation context
        let mut context = match &request.conversation_id {
            Some(id) => {
                match self.store.get(id)? {
                    Some(ctx) => {
                        info!("Found existing conversation: {}", id);
                        ctx
                    },
                    None => {
                        warn!("Conversation ID not found: {}, creating new", id);
                        self.create_context(&request.job_id).await?
                    }
                }
            },
            None => {
                info!("Creating new conversation for job: {}", request.job_id);
                self.create_context(&request.job_id).await?
            }
        };
        
        // Translate the query to a structured query
        let structured_query = match self.query_translator.translate_query(&request.query, &context).await {
            Ok(query) => query,
            Err(e) => {
                error!("Failed to translate query: {}", e);
                return Ok(QueryResponse {
                    conversation_id: context.id,
                    response: format!("I couldn't understand your query: {}", e),
                    data: None,
                    visualization_data: None,
                });
            }
        };

        // Execute the structured query
        let s3_service = self.data_processor.get_s3_service();
        let df = match self.query_translator.execute_query(&structured_query, &context.job_id, s3_service).await {
            Ok(df) => df,
            Err(e) => {
                error!("Failed to execute query: {}", e);
                return Ok(QueryResponse {
                    conversation_id: context.id,
                    response: format!("I couldn't execute your query: {}", e),
                    data: None,
                    visualization_data: None,
                });
            }
        };

        // Check if the DataFrame is empty
        if df.height() == 0 {
            return Ok(QueryResponse {
                conversation_id: context.id,
                response: "No data found for your query.".to_string(),
                data: Some(json!({"result": "empty"})),
                visualization_data: None,
            });
        }

        // Convert the DataFrame to JSON using JsonWriter
        let json_result = match {
            let mut buf = Vec::new();
            let mut df_mut = df.clone();
            JsonWriter::new(&mut buf)
                .with_json_format(JsonFormat::Json)
                .finish(&mut df_mut)
                .context("Failed to write DataFrame to JSON")?;
            let json_string = std::str::from_utf8(&buf)
                .context("Failed to convert JSON bytes to string")?
                .to_string();
            serde_json::from_str::<Value>(&json_string)
                .context("Failed to parse JSON string into Value")
        } {
            Ok(json_value) => json_value,
            Err(e) => {
                error!("Failed to convert DataFrame to JSON: {}", e);
                return Ok(QueryResponse {
                    conversation_id: context.id,
                    response: format!("I couldn't format the results: {}", e),
                    data: None,
                    visualization_data: None,
                });
            }
        };

        // Prepare visualization_data if intent is Visualize
        let mut visualization_data = None;
        use crate::services::query_translator::QueryIntent;
        if let QueryIntent::Visualize = structured_query.intent {
            if let Some(data_array) = json_result.as_array() {
                if !data_array.is_empty() {
                    let first_row = &data_array[0];
                    if let Some(obj) = first_row.as_object() {
                        // Try numeric columns (for averages, distributions)
                        let mut numeric_cols: Vec<String> = Vec::new();
                        for (k, v) in obj.iter() {
                            if v.is_number() || (v.is_string() && v.as_str().unwrap().trim().parse::<f64>().is_ok()) {
                                numeric_cols.push(k.clone());
                            }
                        }
                        if !numeric_cols.is_empty() {
                            // Compute averages for each numeric column
                            let mut averages = Vec::new();
                            for col in &numeric_cols {
                                let mut sum = 0.0;
                                let mut count = 0.0;
                                for row in data_array.iter() {
                                    if let Some(val) = row.get(col) {
                                        if val.is_number() {
                                            if let Some(f) = val.as_f64() {
                                                sum += f;
                                                count += 1.0;
                                            }
                                        } else if val.is_string() {
                                            if let Ok(f) = val.as_str().unwrap().trim().parse::<f64>() {
                                                sum += f;
                                                count += 1.0;
                                            }
                                        }
                                    }
                                }
                                if count > 0.0 {
                                    averages.push(sum / count);
                                } else {
                                    averages.push(0.0);
                                }
                            }
                            let chart_json = serde_json::json!({
                                "type": "bar",
                                "data": {
                                    "labels": numeric_cols,
                                    "datasets": [{
                                        "label": "Average",
                                        "data": averages
                                    }]
                                },
                                "options": {}
                            });
                            visualization_data = Some(chart_json);
                        } else {
                            // Try categorical columns (value counts)
                            let mut categorical_cols: Vec<String> = Vec::new();
                            for (k, v) in obj.iter() {
                                if v.is_string() {
                                    categorical_cols.push(k.clone());
                                }
                            }
                            if !categorical_cols.is_empty() {
                                let col = &categorical_cols[0];
                                let mut counts = std::collections::HashMap::new();
                                for row in data_array.iter() {
                                    if let Some(val) = row.get(col) {
                                        if let Some(s) = val.as_str() {
                                            *counts.entry(s.to_string()).or_insert(0) += 1;
                                        }
                                    }
                                }
                                let mut labels = Vec::new();
                                let mut values = Vec::new();
                                for (label, value) in counts.iter() {
                                    labels.push(label.clone());
                                    values.push(*value);
                                }
                                let chart_json = serde_json::json!({
                                    "type": "bar",
                                    "data": {
                                        "labels": labels,
                                        "datasets": [{
                                            "label": format!("{} count", col),
                                            "data": values
                                        }]
                                    },
                                    "options": {}
                                });
                                visualization_data = Some(chart_json);
                            } else {
                                // Fallback: no suitable columns found, show a table config
                                let columns: Vec<String> = obj.keys().cloned().collect();
                                let rows: Vec<Vec<String>> = data_array.iter().map(|row| {
                                    columns.iter().map(|col| {
                                        row.get(col).map(|v| v.to_string()).unwrap_or_default()
                                    }).collect()
                                }).collect();
                                let chart_json = serde_json::json!({
                                    "type": "table",
                                    "data": {
                                        "columns": columns,
                                        "rows": rows
                                    },
                                    "options": {}
                                });
                                visualization_data = Some(chart_json);
                            }
                        }
                    }
                }
            }
        }

        // Generate a dynamic AI response
        let ai_response = if let Some(ai_service) = &self.ai_service {
            // Compose a prompt with query, intent, and a sample of the data
            let prompt = json!({
                "query": request.query,
                "intent": format!("{:?}", structured_query.intent),
                "result_sample": json_result.as_array().and_then(|arr| arr.get(0)).cloned().unwrap_or(json!({})),
                "result_columns": df.get_column_names(),
                "result_row_count": df.height(),
            });
            match ai_service.generate_data_summary(&prompt).await {
                Ok(summary) => summary.summary,
                Err(e) => {
                    error!("AIService failed to generate summary: {}", e);
                    "Here are the results for your query.".to_string()
                }
            }
        } else {
            "Here are the results for your query.".to_string()
        };

        // Add the real AI response to the conversation
        context.add_turn(request.query.clone(), ai_response.clone());
        self.store.store(context.clone())?;

        Ok(QueryResponse {
            conversation_id: context.id,
            response: ai_response,
            data: Some(json_result),
            visualization_data,
        })
    }

    /// Create a new conversation context for a job
    async fn create_context(&self, job_id: &str) -> Result<ConversationContext> {
        // Get dataset metadata from the data processor
        let metadata = self.get_dataset_metadata(job_id).await?;
        
        // Create a new context
        let context = ConversationContext::new(job_id.to_string(), metadata);
        
        // Store the context
        self.store.store(context.clone())?;
        
        Ok(context)
    }

    /// Get metadata about a dataset
    async fn get_dataset_metadata(&self, job_id: &str) -> Result<DatasetMetadata> {
        // Parse the job ID
        let uuid = match Uuid::parse_str(job_id) {
            Ok(id) => id,
            Err(e) => return Err(anyhow!("Invalid job ID: {}", e)),
        };
        
        // Try to get insights from Redis cache first
        info!("Attempting to get dataset metadata for job {}", job_id);
        let s3_service = self.data_processor.get_s3_service();
        let file_key = format!("uploads/{}.csv", uuid);
        let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "data-pipeline-bucket".to_string());
        
        // Try multiple approaches to get the file from MemoryS3Service with fallbacks
        let csv_data = match s3_service.get_object("", &file_key).await {
            Ok(data) => {
                info!("Successfully loaded CSV data for metadata with direct key: {} bytes", data.len());
                data
            },
            Err(_) => {
                // First fallback: Try with default bucket
                match s3_service.get_object("default-bucket", &file_key).await {
                    Ok(data) => {
                        info!("Successfully loaded CSV data for metadata with default bucket: {} bytes", data.len());
                        data
                    },
                    Err(_) => {
                        // Second fallback: Try with configured bucket
                        match s3_service.get_object(&bucket, &file_key).await {
                            Ok(data) => {
                                info!("Successfully loaded CSV data for metadata with configured bucket: {} bytes", data.len());
                                data
                            },
                            Err(_) => {
                                // Third fallback: Try with just the UUID
                                let simple_key = format!("{}.csv", uuid);
                                match s3_service.get_object("", &simple_key).await {
                                    Ok(data) => {
                                        info!("Successfully loaded CSV data for metadata with simple key: {} bytes", data.len());
                                        data
                                    },
                                    Err(e) => {
                                        // If all attempts fail, return the error
                                        error!("Failed to load CSV data for metadata after all fallback attempts: {}", e);
                                        return Err(anyhow!("Failed to load CSV data: {}", e));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
        
        // Parse the CSV to get column names and data types
        let df = match CsvReader::new(std::io::Cursor::new(csv_data))
            .infer_schema(Some(100))
            .has_header(true)
            .finish() {
            Ok(df) => {
                info!("Successfully parsed CSV data for metadata: {} rows, {} columns", df.height(), df.width());
                df
            },
            Err(e) => {
                error!("Failed to parse CSV data for metadata: {}", e);
                return Err(anyhow!("Failed to parse CSV data: {}", e));
            }
        };
        
        // Extract column names and data types
        let mut columns = Vec::new();
        let mut data_types = HashMap::new();
        
        for col in df.get_column_names() {
            columns.push(col.to_string());
            
            if let Ok(series) = df.column(col) {
                let dtype = match series.dtype() {
                    DataType::Boolean => "boolean",
                    DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => "unsigned integer",
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "integer",
                    DataType::Float32 | DataType::Float64 => "float",
                    DataType::Utf8 => "string",
                    DataType::Date => "date",
                    DataType::Datetime(_, _) => "datetime",
                    DataType::Time => "time",
                    _ => "unknown",
                };
                
                data_types.insert(col.to_string(), dtype.to_string());
            }
        }
        
        let metadata = DatasetMetadata {
            columns,
            row_count: df.height(),
            data_types,
        };
        
        info!("Generated metadata for job {}: {} columns, {} rows", job_id, metadata.columns.len(), metadata.row_count);
        Ok(metadata)
    }

    /// Execute a natural language query
    async fn execute_query(&self, query: &str, context: &ConversationContext) -> Result<(String, Value)> {
        info!("Executing query: {}", query);
        
        // Translate the query to a structured query
        let structured_query = match self.query_translator.translate_query(query, context).await {
            Ok(query) => {
                info!("Translated query: {:?}", query);
                query
            },
            Err(e) => {
                error!("Failed to translate query: {}", e);
                return Ok((format!("I couldn't understand your query: {}", e), json!({"error": e.to_string()})));
            }
        };
        
        // Execute the structured query
        let s3_service = self.data_processor.get_s3_service();
        let df = match self.query_translator.execute_query(&structured_query, &context.job_id, s3_service).await {
            Ok(df) => {
                info!("Query executed successfully");
                df
            },
            Err(e) => {
                error!("Failed to execute query: {}", e);
                return Ok((format!("I couldn't execute your query: {}", e), json!({"error": e.to_string()})));
            }
        };
        
        // Check if the DataFrame is empty
        if df.height() == 0 {
            return Ok(("No data found for your query.".to_string(), json!({"result": "empty"})));
        }
        
        // Convert the DataFrame to JSON using JsonWriter
        let json_result = match {
            // Create a buffer
            let mut buf = Vec::new();
            
            // Create a mutable clone of the DataFrame
            let mut df_mut = df.clone();
            
            // Write DataFrame to buffer as JSON
            JsonWriter::new(&mut buf)
                .with_json_format(JsonFormat::Json)
                .finish(&mut df_mut)
                .context("Failed to write DataFrame to JSON")?;
            
            // Convert buffer to UTF-8 string
            let json_string = std::str::from_utf8(&buf)
                .context("Failed to convert JSON bytes to string")?
                .to_string();
            
            // Parse string into JSON Value
            serde_json::from_str::<Value>(&json_string)
                .context("Failed to parse JSON string into Value")
        } {
            Ok(json_value) => json_value,
            Err(e) => {
                error!("Failed to convert DataFrame to JSON: {}", e);
                return Ok((format!("I couldn't format the results: {}", e), json!({"error": e.to_string()})));
            }
        };
        
        // Generate a natural language response based on the query and results
        let response = self.generate_nl_response(query, &structured_query, &df);
        
        Ok((response, json_result))
    }
    
    /// Generate a natural language response based on the query and results
    fn generate_nl_response(&self, query: &str, structured_query: &StructuredQuery, df: &DataFrame) -> String {
        // In a real implementation, this would use the AI service to generate a natural language response
        // For now, we'll generate a simple response based on the query intent
        
        match structured_query.intent {
            crate::services::query_translator::QueryIntent::Aggregate => {
                format!("Here are the aggregated results for your query: '{}'", query)
            },
            crate::services::query_translator::QueryIntent::Filter => {
                format!("Here are the filtered results for your query: '{}'", query)
            },
            crate::services::query_translator::QueryIntent::Sort => {
                format!("Here are the sorted results for your query: '{}'", query)
            },
            crate::services::query_translator::QueryIntent::Describe => {
                let shape = df.shape();
                format!("The dataset has {} rows and {} columns. Here's a summary of the data.", shape.0, shape.1)
            },
            crate::services::query_translator::QueryIntent::Visualize => {
                format!("Here's a visualization for your query: '{}'", query)
            },
        }
    }
}
