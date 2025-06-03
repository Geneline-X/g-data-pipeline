use anyhow::{Result, anyhow, Context};
use log::{info, warn, error};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use polars::prelude::*;
use uuid::Uuid;

use crate::models::conversation::ConversationContext;
use crate::services::ai::AIService;
use crate::services::S3ServiceTrait;

/// Represents the intent of a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryIntent {
    /// Calculate aggregate statistics (mean, sum, count, etc.)
    Aggregate,
    /// Filter data based on conditions
    Filter,
    /// Sort data
    Sort,
    /// Describe the dataset
    Describe,
    /// Visualize the data
    Visualize,
}

/// Represents a column operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnOperation {
    /// Calculate the mean of a column
    Mean(String),
    /// Calculate the sum of a column
    Sum(String),
    /// Count values in a column
    Count(String),
    /// Group by a column
    GroupBy(String),
    /// Sort by a column
    SortBy(String, bool), // (column name, ascending)
    /// Filter by a condition
    Filter(String, String, String), // (column, operator, value)
}

/// Represents a structured query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredQuery {
    /// The intent of the query
    pub intent: QueryIntent,
    /// The columns to include in the result
    pub columns: Vec<String>,
    /// The operations to perform
    pub operations: Vec<ColumnOperation>,
}

/// Translates natural language queries into structured queries
#[derive(Clone, Debug)]
pub struct QueryTranslator {
    ai_service: Option<AIService>,
    s3_bucket: String,
}

impl QueryTranslator {
    /// Create a new query translator
    pub fn new(ai_service: Option<AIService>) -> Self {
        // Get bucket name from environment or use a default
        let s3_bucket = std::env::var("S3_BUCKET")
            .unwrap_or_else(|_| {
                log::warn!("S3_BUCKET environment variable not set, using default bucket name");
                "data-pipeline-bucket".to_string()
            });

        log::info!("QueryTranslator initialized with bucket: {}", s3_bucket);

        Self {
            ai_service,
            s3_bucket,
        }
    }

    /// Translate a natural language query into a structured query
    pub async fn translate_query(
        &self,
        query: &str,
        context: &ConversationContext,
    ) -> Result<StructuredQuery> {
        // If AI service is available, use it for translation
        if let Some(ai_service) = &self.ai_service {
            info!("Using AI service to translate query: {}", query);

            // Build the prompt
            let prompt = self.build_translation_prompt(query, context);

            // Send to AI service
            let response = ai_service.generate_query_translation(&prompt).await?;

            // Parse the response
            return self.parse_ai_response(response);
        }

        // If no AI service is available, use a simple rule-based approach
        info!(
            "No AI service available, using rule-based translation for query: {}",
            query
        );
        self.rule_based_translation(query, context)
    }

    /// Build a prompt for the AI service
    fn build_translation_prompt(&self, query: &str, context: &ConversationContext) -> Value {
        json!({
            "dataset": {
                "columns": context.dataset_metadata.columns,
                "data_types": context.dataset_metadata.data_types,
                "row_count": context.dataset_metadata.row_count
            },
            "conversation_history": context.history.iter().map(|turn| {
                json!({
                    "query": turn.query,
                    "response": turn.response
                })
            }).collect::<Vec<_>>(),
            "current_query": query,
            "examples": [
                {
                    "query": "What's the average of column1?",
                    "structured_query": {
                        "intent": "Aggregate",
                        "columns": ["column1"],
                        "operations": [{"type": "Mean", "column": "column1"}]
                    }
                },
                {
                    "query": "Show me column1 and column2 where column1 > 10",
                    "structured_query": {
                        "intent": "Filter",
                        "columns": ["column1", "column2"],
                        "operations": [{"type": "Filter", "column": "column1", "operator": ">", "value": "10"}]
                    }
                }
            ]
        })
    }

    /// Parse the AI service response into a structured query
    fn parse_ai_response(&self, _response: Value) -> Result<StructuredQuery> {
        // In a real implementation, parse `response` (JSON) into `StructuredQuery`.
        // For now, return a placeholder.
        Ok(StructuredQuery {
            intent: QueryIntent::Describe,
            columns: vec!["column1".to_string(), "column2".to_string()],
            operations: vec![],
        })
    }

    /// Use a simple rule-based approach to translate a query
    fn rule_based_translation(
        &self,
        query: &str,
        context: &ConversationContext,
    ) -> Result<StructuredQuery> {
        let query = query.to_lowercase();

        // Check for common patterns
        if query.contains("average") || query.contains("mean") {
            // Pick the first column from metadata (fallback to "column1")
            let columns = context.dataset_metadata.columns.clone();
            let column = columns
                .first()
                .cloned()
                .unwrap_or_else(|| "column1".to_string());

            return Ok(StructuredQuery {
                intent: QueryIntent::Aggregate,
                columns: vec![column.clone()],
                operations: vec![ColumnOperation::Mean(column)],
            });
        }

        if query.contains("sum") {
            let columns = context.dataset_metadata.columns.clone();
            let column = columns
                .first()
                .cloned()
                .unwrap_or_else(|| "column1".to_string());

            return Ok(StructuredQuery {
                intent: QueryIntent::Aggregate,
                columns: vec![column.clone()],
                operations: vec![ColumnOperation::Sum(column)],
            });
        }

        if query.contains("count") {
            let columns = context.dataset_metadata.columns.clone();
            let column = columns
                .first()
                .cloned()
                .unwrap_or_else(|| "column1".to_string());

            return Ok(StructuredQuery {
                intent: QueryIntent::Aggregate,
                columns: vec![column.clone()],
                operations: vec![ColumnOperation::Count(column)],
            });
        }

        // Default to describing the dataset
        Ok(StructuredQuery {
            intent: QueryIntent::Describe,
            columns: context.dataset_metadata.columns.clone(),
            operations: vec![],
        })
    }

    /// Execute a structured query on a dataset
    pub async fn execute_query(
        &self,
        structured_query: &StructuredQuery,
        job_id: &str,
        s3_service: &dyn S3ServiceTrait,
    ) -> Result<DataFrame> {
        // 1. Load the CSV from S3
        let uuid = match Uuid::parse_str(job_id) {
            Ok(id) => id,
            Err(e) => return Err(anyhow!("Invalid job ID: {}", e)),
        };

        // Files are stored under "uploads/{job_id}.csv"
        let file_key = format!("uploads/{}.csv", uuid);

        info!("Loading CSV data for job {} with key {}", job_id, file_key);
        let csv_data = match s3_service.get_object("", &file_key).await {
            Ok(data) => {
                info!("Loaded CSV data with direct key ({} bytes)", data.len());
                data
            }
            Err(_) => {
                // Fallback #1: default bucket
                match s3_service.get_object("default-bucket", &file_key).await {
                    Ok(data) => {
                        info!("Loaded CSV data from default bucket ({} bytes)", data.len());
                        data
                    }
                    Err(_) => {
                        // Fallback #2: configured bucket
                        match s3_service.get_object(&self.s3_bucket, &file_key).await {
                            Ok(data) => {
                                info!(
                                    "Loaded CSV data from configured bucket ({} bytes)",
                                    data.len()
                                );
                                data
                            }
                            Err(_) => {
                                // Fallback #3: key = "{job_id}.csv"
                                let simple_key = format!("{}.csv", uuid);
                                match s3_service.get_object("", &simple_key).await {
                                    Ok(data) => {
                                        info!(
                                            "Loaded CSV data with simple key ({} bytes)",
                                            data.len()
                                        );
                                        data
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to load CSV data after all fallbacks: {}",
                                            e
                                        );
                                        return Err(anyhow!("Failed to load CSV data: {}", e));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        // 2. Parse CSV into a DataFrame
        let df = match self.parse_csv_data(&csv_data) {
            Ok(df) => {
                info!("Parsed CSV: {} rows, {} columns", df.height(), df.width());
                df
            }
            Err(e) => {
                error!("Failed to parse CSV data: {}", e);
                return Err(anyhow!("Failed to parse CSV data: {}", e));
            }
        };

        // 3. Apply the structured query operations
        let result_df = self.apply_operations(df, structured_query)?;

        Ok(result_df)
    }

    /// Parse CSV data into a DataFrame
    fn parse_csv_data(&self, csv_data: &[u8]) -> Result<DataFrame> {
        let df = CsvReader::new(std::io::Cursor::new(csv_data))
            .infer_schema(Some(100))
            .has_header(true)
            .finish()
            .context("Failed to parse CSV data")?;
        Ok(df)
    }

    /// Apply operations from a structured query to a DataFrame
    fn apply_operations(&self, df: DataFrame, query: &StructuredQuery) -> Result<DataFrame> {
        let mut result = df;

        match query.intent {
            QueryIntent::Describe => {
                // Return the first 10 rows
                result = result.head(Some(10));
            }

            QueryIntent::Aggregate => {
                // Apply each aggregation operation
                for op in &query.operations {
                    match op {
                        ColumnOperation::Mean(col_name) => {
                            // First compute the mean expression
                            let mean_expr = col(col_name).mean();
                            // Create a new dataframe with just this expression
                            result = result.lazy().select([mean_expr.alias(&format!("mean_{}", col_name))])
                                .collect()?;
                        }
                        ColumnOperation::Sum(col_name) => {
                            // First compute the sum expression
                            let sum_expr = col(col_name).sum();
                            // Create a new dataframe with just this expression
                            result = result.lazy().select([sum_expr.alias(&format!("sum_{}", col_name))])
                                .collect()?;
                        }
                        ColumnOperation::Count(col_name) => {
                            // First compute the count expression
                            let count_expr = col(col_name).count();
                            // Create a new dataframe with just this expression
                            result = result.lazy().select([count_expr.alias(&format!("count_{}", col_name))])
                                .collect()?;
                        }
                        ColumnOperation::GroupBy(col_name) => {
                            // Group by `col_name` and count rows in each group
                            // Use lazy API for groupby and aggregation
                            let count_expr = col(col_name).count().alias(&format!("count_{}", col_name));
                            result = result.lazy()
                                .group_by([col(col_name)])
                                .agg([count_expr])
                                .collect()?;
                        }
                        _ => {
                            // Other ops (e.g., SortBy or Filter) are not handled under Aggregate
                        }
                    }
                }
            }

            QueryIntent::Filter => {
                // Apply each filter operation
                for op in &query.operations {
                    if let ColumnOperation::Filter(col_name, operator, value) = op {
                        let filter_expr = match operator.as_str() {
                            "=" | "==" => col(col_name).eq(lit(value.clone())),
                            ">" => match value.parse::<f64>() {
                                Ok(num) => col(col_name).gt(lit(num)),
                                Err(_) => {
                                    warn!(
                                        "Failed to parse '{}' as number for '>' comparison",
                                        value
                                    );
                                    continue;
                                }
                            },
                            "<" => match value.parse::<f64>() {
                                Ok(num) => col(col_name).lt(lit(num)),
                                Err(_) => {
                                    warn!(
                                        "Failed to parse '{}' as number for '<' comparison",
                                        value
                                    );
                                    continue;
                                }
                            },
                            ">=" => match value.parse::<f64>() {
                                Ok(num) => col(col_name).gt_eq(lit(num)),
                                Err(_) => {
                                    warn!(
                                        "Failed to parse '{}' as number for '>=' comparison",
                                        value
                                    );
                                    continue;
                                }
                            },
                            "<=" => match value.parse::<f64>() {
                                Ok(num) => col(col_name).lt_eq(lit(num)),
                                Err(_) => {
                                    warn!(
                                        "Failed to parse '{}' as number for '<=' comparison",
                                        value
                                    );
                                    continue;
                                }
                            },
                            "!=" | "<>" => col(col_name).neq(lit(value.clone())),
                            _ => {
                                warn!("Unsupported operator: {}", operator);
                                continue;
                            }
                        };

                        // Convert expression to lazy dataframe and collect
                        result = result.lazy().filter(filter_expr).collect()?;
                    }
                }

                // After filtering, select only requested columns (if any)
                if !query.columns.is_empty() {
                    let cols: Vec<&str> = query
                        .columns
                        .iter()
                        .map(|c| c.as_str())
                        .collect();
                    result = result.select(cols)?;
                }
            }

            QueryIntent::Sort => {
                // Apply each sort operation
                for op in &query.operations {
                    if let ColumnOperation::SortBy(col_name, ascending) = op {
                        // Sort by column with specified options
                        result = result.sort([col_name], vec![!ascending], false)?;
                    }
                }

                // After sorting, select only requested columns (if any)
                if !query.columns.is_empty() {
                    let cols: Vec<&str> = query
                        .columns
                        .iter()
                        .map(|c| c.as_str())
                        .collect();
                    result = result.select(cols)?;
                }
            }

            QueryIntent::Visualize => {
                // Select requested columns (if any)
                if !query.columns.is_empty() {
                    let cols: Vec<&str> = query
                        .columns
                        .iter()
                        .map(|c| c.as_str())
                        .collect();
                    result = result.select(cols)?;
                }

                // Limit the number of rows to avoid sending too much data
                result = result.head(Some(100));
            }
        }

        Ok(result)
    }
}
