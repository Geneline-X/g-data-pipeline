use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;

/// Represents a user query and its response in a conversation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationTurn {
    /// The user's natural language query
    pub query: String,
    /// The system's response to the query
    pub response: String,
    /// When this turn occurred
    pub timestamp: DateTime<Utc>,
}

/// Metadata about the dataset being queried
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetMetadata {
    /// Names of columns in the dataset
    pub columns: Vec<String>,
    /// Number of rows in the dataset
    pub row_count: usize,
    /// Data types of each column
    pub data_types: HashMap<String, String>,
}

/// Represents the state of a conversation about a dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationContext {
    /// Unique identifier for this conversation
    pub id: String,
    /// The job ID associated with the dataset
    pub job_id: String,
    /// History of the conversation
    pub history: Vec<ConversationTurn>,
    /// Metadata about the dataset
    pub dataset_metadata: DatasetMetadata,
    /// When the conversation was created
    pub created_at: DateTime<Utc>,
    /// When the conversation was last updated
    pub updated_at: DateTime<Utc>,
}

impl ConversationContext {
    /// Create a new conversation context
    pub fn new(job_id: String, dataset_metadata: DatasetMetadata) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            job_id,
            history: Vec::new(),
            dataset_metadata,
            created_at: now,
            updated_at: now,
        }
    }

    /// Add a turn to the conversation
    pub fn add_turn(&mut self, query: String, response: String) {
        let turn = ConversationTurn {
            query,
            response,
            timestamp: Utc::now(),
        };
        self.history.push(turn);
        self.updated_at = Utc::now();
    }
}

/// Request to query a dataset using natural language
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    /// The job ID associated with the dataset
    pub job_id: String,
    /// The natural language query
    pub query: String,
    /// Optional conversation ID for follow-up queries
    pub conversation_id: Option<String>,
}

/// Response to a natural language query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    /// The conversation ID
    pub conversation_id: String,
    /// Natural language response to the query
    pub response: String,
    /// Any data returned by the query (as JSON)
    pub data: Option<serde_json::Value>,
    /// Optional JSON data for visualization (e.g., Chart.js config)
    pub visualization_data: Option<serde_json::Value>,
}
