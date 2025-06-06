use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::collections::HashMap;

/// Response for file upload endpoint
#[derive(Debug, Serialize, Deserialize)]
pub struct UploadResponse {
    pub job_id: Uuid,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Represents a category average for insights
#[derive(Debug, Serialize, Deserialize)]
pub struct CategoryAverage {
    pub category: String,
    pub avg_score: f64,
}

/// Statistics for a single column in the dataset
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ColumnStatistics {
    pub name: String,
    pub data_type: String,
    pub null_count: usize,
    pub unique_count: usize,
    pub min: Option<String>,
    pub max: Option<String>,
    pub mean: Option<String>,
    pub median: Option<String>,
    pub std_dev: Option<String>,
    pub percentile_25: Option<String>,
    pub percentile_75: Option<String>,
    pub frequent_values: Option<HashMap<String, u32>>,
}

/// Summary of the dataset
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DataSummary {
    pub row_count: usize,
    pub column_count: usize,
    pub numeric_columns: Vec<String>,
    pub categorical_columns: Vec<String>,
    pub date_columns: Vec<String>,
    pub summary_text: String,
}

/// Visualization recommendation from AI analysis
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct VisualizationRecommendation {
    pub chart_type: String,
    pub title: String,
    pub description: String,
    pub columns: Vec<String>,
}

/// AI-generated summary and recommendations
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AISummary {
    pub summary: String,
    pub key_insights: Vec<String>,
    pub actionable_recommendations: Vec<ActionableRecommendation>,
    pub visualization_recommendations: Vec<VisualizationRecommendation>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ActionableRecommendation {
    pub recommendation: String,
    pub rationale: String,
}

/// Represents insights generated from data analysis
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Insights {
    pub data_summary: DataSummary,
    pub column_statistics: Vec<ColumnStatistics>,
    pub correlations: Option<HashMap<String, f64>>,
    pub ai_analysis: Option<AISummary>,
}

/// Response for insights endpoint
#[derive(Debug, Serialize, Deserialize)]
pub struct InsightsResponse {
    pub job_id: Uuid,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub insights: Option<Insights>,
}

/// Error response for API
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub status_code: u16,
}
