use std::time::Duration;
use anyhow::{Result, anyhow};
use log::{info, error, debug};
use reqwest::Client;
use serde_json::{json, Value};

use crate::models::response::AISummary;
use crate::config::Config;

/// Service for AI-powered data analysis and insights
#[derive(Clone, Debug)]
pub struct AIService {
    client: Client,
    api_key: Option<String>,
}

impl AIService {
    /// Create a new AIService using Config
    pub fn new(config: &Config) -> Result<Option<Self>> {
        // Check if the OpenAI API key is set in the config
        match &config.open_ai_key {
            Some(api_key) if !api_key.trim().is_empty() => {
                info!("AIService initialized with OpenAI API key");
                Ok(Some(Self {
                    client: Client::new(),
                    api_key: Some(api_key.clone()),
                }))
            },
            _ => {
                info!("OpenAI API key not set in config, AIService not initialized");
                Ok(None)
            }
        }
    }
    
    /// Generate a data summary from insights JSON
    pub async fn generate_data_summary(&self, insights: &Value) -> Result<AISummary> {
        // Check if API key is available
        let api_key = match &self.api_key {
            Some(key) if !key.trim().is_empty() => key,
            _ => {
                error!("OpenAI API key is not available. Cannot generate AI summary.");
                return Err(anyhow!("OpenAI API key is not available"));
            }
        };
        
        info!("Using OpenAI API key (first 3 chars): {}...", api_key.chars().take(3).collect::<String>());

        // Log a sample of the insights for debugging
        let insights_sample = insights.to_string()
            .chars()
            .take(500)
            .collect::<String>();
        info!("Insights sample (first 500 chars): {}", insights_sample);
        
        let prompt = format!(r#"
Here is a JSON object containing data insights from a CSV file analysis:

{}

Based on this data, please provide:
1. A concise summary of the dataset (2-3 sentences)
2. 3-5 key business-relevant insights from the data (these should be descriptive, highlight trends, patterns, or anomalies, and may include actionable points)
3. 3-5 actionable business recommendations and suggestions for improvement, with a brief rationale for each
4. 3-5 recommended visualization types with titles, descriptions, and relevant columns

IMPORTANT: Do NOT return empty arrays or blank fields. If you cannot find any insights or recommendations, explain why in the summary and provide at least one general suggestion. Your response must always contain non-empty, meaningful content for each field.

Format your response as a JSON object with the following structure:
{{
    "summary": "A brief summary of the dataset",
    "key_insights": ["Insight 1", "Insight 2", ...],
    "actionable_recommendations": [
        {{
            "recommendation": "Increase marketing spend in region X",
            "rationale": "Region X has the highest growth potential based on recent sales trends."
        }},
        ...
    ],
    "visualization_recommendations": [
        {{
            "chart_type": "bar_chart",
            "title": "Distribution of Values",
            "description": "Shows the distribution of values across categories",
            "columns": ["column1", "column2"]
        }},
        ...
    ]
}}
"#, insights);

        info!("Sending request to OpenAI API");
        
        // Create a client with a 30-second timeout
        let client = match Client::builder()
            .timeout(Duration::from_secs(30))
            .build() {
                Ok(client) => client,
                Err(e) => {
                    error!("Failed to build HTTP client: {}", e);
                    return Err(anyhow!("Failed to build HTTP client: {}", e));
                }
            };
            
        let request_body = json!({
            "model": "gpt-4o",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a data analysis assistant that helps interpret data insights and recommend visualizations. Provide concise, business-focused analysis."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "response_format": { "type": "json_object" }
        });
        
        info!("Sending request to OpenAI API with model: gpt-4o");
        
        // Send the request with detailed error handling
        let response = match client
            .post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await {
                Ok(resp) => resp,
                Err(e) => {
                    error!("Failed to send request to OpenAI API: {}", e);
                    if e.is_timeout() {
                        error!("Request timed out after 30 seconds");
                        return Err(anyhow!("OpenAI API request timed out after 30 seconds"));
                    } else if e.is_connect() {
                        error!("Connection error: {}", e);
                        return Err(anyhow!("Failed to connect to OpenAI API: {}", e));
                    } else {
                        return Err(anyhow!("Failed to send request to OpenAI API: {}", e));
                    }
                }
            };

        let status = response.status();
        info!("OpenAI API response status: {}", status);
        
        if !status.is_success() {
            let error_text = response.text().await
                .unwrap_or_else(|_| "Could not read error response".to_string());
            error!("OpenAI API error: Status {}, Details: {}", status, error_text);
            return Err(anyhow!("OpenAI API error: Status {}, Details: {}", status, error_text));
        }
        
        // Parse the response with detailed error handling
        let response_json: Value = match response.json().await {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to parse OpenAI API response as JSON: {}", e);
                return Err(anyhow!("Failed to parse OpenAI API response: {}", e));
            }
        };
            
        debug!("OpenAI API response received");
        
        // Extract the content from the response
        let content = match response_json["choices"][0]["message"]["content"].as_str() {
            Some(content) => content,
            None => {
                error!("Could not extract content from OpenAI response: {:?}", response_json);
                return Err(anyhow!("Could not extract content from OpenAI response"));
            }
        };

        info!("Parsing AI summary from OpenAI response");
        let ai_summary: AISummary = match serde_json::from_str(content) {
            Ok(summary) => summary,
            Err(e) => {
                error!("Failed to parse AI summary from OpenAI response: {}", e);
                error!("Raw AI response content: {}", content);
                // Try to extract JSON substring from the content
                if let Some(start) = content.find('{') {
                    if let Some(end) = content.rfind('}') {
                        let json_str = &content[start..=end];
                        match serde_json::from_str::<AISummary>(json_str) {
                            Ok(summary) => {
                                info!("Successfully parsed AISummary from extracted JSON substring");
                                return Ok(summary);
                            },
                            Err(e2) => {
                                error!("Failed to parse extracted JSON substring as AISummary: {}", e2);
                                error!("Extracted JSON substring: {}", json_str);
                            }
                        }
                    }
                }
                error!("Raw content received: {}", content);
                return Err(anyhow!("Failed to parse AI summary from OpenAI response: {}", e));
            }
        };

        info!("Successfully generated AI summary");
        Ok(ai_summary)
    }
    
    /// Generate a structured query from a natural language query
    pub async fn generate_query_translation(&self, prompt_data: &Value) -> Result<Value> {
        // Check if API key is available
        let api_key = match &self.api_key {
            Some(key) if !key.trim().is_empty() => key,
            _ => {
                error!("OpenAI API key is not available. Cannot translate query.");
                return Err(anyhow!("OpenAI API key is not available"));
            }
        };
        
        info!("Translating natural language query to structured query");
        
        // Create a client with a 15-second timeout
        let client = match Client::builder()
            .timeout(Duration::from_secs(15))
            .build() {
                Ok(client) => client,
                Err(e) => {
                    error!("Failed to build HTTP client: {}", e);
                    return Err(anyhow!("Failed to build HTTP client: {}", e));
                }
            };
            
        // Construct the system prompt
        let system_prompt = r#"You are a data query translator that converts natural language queries into structured queries for data analysis. 
You analyze the user's query in the context of their dataset and conversation history, then return a structured JSON representation of the query that can be executed by a data processing system.

Your response must be a valid JSON object with the following structure:
{
  "intent": "Aggregate|Filter|Sort|Describe|Visualize",
  "columns": ["column1", "column2", ...],
  "operations": [
    {"type": "Mean", "column": "column_name"},
    {"type": "GroupBy", "column": "column_name"},
    {"type": "Filter", "column": "column_name", "operator": ">", "value": "10"},
    ...
  ]
}

Be precise and only include columns that exist in the dataset. If the query is ambiguous, make a reasonable guess based on the dataset schema and conversation history."#;
        
        // Convert prompt_data to a JSON string for the API
        let prompt_data_str = serde_json::to_string(&prompt_data).map_err(|e| {
            error!("Failed to serialize prompt_data for AI query: {}", e);
            anyhow!("Failed to serialize prompt_data for AI query")
        })?;

        // Create the request body
        let request_body = json!({
            "model": "gpt-4o",
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": prompt_data_str // Use the stringified version here
                }
            ],
            "response_format": { "type": "json_object" }
        });
        
        info!("Sending query translation request to OpenAI API");
        
        // Send the request with detailed error handling
        let response = match client
            .post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await {
                Ok(resp) => resp,
                Err(e) => {
                    error!("Failed to send request to OpenAI API: {}", e);
                    if e.is_timeout() {
                        error!("Request timed out after 15 seconds");
                        return Err(anyhow!("OpenAI API request timed out after 15 seconds"));
                    } else if e.is_connect() {
                        error!("Connection error: {}", e);
                        return Err(anyhow!("Failed to connect to OpenAI API: {}", e));
                    } else {
                        return Err(anyhow!("Failed to send request to OpenAI API: {}", e));
                    }
                }
            };

        let status = response.status();
        info!("OpenAI API response status: {}", status);
        
        if !status.is_success() {
            let error_text = response.text().await
                .unwrap_or_else(|_| "Could not read error response".to_string());
            error!("OpenAI API error: Status {}, Details: {}", status, error_text);
            return Err(anyhow!("OpenAI API error: Status {}, Details: {}", status, error_text));
        }
        
        // Parse the response with detailed error handling
        let response_json: Value = match response.json().await {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to parse OpenAI API response as JSON: {}", e);
                return Err(anyhow!("Failed to parse OpenAI API response: {}", e));
            }
        };
            
        debug!("OpenAI API response received");
        
        // Extract the content from the response
        let content = match response_json["choices"][0]["message"]["content"].as_str() {
            Some(content) => content,
            None => {
                error!("Could not extract content from OpenAI response: {:?}", response_json);
                return Err(anyhow!("Could not extract content from OpenAI response"));
            }
        };

        info!("Successfully translated query");
        
        // Parse the content as JSON
        match serde_json::from_str::<Value>(content) {
            Ok(parsed) => Ok(parsed),
            Err(e) => {
                error!("Failed to parse query translation from OpenAI response: {}", e);
                error!("Raw content received: {}", content);
                return Err(anyhow!("Failed to parse query translation: {}", e));
            }
        }
    }
}
