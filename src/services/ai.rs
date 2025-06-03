use std::time::Duration;
use anyhow::{Result, anyhow, Context};
use log::{info, error, debug, warn};
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
            2. 3-5 key business-relevant insights from the data
            3. 3-5 recommended visualization types with titles, descriptions, and relevant columns
            
            Format your response as a JSON object with the following structure:
            {{
                "summary": "A brief summary of the dataset",
                "key_insights": ["Insight 1", "Insight 2", ...],
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
                error!("Raw content received: {}", content);
                return Err(anyhow!("Failed to parse AI summary from OpenAI response: {}", e));
            }
        };

        info!("Successfully generated AI summary");
        Ok(ai_summary)
    }
}
