use anyhow::{Result, anyhow, Context};
use polars::prelude::*;
use std::collections::HashMap;
use uuid::Uuid;

use crate::models::job::JobStatus;
use crate::models::response::{Insights, DataSummary, ColumnStatistics};
use crate::services::{S3ServiceTrait, DatabaseServiceTrait, RedisServiceTrait};

#[derive(Clone, Debug)]
pub struct DataProcessor<S, D, R>
where
    S: S3ServiceTrait + Clone + std::fmt::Debug,
    D: DatabaseServiceTrait + Clone + std::fmt::Debug,
    R: RedisServiceTrait + Clone + std::fmt::Debug,
{
    s3_service: S,
    db_service: D,
    redis_service: R,
    s3_bucket: String,
}

impl<S, D, R> DataProcessor<S, D, R>
where
    S: S3ServiceTrait + Clone + std::fmt::Debug,
    D: DatabaseServiceTrait + Clone + std::fmt::Debug,
    R: RedisServiceTrait + Clone + std::fmt::Debug,
{
    pub fn new(
        s3_service: S,
        db_service: D,
        redis_service: R,
        s3_bucket: String,
    ) -> Self {
        Self {
            s3_service,
            db_service,
            redis_service,
            s3_bucket,
        }
    }

    /// Process a job with the given ID
    ///  - parse CSV 
    ///  - generate insights (no chart rendering here)
    ///  - cache the JSON(insights) in Redis
    pub async fn process_job(&self, job_id: Uuid) -> Result<()> {
        log::info!("🔍 [Job-{}] Starting job processing", job_id);
        log::info!("📃 [Job-{}] Processing details: bucket={}", job_id, self.s3_bucket);
        
        // 1) Mark job as "Processing"
        log::info!("⏳ [Job-{}] Updating status to Processing", job_id);
        self.db_service
            .update_job_status(job_id, JobStatus::Processing)
            .await
            .map_err(|e| {
                log::error!("❌ [Job-{}] Failed to update status to Processing: {}", job_id, e);
                e
            })?;
    
        // 2) Fetch job details
        log::info!("📋 [Job-{}] Fetching job details from database", job_id);
        let job = match self.db_service.get_job(job_id).await? {
            Some(job) => job,
            None => {
                let err = anyhow!("Job not found");
                log::error!("❌ [Job-{}] {}", job_id, err);
                return Err(err);
            }
        };
    
        log::info!("📥 [Job-{}] Downloading file: {} from bucket: {}", job_id, job.file_key, self.s3_bucket);
        log::info!("🔎 [Job-{}] Attempting to retrieve file with key: {}", job_id, job.file_key);
        
        match self.s3_service.get_object(&self.s3_bucket, &job.file_key).await {
            Ok(data) => {
                log::info!("✅ [Job-{}] Successfully downloaded file: {} (size: {} bytes)", job_id, job.file_key, data.len());
                let csv_data = data;
        
                log::info!("📊 [Job-{}] Parsing CSV data (size: {} bytes)", job_id, csv_data.len());
                let parse_start = std::time::Instant::now();
                match self.parse_csv_data(&csv_data) {
                    Ok(dataframe) => {
                        let parse_duration = parse_start.elapsed();
                        log::info!("✅ [Job-{}] Successfully parsed CSV in {:.2?}: {} rows, {} columns", 
                            job_id, parse_duration, dataframe.height(), dataframe.width());
                        let df = dataframe;
        
                        log::info!("🧠 [Job-{}] Generating insights for dataframe", job_id);
                        let insights_start = std::time::Instant::now();
                        match self.generate_insights(&df) {
                            Ok(result) => {
                                let insights_duration = insights_start.elapsed();
                                log::info!("✅ [Job-{}] Successfully generated insights in {:.2?}", job_id, insights_duration);
                                let insights = result;
    
                                log::info!("💾 [Job-{}] Caching insights in Redis", job_id);
                                match self.redis_service.cache_insights(job_id, &insights) {
                                    Ok(_) => {
                                        log::info!("✅ [Job-{}] Successfully cached insights in Redis", job_id);
                                    },
                                    Err(e) => {
                                        log::error!("❌ [Job-{}] Failed to cache insights: {}", job_id, e);
                                        return Err(e.into());
                                    }
                                };
    
                                log::info!("✅ [Job-{}] Updating status to Completed", job_id);
                                match self.db_service.update_job_status(job_id, JobStatus::Completed).await {
                                    Ok(_) => {
                                        log::info!("✅ [Job-{}] Successfully updated status to Completed", job_id);
                                    },
                                    Err(e) => {
                                        log::error!("❌ [Job-{}] Failed to update status to Completed: {}", job_id, e);
                                        return Err(e.into());
                                    }
                                };
    
                                log::info!("🎉 [Job-{}] Successfully completed processing", job_id);
                                return Ok(());
                            },
                            Err(e) => {
                                log::error!("❌ [Job-{}] Failed to generate insights: {}", job_id, e);
                                return Err(e);
                            }
                        }
                    },
                    Err(e) => {
                        log::error!("❌ [Job-{}] Failed to parse CSV: {}", job_id, e);
                        return Err(e);
                    }
                }
            },
            Err(e) => {
                log::error!("❌ [Job-{}] Failed to download file: {}", job_id, e);
                return Err(e);
            }
        }
    }

    /// Parse raw CSV bytes into a `DataFrame`
    fn parse_csv_data(&self, csv_data: &[u8]) -> Result<DataFrame> {
        let cursor = std::io::Cursor::new(csv_data);
        let df = CsvReader::new(cursor)
            .infer_schema(Some(100))
            .has_header(true)
            .finish()
            .context("Failed to parse CSV data")?;
        Ok(df)
    }

    /// Generate summary statistics + per‐column stats + correlations
    fn generate_insights(&self, df: &DataFrame) -> Result<Insights> {
        // 1) Basic counts
        let row_count = df.height();
        let col_count = df.width();

        // 2) Bucket column names by dtype
        let mut numeric_columns = Vec::new();
        let mut categorical_columns = Vec::new();
        let mut date_columns = Vec::new();

        for s in df.get_columns() {
            let name = s.name().to_string();
            match s.dtype() {
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64 => {
                    numeric_columns.push(name);
                }
                DataType::Date | DataType::Datetime(_, _) => {
                    date_columns.push(name);
                }
                _ => {
                    categorical_columns.push(name);
                }
            }
        }

        // 3) Top‐level summary text
        let summary_text = format!(
            "Dataset has {} rows and {} columns ({} numeric, {} categorical, {} date).",
            row_count,
            col_count,
            numeric_columns.len(),
            categorical_columns.len(),
            date_columns.len()
        );
        let data_summary = DataSummary {
            row_count,
            column_count: col_count,
            numeric_columns: numeric_columns.clone(),
            categorical_columns: categorical_columns.clone(),
            date_columns: date_columns.clone(),
            summary_text,
        };

        // 4) Per‐column statistics
        let mut column_stats: Vec<ColumnStatistics> = Vec::new();

        for s in df.get_columns() {
            let name = s.name().to_string();
            let dtype = format!("{:?}", s.dtype());
            let null_count = s.null_count();

            // Unique count as usize
            let unique_count = s.n_unique().unwrap_or(0) as usize;

            // Initialize placeholders
            let mut min_str: Option<String> = None;
            let mut max_str: Option<String> = None;
            let mut mean_str: Option<String> = None;
            let mut median_str: Option<String> = None;
            let mut std_str: Option<String> = None;
            let mut percentile_25_str: Option<String> = None;
            let mut percentile_75_str: Option<String> = None;
            let mut freq_vals: Option<HashMap<String, u32>> = None;

            match s.dtype() {
                // ─────────── Numeric branch ───────────
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64 => {
                    // Cast to Float64 → ChunkedArray<Float64Type>
                    if let Ok(ca_f64) = s.cast(&DataType::Float64)?.f64() {
                        min_str = ca_f64.min().map(|v| v.to_string());
                        max_str = ca_f64.max().map(|v| v.to_string());
                        mean_str = ca_f64.mean().map(|v| format!("{:.2}", v));
                        median_str = ca_f64.median().map(|v| format!("{:.2}", v));
                        std_str = ca_f64.std(1).map(|v| format!("{:.2}", v));
                        
                        // Calculate 25th percentile
                        if let Ok(s_f64) = s.cast(&DataType::Float64) {
                            // Using the correct Polars API for percentile calculation
                            if let Ok(p25) = s_f64.quantile_as_series(0.25, QuantileInterpolOptions::Linear) {
                                if let Some(p25_val) = p25.f64()?.get(0) {
                                    percentile_25_str = Some(format!("{:.2}", p25_val));
                                }
                            }
                            
                            // Calculate 75th percentile
                            if let Ok(p75) = s_f64.quantile_as_series(0.75, QuantileInterpolOptions::Linear) {
                                if let Some(p75_val) = p75.f64()?.get(0) {
                                    percentile_75_str = Some(format!("{:.2}", p75_val));
                                }
                            }
                        }
                    }
                }

                // ───────── Non‐numeric (e.g. Utf8, Boolean) ─────────
                _ => {
                    // No numeric stats for non‐numeric
                    min_str = None;
                    max_str = None;
                    mean_str = None;
                    median_str = None;
                    std_str = None;

                    // If it’s a categorical column, compute top‐10 frequent values
                    if categorical_columns.contains(&name) {
                        if let Ok(vc_df) = s.value_counts(false, false) {
                            // vc_df: [ { col_name }, "counts" ]
                            if let (Ok(vals), Ok(cnts)) = (
                                vc_df.column(&name)?.utf8(),
                                vc_df.column("counts")?.u32(),
                            ) {
                                let mut map = HashMap::new();
                                for i in 0..vals.len().min(10) {
                                    if let (Some(val_str), Some(cnt)) =
                                        (vals.get(i), cnts.get(i))
                                    {
                                        map.insert(val_str.to_string(), cnt);
                                    }
                                }
                                freq_vals = Some(map);
                            }
                        }
                    }
                }
            }

            column_stats.push(ColumnStatistics {
                name: name.clone(),
                data_type: dtype.clone(),
                null_count,
                unique_count,
                min: min_str,
                max: max_str,
                mean: mean_str,
                median: median_str,
                std_dev: std_str,
                percentile_25: percentile_25_str,
                percentile_75: percentile_75_str,
                frequent_values: freq_vals,
            });
        }

        // 5) Pairwise correlations (only if ≥2 numeric columns)
        let correlations = if numeric_columns.len() >= 2 {
            let mut corr_map = HashMap::new();
            for i in 0..numeric_columns.len() {
                for j in (i + 1)..numeric_columns.len() {
                    let c1 = &numeric_columns[i];
                    let c2 = &numeric_columns[j];
        
                    // 1) Cast each Series to Float64
                    if let (Ok(s1), Ok(s2)) = (
                        df.column(c1)?.cast(&DataType::Float64),
                        df.column(c2)?.cast(&DataType::Float64)
                    ) {
                        // 2) Calculate correlation manually
                        if let Ok(corr_val) = calculate_correlation(&s1, &s2) {
                            corr_map.insert(format!("{}-{}", c1, c2), corr_val);
                        }
                    }
                }
            }
            Some(corr_map)
        } else {
            None
        };
        

        Ok(Insights {
            data_summary,
            column_statistics: column_stats,
            correlations,
        })
    }
}
/// Calculate the Pearson correlation coefficient between two Series
/// Both Series should already be cast to Float64 type
fn calculate_correlation(s1: &Series, s2: &Series) -> Result<f64> {
    // Get float arrays from the Series
    let ca1 = s1.f64()?;
    let ca2 = s2.f64()?;
    
    // Check if they have the same length
    if ca1.len() != ca2.len() {
        return Err(anyhow!("Series must have the same length"));
    }
    
    // Calculate means
    let mean1 = ca1.mean().ok_or_else(|| anyhow!("Cannot compute mean of first series"))?;
    let mean2 = ca2.mean().ok_or_else(|| anyhow!("Cannot compute mean of second series"))?;
    
    // Calculate correlation components
    let mut cov_sum = 0.0;
    let mut var1_sum = 0.0;
    let mut var2_sum = 0.0;
    let mut valid_count = 0.0;
    
    // Iterate through both arrays simultaneously
    for (v1, v2) in ca1.into_iter().zip(ca2.into_iter()) {
        // Only use pairs where both values are not null
        if let (Some(x), Some(y)) = (v1, v2) {
            let dx = x - mean1;
            let dy = y - mean2;
            
            cov_sum += dx * dy;
            var1_sum += dx * dx;
            var2_sum += dy * dy;
            valid_count += 1.0;
        }
    }
    
    // Need at least 2 pairs to calculate correlation
    if valid_count < 2.0 {
        return Err(anyhow!("Not enough valid data points to compute correlation"));
    }
    
    // Check for division by zero
    if var1_sum.abs() < f64::EPSILON || var2_sum.abs() < f64::EPSILON {
        return Err(anyhow!("Cannot compute correlation: one or both series have zero variance"));
    }
    
    // Calculate the correlation coefficient
    let correlation = cov_sum / (var1_sum.sqrt() * var2_sum.sqrt());
    
    // Ensure the result is between -1 and 1
    if correlation < -1.0 || correlation > 1.0 {
        // Handle floating point precision issues
        if (correlation + 1.0).abs() < f64::EPSILON {
            return Ok(-1.0);
        }
        if (correlation - 1.0).abs() < f64::EPSILON {
            return Ok(1.0);
        }
        return Err(anyhow!("Invalid correlation coefficient: {}", correlation));
    }
    
    Ok(correlation)
}

