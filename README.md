# G-Data-Pipeline: Rust Data Processing API

A high-performance, scalable API for processing CSV data, generating insights, and visualizing results. Built with Rust for maximum efficiency and reliability.

## Features

- CSV file upload via HTTP POST endpoint
- Efficient data processing with Polars DataFrame library
- Advanced statistical analysis including:
  - Mean, median, min, max, standard deviation
  - 25th and 75th percentiles
  - Frequency analysis and unique value counts
- AI-powered data analysis and visualization recommendations
  - Automatic data summarization in business-friendly language
  - Key insights extraction from complex datasets
  - Smart visualization recommendations based on data types
- Intelligent date parsing for automatic conversion of string columns to date types
- Chart generation with Plotters
- Caching with Redis for fast retrieval
- File storage with AWS S3 (with memory-based implementation for testing)
- Metadata storage with PostgreSQL
- RESTful API with Actix-web

## Architecture

The API follows a modular design with the following components:

- **Upload Endpoint**: Accepts CSV files and stores them in S3
- **Background Processing Worker**: Asynchronously analyzes CSV data and generates insights
- **Insights Endpoint**: Returns detailed analysis results and chart URLs
- **Service Layer**: Modular services for S3, database, Redis, and data processing
- **Models**: Strongly-typed data structures for requests, responses, and internal data flow

## Prerequisites

- Rust (latest stable version)
- PostgreSQL
- Redis
- AWS S3 credentials (or a local alternative like MinIO)

## Configuration

Create a `.env` file in the project root with the following variables:

```
DATABASE_URL=postgres://username:password@localhost:5432/dbname
REDIS_URL=redis://localhost:6379
AWS_REGION=us-east-1
S3_BUCKET=your-bucket-name
SERVER_PORT=8080
```

## Setup

1. Install dependencies:

```bash
cargo build
```

2. Create the database:

```bash
psql -c "CREATE DATABASE windserf"
```

3. Run the server:

```bash
cargo run
```

## API Endpoints

### Upload CSV

```
POST /upload
Content-Type: multipart/form-data
```

Request:
- `file`: CSV file (required)

Response:
```json
{
  "job_id": "uuid",
  "status": "queued"
}
```

### Get Insights

```
GET /insights/{job_id}
```

Response:
```json
{
  "job_id": "uuid",
  "status": "completed",
  "file_name": "example.csv",
  "insights": {
    "column_statistics": [
      {
        "name": "age",
        "data_type": "integer",
        "null_count": 0,
        "unique_count": 45,
        "min": 18,
        "max": 65,
        "mean": 32.7,
        "median": 30.0,
        "std_dev": 8.9,
        "percentile_25": 26.0,
        "percentile_75": 39.0,
        "frequent_values": [
          { "value": 28, "count": 15 },
          { "value": 35, "count": 12 }
        ]
      },
      {
        "name": "registration_date",
        "data_type": "date",
        "null_count": 2,
        "unique_count": 120,
        "min": "2022-01-01",
        "max": "2023-12-31",
        "mean": null,
        "median": null,
        "std_dev": null,
        "percentile_25": null,
        "percentile_75": null,
        "frequent_values": [
          { "value": "2023-06-15", "count": 8 },
          { "value": "2023-07-01", "count": 7 }
        ]
      }
    ],
    "column_types": {
      "numeric": ["age", "score", "income"],
      "categorical": ["gender", "country", "education"],
      "date": ["registration_date", "last_login"],
      "text": ["comments"]
    },
    "ai_analysis": {
      "summary": "This dataset contains demographic and registration information for 4,807 individuals, predominantly Sierra Leonean nationals (99.9%). The gender distribution shows 60.6% male and 39.4% female participants. Most individuals are single (95.1%) with a small percentage married (4.6%). The data spans multiple provinces with Eastern (28%), Southern (26%), and Northern (25.5%) provinces having similar representation, while Western province accounts for 20.3%.",
      "key_insights": [
        "The dataset shows a significant gender imbalance with males representing over 60% of records",
        "Almost all individuals (95.1%) are recorded as single, which may indicate a young population or data collection bias",
        "Income values appear to be inconsistently formatted and would benefit from standardization",
        "Date columns (date of birth, date joined scheme) are currently stored as strings and should be converted to proper date types",
        "Geographic distribution across provinces is relatively balanced, with slightly higher representation from Eastern province"
      ],
      "visualization_recommendations": [
        {
          "chart_type": "pie_chart",
          "title": "Gender Distribution",
          "description": "Visualize the proportion of males vs females in the dataset",
          "columns": ["sex"]
        },
        {
          "chart_type": "bar_chart",
          "title": "Provincial Distribution",
          "description": "Compare the number of individuals across different provinces",
          "columns": ["province"]
        },
        {
          "chart_type": "histogram",
          "title": "Income Distribution",
          "description": "Analyze the distribution of income values after standardization",
          "columns": [" income as at joining scheme "]
        }
      ]
    }
  },
  "chart_url": "s3://bucket/charts/uuid.png"
```

## Performance

- Handles CSV files with millions of records efficiently using Polars' columnar processing
- Streams data to minimize memory usage during file uploads and downloads
- Caches analysis results in Redis for fast retrieval
- Processes data asynchronously in background workers to keep the API responsive
- Uses Rust's zero-cost abstractions for maximum performance

## Implementation Details

### Data Processing Pipeline

1. **CSV Parsing**: Uses Polars' high-performance CSV reader with schema inference
2. **Date Detection**: Automatically identifies and converts date columns using pattern matching
3. **Statistical Analysis**: Calculates comprehensive statistics for each column including percentiles
4. **Type Classification**: Categorizes columns into numeric, categorical, date, and text types
5. **Insight Generation**: Produces structured insights based on data patterns

### Recent Features

#### Percentile Calculations
- Added 25th and 75th percentile calculations for numeric columns
- Implemented using Polars' quantile_as_series method with linear interpolation
- Results formatted to two decimal places for readability

#### Intelligent Date Parsing
- Automatically detects potential date columns based on column names and content patterns
- Supports multiple date formats (ISO, US, European, etc.)
- Converts string columns to proper date types for better analysis and visualization

## Future Improvements

- Numeric value standardization for handling inconsistently formatted numbers
- More advanced visualization hints based on data types and distributions
- Distributed processing for larger datasets
- Machine learning integration for predictive analytics
- Real-time data streaming capabilities
- Frontend dashboard with interactive visualizations

## Development Workflow

### Incremental Feature Implementation

This project follows an incremental development approach:

1. Implement one feature at a time to maintain stability
2. Write comprehensive tests for each new feature
3. Validate with real-world data before moving to the next feature

### Running Tests

```bash
cargo test
```

### Development Mode

For development with memory-based services (no external dependencies):

```bash
cargo run --features memory-services
```

### Production Mode

For production with external services (PostgreSQL, Redis, S3):

```bash
cargo run --features external-services
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
