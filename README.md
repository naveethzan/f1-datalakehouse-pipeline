# F1 Data Engineering Pipeline

A comprehensive data engineering project that implements a medallion architecture to process Formula 1 data from the OpenF1 API using AWS cloud services.

## Overview

This project demonstrates modern data engineering practices by building a scalable data pipeline that:
- Extracts F1 session data (Qualifying and Race) from the OpenF1 API
- Implements a medallion architecture (Bronze → Silver → Gold) for data processing
- Uses Apache Iceberg for data lake table format
- Orchestrates workflows with Apache Airflow
- Processes data with Apache Spark
- Deploys on AWS using managed services

## Architecture

The pipeline follows a medallion architecture pattern:
- **Bronze Layer**: Raw data ingestion from OpenF1 API
- **Silver Layer**: Cleaned and normalized data
- **Gold Layer**: Business-ready aggregated data

## Technology Stack

- **Orchestration**: Apache Airflow (AWS MWAA for production)
- **Data Processing**: Apache Spark (AWS Glue for production)
- **Data Storage**: Apache Iceberg tables on S3
- **Data Catalog**: AWS Glue Catalog
- **Query Engine**: AWS Athena
- **Development**: Docker Compose for local environment

## Project Structure

```
f1-data-pipeline/
├── dags/                    # Airflow workflow orchestration
├── jobs/                    # Spark ETL job implementations
├── config/                  # Environment configurations
├── docker/                  # Docker configurations
├── scripts/                 # Setup and deployment automation
├── transformations/         # Business logic transformations (future)
├── quality/                 # Data quality validation rules (future)
├── sql/                     # SQL queries and views (future)
├── utils/                   # Shared utility functions (future)
├── tests/                   # Comprehensive test suite (future)
└── docs/                    # Project documentation (future)
```

## Getting Started

### Prerequisites

- Docker and Docker Compose
- AWS CLI configured with appropriate permissions
- Python 3.10+

### Local Development Setup

1. Clone the repository
2. Set up environment variables (copy `.env.example` to `.env`)
3. Start the development environment:
   ```bash
   make dev-up
   ```
4. Access Airflow UI at http://localhost:8080

## Data Scope

This pipeline focuses on Formula 1 data processing for:
- **Session Types**: Qualifying and Race sessions only
- **Data Entities**: Sessions, drivers, results, laps, stints, and pit stops
- **Partitioning**: By year, grand prix, and session type

## Development Status

This project is currently in the setup phase, establishing the foundational architecture and development environment.

## Contributing

This is a portfolio project demonstrating data engineering best practices. The focus is on clean architecture, proper AWS service usage, and scalable data processing patterns.

## License

This project is for educational and portfolio purposes.