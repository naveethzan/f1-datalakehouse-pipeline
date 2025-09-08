# Docker Directory

This directory contains Docker configuration files for local development environment setup.

## Purpose
- Define custom Docker images for Airflow and Spark
- Configure development environment services
- Manage container dependencies and networking

## Structure
Future Docker files will include:
- `airflow/Dockerfile` - Custom Airflow image with required dependencies
- `spark/Dockerfile` - Custom Spark image with Iceberg support
- `docker-compose.yml` - Service orchestration configuration

## Development
- Use specific image versions for reproducibility
- Configure volume mounts for code hot-reloading
- Ensure proper service dependencies and health checks