# Config Directory

This directory contains environment-specific configuration files for the F1 data pipeline.

## Purpose
- Manage environment-specific settings (dev, prod)
- Define AWS service configurations
- Store API endpoints and connection parameters

## Files
- `dev.yaml` - Development environment configuration
- `prod.yaml` - Production environment configuration
- Environment variables should be managed via `.env` files

## Usage
Configuration files are loaded by the application based on the environment setting.
Use YAML format for structured configuration data.