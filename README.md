# Medallion Architecture Data Pipeline

## Overview
An end-to-end local data pipeline demonstrating the Medallion Architecture (Bronze, Silver, Gold). This project extracts raw nested JSON data, transforms it into a clean relational format, and aggregates it into business-ready metrics, fully orchestrated via Apache Airflow within a containerized Docker environment.

## Tech Stack
* **Languages:** Python, SQL
* **Data Processing:** Pandas
* **Orchestration:** Apache Airflow
* **Infrastructure:** Docker, Docker Compose
* **Version Control:** Git, GitHub

## Architecture Layers
1. **Bronze Layer (Ingestion):** Extracts raw, nested JSON payloads from external sources and lands them unchanged into the data lake.
2. **Silver Layer (Transformation):** Flattens nested arrays, enforces schema, and cleans raw data into structured, relational CSV files.
3. **Gold Layer (Aggregation):** Computes high-level business metrics (e.g., total user revenue) optimized for stakeholder dashboards and BI tools.

## Local Execution
To run this pipeline locally:
1. Clone this repository.
2. Initialize the Docker environment: `docker compose up -d`
3. Access the Airflow UI at `localhost:8080` to trigger the DAGs.