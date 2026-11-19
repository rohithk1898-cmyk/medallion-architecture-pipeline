# Azure Databricks Medallion Architecture Pipeline

An end-to-end cloud data engineering pipeline implementing the **Medallion Architecture (Bronze, Silver, Gold)** on Microsoft Azure. This project ingests raw sales data into Azure Data Lake Storage Gen2, transforms and refines it into Delta Lake tables using PySpark and Spark SQL in Azure Databricks, governs the curated dataset through Unity Catalog, automates daily execution via Databricks Workflows, and serves live business metrics to Power BI.

## Architecture Overview

    [Raw Sales Data]
           │
           ▼
    ┌───────────────────────────────────────────────────────────┐
    │          Azure Data Lake Storage Gen2 (ADLS Gen2)         │
    │   Bronze (Raw) ──► Silver (Cleaned) ──► Gold (Aggregated) │
    └─────────────────────────────┬─────────────────────────────┘
                                  │  PySpark & Spark SQL (Delta Lake)
                                  ▼
    ┌───────────────────────────────────────────────────────────┐
    │                     Azure Databricks                      │
    │  • ETL Notebook: medallion_etl_pipeline                   │
    │  • Orchestration: Databricks Workflows (Daily @ 2:00 AM)  │
    │  • Governance: Unity Catalog Managed Table                │
    └─────────────────────────────┬─────────────────────────────┘
                                  │  DirectQuery (Scoped PAT)
                                  ▼
    ┌───────────────────────────────────────────────────────────┐
    │                     Power BI Desktop                      │
    │          Customer Lifetime Value (CLV) Dashboard          │
    └───────────────────────────────────────────────────────────┘

## Tech Stack

| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Cloud Storage** | Azure Data Lake Storage Gen2 | Hierarchical namespace storage organized into `bronze`, `silver`, and `gold` containers |
| **Processing & ETL** | Azure Databricks (PySpark, Spark SQL) | Distributed data ingestion, cleansing, deduplication, and aggregation |
| **Storage Format** | Delta Lake | ACID transactions, schema enforcement, and reliable lakehouse tables |
| **Data Governance** | Databricks Unity Catalog | Centralized metadata management and secure table serving |
| **Orchestration** | Databricks Workflows (Jobs) | Automated end-to-end pipeline scheduling (daily at 2:00 AM) |
| **BI & Analytics** | Power BI Desktop | DirectQuery visualization of Customer Lifetime Value metrics |
| **Security & Auth** | Azure Service Principal (OAuth 2.0) & PAT | Scoped storage authentication and secure BI connectivity |
| **Version Control** | Databricks Git Folders & GitHub | Native workspace-to-GitHub source control |

## Pipeline Stages

1. **Bronze Layer (Raw Ingestion):** Authenticates to ADLS Gen2 via an Azure Service Principal and ingests raw source datasets into the `bronze` container in Delta format, preserving an immutable historical record for auditability and reprocessing.
2. **Silver Layer (Cleaned & Conformed):** Applies PySpark and Spark SQL transformations to handle nulls, standardize schemas, and deduplicate records before writing curated datasets to the `silver` layer.
3. **Gold Layer (Business Aggregations):** Aggregates transactional records to compute key business KPIs—specifically `customer_id` and `total_lifetime_value`—and stores the output in the `gold` layer.
4. **Unity Catalog Serving:** Materializes the Gold dataset as a governed Unity Catalog Managed Table (`gold_customer_lifetime_value`), ensuring seamless discovery and secure access for downstream analytics tools without relying on legacy Hive Metastore access.
5. **Orchestration & Visualization:** A scheduled Databricks Job executes the full notebook daily at 2:00 AM. Power BI connects directly to Unity Catalog via **DirectQuery** to render a descending-sorted **Customer Lifetime Value** clustered column chart.

## Repository Structure

    ├── medallion_etl_pipeline.ipynb   # End-to-end Bronze-Silver-Gold PySpark & SQL notebook
    └── README.md                      # Project documentation and architecture overview

## Setup & Configuration

To run this pipeline in your own Azure Databricks environment:

1. **Provision Azure Resources:**
   * Create an **Azure Data Lake Storage Gen2** account with `bronze`, `silver`, and `gold` containers.
   * Register an **Azure Service Principal** (Microsoft Entra ID) and assign it the **Storage Blob Data Contributor** role on the storage account.
   * Provision an **Azure Databricks** workspace with Unity Catalog enabled.
2. **Configure Credentials:**
   * Open `medallion_etl_pipeline.ipynb` and replace the placeholder values in Cell 1 (`client_id`, `tenant_id`, `client_secret`) with your Service Principal details (or bind them via Databricks Secret Scopes).
3. **Run & Schedule:**
   * Attach the notebook to a Databricks cluster and run all cells to verify Bronze, Silver, and Gold outputs.
   * Create a **Databricks Workflow (Job)** pointing to the notebook with a daily schedule.
4. **Connect Power BI:**
   * Open Power BI Desktop, select **Azure Databricks** as the data source, authenticate with a Personal Access Token (PAT), and connect to the Gold table in Unity Catalog using **DirectQuery**.

## Production Hardening & Future Enhancements

* **Secret Management:** Migrate Service Principal credentials from notebook variables to an **Azure Key Vault-backed Databricks Secret Scope** (`dbutils.secrets.get()`).
* **Unity Catalog External Locations:** Configure a Unity Catalog Storage Credential and External Location for the Gold ADLS Gen2 path to query external Delta tables directly without copying into managed storage.
* **Cost Optimization (FinOps):** Transition the scheduled Workflow from an All-Purpose cluster to an ephemeral **Job Cluster** to reduce DBU costs and guarantee automatic compute teardown.
