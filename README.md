# GitHub Archive ELT Pipeline

A modern Data Engineering project that automates the journey from raw **GHArchive** JSON events to high-value analytical views. This project utilizes **Airflow** for orchestration, **DuckDB** for in-memory processing, and **Terraform** for Infrastructure as Code (IaC).

## 🚀 Architecture Overview

The pipeline ensures data integrity and scalability by separating the infrastructure, the compute-heavy transformation, and the warehouse storage logic:

1. **Infrastructure:** All AWS S3 buckets and Snowflake stages are provisioned via Terraform to ensure a reproducible environment.
2. **Ingestion & Transformation:** **Airflow** fetches the raw JSON from GHArchive. To optimize performance, **DuckDB** is used within the Airflow worker to transform this raw JSON into an optimized **Parquet** format. Both the raw JSON (for auditability) and the processed Parquet (for speed) are saved to **S3**.
3. **Orchestration & Snowflake Storage:** **Airflow** manages the Medallion-style lifecycle within Snowflake and the on-demand refresh of analytical logic:
    * **Staging:** High-speed ingestion of the processed Parquet files into a temporary layer.
    * **Transformations/Dims:** Deduplication of events and entity resolution for repositories and actors (handling renames).
    * **Marts:** Creation of business-ready, dynamic visualizations for end-user reporting.

## 🛠️ Tech Stack

* **Orchestration:** Apache Airflow
* **Data Lake:** AWS S3
* **Data Warehouse:** Snowflake
* **Language:** SQL & Python

## 📂 Project Structure

```text
.
├── airflow/
│   ├── dags/
│   │   ├── gharchive_through_s3_into_snowflake.py  # Main ETL (Hourly)
│   │   └── create_snowflake_views.py               # Analytical Setup (On-demand)
│   └── sql/
│       ├── ddl/             # Schema and Table definitions
│       ├── staging/         # S3 Parquet to Snowflake COPY logic
│       ├── transformations/ # MERGE Staging into Core table
│       ├── dims/            # Dimension views (Actors & Repositories)
│       └── marts/           # High-level analytical views
│
├── terraform/
│   └── main.tf              # Cloud infrastructure (S3)
│
└── snowflake/
    └── sql/                 # Snowflake-native logic and scripts