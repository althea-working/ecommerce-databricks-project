# ecommerce-databricks-project

## 📌 Overview
This project simulates a production-grade data engineering pipeline built on Databricks using a Lakehouse architecture.

It processes e-commerce data through multiple layers:
- Bronze (raw ingestion)
- Silver (data cleaning & transformation)
- Gold (aggregated analytics)

## 🏗️ Architecture
- Databricks (Delta Lake)
- Unity Catalog
- Volume for raw data (Bronze)
- Managed Tables for Silver and Gold
- Workflow orchestration with parameterized jobs

## 📂 Project Structure

ecommerce-databricks-project/
│
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   ├── common/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── conf/
├── tests/
└── README.md


## ⚙️ Features
- Parameterized pipelines with run_date
- Backfill support (date range processing)
- Incremental processing using MERGE
- Modular code structure (src + notebooks separation)

## 🚀 Future Improvements
- Data quality checks
- Partition optimization
- CI/CD integration
