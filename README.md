# ecommerce-databricks-project

## 📌 Overview

This project simulates a production-grade end-to-end data engineering and analytics solution for an e-commerce platform.

It covers the full pipeline from raw data ingestion to business-level analytics and dashboard visualization, with a focus on user behavior, retention, and conversion insights.

---

## 🏗️ Architecture

* **Databricks Lakehouse (Delta Lake)**
* **Unity Catalog** for data governance
* **Bronze Layer** – raw data ingestion from volumes
* **Silver Layer** – cleaned and structured data
* **Gold Layer** – business-ready aggregated tables
* **Power BI** – dashboard and visualization layer

---

## 📊 Analytics & Dashboards

This project includes a complete set of business dashboards built in Power BI:

### 1️⃣ User Activity Analysis

* DAU / WAU / MAU tracking
* 30-day activity trend visualization

### 2️⃣ Retention Analysis

* Cohort-based retention (D1 / D7 / D30)
* Retention trend (rolling 60-day window)
* Cohort heatmap analysis

### 3️⃣ Funnel Analysis

* User conversion funnel (view → cart → purchase)
* Step-by-step drop-off analysis

### 4️⃣ User Profile Analysis

* RFM segmentation (Recency, Frequency, Monetary)
* High-value user identification
* User preference insights (Top categories & brands)

---

## 🧱 Data Model (Gold Layer)

### 👤 User Behavior

* `gold_user_activity_metrics`
* `gold_user_activity_metrics_30d`
* `gold_session_metrics_daily`

### 🔁 Retention

* `gold_user_cohort`
* `gold_user_retention`
* `gold_user_retention_summary`
* `gold_user_retention_trend_30d`
* `gold_user_retention_cohort_30d`

### 🔄 Funnel

* `gold_user_funnel_daily`
* `gold_user_funnel_daily_30d`

### 👤 User Profile & Segmentation

* `gold_user_profile_current`
* `gold_user_profile_snapshot`
* `gold_user_rfm_segment`

### ❤️ User Preferences

* `gold_user_preference_detail`
* `gold_user_preference_summary`
* `gold_top_category_distribution`
* `gold_top_brand_top10`

### 🛍️ Interaction

* `gold_user_product_interaction`

---

## 🧹 Silver Layer

* `silver_events`
* `silver_users`
* `silver_products`
* `silver_user_activity_daily`

---

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
├── dashboards/
│   ├── ecommerce_user_analytics.pbix
│   └── screenshots/
│
├── conf/
├── tests/
└── README.md

---

## ⚙️ Features

- Parameterized pipelines using run_date  
- Backfill support for historical data processing  
- Incremental processing with Delta MERGE and replaceWhere  
- Workflow orchestration using Databricks Jobs with task dependencies  
- Basic data quality validation with custom checks  
- Alerting mechanism via webhook (Slack integration)  
- Modular project structure (src + notebooks separation)  
- Version control using Git (feature branch workflow)
---

## 📈 Dashboard Preview

### User Activity
https://dbc-b91e2fae-9353.cloud.databricks.com/editor/files/2605602905518835?o=7474647720570546$0
### Retention Analysis
https://dbc-b91e2fae-9353.cloud.databricks.com/editor/files/2605602905518835?o=7474647720570546$0
### Funnel Analysis
https://dbc-b91e2fae-9353.cloud.databricks.com/editor/files/2605602905518836?o=7474647720570546$0
### User Profile
https://dbc-b91e2fae-9353.cloud.databricks.com/editor/files/2605602905518837?o=7474647720570546$0
---

## 🚀 Future Improvements

## 🚀 Future Improvements

- Integrate a standardized data quality framework (e.g. Great Expectations)  
- Implement CI/CD pipeline with automated testing and deployment  
- Optimize performance with Z-ordering and indexing strategies  
- Add real-time streaming pipeline (Spark Structured Streaming)
---

## 💡 Key Highlights

* End-to-end data pipeline from ingestion to visualization
* Business-focused metrics (Retention, Funnel, RFM)
* Scalable data modeling using Delta Lake
* Production-style project structure
* Clear separation between data engineering and analytics layers

---
