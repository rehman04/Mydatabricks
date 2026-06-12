<div align="center">
<img src="https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png" width="280"/>

# 🚀 My Databricks Workspace

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)](https://en.wikipedia.org/wiki/SQL)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=for-the-badge&logo=databricks&logoColor=white)](https://delta.io)

*A comprehensive collection of Databricks notebooks, pipelines, and data engineering projects.*

</div>

---

## 📋 Table of Contents
- [📖 Overview](#-overview)
- [📁 Repository Structure](#-repository-structure)
- [🎓 Learning Paths](#-learning-paths)
- [🔬 Projects](#-projects)
- [🛠️ Technologies](#️-technologies)
- [🚀 Getting Started](#-getting-started)
- [📊 Data Assets](#-data-assets)

---

## 📖 Overview

This repository contains my complete **Databricks workspace** — spanning data engineering coursework, real-world analytics projects, and hands-on experiments with **Apache Spark**, **Delta Lake**, and the Databricks Lakehouse Platform.

Topics covered include **Auto Loader**, **Delta Live Tables**, **Unity Catalog**, **MLflow**, fraud detection ML models, geospatial analytics, and more.

> 📓 All notebooks are in **Jupyter format** (`.ipynb`) — they render directly in GitHub with full outputs, charts, and results.

---

## 📁 Repository Structure

```
Mydatabricks/
│
├── 📚 data-engineering-with-databricks/           # Core DE course
├── 📚 advanced-data-engineering-with-databricks/  # Advanced DE techniques  
├── 📚 apache-spark-programming-with-databricks/   # Spark fundamentals
│
├── 🔬 BD Assess/                                  # Big Data assessment projects
│   └── Fraud Detection ML Model.ipynb            # AutoML fraud detection
├── 🔬 Insurance Data quality/                     # Insurance data quality checks
├── 🔬 testinsightvsfinact/                        # Insight vs financial actuals
├── 🔬 ev json/                                    # EV JSON ingestion pipeline
├── 🔬 create-pipeline-from-sample-data/           # Sample data pipeline
│
├── 📓 map visualization for bad deliveries.ipynb  # Geospatial delivery analysis
├── 📓 demo test json ingestion from s3.ipynb      # S3 JSON ingestion demo
├── 📓 demoVlmflower.ipynb                         # MLflow / VLM model demo
├── 📓 secrects demo.ipynb                         # Databricks Secrets usage
├── 📓 Sample job - ingestion task.ipynb           # Sample ingestion job
├── 📓 Sample job - filtering and reporting.ipynb  # Filtering & reporting
│
├── 🗺️ delivery_compliance_map.html               # Interactive London delivery map
├── 🗺️ non_compliant_deliveries_map.html          # Non-compliant zones map
└── 📊 *.csv                                       # Sample datasets
```

---

## 🎓 Learning Paths

### 🟢 Data Engineering with Databricks
| Topic | Description |
|-------|-------------|
| Delta Lake | ACID transactions, time travel, schema enforcement |
| Auto Loader | Incremental file ingestion with `cloudFiles` |
| Delta Live Tables | Declarative pipeline development |
| Workflow Jobs | Orchestrating multi-task pipelines |
| Unity Catalog | Governance, access control & lineage |

### 🔵 Advanced Data Engineering
| Topic | Description |
|-------|-------------|
| Change Data Capture | Auto CDC with `APPLY CHANGES INTO` |
| Incremental Processing | Watermarks and stateful streaming |
| Performance Tuning | Z-ordering, partitioning, liquid clustering |
| Testing & Monitoring | Data quality expectations and alerting |

### 🟣 Apache Spark Programming
| Topic | Description |
|-------|-------------|
| Spark SQL | DataFrames, SQL queries, query optimisation |
| Structured Streaming | Real-time data processing patterns |
| Spark ML | Machine learning pipelines at scale |
| Performance | Caching, broadcast joins, Adaptive Query Execution |

---

## 🔬 Projects

### 🤖 Fraud Detection ML Model
> `BD Assess/Big Data Asses Fraud Detection Creating ML Model.ipynb`

End-to-end fraud detection pipeline using Databricks AutoML and MLflow:
- 🔍 Exploratory analysis on transaction types (fraud vs normal)
- 🏗️ Feature engineering with **Databricks Feature Store**
- 🤖 AutoML classification with automatic hyperparameter tuning
- 📦 Model registration in **Unity Catalog** model registry
- 🔮 Real-time predictions with schema-aligned inference

### 🚚 Delivery Compliance Analytics
> `map visualization for bad deliveries.ipynb` · `delivery_compliance_map.html`

Interactive geospatial analysis of delivery compliance across London postcodes:
- 🗺️ Interactive Folium HTML maps highlighting non-compliant zones
- 📍 Postcode-level compliance scoring with real London coordinates
- 📊 Synthetic delivery dataset for reproducible analysis

### ⚡ EV Data Ingestion Pipeline
> `ev json/`

End-to-end JSON ingestion pipeline for Electric Vehicle datasets with Delta Lake storage.

### 🏥 Insurance Data Quality
> `Insurance Data quality/`

Data quality framework — validation rules, anomaly detection, and quality reporting dashboards.

### 📈 Business Data Assessment
> `BD Assess/`

BI assessment framework comparing insight data against financial actuals with automated reporting.

### ☁️ S3 JSON Ingestion Demo
> `demo test json ingestion from s3.ipynb`

Demonstrates reading and processing JSON files from AWS S3 into the Databricks Lakehouse.

---

## 🛠️ Technologies

<div align="center">

| Category | Technologies |
|----------|-------------|
| **Platform** | Databricks, Apache Spark 3.x, AWS |
| **Storage** | Delta Lake, DBFS, Unity Catalog |
| **Languages** | Python (PySpark), SQL, Scala |
| **Ingestion** | Auto Loader, Delta Live Tables |
| **ML & AI** | MLflow, Databricks AutoML, Feature Store |
| **Visualisation** | Plotly, Folium, HTML interactive maps |
| **Orchestration** | Databricks Jobs |
| **Security** | Databricks Secrets, Unity Catalog RBAC |

</div>

---

## 🚀 Getting Started

### Prerequisites
- Databricks workspace (DBR 12.0+ recommended)
- Unity Catalog enabled
- AWS S3 access (for cloud storage demos)

### Import to Your Databricks Workspace

```
Workspace → Repos → Add Repo
→ https://github.com/rehman04/Mydatabricks.git
```

Then attach a cluster (**DBR 13.x LTS** or Serverless) and open any notebook.

### Recommended Learning Order
```
1️⃣  apache-spark-programming-with-databricks-2.3.1/
2️⃣  data-engineering-with-databricks/
3️⃣  advanced-data-engineering-with-databricks/
4️⃣  BD Assess/  ← Fraud Detection ML project
5️⃣  Explore remaining project folders
```

---

## 📊 Data Assets

| File | Description |
|------|-------------|
| `synthetic_delivery_data.csv` | Synthetic London delivery records |
| `real_london_postcodes_data.csv` | Real UK postcode reference data |
| `real_london_postcodes_lat_long.csv` | Postcode latitude/longitude coordinates |

---

<div align="center">

⭐ **If you find this helpful, please give it a star!**

Made with ❤️ using [Databricks](https://databricks.com) on AWS

</div>
