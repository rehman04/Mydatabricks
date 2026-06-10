<div align="center">

<img src="https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png" alt="Databricks Logo" width="300"/>

# 🚀 My Databricks Workspace

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)](https://en.wikipedia.org/wiki/SQL)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/rehman04/Mydatabricks)

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

This repository contains my complete Databricks workspace — spanning data engineering coursework, real-world analytics projects, and hands-on experiments with Apache Spark, Delta Lake, and the Databricks platform.

It covers everything from foundational Spark programming to advanced data engineering patterns including **Auto Loader**, **Delta Live Tables**, **Unity Catalog**, and **MLflow**.

---

## 📁 Repository Structure

```
Mydatabricks/
│
├── 📚 data-engineering-with-databricks/          # Core DE course content
├── 📚 advanced-data-engineering-with-databricks/ # Advanced DE techniques
├── 📚 apache-spark-programming-with-databricks-2.3.1/ # Spark fundamentals
│
├── 🔬 Insurance Data quality/                    # Insurance data quality project
├── 🔬 BD Assess/                                 # Business data assessment
├── 🔬 testinsightvsfinact/                        # Insight vs financial actuals
├── 🔬 ev json/                                    # EV data JSON ingestion
├── 🔬 create-pipeline-from-sample-data/           # Pipeline from sample data
│
├── 📓 map visualization for bad deliveries.py    # Delivery compliance maps
├── 📓 demo test json ingestion from s3.py        # S3 JSON ingestion demo
├── 📓 demoVlmflower.py                           # VLM/MLflow demo
├── 📓 secrects demo.py                           # Databricks Secrets demo
├── 📓 Sample job - ingestion task.py             # Sample ingestion job
├── 📓 Sample job - filtering and reporting task.py # Filtering & reporting
├── 📓 2025-04-06 - DBFS Example.py              # DBFS usage example
│
├── 🗺️ delivery_compliance_map.html               # Interactive compliance map
├── 🗺️ non_compliant_deliveries_map.html          # Non-compliant deliveries map
│
└── 📊 *.csv                                      # Sample datasets
```

---

## 🎓 Learning Paths

### 🟢 Data Engineering with Databricks
> *Foundational data engineering concepts on the Databricks Lakehouse Platform*

| Topic | Description |
|-------|-------------|
| Delta Lake | ACID transactions, time travel, schema enforcement |
| Auto Loader | Incremental file ingestion with `cloudFiles` |
| Delta Live Tables | Declarative pipeline development |
| Workflow Jobs | Orchestrating multi-task pipelines |
| Unity Catalog | Data governance and access control |

### 🔵 Advanced Data Engineering
> *Production-grade patterns and best practices*

| Topic | Description |
|-------|-------------|
| Change Data Capture | Auto CDC with `APPLY CHANGES INTO` |
| Incremental Processing | Watermarks and stateful streaming |
| Performance Tuning | Z-ordering, partitioning, liquid clustering |
| Testing & Monitoring | Data quality expectations and alerts |

### 🟣 Apache Spark Programming
> *Deep dive into Spark APIs and distributed computing*

| Topic | Description |
|-------|-------------|
| Spark SQL | DataFrames, SQL queries, optimizations |
| Structured Streaming | Real-time data processing |
| Spark ML | Machine learning pipelines |
| Performance | Caching, broadcast joins, AQE |

---

## 🔬 Projects

### 🚚 Delivery Compliance Analytics
**Files:** `map visualization for bad deliveries.py` · `delivery_compliance_map.html` · `non_compliant_deliveries_map.html`

Interactive geospatial analysis of delivery compliance across London postcodes. Features:
- 🗺️ Interactive HTML maps visualising bad delivery zones
- 📍 Postcode-level compliance scoring
- 📊 Synthetic delivery dataset generation

### ⚡ EV Data Ingestion
**Folder:** `ev json/`

JSON data ingestion pipeline for Electric Vehicle datasets with schema inference and Delta Lake storage.

### 🏥 Insurance Data Quality
**Folder:** `Insurance Data quality/`

Data quality framework for insurance datasets — validation rules, anomaly detection, and quality reporting.

### 📈 Business Data Assessment
**Folder:** `BD Assess/`

Business intelligence assessment framework comparing insight data against financial actuals.

### 🔐 Secrets & Security Demo
**File:** `secrects demo.py`

Demonstration of Databricks Secrets management for secure credential handling.

---

## 🛠️ Technologies

<div align="center">

| Category | Technologies |
|----------|-------------|
| **Platform** | Databricks, Apache Spark 3.x |
| **Storage** | Delta Lake, DBFS, Unity Catalog |
| **Languages** | Python (PySpark), SQL, Scala |
| **Ingestion** | Auto Loader, Delta Live Tables |
| **ML** | MLflow, VLM models |
| **Visualisation** | Folium, HTML maps |
| **Orchestration** | Databricks Jobs & Workflows |
| **Security** | Databricks Secrets, Unity Catalog RBAC |

</div>

---

## 🚀 Getting Started

### Prerequisites
- Databricks workspace (DBR 12.0+ recommended)
- Unity Catalog enabled
- Python 3.9+

### Import to Databricks

1. **Clone this repo** into your Databricks workspace:
   ```
   Repos → Add Repo → https://github.com/rehman04/Mydatabricks.git
   ```

2. **Attach a cluster** — DBR 13.x LTS or Serverless compute

3. **Run a notebook** — start with any folder's `00-Introduction` or `README` notebook

### Recommended Learning Order
```
1. apache-spark-programming-with-databricks-2.3.1/
2. data-engineering-with-databricks/
3. advanced-data-engineering-with-databricks/
4. Explore project folders
```

---

## 📊 Data Assets

| File | Description | Size |
|------|-------------|------|
| `synthetic_delivery_data.csv` | Synthetic London delivery records | ~47 MB |
| `real_london_postcodes_data.csv` | Real London postcode data | — |
| `real_london_postcodes_lat_long.csv` | Postcode lat/long coordinates | — |

---

<div align="center">

**⭐ If you find this useful, give it a star!**

Made with ❤️ using [Databricks](https://databricks.com)

![Visitor Count](https://visitor-badge.laobi.icu/badge?page_id=rehman04.Mydatabricks)

</div>
