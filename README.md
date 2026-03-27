# PySpark Retail Sales ETL Pipeline

An end-to-end **PySpark Data Engineering project** built using **Bronze / Silver / Gold architecture**.  
This project simulates a real-world retail sales batch pipeline using CSV input files and transforms them into analytics-ready outputs using **PySpark**.

---

## 🚀 Project Overview

This project demonstrates how a Data Engineer can build a local batch ETL pipeline using PySpark on Windows.

### Pipeline Flow
- **Raw Layer** → Input CSV files
- **Bronze Layer** → Raw data ingestion and standardization
- **Silver Layer** → Business transformations and joins
- **Gold Layer** → Aggregated KPI datasets for analytics

---

## 🧱 Architecture

```text
Raw CSV Files
    ↓
Bronze Layer (Raw ingestion / schema alignment)
    ↓
Silver Layer (Joins / transformations / revenue calculations)
    ↓
Gold Layer (Business KPIs / aggregated reports)