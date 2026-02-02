

# Real-Time Fraud & Performance Analytics Platform

A **production-grade, real-time Big Data analytics pipeline** for digital payments, built as part of the **Big Data Analytics course group project**.
The system simulates live transaction streams, performs real-time KPI computation, enforces storage governance, and visualizes insights via a live dashboard.

---

## 📌 Problem Domain

**Domain:** FinTech / Digital Payments

Modern payment platforms process millions of transactions per day. Batch analytics are insufficient for:

* **Real-time fraud detection**
* **Operational health monitoring**
* **Revenue velocity tracking**

This project implements a **streaming architecture** to deliver immediate insights with low latency.

---

## 🏗️ System Architecture (High Level)

```
<img width="404" height="660" alt="image" src="https://github.com/user-attachments/assets/dd67f40d-465e-4b0d-a704-42d2d882d00a" />

```

---

## 🔧 Technology Stack

| Component              | Technology              |
| ---------------------- | ----------------------- |
| Orchestration          | Apache Airflow          |
| Containerization       | Docker & Docker Compose |
| Streaming / Processing | Apache Spark (PySpark)  |
| NoSQL Database         | MongoDB                 |
| Distributed Storage    | Hadoop HDFS             |
| Caching                | Redis                   |
| Visualization          | Streamlit               |
| Data Generation        | SDV (CTGAN)             |

---

## 📂 Repository Structure

```
BDA-PROJECT/
│
├── dags/                     # Airflow DAGs
│
├── data/
│   └── archive/              # HDFS archived data (Parquet)
│
├── jars/                     # Spark / connector JARs
├── logs/                     # Airflow & Spark logs
│
├── scripts/
│   ├── ai_generator.py       # AI-based streaming data generator
│   ├── archive_job.py        # Spark job: MongoDB → HDFS
│   ├── kpi_job.py            # Spark job: KPI computation
│   └── setup_dimensions.py  # Dimension table setup
│
├── streamlit/
│   └── app.py                # Live BI dashboard
│
├── Dockerfile                # Service container definitions
├── docker-compose.yml        # Full system orchestration
├── requirements.txt          # Python dependencies
└── README.md
```

---

## 🔄 Data Pipeline Overview

### 1️⃣ Data Generation

* **AI-based streaming generator** using **CTGAN (SDV)**
* Learns realistic fraud and transaction patterns
* Streams one transaction per second into MongoDB

📄 `scripts/ai_generator.py`

---

### 2️⃣ Operational Storage (Hot)

* MongoDB stores high-velocity incoming data
* Acts as the **fact table** in a star schema

---

### 3️⃣ KPI Processing (Spark)

Spark performs:

* Data cleansing & validation
* Fact → Dimension joins
* Aggregations for KPIs

**Key KPIs:**

* Total Transaction Volume
* Average Ticket Size
* Fraud Rate (%)
* Processing Latency
* Transactions Per Minute
* Revenue by Category

📄 `scripts/kpi_job.py`

---

### 4️⃣ Caching Layer

* Aggregated KPIs pushed to **Redis**
* Enables **sub-second dashboard refresh**
* Prevents repeated database scans

---

### 5️⃣ Archiving & Data Governance

* MongoDB size monitored continuously
* **300MB hot-storage threshold**
* Older data archived to **HDFS (Parquet format)**
* Metadata logged for traceability

📄 `scripts/archive_job.py`

---

## ⏱️ Airflow Orchestration

Two production DAGs orchestrate the system:

### 🔹 `kpi_refresh_dag`

* Runs periodically
* Triggers Spark KPI job
* Updates Redis cache

### 🔹 `mongo_to_hdfs_archiving`

* Monitors MongoDB size
* Archives old data to HDFS
* Cleans hot storage

📁 `dags/`

---

## 📊 Live Dashboard (Streamlit)

Features:

* Auto-refresh every 30 seconds
* Live transaction metrics
* Fraud spikes visualization
* Revenue & category analytics

📄 `streamlit/app.py`

---

## 🚀 How to Run

```bash
# Start the full system
docker-compose up -d

# Access Airflow
http://localhost:8080

# Access Streamlit Dashboard
http://localhost:8501
```

---

## ✅ Project Outcome

* Fully containerized, scalable architecture
* Real-time analytics with low latency
* Automated orchestration & governance
* Production-ready Big Data pipeline

---

## 📜 License

This project is developed for academic purposes as part of the **Big Data Analytics course**.

