# Earthquakes-ASVSP 🌍⚡  
**End-to-End Batch + Streaming Data Pipeline with Kafka, Spark & ML**

This project implements a full data engineering pipeline for earthquake data, combining **batch processing**, **real-time streaming**, and a **machine learning prediction workflow**. The system ingests data from both historical and live sources, processes it using Apache Spark (batch + Structured Streaming), stores curated datasets, and supports visualization and real-time serving via a backend API.

---

## 🧩 System Architecture

The pipeline is composed of four modules:

### 1) Batch Processing Module
- **Batch data source:** Stanford earthquakes dataset  
- **Storage layers:** HDFS (Raw zone → Transformation zone)  
- **Processing:** Apache Spark batch jobs  
- **Curated output:** PostgreSQL (analytics-ready dataset)

### 2) Real-Time Streaming Module
- **Live data source:** USGS Earthquake API  
- **Message broker:** Apache Kafka  
- **Stream processing:** Spark Structured Streaming  
- **Storage:** MongoDB (near real-time events)

### 3) Prediction Module
- **ML pipeline (Spark ML):**
  - VectorAssembler  
  - RandomForestRegressor  
  - Model evaluation  
- **Output:** trained prediction model + streaming scoring workflow

### 4) Visualization Module
- **Metabase** for analytics dashboards  
- **Kepler.gl** for map-based earthquake visualization  
- **Backend:** FastAPI + WebSocket for real-time updates

---

## 🛠 Tech Stack

- **Apache Spark** 
- **Apache Kafka**
- **HDFS**
- **PostgreSQL**
- **MongoDB**
- **FastAPI + WebSocket**
- **Metabase**
- **Kepler.gl**
- **Docker**
- **Shell scripts** (orchestration)

---

## 🚀 Key Features

- End-to-end pipeline with both **batch** and **real-time** ingestion
- Spark-based transformation layer (ETL) with curated outputs
- Streaming ingestion through Kafka with near real-time processing
- Separate curated stores for analytics (PostgreSQL) and events (MongoDB)
- ML training pipeline using Spark MLlib
- Real-time backend updates via WebSocket
- Fully containerized setup using Docker


