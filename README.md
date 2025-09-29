# NASA APOD ETL Pipeline with Apache Airflow

## Introduction
The **NASA Astronomy Picture of the Day (APOD)** is a public API that publishes one astronomy-related image every day since **1995**.  
Each record contains:
- `date` → the publication date  
- `title` → title of the image  
- `explanation` → scientific description  
- `url` → image or video link  

This project demonstrates how to build a **modern ETL pipeline** for APOD data using **Apache Airflow**, **MinIO (S3-compatible storage)**, and **PostgreSQL**.  
It’s lightweight, visual, and expandable — perfect for analysts and portfolio showcase.  

---

## Architecture

```text
NASA API → Extract → MinIO (Raw) → Transform → MinIO (Staged) → Load → PostgreSQL
