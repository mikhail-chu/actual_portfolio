# 📊 BI Analytical Platform  
### Automated Analytics System for Business Decision-Making (FYP Project)

---

## 🧠 Project Overview

This project is an end-to-end **Business Intelligence and Analytics platform** designed to transform raw, fragmented business data into **structured, reliable, and interactive insights**.

It was developed as part of a **Bachelor Final Year Project (FYP)** and is based on real-world challenges encountered while working with operational business data in a multi-channel retail environment.

The system focuses on **automation, data quality, and usability**, aiming to support **data-driven decision-making** rather than isolated exploratory analysis.

---

## 🎯 Motivation

> I started this project after repeatedly facing the same issue while working with business data:  
> the data existed, but using it for analysis was slow, fragmented, and often unreliable.
>
> Sales, inventory, and marketing metrics were scattered across different sources, and preparing even a simple report required manual work and constant verification.
>
> This project is an attempt to solve that problem by building an automated analytics system that turns raw data into clear, structured, and interactive insights.
>
> The goal was not only to analyse data, but to design a solution that could realistically be used by a business to monitor performance, evaluate results, and support data-driven decisions.

---

## 🏗️ System Architecture

The platform follows a **modern BI architecture** and is built around the following layers:

<img width="316" height="245" alt="image" src="https://github.com/user-attachments/assets/4dffa314-96c9-4539-936f-bb4db851d7a7" />

### Key Design Principles
- **Automation first** – minimal manual intervention
- **Reproducibility** – deterministic pipelines and SQL logic
- **Scalability** – easy extension with new data sources
- **Business-oriented metrics** – not just raw data, but meaningful KPIs

---

## 🧩 Key Components

### 🔹 Data Ingestion & ETL
- Automated pipelines orchestrated with **Apache Airflow**
- Loading and transformation logic implemented via **SQL templates**
- Support for staging, dimension, fact, and mart layers
- Incremental and idempotent data loading strategies

### 🔹 Data Warehouse
- Centralised **PostgreSQL** storage
- Clear separation between:
  - staging tables
  - dimensions
  - fact tables
  - analytical marts
- Schema designed for analytical queries and reporting

### 🔹 BI Dashboard
- Interactive web application built with **Streamlit**
- Dynamic filtering by date, channel, category, and other business dimensions
- Visualisation of:
  - sales performance
  - revenue dynamics
  - inventory vs demand
  - campaign effectiveness
- Designed for **non-technical business users**

---

## 📂 Project Structure

<img width="599" height="290" alt="image" src="https://github.com/user-attachments/assets/41565c89-7cff-4b35-9833-20c46d60f99b" />

---

## 🛠️ Technology Stack

| Layer            | Tools & Technologies |
|------------------|----------------------|
| Orchestration    | Apache Airflow       |
| Backend / ETL    | Python, SQL          |
| Data Warehouse   | PostgreSQL           |
| BI & Visuals     | Streamlit            |
| Infrastructure  | Docker, Docker Compose |
| Analytics        | Pandas, SQL-based Marts |

---

## 📈 Use Cases

- Monitoring sales and revenue trends
- Analysing performance across channels and regions
- Evaluating marketing campaign effectiveness
- Comparing inventory levels with demand
- Providing a single source of truth for business analytics

---

## 🎓 Academic Context

- **Degree:** Bachelor of Computer Science (Data Analytics)
- **Project Type:** Final Year Project (FYP)
- **Focus Areas:**
  - Data Engineering
  - Business Intelligence
  - ETL Automation
  - Analytical System Design

The project bridges **academic concepts** with **real-world BI practices**, demonstrating how theoretical knowledge can be applied to practical business problems.

---

## 🚀 Future Improvements

- Role-based access control (RBAC)
- Automated data quality checks
- Real-time or near-real-time ingestion
- Deployment to cloud infrastructure
- Extended analytical models and forecasting

---

## 📌 Disclaimer

This project is a **technical and academic implementation**.  
Schemas, metrics, and examples are adapted for demonstration purposes and do not represent real company data.

---

## 👤 Author

**Mikhail Churilov**  
Bachelor FYP — Business Intelligence & Data Analytics
