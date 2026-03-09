Basel III Regulatory Compliance Reporting Pipeline
📊 Overview
This project implements an end-to-end Big Data pipeline designed for the financial sector. It automates the calculation of core Basel III Liquidity and Capital metrics (CAR, LCR, NPL) using Apache Spark, orchestrated by Airflow, and served via a Plotly Dash dashboard.

🏗️ Architecture
Data Layer: Simulated bank balance sheets (millions of records) generated via Python/Faker.

Ingestion: External industry benchmarks fetched via the FDIC BankFind API.

Processing: PySpark cluster (Master/Worker) performing distributed aggregations, window functions, and broadcast joins.

Orchestration: Apache Airflow DAG managing task dependencies, retries, and SLAs.

Serving Layer: PostgreSQL for metric storage and Plotly Dash for the regulatory UI.

🛠️ Tech Stack
Language: Python 3.13

Big Data: Apache Spark (PySpark)

Orchestration: Apache Airflow

Database: PostgreSQL

Infrastructure: Docker & Docker Compose

Visualization: Plotly Dash

🚀 How to Run
Clone the repository.

Ensure Docker Desktop is running.

Run docker-compose up -d.

Access Airflow at localhost:8080 to trigger the pipeline.

Run python dashboard.py to view results at localhost:8050
