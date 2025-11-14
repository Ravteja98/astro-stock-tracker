📌 README — End-to-End Stock Market Data Pipeline (Airflow + Spark + MinIO + Postgres + Metabase)
🚀 Overview

This project is a fully containerized end-to-end Data Engineering pipeline built using free and open-source tools.
It ingests stock market data from the Yahoo Finance API, stores raw JSON in MinIO, processes it with PySpark, loads it into Postgres, and visualizes everything in Metabase.

All orchestration is handled by Apache Airflow, running inside a Docker network that simulates a small self-contained “cloud”.

🧩 Architecture

<img width="3187" height="909" alt="Stock Pipeline Architecture" src="https://github.com/user-attachments/assets/2fd7e418-074e-4707-a2b7-d18c83d90375" />

===============
🛠️ Tech Stack
===============

Component	Purpose
Airflow	Orchestration and scheduling
MinIO	S3-compatible object storage
PySpark	JSON flattening → Parquet conversion
Docker	Local containerized environment
Postgres	Data warehouse
Metabase	BI dashboard and analytics
Yahoo Finance API	Data source


=====================
📂 Project Structure
=====================

multiple_stock_prices/
│
├── dags/
│   └── stock_market.py
│
├── include/
│   └── stock_market/
│       ├── task.py
│       └── stock_transform.py
│
├── spark/
│   ├── master/
│   └── worker/
│
├── docker-compose.yml
├── requirements.txt
├── Dockerfile
└── README.md


⚙️ Pipeline Breakdown
1. API Availability Check

Airflow @task.sensor pings the Yahoo Finance API to ensure it's alive before any downstream tasks run.

2. Fetch Stock Prices

Pulls 90 days of data for the following symbols:

['AAPL', 'AMZN', 'GOOG', 'MSFT', 'NVDA']

3. Store Raw JSON → MinIO

Each API response is saved at:

minio://stock-market/<symbol>/<YYYY-MM-DD>/prices.json

4. PySpark JSON → Parquet

The Spark job:

Reads raw JSON

Explodes nested structures

Normalizes timestamps & indicators

Writes formatted Parquet files

Output path:

minio://stock-market/processed/<symbol>/*.parquet

5. Load into Postgres

Each processed batch is appended into:

public.stock_market


Columns include:

timestamp

symbol

open

close

high

low

volume

6. Visualize in Metabase

Metabase connects directly to Postgres to generate:

Performance comparisons

RSI signals

Volatility metrics

Daily trends

Volume distributions

🐳 Running the Project Locally
1. Start the Entire Stack
docker-compose up -d


This launches:

Airflow (webserver, scheduler, triggerer)

Spark master + worker

MinIO

Postgres

Metabase

2. Access Services
Service	URL
Airflow UI	http://localhost:8080

MinIO Console	http://localhost:9001

Metabase	http://localhost:3000

Postgres	localhost:5432
3. Trigger the DAG

In Airflow UI:

DAG: stock_market


Run manually or wait for the daily schedule.

📊 Example Dashboard (Metabase)

Attach the image you shared here:

![Metabase Dashboard](./include/data/metabase/dashboard.png)

🧪 Environment Variables

Make sure these are set in your .env or Airflow connections:

MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
AWS_ACCESS_KEY_ID=minio
AWS_SECRET_ACCESS_KEY=minio123
ENDPOINT=http://minio:9000

🔮 Future Enhancements

Airflow SLA + email alerts

Retry logic for flaky APIs

Partitioned warehouse tables

Adding Delta Lake support

Deploying to Kubernetes

🤝 Contributing

Pull requests, suggestions, and improvements are welcome.

📬 Contact

If you want a walkthrough or collaboration, reach out on LinkedIn - https://www.linkedin.com/in/ravichintalapudi/
