# airflow-postgres-etl
A simple Airflow project running on Docker

# Airflow + PostgreSQL ETL (Beginner Project)

This project is a simple ETL (Extract, Transform, Load) pipeline built with Apache Airflow and PostgreSQL, all running locally with Docker. The goal was to learn the basics of orchestrating data pipelines, moving data through an ETL workflow, and loading the processed data into a database.

## Overview

```
data/sample_sales.csv
        │
        ▼
    Extract
        │
        ▼
    Transform
        │
        ▼
       Load
        │
        ▼
PostgreSQL (sales_orders)
```

The pipeline reads a CSV file containing sample sales data, removes any incomplete records, calculates a new `total_amount` field, and loads the cleaned data into a PostgreSQL table named `sales_orders`.

> This project uses Airflow 3.2.2 in standalone mode, which runs the web interface, scheduler, triggerer, and other required services in a single process. It's the easiest way to get started with Airflow on a local machine.

## Project Structure

```text
airflow-postgres-etl/
├── dags/
│   └── etl_csv_to_postgres.py   # Airflow DAG
├── data/
│   └── sample_sales.csv         # Sample input data
├── logs/                        # Airflow task logs
├── plugins/                     # Optional custom plugins
├── docker-compose.yml           # Airflow and PostgreSQL services
├── requirements.txt             # Additional Python dependencies
└── README.md
```

## Prerequisites

Before running the project, make sure you have:

* Docker Desktop installed and running
* Around 4 GB of available RAM for Docker

## Running the Project

### 1. Start the services

From the project directory, run:

```bash
docker compose up -d
```

If this is your first time starting the project, Docker will download the required Airflow and PostgreSQL images and install a few additional Python packages, including `pandas` and the PostgreSQL provider. This may take a few minutes.

### 2. Open the Airflow dashboard

Once everything has started, open:

```
http://localhost:8081
```

Use the following username:

```
admin
```

Airflow generates a random password during the first startup. You can retrieve it with:

```bash
docker compose logs airflow | grep -A 1 "Simple auth manager"
```

You can also search through the startup logs for a line similar to:

```
Password for user 'admin': xxxxxxxx
```

## Running the ETL Pipeline

1. Open the Airflow dashboard.
2. Locate the etl_csv_to_postgres DAG.
3. Toggle it on to unpause it.
4. Click Trigger DAG.
5. Watch the three tasks (`extract`, `transform`, and `load`) complete successfully.

## Verifying the Results

To confirm the data was loaded correctly, query the PostgreSQL database:

```bash
docker compose exec postgres_dw psql -U dw_user -d data_warehouse -c "SELECT * FROM sales_orders;"
```

You should see 14 rows.

The sample dataset originally contains 15 records, but one (`Webcam HD`) has a missing quantity. During the transformation step, incomplete rows are removed, demonstrating a simple data-cleaning operation.

## Stopping the Project

To stop all containers:

```bash
docker compose down
```

To stop everything and remove all stored data:

```bash
docker compose down -v
```

## Things to Try

Once the project is running, here are a few ways to experiment:

* Add more records to `data/sample_sales.csv` and trigger the DAG again.
* Change the schedule from `schedule=None` to `schedule="@daily"` so the pipeline runs automatically.
* Add another transformation step, such as filtering orders below a certain value or grouping sales by product.

## Troubleshooting

**Ports already in use**

If ports 8080, 5432, or 5433 are already occupied, either stop the application using them or update the port mappings in `docker-compose.yml`.

The DAG doesn't appear in Airflow

Airflow scans the `dags/` directory periodically, so wait about 30 seconds. If it still doesn't appear, check the logs:

```bash
docker compose logs airflow | grep -i error
```

**Forgot the admin password**

Retrieve it again with:

```bash
docker compose logs airflow | grep -i password
```

