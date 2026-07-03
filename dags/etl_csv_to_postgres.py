"""
Beginner-friendly ETL DAG
=========================

What this pipeline does:
1. EXTRACT  - Reads a CSV file of sales orders from the /opt/airflow/data folder
2. TRANSFORM - Cleans the data (drops bad rows, adds a "total_amount" column)
3. LOAD     - Writes the cleaned data into a Postgres table

Every task passes data to the next one using XCom (Airflow's built-in way
of sharing small amounts of data between tasks).
"""

from __future__ import annotations

import pendulum
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# The connection ID below ("postgres_dw") matches the AIRFLOW_CONN_POSTGRES_DW
# environment variable defined in docker-compose.yml. Airflow automatically
# reads that env var and turns it into a usable connection - no manual setup
# needed in the Airflow UI.
POSTGRES_CONN_ID = "postgres_dw"

# Path to the CSV file inside the Airflow container.
# docker-compose.yml mounts your local ./data folder to /opt/airflow/data
CSV_PATH = "/opt/airflow/data/sample_sales.csv"

# Name of the table we will create/load data into
TARGET_TABLE = "sales_orders"


@dag(
    dag_id="etl_csv_to_postgres",
    description="A beginner-friendly ETL: CSV -> clean -> Postgres",
    schedule=None,  # set to e.g. "@daily" if you want it to run automatically
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["beginner", "etl", "postgres"],
)
def etl_csv_to_postgres():

    @task
    def extract() -> str:
        """Read the CSV file and return it as JSON so it can pass through XCom."""
        df = pd.read_csv(CSV_PATH)
        print(f"Extracted {len(df)} rows from {CSV_PATH}")
        return df.to_json(orient="records")

    @task
    def transform(raw_json: str) -> str:
        """Clean the data and add a computed column."""
        df = pd.read_json(raw_json, orient="records")

        before = len(df)
        # Drop rows missing a quantity or price - can't calculate totals without them
        df = df.dropna(subset=["quantity", "price"])
        after = len(df)
        print(f"Dropped {before - after} row(s) with missing quantity/price")

        # Make sure types are correct
        df["quantity"] = df["quantity"].astype(int)
        df["price"] = df["price"].astype(float)

        # Add a new column: total_amount = quantity * price
        df["total_amount"] = (df["quantity"] * df["price"]).round(2)

        print(f"Transformed data, {len(df)} rows ready to load")
        return df.to_json(orient="records")

    @task
    def load(clean_json: str) -> None:
        """Create the target table (if needed) and insert the cleaned rows."""
        df = pd.read_json(clean_json, orient="records")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            order_id INTEGER PRIMARY KEY,
            product VARCHAR(255),
            quantity INTEGER,
            price NUMERIC(10, 2),
            order_date DATE,
            total_amount NUMERIC(10, 2)
        );
        """
        hook.run(create_table_sql)

        # Upsert so re-running the DAG doesn't create duplicate rows
        insert_sql = f"""
        INSERT INTO {TARGET_TABLE}
            (order_id, product, quantity, price, order_date, total_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO UPDATE SET
            product = EXCLUDED.product,
            quantity = EXCLUDED.quantity,
            price = EXCLUDED.price,
            order_date = EXCLUDED.order_date,
            total_amount = EXCLUDED.total_amount;
        """

        rows = [
            (
                int(r["order_id"]),
                r["product"],
                int(r["quantity"]),
                float(r["price"]),
                pd.to_datetime(r["order_date"]).date(),
                float(r["total_amount"]),
            )
            for r in df.to_dict(orient="records")
        ]

        hook.insert_rows(
            table=TARGET_TABLE,
            rows=rows,
            target_fields=["order_id", "product", "quantity", "price", "order_date", "total_amount"],
            replace=True,
            replace_index="order_id",
        )
        print(f"Loaded {len(rows)} rows into '{TARGET_TABLE}'")

    # Wire the tasks together: extract -> transform -> load
    raw_data = extract()
    clean_data = transform(raw_data)
    load(clean_data)


etl_csv_to_postgres()
