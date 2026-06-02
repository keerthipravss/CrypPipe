from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from opentelemetry import context
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
 
# ---------- EXTRACT ----------
def extract_crypto(**kwargs):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
 
    ti = kwargs["ti"]
    ti.xcom_push(key="crypto_data", value=data)
 
# ---------- Latest Data LOAD ----------
def load_crypto(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(
        key="crypto_data",
        task_ids="extract"
    )
    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres/airflow"
    )
    insert_query = text("""
        INSERT INTO crypto.crypto_market
        (id, symbol, name, current_price, total_volume, high_24h, low_24h)
        VALUES
        (:id, :symbol, :name, :current_price, :total_volume, :high_24h, :low_24h)
        ON CONFLICT (id) DO UPDATE
        SET
            current_price = EXCLUDED.current_price,
            total_volume = EXCLUDED.total_volume,
            high_24h = EXCLUDED.high_24h,
            low_24h = EXCLUDED.low_24h,
            created_at = CURRENT_TIMESTAMP;
    """)
 
    with engine.begin() as connection:
        for i in data:
            connection.execute(insert_query, {
                "id": i["id"],
                "symbol": i["symbol"],
                "name": i["name"],
                "current_price": i["current_price"],
                "total_volume": i["total_volume"],
                "high_24h": i["high_24h"],
                "low_24h": i["low_24h"],
            })
 
 
# ---------- Historical Data LOAD ----------
def load_history(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(
        key="crypto_data",
        task_ids="extract"
    )
    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@airflow-postgresql/airflow"
    )
    insert_query = text("""
    INSERT INTO crypto.crypto_market_history
    (id, symbol, name, current_price, total_volume, high_24h, low_24h)
    VALUES
    (:id, :symbol, :name, :current_price, :total_volume, :high_24h, :low_24h)
""")
 
    with engine.begin() as connection:
        for i in data:
            connection.execute(insert_query, {
                "id": i["id"],
                "symbol": i["symbol"],
                "name": i["name"],
                "current_price": i["current_price"],
                "total_volume": i["total_volume"],
                "high_24h": i["high_24h"],
                "low_24h": i["low_24h"],
            })
 
 
 
# ---------- DAG ----------
default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=10)
}
 
with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",   # runs every hour
    catchup=False,
    max_active_runs=1,
    default_args=default_args
):
 
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_crypto
    )
 
    update_history = PythonOperator(
        task_id="update_history",
        python_callable=load_history
    )
 
    latest_load = PythonOperator(
        task_id="latest_load",
        python_callable=load_crypto
    )
 
    extract >> latest_load >> update_history