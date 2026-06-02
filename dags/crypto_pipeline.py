from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text


DB_URI = "postgresql://airflow:airflow@postgres:5432/airflow"


def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/simple/price"

    params = {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    engine = create_engine(DB_URI)

    with engine.begin() as conn:
        for coin in ["bitcoin", "ethereum"]:
            current_price = float(data[coin]["usd"])

            last_price_result = conn.execute(
                text("""
                    SELECT price_usd
                    FROM crypto_prices
                    WHERE coin = :coin
                    ORDER BY timestamp DESC
                    LIMIT 1
                """),
                {"coin": coin}
            ).fetchone()

            if last_price_result is None:
                price_change = None
                should_insert = True
            else:
                last_price = float(last_price_result[0])
                price_change = round(current_price - last_price, 2)
                should_insert = price_change != 0

            if should_insert:
                conn.execute(
                    text("""
                        INSERT INTO crypto_prices
                            (coin, price_usd, price_change, timestamp)
                        VALUES
                            (:coin, :price_usd, :price_change, NOW())
                    """),
                    {
                        "coin": coin,
                        "price_usd": current_price,
                        "price_change": price_change
                    }
                )

                print(f"Inserted {coin}: {current_price}, change: {price_change}")
            else:
                print(f"No price change for {coin}. Skipping insert.")


default_args = {
    "owner": "keerthi",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="crypto_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 5, 30),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["crypto", "incremental", "postgres"],
) as dag:

    fetch_crypto_task = PythonOperator(
        task_id="fetch_crypto_data",
        python_callable=fetch_crypto_data,
    )