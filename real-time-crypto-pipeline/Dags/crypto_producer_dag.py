from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from crypto_producer import fetch_crypto_price       # fetch data from API
from crypto_snowflake import insert_into_snowflake   # insert data into Snowflake

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def fetch_and_load():
    # Fetch crypto prices
    data = fetch_crypto_price()
    bitcoin = data['bitcoin']['usd']
    ethereum = data['ethereum']['usd']

    print("Inserting to Snowflake:", bitcoin, ethereum)

    # Insert into Snowflake
    insert_into_snowflake(bitcoin, ethereum)

with DAG(
    dag_id='crypto_producer_dag',
    default_args=default_args,
    description='Fetch crypto prices, send to Kafka, and load to Snowflake',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2024, 9, 29),
    catchup=False,
) as dag:

    fetch_and_load_task = PythonOperator(
        task_id='fetch_and_load_task',
        python_callable=fetch_and_load
    )