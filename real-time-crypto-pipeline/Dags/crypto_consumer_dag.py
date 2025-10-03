from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import json
import snowflake.connector

# Snowflake connection parameters
SNOWFLAKE_USER = "JEEVITAK"
SNOWFLAKE_PASSWORD = "Imdnwtevrythng@745"
SNOWFLAKE_ACCOUNT = "GLPLNTQ-YMB56622"
SNOWFLAKE_DATABASE = "CRYPTO_DB"
SNOWFLAKE_SCHEMA = "STAGING"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"  # <-- add your warehouse name here
SNOWFLAKE_TABLE = "CRYPTO_PRICES"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def insert_into_snowflake(bitcoin, ethereum, run_id):
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cs = ctx.cursor()
    try:
        cs.execute(f"""
            INSERT INTO {SNOWFLAKE_TABLE} (bitcoin_usd, ethereum_usd, dag_run_id, source)
            VALUES ({bitcoin}, {ethereum}, '{run_id}', 'crypto_producer_dag')
        """)
        ctx.commit()
        print(f"Inserted into Snowflake: {bitcoin}, {ethereum}, run_id={run_id}")
    finally:
        cs.close()
        ctx.close()

def consume_and_load(**kwargs):
    run_id = kwargs['run_id']
    consumer = KafkaConsumer(
        'crypto_prices',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Consume messages
    for message in consumer:
        data = message.value
        bitcoin = data['bitcoin']['usd']
        ethereum = data['ethereum']['usd']
        insert_into_snowflake(bitcoin, ethereum, run_id)
        break  # remove break if you want continuous consumption

with DAG(
    dag_id='crypto_consumer_dag',
    default_args=default_args,
    description='Consume crypto prices from Kafka and load to Snowflake',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2024, 9, 29),
    catchup=False,
) as dag:

    consume_task = PythonOperator(
        task_id='consume_and_load_task',
        python_callable=consume_and_load,
        provide_context=True

    )