from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "owner" : "airflow",
    "depends_on_past" : False,
    "email_on_failure" : False,
    "email_on_retry" : False,
    "retries" : 1,
}
project_id = 'playground-s-11-a1e71bd7'
dataset_id = 'staging_dataset'
transform_dataset_id = 'transform_dataset'
reporting_dataset_id = 'reporting_dataset'
source_table = f'{project_id}.{dataset_id}.sales_table'
Regions = ['Asia', 'Middle East and North Africa', 'Europe', 'Sub-Saharan Africa', 'Central America and the Caribbean','Australia and Oceania', 'North America']
create_table_tasks = []
create_view_tasks = []

with DAG(
    dag_id = "AnalyticsDAG",
    default_args = default_args,
    schedule_interval = None,
    start_date = datetime(2024,1,1),
    catchup = False,
    tags = ['BigQuery', 'GCS', 'CSV']
) as dag:
    check_file_existence = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket = 'sales_src_bkt',
        object = '1000 Sales Records.csv',
        timeout = 300,
        poke_interval = 30,
        mode = 'poke'
    )
    load_csv_to_BigQuery = GCSToBigQueryOperator(
        task_id = 'load_csv_to_bq',
        bucket = 'sales-src-bkt',
        source_objects = ['1000 Sales Records.csv'],
        destination_project_dataset_table = 'playground-s-11-a1e71bd7.staging_dataset.sales_table',
        source_format = 'CSV',
        allow_jagged_rows = True,
        write_desposition = 'WRITE_TRUNCATE',
        allow_jagged_cross = True,
        skip_leading_rows = 1,
        field_delimiter = ',',
        autodetect = True

    )
    for Region in Regions:
        create_table_task = BigQueryInsertJobOperator(
            task_id = f'create_table_{Region.lower().replace(" ", "_")}',
            configuration = {
                "query": {
                    "query": f"""
                     create or replace table `{project_id}.{transform_dataset_id}.{Region.lower().replace(" ", " ")}_table`
                      as select *from `{source_table}`
                      where Region = '{Region}'
                    """,
                    "useLegacySql": False,
                }
            },
        )
        create_view_task = BigQueryInsertJobOperator(
            task_id = f'create_view_{Region.lower().replace(" ", "_")}_table',
            configuration={
                "query": {
                    "query": f"""
                     create or replace view `{project_id}.{reporting_dataset_id}.{Region.lower().replace(" ", "_")}_table`
                     as select Country, Itemtype, SalesChannel, UnitsSold, UnitPrice, TotalRevenue
                     FROM `{source_table}`
                     where Region = '{Region}'
                     """,
                    "useLegacySql": False,
                },
            },
        )
        create_table_task.set_upstream(load_csv_to_BigQuery)
        create_view_task.set_upstream(create_table_task)
        create_table_tasks.append(create_table_task)
        create_view_tasks.append(create_view_task)
    success_task = DummyOperator(
        task_id = 'success_task',
    )
    check_file_existence >> load_csv_to_BigQuery
    for create_table_task,create_view_task in zip(create_table_tasks, create_view_tasks):
        create_table_task >> create_view_task >> success_task
