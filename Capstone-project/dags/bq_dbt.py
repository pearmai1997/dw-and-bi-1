from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator


import os
import glob


with DAG(
    "dbt",
    start_date=timezone.datetime(2024,4,25),
    schedule_interval="@daily",
    tags=["swu"],
) as dag:

# # dbt_check = BashOperator(
# #     task_id="dbt_check",
# #     bash_command="cd /usr/local/airflow/include/dbt; source /usr/local/airflow/dbt_venv/bin/activate; dbt debug --profiles-dir /usr/local/airflow/include/dbt/",
# #     dag=dag,
# # )

#     dbt_run = BashOperator(
#         task_id="dbt_run",
#         bash_command="
#         cd /workspaces/dw-and-bi/Capstone-project; source ENV/bin/activate; cd projectcapstone/; dbt run",
#         dag=dag,
#     )

#     dbt_test = BashOperator(
#         task_id="dbt_test",
#         bash_command="cd ../Capstone-project; source ../ENV/bin/activate; cd ../projectcapstone; dbt test",
#         dag=dag,
#     )

# with dag:
#     dbt_run >> dbt_test

# Define dbt tasks using BashOperator
    task1 = BashOperator(
        task_id='dbt_task1',
        bash_command='cd Capstone-project && dbt run --models sales_report.sql',
        dag=dag
    )

task2 = BashOperator(
    task_id='dbt_test1',
    bash_command='dbt test --models sales_report.sql',
    dag=dag
)

task1 >> task2