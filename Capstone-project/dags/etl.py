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

import os
import glob

def _get_files(filepath: str):
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.csv"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


with DAG(
    "etl",
    start_date=timezone.datetime(2024,4,25),
    schedule_interval="@daily",
    tags=["swu"],
) as dag:

# Dummy strat task   
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )


    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
        op_kwargs={"filepath": "/opt/airflow/dags/data"},
    )

    # Task to load data into Google Cloud Storage
    upload_file_customers = LocalFilesystemToGCSOperator(
        task_id='upload_file_customers',
        src='/opt/airflow/dags/data/olist_customers_dataset.csv',
        dst='olist_customers_dataset.csv',  # Destination file in the bucket
        bucket='swu-ds-525-112233',  # Bucket name
        gcp_conn_id='my_gcp_conn',  # Google Cloud connection id
        mime_type='text/csv'
    )
    upload_file_geolocation = LocalFilesystemToGCSOperator(
        task_id='upload_file_geolocation',
        src='/opt/airflow/dags/data/olist_geolocation_dataset.csv',
        dst='olist_geolocation_dataset.csv',  # Destination file in the bucket
        bucket='swu-ds-525-112233',  # Bucket name
        gcp_conn_id='my_gcp_conn',  # Google Cloud connection id
        mime_type='text/csv'
    )
    upload_file_items = LocalFilesystemToGCSOperator(
        task_id='upload_file_items',
        src='/opt/airflow/dags/data/olist_order_items_dataset.csv',
        dst='olist_order_items_dataset.csv',  # Destination file in the bucket
        bucket='swu-ds-525-112233',  # Bucket name
        gcp_conn_id='my_gcp_conn',  # Google Cloud connection id
        mime_type='text/csv'
    )
    upload_file_payments = LocalFilesystemToGCSOperator(
        task_id='upload_file_payments',
        src='/opt/airflow/dags/data/olist_order_payments_dataset.csv',
        dst='olist_order_payments_dataset.csv',  # Destination file in the bucket
        bucket='swu-ds-525-112233',  # Bucket name
        gcp_conn_id='my_gcp_conn',  # Google Cloud connection id
        mime_type='text/csv'
    )
    upload_file_reviews = LocalFilesystemToGCSOperator(
        task_id='upload_file_reviews',
        src='/opt/airflow/dags/data/olist_order_reviews_dataset.csv',
        dst='olist_order_reviews_dataset.csv',  # Destination file in the bucket
        bucket='swu-ds-525-112233',  # Bucket name
        gcp_conn_id='my_gcp_conn',  # Google Cloud connection id
        mime_type='text/csv'
    )
    upload_file_orders = LocalFilesystemToGCSOperator(
        task_id='upload_file_orders',
        src='/opt/airflow/dags/data/olist_orders_dataset.csv',
        dst='olist_orders_dataset.csv',  # Destination file in the bucket
        bucket='swu-ds-525-112233',  # Bucket name
        gcp_conn_id='my_gcp_conn',  # Google Cloud connection id
        mime_type='text/csv'
    )
    upload_file_products = LocalFilesystemToGCSOperator(
        task_id='upload_file_products',
        src='/opt/airflow/dags/data/olist_products_dataset.csv',
        dst='olist_products_dataset.csv',  # Destination file in the bucket
        bucket='swu-ds-525-112233',  # Bucket name
        gcp_conn_id='my_gcp_conn',  # Google Cloud connection id
        mime_type='text/csv'
    )
    upload_file_sellers = LocalFilesystemToGCSOperator(
        task_id='upload_file_sellers',
        src='/opt/airflow/dags/data/olist_sellers_dataset.csv',
        dst='olist_sellers_dataset.csv',  # Destination file in the bucket
        bucket='swu-ds-525-112233',  # Bucket name
        gcp_conn_id='my_gcp_conn',  # Google Cloud connection id
        mime_type='text/csv'
    )
    upload_file_name = LocalFilesystemToGCSOperator(
        task_id='upload_file_name',
        src='/opt/airflow/dags/data/product_category_name_translation.csv',
        dst='product_category_name_translation.csv',  # Destination file in the bucket
        bucket='swu-ds-525-112233',  # Bucket name
        gcp_conn_id='my_gcp_conn',  # Google Cloud connection id
        mime_type='text/csv'
    )
#------------------------------------------------------------------------------------------------------#

    create_order_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_order_dataset',
        dataset_id='order',
        gcp_conn_id='my_gcp_conn',
    )

#-----------------------------------------------------------------------------------------------------#

    gcs_to_bq_customers = GCSToBigQueryOperator(
    task_id                             = "gcs_to_bq_customers",
    bucket                              = 'swu-ds-525-112233',
    source_objects                      = ['olist_customers_dataset.csv'],
    destination_project_dataset_table   ='order.account_transactions',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='my_gcp_conn'
    )

    gcs_to_bq_geolocation = GCSToBigQueryOperator(
    task_id                             = "gcs_to_bq_geolocation",
    bucket                              = 'swu-ds-525-112233',
    source_objects                      = ['olist_geolocation_dataset.csv'],
    destination_project_dataset_table   ='order.olist_geolocation_dataset',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='my_gcp_conn'
    )

    gcs_to_bq_items = GCSToBigQueryOperator(
    task_id                             = "gcs_to_bq_items",
    bucket                              = 'swu-ds-525-112233',
    source_objects                      = ['olist_order_items_dataset.csv'],
    destination_project_dataset_table   ='order.olist_items_dataset',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='my_gcp_conn'
    )

    gcs_to_bq_payments = GCSToBigQueryOperator(
    task_id                             = "gcs_to_bq_payments",
    bucket                              = 'swu-ds-525-112233',
    source_objects                      = ['olist_order_payments_dataset.csv'],
    destination_project_dataset_table   ='order.olist_payments_dataset',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='my_gcp_conn'
    )

    gcs_to_bq_reviews = GCSToBigQueryOperator(
    task_id                             = "gcs_to_bq_reviews",
    bucket                              = 'swu-ds-525-112233',
    source_objects                      = ['olist_order_reviews_dataset.csv'],
    destination_project_dataset_table   ='order.olist_reviews_dataset',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='my_gcp_conn'
    )

    gcs_to_bq_orders = GCSToBigQueryOperator(
    task_id                             = "gcs_to_bq_orders",
    bucket                              = 'swu-ds-525-112233',
    source_objects                      = ['olist_orders_dataset.csv'],
    destination_project_dataset_table   ='order.olist_orders_dataset',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='my_gcp_conn'
    )

    gcs_to_bq_products = GCSToBigQueryOperator(
    task_id                             = "gcs_to_bq_products",
    bucket                              = 'swu-ds-525-112233',
    source_objects                      = ['olist_products_dataset.csv'],
    destination_project_dataset_table   ='order.olist_products_dataset',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='my_gcp_conn'
    )

    gcs_to_bq_sellers = GCSToBigQueryOperator(
    task_id                             = "gcs_to_bq_sellers",
    bucket                              = 'swu-ds-525-112233',
    source_objects                      = ['olist_sellers_dataset.csv'],
    destination_project_dataset_table   ='order.olist_sellers_dataset',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='my_gcp_conn'
    )

    gcs_to_bq_translation = GCSToBigQueryOperator(
    task_id                             = "gcs_to_bq_translation",
    bucket                              = 'swu-ds-525-112233',
    source_objects                      = ['product_category_name_translation.csv'],
    destination_project_dataset_table   ='order.product_category_name_translation',
    write_disposition='WRITE_TRUNCATE',
    create_disposition          = 'CREATE_IF_NEEDED',
    gcp_conn_id='my_gcp_conn'
    )


#     load_dataset_order = GCSToBigQueryOperator(
#         task_id = 'load_dataset_order',
#         bucket = 'swu-ds-525-112233',
#         source_objects = ['order_bazilian.csv'],
#         destination_project_dataset_table = f'{'dataengineer-415510'}:{'order'}.dataset_order',
#         write_disposition='WRITE_TRUNCATE',
#         source_format = 'csv',
#         allow_quoted_newlines = 'true',
#         gcp_conn_id='my_gcp_conn',
#         skip_leading_rows = 1,
#         )

#     transform_bq = BigQueryInsertJobOperator(
#     task_id="load_data_into_new_table",
#     gcp_conn_id='my_gcp_conn',
#     configuration={
#         "query": {
#             "query": """
#             CREATE OR REPLACE TABLE `dataengineer-415510.order.Finance` AS
#             SELECT 
#                 customerid, 
#                 paymenttype
#             FROM `dataengineer-415510.order.dataset_order`
#             """,
#             "useLegacySql": False
#         }
#     },
#     dag=dag
# )



# Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    # start >> get_files >> upload_file >> create_order_dataset >> load_dataset_order >> transform_bq >> end
    start >> get_files >> [upload_file_customers, upload_file_geolocation, upload_file_items, upload_file_name ,upload_file_orders, upload_file_payments, upload_file_products, upload_file_reviews, upload_file_sellers] >> create_order_dataset >> [gcs_to_bq_customers, gcs_to_bq_geolocation, gcs_to_bq_items, gcs_to_bq_orders, gcs_to_bq_payments, gcs_to_bq_products, gcs_to_bq_reviews, gcs_to_bq_sellers, gcs_to_bq_translation]
