from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from extract import extraction_layer
from transform import transformation_layer
from load import loading_layer
import os

# Import functions from the script

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email': 'your_emailijonijosey@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Train ETL pipeline with XCom data transfer',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 10, 25),
    catchup=False,
)

extraction_task = PythonOperator(
    task_id='data_ingestion',
    python_callable=extraction_layer,
    provide_context=True,
    dag=dag,
)

transformation_task = PythonOperator(
    task_id='data_transformation',
    python_callable=transformation_layer,
    provide_context=True,
    dag=dag,
)

duplication_task = PythonOperator(
    task_id='data_duplication',
    python_callable=loading_layer,
    provide_context=True,
    dag=dag,
)

extraction_task >> transformation_task >> duplication_task