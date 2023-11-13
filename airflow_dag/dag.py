from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain

from api_etl import extract_api, transform_api, load_api
from db_etl import extract_db, transform_db, load_db
from kafka_stream import stream_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 11),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    'proyect_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
) as dag:

    read_api = PythonOperator(
        task_id = 'read_api',
        python_callable = extract_api,
        provide_context = True,
    )

    transform_api = PythonOperator(
        task_id = 'transform_api',
        python_callable = transform_api,
        provide_context = True,
    )

    load_api = PythonOperator(
        task_id ='load_api',
        python_callable = load_api,
        provide_context = True,
    )

    read_db = PythonOperator(
        task_id = 'read_db',
        python_callable = extract_db,
        provide_context = True,
    )

    transform_db = PythonOperator(
        task_id = 'transform_db',
        python_callable = transform_db,
        provide_context = True,
    )

    load_db = PythonOperator(
        task_id ='load',
        python_callable = load_db,
        provide_context = True,
    )

    kafka = PythonOperator(
        task_id ='kafka',
        python_callable = stream_data,
        provide_context = True,
    )

    read_api >> transform_api >> load_api >> kafka
    read_db >> transform_db >> load_db >> kafka