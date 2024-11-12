from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.python import PythonOperator
from messages import send_telegram_success_message, send_telegram_failure_message
from apartment_load import extract, load, create_table
from apartment_clean import extract_raw, load_clean, create_table_clean, transform_raw
import pendulum


with DAG(
    dag_id='apartment_raw_load',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
    tags=["ETL"]) as dag:
    
    create_table_step = PythonOperator(task_id='create_table', python_callable=create_table)
    extract_step = PythonOperator(task_id='extract', python_callable=extract)
    load_step = PythonOperator(task_id='load', python_callable=load)

    create_table_step >> extract_step >> load_step

with DAG(
    dag_id='apartment_clean_load',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
    tags=["ETL"]) as dag:
    
    create_table_step = PythonOperator(task_id='create_table_clean', python_callable=create_table_clean)
    extract_step = PythonOperator(task_id='extract_raw', python_callable=extract_raw)
    transform_step = PythonOperator(task_id='transform_raw', python_callable=transform_raw)
    load_step = PythonOperator(task_id='load_clean', python_callable=load_clean)

    create_table_step >> extract_step >> transform_step >> load_step