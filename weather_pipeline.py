from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from fetch_weather import fetch_data, transform_data, load_data_to_db

default_args = {
    "owner": "airflow",
    "email": "oriseibude@gmail.com",
    "email_on_failure": True,
    "depends_on_past": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A simple weather ETL pipeline',
    schedule_interval= None,
    start_date=days_ago(0),
    tags=['weather', 'etl'],
) as dag:
    
    fetch_weather_data = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_data,
        dag=dag,
    )

    transform_weather_data = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_data,
        dag=dag,
    )

    load_weather_data = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_data_to_db,
        dag=dag,
    )

    fetch_weather_data >> transform_weather_data >> load_weather_data