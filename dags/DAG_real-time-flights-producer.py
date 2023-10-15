from datetime import timedelta
from airflow import DAG
from airflow import PythonOperator
from datetime import datetime
from real_time_flights_producer import start_streaming


start_date = datetime(2023, 10, 15, 15, 45)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('Airlabs_Data'
        , default_args=default_args
        , schedule_interval='*/5 * * * *'
        , catchup=False) as dag:
    
    data_stream_task = PythonOperator(
        task_id='kafka_data_stream',
        python_callable=start_streaming,
        dag=dag,
    )

    data_stream_task