from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import sys
sys.path.append('/opt/airflow/scripts')
from kafka_producer.push_fhvhv_data import stream_parquet_to_kafka

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='stream_tripdata_to_kafka',
    default_args=default_args,
    description='Stream trip data from parquet file to Kafka topic',
    schedule_interval=None,  # Trigger manually
    catchup=False,
) as dag:

    stream_to_kafka = PythonOperator(
        task_id='stream_parquet_to_kafka',
        python_callable=stream_parquet_to_kafka,
        op_kwargs={
            'parquet_file_path': '/opt/airflow/data/fhvhv_tripdata_2025-01.parquet',
            'kafka_bootstrap_servers': 'broker:29092',
            'kafka_topic': 'nyc_taxi_stream',
            'sleep_time': 0.1
        }
    )

    stream_to_kafka
