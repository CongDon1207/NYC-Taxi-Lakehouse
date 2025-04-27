from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'binance_ws_ingestion_dag',
    default_args=default_args,
    description='Ingest Binance WebSocket stream into Kafka',
    schedule_interval='@once',  # Chạy 1 lần, hoặc manual trigger
    start_date=datetime(2024, 4, 27),
    catchup=False,
    tags=['binance', 'kafka', 'ingestion'],
) as dag:

    start_ws_client = BashOperator(
        task_id='start_binance_ws_client',
        bash_command='python /opt/airflow/scripts/binance_ws_client.py',
    )

    start_ws_client
