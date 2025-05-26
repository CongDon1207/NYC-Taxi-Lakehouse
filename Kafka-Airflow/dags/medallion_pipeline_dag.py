from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    "medallion_taxi_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval="*/5 * * * *",
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "spark-submit "
            "--master yarn "
            "--deploy-mode client "
            "/opt/airflow/spark-scripts/bronze_to_silver.py"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "spark-submit "
            "--master yarn "
            "--deploy-mode client "
            "/opt/airflow/spark-scripts/silver_to_gold.py"
        ),
    )

    register_delta = BashOperator(
        task_id="register_delta_tables",
        bash_command=(
            "spark-submit "
            "--master yarn "
            "--deploy-mode client "
            "/opt/airflow/spark-scripts/register_delta_tables.py"
        ),
    )

    bronze_to_silver >> silver_to_gold >> register_delta
