from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

dag = DAG(
    "kpi_refresh_dag",
    default_args=default_args,
    description="Run Spark KPI Job every minute to refresh Mongo KPIs",
    schedule="0 */3 * * *",   # EVERY 3 hours
    start_date=datetime(2025, 12, 25),
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1
)

run_kpi_job = BashOperator(
    task_id="run_spark_kpi_job",
    bash_command="""
    docker exec spark-master \
    /spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
      /opt/spark/scripts/kpi_job.py
    """,
    dag=dag,
)

run_kpi_job
