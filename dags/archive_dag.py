from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="mongo_to_hdfs_archiving",
    default_args=default_args,
    schedule="0 */4 * * *",   
    catchup=False,
) as dag:

    run_spark_archiver = BashOperator(
        task_id="run_spark_archiver",
        bash_command="""
docker exec spark-master \
/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
  --executor-memory 2g \
  --driver-memory 1g \
  /opt/spark/scripts/archive_job.py
"""
    )

    run_spark_archiver
