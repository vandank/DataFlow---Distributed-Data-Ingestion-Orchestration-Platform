from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def smoke_test():
    print("AIDP Airflow integration is working!")


with DAG(
    dag_id="aidp_airflow_smoke_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aidp", "sprint5", "smoke-test"],
) as dag:

    test_airflow = PythonOperator(
        task_id="test_airflow",
        python_callable=smoke_test,
    )