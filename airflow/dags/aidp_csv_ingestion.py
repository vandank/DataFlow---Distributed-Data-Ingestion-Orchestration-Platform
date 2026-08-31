from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = 'aidp_csv_ingestion'

def _get_conf(context):
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    required = [
        "run_id",
        "source_id",
        "raw_object_path",
    ]

    missing = [key for key in required if key not in conf]

    if missing:
        raise ValueError(
            f"Missing required DAG configuration values: {missing}"
        )

    return conf

def process_csv(**context):
    from app.pipelines.base import PipelineArtifact
    from app.pipelines.csv_pipeline import CSVPipeline

    conf = _get_conf(context)

    artifact = PipelineArtifact(
        run_id = int(conf["run_id"]),
        source_id = int(conf["source_id"]),
        raw_object_path = conf["raw_object_path"],
    )

    pipeline = CSVPipeline()

    artifact = pipeline.process(artifact)

    result = {
        "run_id": artifact.run_id,
        "source_id": artifact.source_id,
        "raw_object_path": artifact.raw_object_path,
        "transformed_object_path": artifact.transformed_object_path,
    }

    print(f"CSV processing completed. Result: {result}")

    return result

def persist_transformed(**context):
    from app.db.session import get_session
    from app.pipelines.base import PipelineArtifact
    from app.pipelines.csv_pipeline import CSVPipeline

    artifact_data = context["ti"].xcom_pull(
        task_ids="process_csv",
    )

    if not artifact_data:
        raise ValueError(
            "No pipeline artifact received from process_csv."
        )
    
    artifact = PipelineArtifact(**artifact_data)

    db = get_session()

    try:
        pipeline = CSVPipeline()
        row_count = pipeline.persist_transformed(
            db=db, 
            artifact=artifact,
        )

        print(f"Persisted {row_count} transformed rows.")
        return {
            **artifact_data,
            "transformed_row_count": row_count,
        }
    finally:
        db.close()

def load_warehouse(**context):
    from app.crud.source import get_source_by_id
    from app.db.session import get_session
    from app.pipelines.base import PipelineArtifact
    from app.pipelines.csv_pipeline import CSVPipeline

    artifact_data = context["ti"].xcom_pull(
        task_ids="persist_transformed"
    )

    if not artifact_data:
        raise ValueError(
            "No pipeline artifact received from persist_transformed."
        )

    artifact = PipelineArtifact(**{
        key: artifact_data[key]
        for key in [
            "run_id",
            "source_id",
            "raw_object_path",
            "transformed_object_path",
        ]
    })

    db = get_session()

    try:
        source = get_source_by_id(
            db=db,
            source_id=artifact.source_id,
        )

        if source is None:
            raise ValueError(
                f"Source {artifact.source_id} not found."
            )

        pipeline = CSVPipeline()

        row_count = pipeline.load_warehouse(
            db=db,
            source=source,
            artifact=artifact,
        )

        print(
            f"Loaded {row_count} rows into the warehouse."
        )

        return {
            **artifact_data,
            "warehouse_row_count": row_count,
        }

    finally:
        db.close()


def finalize_success(**context):
    from app.db.session import get_session
    from app.pipelines.base import PipelineArtifact
    from app.pipelines.csv_pipeline import CSVPipeline

    artifact_data = context["ti"].xcom_pull(
        task_ids="load_warehouse"
    )

    if not artifact_data:
        raise ValueError(
            "No pipeline artifact received from load_warehouse."
        )

    artifact = PipelineArtifact(**{
        key: artifact_data[key]
        for key in [
            "run_id",
            "source_id",
            "raw_object_path",
            "transformed_object_path",
        ]
    })

    db = get_session()

    try:
        pipeline = CSVPipeline()

        run = pipeline.finalize_success(
            db=db,
            artifact=artifact,
        )

        print(
            f"Ingestion run {run.id} marked as successful."
        )

    finally:
        db.close()


def finalize_failure(**context):
    from app.crud.ingestion_run import mark_ingestion_run_failed
    from app.db.session import get_session
    from app.models.ingestion_run import IngestionRun

    conf = _get_conf(context)

    run_id = int(conf["run_id"])

    db = get_session()

    try:
        run = db.get(
            IngestionRun,
            run_id,
        )

        if run is None:
            raise ValueError(
                f"Ingestion run {run_id} not found."
            )

        # Look at the task instances that belong to this DAG run
        dag_run = context["dag_run"]

        failed_tasks = []

        for task_instance in dag_run.get_task_instances():
            if task_instance.task_id == "finalize_failure":
                continue

            if task_instance.state == "failed":
                failed_tasks.append(task_instance.task_id)

        error_message = (
            "Airflow pipeline failed."
        )

        if failed_tasks:
            error_message += (
                f" Failed task(s): {', '.join(failed_tasks)}."
            )

        mark_ingestion_run_failed(
            db=db,
            run=run,
            error_message=error_message,
        )

        print(
            f"Ingestion run {run_id} marked as failed."
        )

    finally:
        db.close()


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aidp", "ingestion"],
) as dag:

    process_csv_task = PythonOperator(
        task_id="process_csv",
        python_callable=process_csv,
    )

    persist_transformed_task = PythonOperator(
        task_id="persist_transformed",
        python_callable=persist_transformed,
    )

    load_warehouse_task = PythonOperator(
        task_id="load_warehouse",
        python_callable=load_warehouse,
    )

    finalize_success_task = PythonOperator(
        task_id="finalize_success",
        python_callable=finalize_success,
    )

    finalize_failure_task = PythonOperator(
        task_id="finalize_failure",
        python_callable=finalize_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    (
        process_csv_task
        >> persist_transformed_task
        >> load_warehouse_task
        >> finalize_success_task
    )

    [
        process_csv_task,
        persist_transformed_task,
        load_warehouse_task,
        finalize_success_task,
    ] >> finalize_failure_task

'''def test_pipeline_import():
    from app.pipelines.csv_pipeline import CSVPipeline

    print(f"Successfully imported: {CSVPipeline}")


def test_database_connection():
    from sqlalchemy import text

    from app.db.session import get_session

    session = get_session()

    try:
        result = session.execute(text("SELECT 1"))
        print(f"Database connection successful: {result.scalar()}")
    finally:
        session.close()


with DAG(
    dag_id="aidp_csv_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aidp", "ingestion"],
) as dag:

    pipeline_import_test = PythonOperator(
        task_id="pipeline_import_test",
        python_callable=test_pipeline_import,
    )

    database_connection_test = PythonOperator(
        task_id="database_connection_test",
        python_callable=test_database_connection,
    )

    pipeline_import_test >> database_connection_test
'''