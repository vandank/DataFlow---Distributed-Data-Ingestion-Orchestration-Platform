from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app.core.config import settings
from app.crud.ingestion_run import mark_ingestion_run_failed
from app.crud.source import get_source_by_id
from app.deps import get_db
from app.pipelines.base import PipelineContext
from app.pipelines.csv_pipeline import CSVPipeline
from app.schemas.ingestion import CSVIngestionResponse, IngestionRunRead
from app.pipelines.engine import PipelineNotFoundError, run_pipeline
from app.services.orchestration.airflow_client import AirflowClient
#from app.services.ingestion.csv_ingestion import process_csv_ingestion

router = APIRouter()


@router.post("/csv", response_model=CSVIngestionResponse, status_code=status.HTTP_201_CREATED)
async def ingest_csv_endpoint(
    source_id: int = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    source = get_source_by_id(db, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    pipeline = CSVPipeline()
    context = PipelineContext(
        db=db,
        source=source,
        file_name=file.filename or "upload.csv",
        content_type=file.content_type,
        file_bytes=await file.read(),
    )
    try:
        #Create ingestion run + upload immutable raw file to MinIO
        artifact = pipeline.ingest_raw(context)
        # 2. Trigger Airflow asynchronously
        airflow_client = AirflowClient()

        airflow_run = airflow_client.trigger_dag(
            dag_id=settings.airflow_dag_id,
            run_id=artifact.run_id,
            source_id=artifact.source_id,
            raw_object_path=artifact.raw_object_path,
        )

    except Exception as exc:
        if context.run is not None:
            mark_ingestion_run_failed(
                db=db,
                run=context.run,
                error_message=str(exc),
            )

        raise HTTPException(
            status_code=502,
            detail=f"Failed to start Airflow ingestion pipeline: {exc}",
        ) from exc

    return CSVIngestionResponse(
        run=IngestionRunRead.model_validate(context.run),
        bucket="raw-data",
        object_path=artifact.raw_object_path,
        message=(
            f"CSV uploaded successfully and Airflow DAG "
            f"'{settings.airflow_dag_id}' was triggered. "
            f"Airflow run: {airflow_run.get('dag_run_id')}"
        ),
    )
        #result = run_pipeline(
         #   db=db,
         #   source=source,
         #   file_name=file.filename or "upload.csv",
         #   content_type=file.content_type,
         #   file_bytes=await file.read(),
        #)
    #except PipelineNotFoundError as exc:
     #   raise HTTPException(status_code=400, detail=str(exc)) from exc

    #return CSVIngestionResponse(
    #    run=IngestionRunRead.model_validate(result.run),
    #    bucket="raw-data",
    #    object_path=result.raw_object_path,
    #    message=f"CSV ingested successfully. Transformed rows: {result.transformed_row_count}, Warehouse rows: {result.warehouse_row_count}",
    #)