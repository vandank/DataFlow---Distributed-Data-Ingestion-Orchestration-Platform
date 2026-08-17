from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app.crud.source import get_source_by_id
from app.deps import get_db
from app.schemas.ingestion import CSVIngestionResponse, IngestionRunRead
from app.pipelines.engine import PipelineNotFoundError, run_pipeline
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

    try:
        result = run_pipeline(
            db=db,
            source=source,
            file_name=file.filename or "upload.csv",
            content_type=file.content_type,
            file_bytes=await file.read(),
        )
    except PipelineNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return CSVIngestionResponse(
        run=IngestionRunRead.model_validate(result.run),
        bucket="raw-data",
        object_path=result.raw_object_path,
        message=f"CSV ingested successfully. Transformed rows: {result.transformed_row_count}, Warehouse rows: {result.warehouse_row_count}",
    )