from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app.crud.source import get_source_by_id
from app.deps import get_db
from app.schemas.ingestion import CSVIngestionResponse, IngestionRunRead
from app.services.ingestion.csv_ingestion import process_csv_ingestion

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

    file_bytes = await file.read()
    run, object_path = process_csv_ingestion(
        db=db,
        source=source,
        file_name=file.filename or "upload.csv",
        content_type=file.content_type,
        file_bytes=file_bytes,
    )

    return CSVIngestionResponse(
        run=IngestionRunRead.model_validate(run),
        bucket="raw-data",
        object_path=object_path,
        message="CSV ingested successfully",
    )