from pathlib import Path
from uuid import uuid4

from sqlalchemy.orm import Session

from app.crud.ingestion_run import (
    create_ingestion_run,
    mark_ingestion_run_failed,
    mark_ingestion_run_success,
)
from app.models.ingestion_source import IngestionSource
from app.services.storage.minio_service import MinIOStorageService
from app.services.processing.csv_cleaner import clean_csv_dataframe
from app.services.processing.csv_loader import load_transformed_rows
from app.services.processing.csv_parser import parse_csv_bytes
from app.services.processing.csv_transformer import transform_csv_dataframe
from app.services.processing.csv_validator import validate_csv_dataframe
from app.warehouse.loader import load_warehouse_dataframe


def process_csv_ingestion(
    db: Session,
    source: IngestionSource,
    file_name: str,
    content_type: str | None,
    file_bytes: bytes,
):
    run = create_ingestion_run(db, source.id)
    storage = MinIOStorageService()

    safe_file_name = Path(file_name or "upload.csv").name.replace(" ", "_")
    object_name = (
        f"{source.source_type}/{source.id}/run_{run.id}/"
        f"{uuid4().hex}_{safe_file_name}"
    )

    try:
        raw_object_path = storage.upload_bytes(
            object_name=object_name,
            data=file_bytes,
            content_type=content_type or "text/csv",
        )

        parsed_df = parse_csv_bytes(file_bytes)
        validate_csv_dataframe(parsed_df)
        cleaned_df = clean_csv_dataframe(parsed_df)
        transformed_df = transform_csv_dataframe(cleaned_df)
        load_transformed_rows(
            db=db,
            run_id=run.id,
            source_id=source.id,
            df=transformed_df,
        )

        load_warehouse_dataframe(
            db=db,
            source=source,
            run_id=run.id,
            df=transformed_df,
        )
        
        run = mark_ingestion_run_success(db, run, raw_object_path)
        return run, raw_object_path
    except Exception as exc:
        mark_ingestion_run_failed(db, run, str(exc))
        raise