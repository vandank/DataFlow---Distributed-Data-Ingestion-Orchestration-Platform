from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.models.ingestion_run import IngestionRun


def create_ingestion_run(db: Session, source_id: int) -> IngestionRun:
    run = IngestionRun(
        source_id=source_id,
        status="running",
        started_at=datetime.now(timezone.utc),
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def mark_ingestion_run_success(
    db: Session,
    run: IngestionRun,
    raw_object_path: str,
) -> IngestionRun:
    run.status = "success"
    run.raw_object_path = raw_object_path
    run.finished_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(run)
    return run


def mark_ingestion_run_failed(
    db: Session,
    run: IngestionRun,
    error_message: str,
) -> IngestionRun:
    run.status = "failed"
    run.error_message = error_message
    run.finished_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(run)
    return run