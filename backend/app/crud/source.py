import json
from typing import Any

from sqlalchemy.orm import Session

from app.models.ingestion_source import IngestionSource
from app.schemas.source import SourceCreate


def create_source(db: Session, source_in: SourceCreate) -> IngestionSource:
    existing = (
        db.query(IngestionSource)
        .filter(IngestionSource.name == source_in.name)
        .first()
    )
    if existing:
        raise ValueError(f"Source with name '{source_in.name}' already exists.")

    source = IngestionSource(
        name=source_in.name,
        source_type=source_in.source_type,
        description=source_in.description,
        config_json=json.dumps(source_in.config or {}),
    )
    db.add(source)
    db.commit()
    db.refresh(source)
    return source


def get_source_by_id(db: Session, source_id: int) -> IngestionSource | None:
    return db.query(IngestionSource).filter(IngestionSource.id == source_id).first()


def source_to_dict(source: IngestionSource) -> dict[str, Any]:
    return {
        "id": source.id,
        "name": source.name,
        "source_type": source.source_type,
        "description": source.description,
        "config": json.loads(source.config_json) if source.config_json else {},
        "is_active": source.is_active,
    }