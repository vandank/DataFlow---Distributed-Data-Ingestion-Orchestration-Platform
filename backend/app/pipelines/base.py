from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.models.ingestion_run import IngestionRun
from app.models.ingestion_source import IngestionSource


@dataclass
class PipelineArtifact:
    run_id: int
    source_id: int
    raw_object_path: str
    transformed_object_path: str | None = None


@dataclass
class PipelineContext:
    db: Session
    source: IngestionSource
    file_name: str
    content_type: str | None
    file_bytes: bytes
    run: IngestionRun | None = None
    raw_object_path: str | None = None
    raw_object_path: str | None = None
    parsed_object_path: str | None = None
    cleaned_object_path: str | None = None
    transformed_object_path: str | None = None
    parsed_data: Any = None
    cleaned_data: Any = None
    transformed_data: Any = None
    transformed_row_count: int = 0
    warehouse_row_count: int = 0


@dataclass
class PipelineResult:
    run: IngestionRun
    raw_object_path: str
    transformed_row_count: int = 0
    warehouse_row_count: int = 0
