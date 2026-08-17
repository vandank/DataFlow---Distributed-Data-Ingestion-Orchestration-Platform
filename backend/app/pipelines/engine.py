from __future__ import annotations

from app.models.ingestion_source import IngestionSource
from app.pipelines.base import PipelineContext, PipelineResult
from app.pipelines.csv_pipeline import CSVPipeline


class PipelineNotFoundError(ValueError):
    pass


PIPELINE_REGISTRY = {
    "csv": CSVPipeline(),
}


def run_pipeline(
    db,
    source: IngestionSource,
    file_name: str,
    content_type: str | None,
    file_bytes: bytes,
) -> PipelineResult:
    source_type = (source.source_type or "").strip().lower()
    pipeline = PIPELINE_REGISTRY.get(source_type)

    if pipeline is None:
        raise PipelineNotFoundError(f"No pipeline registered for source_type='{source_type}'")

    context = PipelineContext(
        db=db,
        source=source,
        file_name=file_name,
        content_type=content_type,
        file_bytes=file_bytes,
    )
    return pipeline.execute(context)