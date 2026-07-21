from datetime import datetime
from pydantic import BaseModel, ConfigDict


class IngestionRunRead(BaseModel):
    id: int
    source_id: int
    status: str
    raw_object_path: str | None = None
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class CSVIngestionResponse(BaseModel):
    run: IngestionRunRead
    bucket: str
    object_path: str
    message: str