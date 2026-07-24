from sqlalchemy import Column, DateTime, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.db.base import Base


class TransformedRow(Base):
    __tablename__ = "transformed_rows"

    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(Integer, ForeignKey("ingestion_runs.id"), nullable=False, index=True)
    source_id = Column(Integer, ForeignKey("ingestion_sources.id"), nullable=False, index=True)
    row_number = Column(Integer, nullable=False)
    data_json = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    run = relationship("IngestionRun")
    source = relationship("IngestionSource")