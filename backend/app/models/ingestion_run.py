# Another platfrom table -> what happened when a source was ingested
from sqlalchemy import Boolean, Column, String, Integer, Text, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.db.base import Base

class IngestionRun(Base):
    __tablename__ = "ingestion_runs"
    id = Column(Integer, primary_key=True, index=True)
    source_id = Column(Integer, ForeignKey("ingestion_sources.id"), nullable=False)
    status = Column(String(30), nullable=False, default="pending")
    raw_object_path = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    source = relationship("IngestionSource")