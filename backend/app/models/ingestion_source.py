#My first platfrom table -> What data sourec exists
from sqlalchemy import Boolean, Column, String, Integer, Text, DateTime
from sqlalchemy.sql import func

from app.db.base import Base

class IngestionSource(Base):
    __tablename__ = "ingestion_sources"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False, unique=True, index=True)
    source_type = Column(String(50), nullable=False)
    description = Column(Text, nullable=True)
    config_json = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)