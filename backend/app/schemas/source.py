from typing import Any
from pydantic import BaseModel, Field, ConfigDict

class SourceCreate(BaseModel):
    name: str = Field(..., max_length=100)
    source_type: str = Field(..., max_length=50)
    description: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)

class SourceRead(BaseModel):
    id: int
    name: str
    source_type: str
    description: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    is_active: bool

    model_config = ConfigDict(from_attributes=True)