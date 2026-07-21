from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.crud.source import create_source, get_source_by_id, source_to_dict
from app.deps import get_db
from app.schemas.source import SourceCreate, SourceRead

router = APIRouter()


@router.post("/", response_model=SourceRead, status_code=status.HTTP_201_CREATED)
def create_source_endpoint(
    source_in: SourceCreate,
    db: Session = Depends(get_db),
):
    try:
        source = create_source(db, source_in)
        return source_to_dict(source)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{source_id}", response_model=SourceRead)
def get_source_endpoint(
    source_id: int,
    db: Session = Depends(get_db),
):
    source = get_source_by_id(db, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    return source_to_dict(source)