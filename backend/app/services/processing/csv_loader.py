from __future__ import annotations

from sqlalchemy.orm import Session

from app.crud.transformed_row import create_transformed_rows


def load_transformed_rows(
    db: Session,
    run_id: int,
    source_id: int,
    df,
) -> int:
    records = df.to_dict(orient="records")

    if not records:
        raise ValueError("No rows available after cleaning/transformation.")

    rows = create_transformed_rows(
        db=db,
        run_id=run_id,
        source_id=source_id,
        records=records,
    )
    return len(rows)