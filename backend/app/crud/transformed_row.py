from sqlalchemy.orm import Session

from app.models.transformed_row import TransformedRow


def create_transformed_rows(
    db: Session,
    run_id: int,
    source_id: int,
    records: list[dict],
) -> list[TransformedRow]:
    rows = [
        TransformedRow(
            run_id=run_id,
            source_id=source_id,
            row_number=index,
            data_json=record,
        )
        for index, record in enumerate(records, start=1)
    ]

    db.add_all(rows)
    db.commit()

    for row in rows:
        db.refresh(row)

    return rows