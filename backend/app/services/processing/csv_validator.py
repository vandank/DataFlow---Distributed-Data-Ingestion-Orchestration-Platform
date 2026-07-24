from __future__ import annotations

import pandas as pd


class CSVValidationError(Exception):
    pass


def load_csv_dataframe(file_bytes: bytes) -> pd.DataFrame:
    try:
        return pd.read_csv(
            pd.io.common.BytesIO(file_bytes)
        )
    except Exception as exc:
        raise CSVValidationError(f"Invalid CSV file: {exc}") from exc


def validate_csv_dataframe(df: pd.DataFrame) -> None:
    if df.empty:
        raise CSVValidationError("CSV file has no data rows.")

    if df.columns.empty:
        raise CSVValidationError("CSV file has no columns.")

    normalised_columns = [str(col).strip() for col in df.columns]

    if any(not col for col in normalised_columns):
        raise CSVValidationError("CSV contains empty column names.")

    if len(normalised_columns) != len(set(normalised_columns)):
        raise CSVValidationError("CSV contains duplicate column names.")