from __future__ import annotations

import pandas as pd


def _clean_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped != "" else None
    return value


def clean_csv_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    unnamed_columns = [col for col in df.columns if str(col).startswith("Unnamed")]
    if unnamed_columns:
        df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

    for col in df.columns:
        df[col] = df[col].map(_clean_value)

    df = df.drop_duplicates()
    df = df.where(pd.notnull(df), None)

    return df