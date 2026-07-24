from __future__ import annotations

import re
import pandas as pd


def _to_snake_case(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_").lower()


def transform_csv_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_to_snake_case(str(col)) for col in df.columns]
    return df