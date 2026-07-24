from io import BytesIO

import pandas as pd


def parse_csv_bytes(file_bytes: bytes) -> pd.DataFrame:
    try:
        return pd.read_csv(BytesIO(file_bytes))
    except Exception as exc:
        raise ValueError(f"Invalid CSV file: {exc}") from exc