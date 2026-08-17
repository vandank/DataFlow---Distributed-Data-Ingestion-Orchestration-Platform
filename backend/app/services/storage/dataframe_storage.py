from io import BytesIO

import pandas as pd

from app.services.storage.minio_service import MinIOStorageService


class DataFrameStorageService:
    def __init__(self) -> None:
        self.storage = MinIOStorageService()

    def write_csv(
        self,
        df: pd.DataFrame,
        object_name: str,
    ) -> str:
        buffer = BytesIO()
        df.to_csv(buffer, index=False)

        self.storage.upload_bytes(
            object_name=object_name,
            data=buffer.getvalue(),
            content_type="text/csv",
        )

        return object_name

    def read_csv(
        self,
        object_name: str,
    ) -> pd.DataFrame:
        data = self.storage.download_bytes(object_name)

        return pd.read_csv(BytesIO(data))