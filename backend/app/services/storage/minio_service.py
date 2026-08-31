from io import BytesIO

from minio import Minio

from app.core.config import settings

#This service will create the bucket automatically the first time you upload the data.
class MinIOStorageService:
    def __init__(self) -> None:
        self.client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_root_user,
            secret_key=settings.minio_root_password,
            secure=False,
        )
        self.bucket_name = settings.raw_bucket_name

    def ensure_bucket_exists(self) -> None:
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

    def upload_bytes(
        self,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        self.ensure_bucket_exists()
        data_stream = BytesIO(data)
        self.client.put_object(
            bucket_name=self.bucket_name,
            object_name=object_name,
            data=data_stream,
            length=len(data),
            content_type=content_type,
        )
        return object_name

    def download_bytes(self, object_name: str) -> bytes:
        self.ensure_bucket_exists()

        response = self.client.get_object(
            bucket_name=self.bucket_name,
            object_name=object_name,
        )

        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def object_exists(self, object_name: str) -> bool:
        try:
            self.client.stat_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
            )
            return True
        except Exception:
            return False