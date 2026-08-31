from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parents[3]

class Settings(BaseSettings):
    app_name: str = "AI Data Engineering Platform"
    postgres_user: str = "aidp_user"
    postgres_password: str = "aidp_password"
    postgres_db: str = "aidp_db"
    postgres_host: str = "localhost"
    postgres_port: int = 5432

    minio_root_user: str = "minioadmin"
    minio_root_password: str = "minioadmin123"
    minio_endpoint: str = "localhost:9000"
    raw_bucket_name: str = "raw-data"

    airflow_api_url: str = "http://localhost:8080"
    airflow_api_username: str = "airflow"
    airflow_api_password: str = "airflow"
    airflow_dag_id: str = "aidp_csv_ingestion"

    @property
    def database_url(self) -> str:
        return(
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    #This makes the DB connection reusable everywhere.

    model_config = SettingsConfigDict(
        env_file=ROOT_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore", #This makes the settings system more forgiving if the future env variables exist that my app does not need yet. It is a good choice for growing project
    )
    #class config:
    #   env_file = ".env"

settings = Settings()