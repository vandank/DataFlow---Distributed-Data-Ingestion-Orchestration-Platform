from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "AI Data Engineering Platform"
    postgres_user: str = "aidp_user"
    postgres_password: str = "aidp_password"
    postgres_db: str = "aidp_db"
    postgres_host: str = "localhost"
    postgres_port: int = 5432

    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin123"
    minio_endpoint: str = "localhost:9000"

    class config:
        env_file = ".env"

settings = Settings()