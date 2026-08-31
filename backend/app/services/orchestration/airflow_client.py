import httpx
from datetime import datetime, timezone
from app.core.config import settings


class AirflowClient:
    def __init__(self) -> None:
        self.base_url = settings.airflow_api_url.rstrip("/")
        self.username = settings.airflow_api_username
        self.password = settings.airflow_api_password

    def _get_token(self) -> str:
        response = httpx.post(
            f"{self.base_url}/auth/token",
            json={
                "username": self.username,
                "password": self.password,
            },
            timeout=10.0,
        )

        response.raise_for_status()

        data = response.json()
        return data["access_token"]

    def trigger_dag(
        self,
        dag_id: str,
        run_id: int,
        source_id: int,
        raw_object_path: str,
    ) -> dict:
        token = self._get_token()

        airflow_run_id = f"aidp_ingestion_{run_id}"

        response = httpx.post(
            f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "dag_run_id": airflow_run_id,
                "logical_date": datetime.now(timezone.utc).isoformat(),
                "conf": {
                    "run_id": run_id,
                    "source_id": source_id,
                    "raw_object_path": raw_object_path,
                },
            },
            timeout=10.0,
        )

        response.raise_for_status()

        return response.json()