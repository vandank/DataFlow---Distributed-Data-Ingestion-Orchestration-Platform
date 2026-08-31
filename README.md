# DataFlow — Distributed Data Ingestion & Orchestration Platform

DataFlow is a containerized data ingestion and processing platform built to demonstrate how raw data can move through a reliable, observable, and repeatable ingestion workflow.

The platform accepts CSV files through a REST API, stores the original files in MinIO, creates a persistent ingestion record in PostgreSQL, and triggers Apache Airflow to execute the downstream processing workflow.

The processing pipeline parses, validates, cleans, transforms, persists, and loads the resulting data into PostgreSQL while maintaining the state of each ingestion run.

The project is designed around a simple principle:

> The API should accept and register data; the workflow engine should own the processing lifecycle.

---

## Why I Built This

Many data engineering projects demonstrate individual technologies in isolation — an API, a database, object storage, or an Airflow DAG.

This project focuses on what happens when those components have to work together as one system.

The goal is to build a small but realistic data platform with clear ownership between services:

- FastAPI handles ingestion requests.
- MinIO provides durable object storage for raw files.
- PostgreSQL stores application metadata and warehouse data.
- Apache Airflow orchestrates asynchronous processing.
- Python/Pandas perform CSV parsing and transformation.
- Docker Compose provides the local distributed environment.

The platform also tracks each ingestion using a persistent run ID so that an uploaded file can be followed from its initial registration through processing and warehouse loading.

---

## Architecture

```text
                         ┌──────────────────────┐
                         │      Client/User      │
                         │   CSV File Upload     │
                         └──────────┬───────────┘
                                    │
                                    │ POST /api/v1/ingestions/csv
                                    ▼
                         ┌──────────────────────┐
                         │       FastAPI        │
                         │    Ingestion API     │
                         └──────────┬───────────┘
                                    │
                     ┌──────────────┴──────────────┐
                     │                             │
                     ▼                             ▼
          ┌───────────────────┐         ┌────────────────────┐
          │      MinIO        │         │     PostgreSQL     │
          │                   │         │                    │
          │ Immutable raw CSV │         │ ingestion_sources  │
          │       files       │         │ ingestion_runs     │
          └─────────┬─────────┘         │ transformed_rows   │
                    │                   │ warehouse tables   │
                    │                   └─────────┬──────────┘
                    │                             │
                    │ raw_object_path             │
                    │                             │
                    ▼                             │
          ┌──────────────────────┐                │
          │      Airflow         │                │
          │                      │                │
          │  DAG:                │                │
          │  aidp_csv_ingestion  │                │
          └──────────┬───────────┘                │
                     │                            │
                     ▼                            │
              ┌───────────────┐                   │
              │  process_csv  │                   │
              └───────┬───────┘                   │
                      │                            │
                      ▼                            │
            ┌──────────────────┐                   │
            │ persist_transformed│                 │
            └────────┬─────────┘                   │
                     │                             │
                     ▼                             │
            ┌──────────────────┐                   │
            │ load_warehouse   │───────────────────┘
            └────────┬─────────┘
                     │
                     ▼
            ┌──────────────────┐
            │ finalize_success │
            │        /         │
            │ finalize_failure │
            └──────────────────┘
```
---
## End-to-End Data Flow

A CSV ingestion follows this sequence:
### 1. Upload
A client sends a CSV file and `source_id` to:

```text
POST /api/v1/ingestions/csv
```
The API verifies that the requested ingestion source exists.


### 2. Register the ingestion

The application creates an `IngestionRun` and assigns it a persistent run ID.

This run ID becomes the identifier connecting the API request, Airflow execution, processing records, and warehouse load.


### 3. Store the raw file

The original CSV is uploaded to MinIO.

The raw object is treated as the immutable source artifact for the ingestion.

The API keeps the resulting object passes it to Airflow.


### 4. Trigger Airflow

The FastAPI service authenticates against the Airflow REST API and triggers:

```text
aidp_csv_ingestion
```

The DAG receives:
```text
{
  "run_id": 1,
  "source_id": 6,
  "raw_object_path": "..."
}
```

The API does not perform the complete processing workflow itself.

It registers the ingestion and hands the downstream work to Airflow.


### 5. Process the CSV

Airflow executes the processing pipeline.

The CSV pipeline performs:

```text
MinIO
  ↓
Parse
  ↓
Validate
  ↓
Clean
  ↓
Transform
  ↓
Persist transformed data
  ↓
Load warehouse
```

The processing code is separated into reusable services for parsing, cleaning, transformation, and loading.


### 6. Persist transformed data

The transformed records are persisted to PostgreSQL with their associated ingestion run and source identifiers.

This provides an intermediate representation of the processed dataset and maintains lineage back to the original ingestion.


### 7. Load the Warehouse

The transformed dataset is loaded into the appropriate warehouse structure based on the ingestion source configuration.

For example, a source can specify a warehouse profile such as:

```text
{
  "warehouse_profile": "sales_orders"
}
```

This allows the same ingestion framework to support different source configurations.


### 8. Finalize the ingestion

The DAG has explicit success and failure paths.

On success:

```text
process_csv
    ↓
persist_transformed
    ↓
load_warehouse
    ↓
finalize_success
```

If a downstream task fails:

```text
failed task
    ↓
finalize_failure
```

The corresponding IngestionRun is updated in PostgreSQL.

This means the application can distinguish between an ingestion that was successfully completed and one that failed during processing.

---

## Key Design Decisions

### API and orchestration are separated

FastAPI is responsible for accepting the file, registering the ingestion, storing the raw artifact, and triggering Airflow.

Airflow owns the asynchronous processing workflow.

This prevents the API request from becoming responsible for the entire data pipeline.


### Raw data is stored separately from processed data

The original CSV is stored in MinIO rather than treating PostgreSQL as the primary storage location for raw files.

This preserves the original input and gives the processing workflow a stable source artifact.


### Persistent run IDs provide lineage

Every ingestion is associated with an application-level run ID.

That identifier is passed from:

```text
FastAPI
  → MinIO metadata
  → Airflow DAG configuration
  → processing records
  → warehouse load
  ```

This provides a common reference for tracing an ingestion through the system.


### Airflow and the application database have different responsibilities

Airflow uses its own metadata database for DAGs, task instances, scheduling state, and Airflow metadata.

The application uses `aidp_db` for:

- ingestion sources
- ingestion runs
- transformed records
- warehouse data

The two databases therefore serve different purposes even though they are hosted by the same PostgreSQL service.


### Containers communicate using service names

Inside Docker Compose, services communicate using Docker's internal DNS.

For example:

```text
postgres:5432
minio:9000
airflow-api-server:8080
```

The host machine can access exposed ports such as:

```text
localhost:5432
localhost:9000
localhost:8000
localhost:8080
```

This distinction is important because `localhost` inside a container refers to that container itself, not the host machine.

---

## Technology Stack

| Technology | Purpose |
|------------|---------|
| Python | Application and pipeline development |
| FastAPI | REST API and file ingestion |
| Apache Airflow | Workflow orchestration |
| PostgreSQL | Application metadata and warehouse storage |
| MinIO | S3-compatible object storage for raw data |
| Pandas | CSV parsing and transformation |
| SQLAlchemy | Database access and ORM |
| Pydantic Settings | Application configuration |
| Docker | Containerization |
| Docker Compose | Local multi-service environment |
| HTTPX | FastAPI → Airflow API communication |
| Alembic | Database migrations |

---

## Project Structure

```bash
ai-data-platform/
│
├── backend/
│   ├── app/
│   │   ├── api/
│   │   │   └── v1/
│   │   │       └── endpoints/
│   │   │           └── ingestions.py
│   │   │
│   │   ├── core/
│   │   │   └── config.py
│   │   │
│   │   ├── crud/
│   │   │
│   │   ├── db/
│   │   │   ├── base.py
│   │   │   └── session.py
│   │   │
│   │   ├── models/
│   │   │
│   │   ├── pipelines/
│   │   │   ├── base.py
│   │   │   ├── csv_pipeline.py
│   │   │   └── engine.py
│   │   │
│   │   ├── schemas/
│   │   │
│   │   └── services/
│   │       ├── orchestration/
│   │       │   └── airflow_client.py
│   │       │
│   │       ├── processing/
│   │       │   ├── csv_parser.py
│   │       │   ├── csv_cleaner.py
│   │       │   ├── csv_transformer.py
│   │       │   └── csv_loader.py
│   │       │
│   │       └── storage/
│   │           └── minio_service.py
│   │
│   └── requirements.txt
│
├── airflow/
│   └── dags/
│       └── aidp_csv_ingestion.py
│
├── docker/
│   └── airflow/
│       └── Dockerfile
│
├── docker-compose.yml
├── .env
├── .gitignore
└── README.md
```

---

## Running the Platform

### Start the Services

```bash
docker compose up -d --build
```

Check the running containers:

```bash
docker compose ps
```

The main services include:
- FastAPI backend
- PostgreSQL
- MinIO
- Airflow API server
- Airflow scheduler
- Airflow DAG processor
- Airflow triggerer

### API

FastAPI:
```bash
http://localhost:8000
```

SwaggerUI:
```bash
http://localhost:8000/docs
```

### Airflow
Airflow UI:
```bash
http://localhost:8080
```

### MinIO
MinIO Console:
```bash
http://localhost:9001
```

---

## Example Ingestion
A CSV can be uploaded through the API:

```bash
curl.exe -X POST "http://localhost:8000/api/v1/ingestions/csv" ^
  -F "source_id=6" ^
  -F "file=@test_sales_ingestion.csv"
```

The API registers the ingestion and triggers the Airflow DAG.

The resulting flow can then be observed through:

```text
FastAPI
  → IngestionRun
  → MinIO raw object
  → Airflow DAG run
  → CSV processing
  → transformed records
  → warehouse tables
  → successful ingestion run
```

---

## Verification

The platform has been tested through the actual application path rather than only testing individual functions.

The verification included:
1. Uploading a real CSV through the FastAPI endpoint.
2. Confirming that the raw object appeared in MinIO.
3. Confirming that an Airflow DAG run was created through the Airflow REST API.
4. Confirming that Airflow executed the processing tasks.
5. Confirming that transformed data was persisted to PostgreSQL.
6. Confirming that warehouse tables were populated.
7. Confirming that the ingestion run was marked with the appropriate final state.

This validates the integration between the API, object storage, orchestration layer, processing pipeline, and database rather than testing each component independently.

---

## Current Status

### Implemented
 - FastAPI CSV ingestion endpoint
 - Ingestion source management
 - Persistent ingestion run tracking
 - Raw CSV storage in MinIO
 - CSV parsing
 - CSV validation
 - CSV cleaning
 - CSV transformation
 - Transformed data persistence
 - Warehouse loading
 - Airflow DAG orchestration
 - FastAPI → Airflow REST API integration
 - Airflow authentication
 - Success/failure pipeline finalization
 - Dockerized Airflow environment
 - End-to-end ingestion verification

---

### Roadmap

The current implementation establishes the core ingestion and orchestration foundation.

Planned extensions include:

### Multiple data sources

Extend the platform beyond manual CSV uploads to support additional ingestion mechanisms such as APIs, scheduled files, and database sources.

### Better data quality handling

Introduce richer validation rules, schema enforcement, data quality metrics, and explicit handling of rejected records.

### Observability

Add structured pipeline metrics, improved logging, ingestion dashboards, and operational monitoring.

### Scalable execution

Move beyond the local Docker Compose environment toward distributed task execution and cloud object storage.

### Metadata and lineage

Expand ingestion metadata to capture schema versions, processing statistics, source lineage, and dataset-level dependencies.

### AI-assisted data engineering

Once the deterministic ingestion foundation is mature, AI capabilities can be introduced for areas such as:

- automatic schema inference
- column mapping suggestions
- data quality anomaly detection
- transformation recommendations
- natural-language pipeline configuration

AI is intentionally not part of the current core implementation. The initial focus is building a reliable data ingestion and orchestration foundation that can support those capabilities later.

---

## Engineering Focus

The project is intentionally built around production-oriented engineering concepts:

- clear service boundaries
- asynchronous workflow orchestration
- immutable raw data
- persistent ingestion state
- reusable processing components
- explicit success and failure paths
- database-backed lineage
- containerized infrastructure
- API-driven service integration
- end-to-end verification
