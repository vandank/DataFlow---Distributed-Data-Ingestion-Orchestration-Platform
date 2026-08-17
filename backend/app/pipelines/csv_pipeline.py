from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from app.crud.ingestion_run import (
    create_ingestion_run,
    mark_ingestion_run_failed,
    mark_ingestion_run_success,
)

from app.models.ingestion_run import IngestionRun
from app.pipelines.base import PipelineArtifact, PipelineContext, PipelineResult
#from app.crud.transformed_row import create_transformed_rows #Commenting this out
from app.services.processing.csv_loader import load_transformed_rows
from app.pipelines.base import PipelineContext, PipelineResult
from app.services.processing.csv_cleaner import clean_csv_dataframe
from app.services.processing.csv_parser import parse_csv_bytes
from app.services.processing.csv_transformer import transform_csv_dataframe
from app.services.processing.csv_validator import validate_csv_dataframe
from app.services.storage.minio_service import MinIOStorageService
from app.services.storage.dataframe_storage import DataFrameStorageService
from app.warehouse.loader import load_warehouse_dataframe



class CSVPipeline:
    #This stage has one responsibilty: Create the Ingestion run and put the immutable input into MinIO.
    def ingest_raw(self, context: PipelineContext) -> PipelineArtifact:
        db = context.db
        source = context.source

        run = create_ingestion_run(
            db = db,
            source_id = source.id,
        )

        context.run = run

        storage = MinIOStorageService()

        safe_file_name = (
            Path(context.file_name or "upload.csv")
            .name
            .replace(" ", "_")
        )

        object_name = (
            f"{source.source_type}/"
            f"{source.id}/"
            f"run_{run.id}/"
            f"{uuid4().hex}_{safe_file_name}"
        )

        storage.upload_bytes(
            object_name = object_name,
            data = context.file_bytes,
            content_type = context.content_type or "text/csv",
        )

        context.raw_object_path = object_name

        return PipelineArtifact(
            run_id = run.id,
            source_id = source.id,
            raw_object_path = object_name,
        )


    def process(self, artifact: PipelineArtifact) -> PipelineArtifact:
        storage = MinIOStorageService()
        dataframe_storage = DataFrameStorageService()

        raw_bytes = storage.download_bytes(
            artifact.raw_object_path
        )

        parsed_df = parse_csv_bytes(raw_bytes)

        validate_csv_dataframe(parsed_df)

        cleaned_df = clean_csv_dataframe(parsed_df)

        transformed_df = transform_csv_dataframe(cleaned_df)

        if transformed_df.empty:
            raise ValueError(
                "No rows available after cleaning/transformation."
            )

        transformed_object_path = (
            f"staging/"
            f"{artifact.source_id}/"
            f"run_{artifact.run_id}/"
            f"transformed.csv"
        )

        dataframe_storage.write_csv(
            df=transformed_df,
            object_name=transformed_object_path,
        )

        return PipelineArtifact(
            run_id=artifact.run_id,
            source_id=artifact.source_id,
            raw_object_path=artifact.raw_object_path,
            transformed_object_path=transformed_object_path,
        )

    def persist_transformed(self, db, artifact: PipelineArtifact) -> int:
        if not artifact.transformed_object_path:
            raise ValueError(
                    "Transformed object path is missing."
            )

        dataframe_storage = DataFrameStorageService()

        transformed_df = dataframe_storage.read_csv(
            artifact.transformed_object_path
        )

        return load_transformed_rows(
            db=db,
            run_id=artifact.run_id,
            source_id=artifact.source_id,
            df=transformed_df,
        )


    def load_warehouse(self, db, source, artifact: PipelineArtifact) -> int:
        if not artifact.transformed_object_path:
            raise ValueError(
                "Transformed object path is missing."
            )

        dataframe_storage = DataFrameStorageService()

        transformed_df = dataframe_storage.read_csv(
            artifact.transformed_object_path
        )

        return load_warehouse_dataframe(
            db=db,
            source=source,
            run_id=artifact.run_id,
            df=transformed_df,
        )


    def finalize_success(self, db, artifact: PipelineArtifact):
        run = db.get(
            IngestionRun,
            artifact.run_id,
        )

        if run is None:
            raise ValueError(
                f"Ingestion run {artifact.run_id} not found."
            )

        return mark_ingestion_run_success(
            db=db,
            run=run,
            raw_object_path=artifact.raw_object_path,
        )


    def finalize_failure(self, db, run_id: int, error_message: str):
        run = db.get(
            IngestionRun,
            run_id,
        )

        if run is None:
            raise ValueError(
                f"Ingestion run {run_id} not found."
            )

        return mark_ingestion_run_failed(
            db=db,
            run=run,
            error_message=error_message,
        )

    #Commenting this version of execute. Replacing it with the new version
    '''
    def execute(self, context: PipelineContext) -> PipelineResult:
        db = context.db
        source = context.source

        run = create_ingestion_run(db, source.id)
        context.run = run

        storage = MinIOStorageService()

        safe_file_name = Path(context.file_name or "upload.csv").name.replace(" ", "_")
        object_name = (
            f"{source.source_type}/{source.id}/run_{run.id}/"
            f"{uuid4().hex}_{safe_file_name}"
        )

        try:
            #------------------------------
            # 1. Raw Ingestion
            #------------------------------
            raw_object_path = storage.upload_bytes(
                object_name=object_name,
                data=context.file_bytes,
                content_type=context.content_type or "text/csv",
            )
            context.raw_object_path = raw_object_path

            #------------------------------
            # 2. Parse
            #------------------------------
            parsed_df = parse_csv_bytes(context.file_bytes)
            context.parsed_data = parsed_df

            #------------------------------
            # 3. Validate
            #------------------------------
            validate_csv_dataframe(parsed_df)

            #------------------------------
            # 4. Clean
            #------------------------------
            cleaned_df = clean_csv_dataframe(parsed_df)
            context.cleaned_data = cleaned_df

            #------------------------------
            # 5. Transform
            #------------------------------
            transformed_df = transform_csv_dataframe(cleaned_df)
            context.transformed_data = transformed_df

            if transformed_df.empty:
                raise ValueError(
                    "No rows available after cleaning/transformation."
            )

            # ------------------------------
            # 6. Persist transformed/staging rows
            # ------------------------------
            
            #transformed_rows = create_transformed_rows(
            #    db=db,
            #    run_id=run.id,
            #    source_id=source.id,
            #    records=transformed_df.to_dict(orient="records"),
            #)
            #context.transformed_row_count = len(transformed_rows)
            
            context.transformed_row_count = load_transformed_rows(
            db=db,
            run_id=run.id,
            source_id=source.id,
            df=transformed_df,
            )
            # ------------------------------
            # 7. Warehouse Load
            # ------------------------------
            warehouse_row_count = load_warehouse_dataframe(
                db=db,
                source=source,
                run_id=run.id,
                df=transformed_df,
            )
            context.warehouse_row_count = warehouse_row_count

            #------------------------------
            # 8. Mark Successful
            #------------------------------
            run = mark_ingestion_run_success(db, run, raw_object_path)

            return PipelineResult(
                run=run,
                raw_object_path=raw_object_path,
                transformed_row_count=context.transformed_row_count,
                warehouse_row_count=warehouse_row_count,
            )

        except Exception as exc:
            mark_ingestion_run_failed(db, run, str(exc))
            raise
    '''

    def execute(self, context: PipelineContext) -> PipelineResult:
        try:
            artifact = self.ingest_raw(context)

            artifact = self.process(artifact)

            transformed_count = self.persist_transformed(
                db=context.db,
                artifact=artifact,
            )

            warehouse_count = self.load_warehouse(
                db=context.db,
                source=context.source,
                artifact=artifact,
            )

            run = self.finalize_success(
                db=context.db,
                artifact=artifact,
            )

            return PipelineResult(
                run=run,
                raw_object_path=artifact.raw_object_path,
                transformed_row_count=transformed_count,
                warehouse_row_count=warehouse_count,
            )

        except Exception as exc:
            if context.run is not None:
                self.finalize_failure(
                    db=context.db,
                    run_id=context.run.id,
                    error_message=str(exc),
                )
            raise 