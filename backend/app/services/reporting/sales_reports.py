from datetime import date

from sqlalchemy import desc, func
from sqlalchemy.orm import Session

from app.models.ingestion_run import IngestionRun
from app.models.ingestion_source import IngestionSource
from app.schemas.reporting import (
    CustomerRevenueRead,
    ProductRevenueRead,
    RecentWarehouseRunRead,
    SalesSummaryRead,
)
from app.warehouse.models import DimCustomer, DimDate, DimProduct, FactSales


def _apply_date_filter(query, start_date: date | None, end_date: date | None):
    if start_date:
        query = query.filter(DimDate.calendar_date >= start_date)
    if end_date:
        query = query.filter(DimDate.calendar_date <= end_date)
    return query


def get_sales_summary(
    db: Session,
    start_date: date | None = None,
    end_date: date | None = None,
) -> SalesSummaryRead:
    query = (
        db.query(
            func.count(FactSales.id).label("order_count"),
            func.coalesce(func.sum(FactSales.quantity), 0).label("total_quantity"),
            func.coalesce(func.sum(FactSales.revenue), 0.0).label("total_revenue"),
            func.coalesce(func.avg(FactSales.discount), 0.0).label("average_discount"),
        )
        .join(DimDate, FactSales.date_id == DimDate.id)
    )

    query = _apply_date_filter(query, start_date, end_date)
    row = query.one()

    return SalesSummaryRead(
        order_count=int(row.order_count or 0),
        total_quantity=int(row.total_quantity or 0),
        total_revenue=float(row.total_revenue or 0.0),
        average_discount=float(row.average_discount or 0.0),
    )


def get_revenue_by_product(
    db: Session,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[ProductRevenueRead]:
    total_quantity = func.coalesce(func.sum(FactSales.quantity), 0).label("total_quantity")
    total_revenue = func.coalesce(func.sum(FactSales.revenue), 0.0).label("total_revenue")

    query = (
        db.query(
            DimProduct.product_name,
            DimProduct.product_category,
            total_quantity,
            total_revenue,
        )
        .join(DimProduct, FactSales.product_id == DimProduct.id)
        .join(DimDate, FactSales.date_id == DimDate.id)
    )

    query = _apply_date_filter(query, start_date, end_date)

    rows = (
        query
        .group_by(DimProduct.product_name, DimProduct.product_category)
        .order_by(desc(total_revenue))
        .all()
    )

    return [
        ProductRevenueRead(
            product_name=row.product_name,
            product_category=row.product_category,
            total_quantity=int(row.total_quantity or 0),
            total_revenue=float(row.total_revenue or 0.0),
        )
        for row in rows
    ]


def get_revenue_by_customer(
    db: Session,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[CustomerRevenueRead]:
    total_quantity = func.coalesce(func.sum(FactSales.quantity), 0).label("total_quantity")
    total_revenue = func.coalesce(func.sum(FactSales.revenue), 0.0).label("total_revenue")

    query = (
        db.query(
            DimCustomer.customer_name,
            DimCustomer.customer_region,
            total_quantity,
            total_revenue,
        )
        .join(DimCustomer, FactSales.customer_id == DimCustomer.id)
        .join(DimDate, FactSales.date_id == DimDate.id)
    )

    query = _apply_date_filter(query, start_date, end_date)

    rows = (
        query
        .group_by(DimCustomer.customer_name, DimCustomer.customer_region)
        .order_by(desc(total_revenue))
        .all()
    )

    return [
        CustomerRevenueRead(
            customer_name=row.customer_name,
            customer_region=row.customer_region,
            total_quantity=int(row.total_quantity or 0),
            total_revenue=float(row.total_revenue or 0.0),
        )
        for row in rows
    ]


def get_recent_warehouse_runs(db: Session, limit: int = 20) -> list[RecentWarehouseRunRead]:
    rows = (
        db.query(
            IngestionRun.id.label("run_id"),
            IngestionSource.name.label("source_name"),
            IngestionRun.status.label("status"),
            IngestionRun.raw_object_path.label("raw_object_path"),
            IngestionRun.started_at.label("started_at"),
            IngestionRun.finished_at.label("finished_at"),
            func.count(FactSales.id).label("warehouse_row_count"),
        )
        .join(IngestionSource, IngestionRun.source_id == IngestionSource.id)
        .outerjoin(FactSales, FactSales.run_id == IngestionRun.id)
        .group_by(
            IngestionRun.id,
            IngestionSource.name,
            IngestionRun.status,
            IngestionRun.raw_object_path,
            IngestionRun.started_at,
            IngestionRun.finished_at,
        )
        .order_by(desc(IngestionRun.id))
        .limit(limit)
        .all()
    )

    return [
        RecentWarehouseRunRead(
            run_id=row.run_id,
            source_name=row.source_name,
            status=row.status,
            raw_object_path=row.raw_object_path,
            started_at=row.started_at,
            finished_at=row.finished_at,
            warehouse_row_count=int(row.warehouse_row_count or 0),
        )
        for row in rows
    ]