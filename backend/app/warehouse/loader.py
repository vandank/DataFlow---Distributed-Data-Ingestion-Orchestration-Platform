from __future__ import annotations

from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Session

from app.models.ingestion_source import IngestionSource
from app.warehouse.mapping import (
    WarehouseMappingError,
    get_profile,
    get_profile_name_from_source,
    map_dataframe_to_profile,
)
from app.warehouse.models import DimCustomer, DimDate, DimProduct, FactSales


def _get_or_create_customer(db: Session, customer_name: str, customer_region: str | None) -> DimCustomer:
    customer = db.query(DimCustomer).filter(DimCustomer.customer_name == customer_name).first()
    if customer:
        return customer

    customer = DimCustomer(
        customer_name=customer_name,
        customer_region=customer_region,
    )
    db.add(customer)
    db.flush()
    return customer


def _get_or_create_product(db: Session, product_name: str, product_category: str | None) -> DimProduct:
    product = db.query(DimProduct).filter(DimProduct.product_name == product_name).first()
    if product:
        return product

    product = DimProduct(
        product_name=product_name,
        product_category=product_category,
    )
    db.add(product)
    db.flush()
    return product


def _get_or_create_date(db: Session, calendar_date: datetime) -> DimDate:
    date_value = calendar_date.date()
    dim_date = db.query(DimDate).filter(DimDate.calendar_date == date_value).first()
    if dim_date:
        return dim_date

    dim_date = DimDate(
        calendar_date=date_value,
        year=calendar_date.year,
        month=calendar_date.month,
        day=calendar_date.day,
        week=calendar_date.isocalendar().week,
    )
    db.add(dim_date)
    db.flush()
    return dim_date


def _to_python_datetime(value) -> datetime:
    if pd.isna(value):
        raise WarehouseMappingError("order_date cannot be null.")

    if isinstance(value, datetime):
        return value

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    parsed = pd.to_datetime(value, errors="raise")
    if isinstance(parsed, pd.Timestamp):
        return parsed.to_pydatetime()

    raise WarehouseMappingError(f"Unable to convert order_date value: {value!r}")


def _load_sales_orders(
    db: Session,
    source_id: int,
    run_id: int,
    df,
) -> int:
    inserted = 0

    for _, row in df.iterrows():
        required_fields = ["order_id", "order_date", "customer_name", "product_name", "quantity", "revenue"]
        for field in required_fields:
            if pd.isna(row[field]):
                raise WarehouseMappingError(f"Warehouse load failed. Row contains null in required field '{field}'.")

        order_date = _to_python_datetime(row["order_date"])

        customer = _get_or_create_customer(
            db=db,
            customer_name=str(row["customer_name"]),
            customer_region=None if pd.isna(row.get("customer_region")) else str(row["customer_region"]),
        )
        product = _get_or_create_product(
            db=db,
            product_name=str(row["product_name"]),
            product_category=None if pd.isna(row.get("product_category")) else str(row["product_category"]),
        )
        dim_date = _get_or_create_date(db=db, calendar_date=order_date)

        fact = FactSales(
            source_id=source_id,
            run_id=run_id,
            order_id=str(row["order_id"]),
            customer_id=customer.id,
            product_id=product.id,
            date_id=dim_date.id,
            quantity=int(row["quantity"]),
            revenue=float(row["revenue"]),
            discount=None if pd.isna(row.get("discount")) else float(row["discount"]),
        )
        db.add(fact)
        inserted += 1

    db.commit()
    return inserted


def load_warehouse_dataframe(
    db: Session,
    source: IngestionSource,
    run_id: int,
    df,
) -> int:
    profile_name = get_profile_name_from_source(source)
    if not profile_name:
        return 0

    profile = get_profile(profile_name)
    mapped_df = map_dataframe_to_profile(df, profile)

    if profile.name == "sales_orders":
        return _load_sales_orders(
            db=db,
            source_id=source.id,
            run_id=run_id,
            df=mapped_df,
        )

    raise WarehouseMappingError(f"Unsupported warehouse profile: {profile.name}")