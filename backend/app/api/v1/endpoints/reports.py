from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.deps import get_db
from app.schemas.reporting import (
    CustomerRevenueRead,
    ProductRevenueRead,
    RecentWarehouseRunRead,
    SalesSummaryRead,
)
from app.services.reporting.sales_reports import (
    get_recent_warehouse_runs,
    get_revenue_by_customer,
    get_revenue_by_product,
    get_sales_summary,
)

router = APIRouter()


@router.get("/sales/summary", response_model=SalesSummaryRead)
def sales_summary(
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    db: Session = Depends(get_db),
):
    return get_sales_summary(db, start_date=start_date, end_date=end_date)


@router.get("/sales/by-product", response_model=list[ProductRevenueRead])
def revenue_by_product(
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    db: Session = Depends(get_db),
):
    return get_revenue_by_product(db, start_date=start_date, end_date=end_date)


@router.get("/sales/by-customer", response_model=list[CustomerRevenueRead])
def revenue_by_customer(
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    db: Session = Depends(get_db),
):
    return get_revenue_by_customer(db, start_date=start_date, end_date=end_date)


@router.get("/runs/recent", response_model=list[RecentWarehouseRunRead])
def recent_runs(
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    return get_recent_warehouse_runs(db, limit=limit)