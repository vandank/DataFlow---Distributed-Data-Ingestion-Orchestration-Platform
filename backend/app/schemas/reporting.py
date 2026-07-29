from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


class SalesSummaryRead(BaseModel):
    order_count: int
    total_quantity: int
    total_revenue: float
    average_discount: float

    model_config = ConfigDict(from_attributes=True)


class ProductRevenueRead(BaseModel):
    product_name: str
    product_category: str | None = None
    total_quantity: int
    total_revenue: float

    model_config = ConfigDict(from_attributes=True)


class CustomerRevenueRead(BaseModel):
    customer_name: str
    customer_region: str | None = None
    total_quantity: int
    total_revenue: float

    model_config = ConfigDict(from_attributes=True)


class RecentWarehouseRunRead(BaseModel):
    run_id: int
    source_name: str
    status: str
    raw_object_path: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    warehouse_row_count: int

    model_config = ConfigDict(from_attributes=True)