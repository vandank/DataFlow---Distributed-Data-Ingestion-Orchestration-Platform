from sqlalchemy import Column, Date, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.db.base import Base


class DimCustomer(Base):
    __tablename__ = "dim_customers"

    id = Column(Integer, primary_key=True, index=True)
    customer_name = Column(String(255), nullable=False, unique=True, index=True)
    customer_region = Column(String(100), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DimProduct(Base):
    __tablename__ = "dim_products"

    id = Column(Integer, primary_key=True, index=True)
    product_name = Column(String(255), nullable=False, unique=True, index=True)
    product_category = Column(String(100), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DimDate(Base):
    __tablename__ = "dim_dates"

    id = Column(Integer, primary_key=True, index=True)
    calendar_date = Column(Date, nullable=False, unique=True, index=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class FactSales(Base):
    __tablename__ = "fact_sales"
    __table_args__ = (
        UniqueConstraint("source_id", "run_id", "order_id", name="uq_fact_sales_source_run_order"),
    )

    id = Column(Integer, primary_key=True, index=True)
    source_id = Column(Integer, ForeignKey("ingestion_sources.id"), nullable=False, index=True)
    run_id = Column(Integer, ForeignKey("ingestion_runs.id"), nullable=False, index=True)

    order_id = Column(String(100), nullable=False, index=True)
    customer_id = Column(Integer, ForeignKey("dim_customers.id"), nullable=False, index=True)
    product_id = Column(Integer, ForeignKey("dim_products.id"), nullable=False, index=True)
    date_id = Column(Integer, ForeignKey("dim_dates.id"), nullable=False, index=True)

    quantity = Column(Integer, nullable=False)
    revenue = Column(Float, nullable=False)
    discount = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    customer = relationship("DimCustomer")
    product = relationship("DimProduct")
    date = relationship("DimDate")