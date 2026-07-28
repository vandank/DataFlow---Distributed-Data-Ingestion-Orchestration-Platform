from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from typing import Callable

import pandas as pd

from app.models.ingestion_source import IngestionSource


class WarehouseMappingError(ValueError):
    pass


def _normalise(name: str) -> str:
    name = str(name).strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return re.sub(r"_+", "_", name).strip("_")


def _to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="raise")


def _to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="raise").astype("Int64")


def _to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="raise").astype(float)


@dataclass(frozen=True)
class FieldSpec:
    canonical_name: str
    aliases: tuple[str, ...] = ()
    required: bool = True
    converter: Callable[[pd.Series], pd.Series] | None = None


@dataclass(frozen=True)
class WarehouseProfile:
    name: str
    fields: tuple[FieldSpec, ...]
    preserve_unmapped: bool = False


SALES_ORDERS_PROFILE = WarehouseProfile(
    name="sales_orders",
    fields=(
        FieldSpec("order_id", aliases=("order_no", "order_number"), required=True),
        FieldSpec("order_date", aliases=("order_dt", "date", "created_at"), required=True, converter=_to_datetime),
        FieldSpec("customer_name", aliases=("cust_name", "customer"), required=True),
        FieldSpec("customer_region", aliases=("cust_region", "region"), required=False),
        FieldSpec("product_name", aliases=("prod_name", "product"), required=True),
        FieldSpec("product_category", aliases=("prod_category", "category"), required=False),
        FieldSpec("quantity", aliases=("qty", "units"), required=True, converter=_to_int),
        FieldSpec("revenue", aliases=("amount", "sales"), required=True, converter=_to_float),
        FieldSpec("discount", aliases=("discount_pct", "disc"), required=False, converter=_to_float),
    ),
    preserve_unmapped=False,
)

PROFILES = {
    _normalise(SALES_ORDERS_PROFILE.name): SALES_ORDERS_PROFILE,
}


def get_profile(name: str) -> WarehouseProfile:
    key = _normalise(name)
    try:
        return PROFILES[key]
    except KeyError as exc:
        raise WarehouseMappingError(f"Unknown warehouse profile: {name}") from exc


def get_profile_name_from_source(source: IngestionSource) -> str | None:
    if not source.config_json:
        return None

    try:
        config = json.loads(source.config_json)
    except json.JSONDecodeError as exc:
        raise WarehouseMappingError(f"Invalid JSON in source config for source_id={source.id}") from exc

    profile_name = config.get("warehouse_profile")
    if not profile_name:
        return None

    return _normalise(str(profile_name))


def map_dataframe_to_profile(df: pd.DataFrame, profile: WarehouseProfile) -> pd.DataFrame:
    mapped = df.copy()
    mapped.columns = [_normalise(str(col)) for col in mapped.columns]

    alias_to_canonical: dict[str, str] = {}
    canonical_order: list[str] = []

    for field in profile.fields:
        canonical = _normalise(field.canonical_name)
        canonical_order.append(canonical)
        alias_to_canonical[canonical] = canonical

        for alias in field.aliases:
            alias_to_canonical[_normalise(alias)] = canonical

    target_columns = [alias_to_canonical.get(col, col) for col in mapped.columns]
    collisions = [name for name, count in Counter(target_columns).items() if count > 1]
    if collisions:
        raise WarehouseMappingError(
            f"Multiple source columns map to the same warehouse column(s): {sorted(collisions)}"
        )

    mapped.columns = target_columns

    required_columns = [
        _normalise(field.canonical_name)
        for field in profile.fields
        if field.required
    ]
    missing = [col for col in required_columns if col not in mapped.columns]
    if missing:
        raise WarehouseMappingError(
            f"Missing required warehouse columns: {sorted(missing)}"
        )

    for field in profile.fields:
        canonical = _normalise(field.canonical_name)
        if canonical in mapped.columns and field.converter is not None:
            mapped[canonical] = field.converter(mapped[canonical])

    canonical_set = set(canonical_order)

    if profile.preserve_unmapped:
        extra_columns = [col for col in mapped.columns if col not in canonical_set]
        ordered_columns = canonical_order + extra_columns
        mapped = mapped.loc[:, ordered_columns]
    else:
        mapped = mapped.loc[:, [col for col in canonical_order if col in mapped.columns]]

    return mapped