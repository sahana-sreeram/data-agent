"""Derives DatasetMetadata for one curated/raw Parquet dataset purely from what's actually in
it -- columns, dtypes, nullability, row count, a few candidate keys, sample value summaries.
No business meaning is inferred here; that's the human-annotation layer's job.
"""

from __future__ import annotations

import pandas as pd

from src.context_store.models import DatasetMetadata
from src.storage import S3Storage

MAX_SAMPLE_VALUES = 5


def _candidate_keys(df: pd.DataFrame) -> list[str]:
    """Columns whose values are unique and non-null across every row -- a structural
    (not business) signal that a column COULD be a primary/candidate key."""
    candidates = []
    for column in df.columns:
        series = df[column]
        if series.isna().any():
            continue
        if series.is_unique and len(series) > 0:
            candidates.append(column)
    return candidates


def _sample_value_summary(series: pd.Series) -> dict:
    non_null = series.dropna()
    summary: dict = {"distinct_count": int(non_null.nunique())}
    if pd.api.types.is_numeric_dtype(series) and len(non_null) > 0:
        summary["min"] = non_null.min().item() if hasattr(non_null.min(), "item") else non_null.min()
        summary["max"] = non_null.max().item() if hasattr(non_null.max(), "item") else non_null.max()
    else:
        summary["sample_values"] = non_null.unique()[:MAX_SAMPLE_VALUES].tolist()
    return summary


def introspect_dataset(storage: S3Storage, dataset_name: str, s3_key: str) -> DatasetMetadata:
    """Read `s3_key` (a Parquet object) and derive its DatasetMetadata."""
    df = storage.read_parquet(s3_key)

    columns = {column: str(dtype) for column, dtype in df.dtypes.items()}
    nullable_columns = [column for column in df.columns if df[column].isna().any()]
    sample_value_summaries = {column: _sample_value_summary(df[column]) for column in df.columns}

    return DatasetMetadata(
        dataset_name=dataset_name,
        physical_location=f"s3://{storage.bucket}/{s3_key}",
        columns=columns,
        nullable_columns=nullable_columns,
        row_count_estimate=len(df),
        sample_value_summaries=sample_value_summaries,
        candidate_keys=_candidate_keys(df),
    )
