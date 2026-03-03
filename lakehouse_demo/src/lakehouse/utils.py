from __future__ import annotations

from pathlib import Path

import pandas as pd

from lakehouse.config import PARQUET_COMPRESSION


def log(message: str) -> None:
    print(f"[lakehouse] {message}")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    ensure_parent(path)
    df.to_parquet(path, index=False, compression=PARQUET_COMPRESSION)


def read_json_records(path: Path) -> pd.DataFrame:
    return pd.read_json(path, lines=True)


def date_to_key(date_series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(date_series, errors="coerce")
    return parsed.dt.strftime("%Y%m%d").astype("Int64")


def markdown_table(df: pd.DataFrame, max_rows: int = 10) -> str:
    if df.empty:
        return "_No rows_"
    return df.head(max_rows).to_markdown(index=False)
