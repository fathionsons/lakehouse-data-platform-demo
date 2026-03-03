from __future__ import annotations

import numpy as np
import pandas as pd

from lakehouse.config import ensure_base_directories, gold_files, silver_files
from lakehouse.utils import date_to_key, log, write_parquet


def build_dim_date(reviews: pd.DataFrame, policies: pd.DataFrame) -> pd.DataFrame:
    date_candidates = pd.concat(
        [reviews["review_date"], policies["start_date"]],
        ignore_index=True,
    ).dropna()

    if date_candidates.empty:
        return pd.DataFrame(
            columns=["date_key", "date", "year", "month", "day", "week", "weekday_name"]
        )

    date_range = pd.date_range(
        date_candidates.min().normalize(), date_candidates.max().normalize(), freq="D"
    )
    dim_date = pd.DataFrame({"date": date_range})
    dim_date["date_key"] = date_to_key(dim_date["date"]).astype("int32")
    dim_date["year"] = dim_date["date"].dt.year.astype("int16")
    dim_date["month"] = dim_date["date"].dt.month.astype("int8")
    dim_date["day"] = dim_date["date"].dt.day.astype("int8")
    dim_date["week"] = dim_date["date"].dt.isocalendar().week.astype("int16")
    dim_date["weekday_name"] = dim_date["date"].dt.day_name()
    return dim_date[["date_key", "date", "year", "month", "day", "week", "weekday_name"]]


def build_dim_company(
    companies: pd.DataFrame, min_valid_from: pd.Timestamp | None = None
) -> pd.DataFrame:
    history = companies[["company_id", "name", "city", "industry", "created_at"]].copy()
    history["created_at"] = pd.to_datetime(history["created_at"], errors="coerce").dt.normalize()
    history = history.dropna(subset=["company_id", "created_at"])
    history = history.sort_values(["company_id", "created_at"], kind="mergesort")
    history = history.drop_duplicates(
        subset=["company_id", "created_at", "name", "city", "industry"],
        keep="last",
    )

    scd_columns = ["name", "city", "industry"]
    previous_values = history.groupby("company_id")[scd_columns].shift()
    first_record = history["company_id"].ne(history["company_id"].shift())
    changed_values = history[scd_columns].ne(previous_values).any(axis=1)

    scd2 = history[first_record | changed_values].copy()
    scd2["valid_from"] = scd2["created_at"]

    if min_valid_from is not None and not pd.isna(min_valid_from):
        floor_date = pd.Timestamp(min_valid_from).normalize()
        first_version = scd2.groupby("company_id").cumcount().eq(0)
        scd2.loc[first_version, "valid_from"] = scd2.loc[first_version, "valid_from"].clip(
            upper=floor_date
        )

    scd2 = scd2.sort_values(["company_id", "valid_from"], kind="mergesort")
    scd2["next_valid_from"] = scd2.groupby("company_id")["valid_from"].shift(-1)
    scd2["valid_to"] = scd2["next_valid_from"] - pd.to_timedelta(1, unit="D")
    scd2["is_current"] = scd2["next_valid_from"].isna()
    scd2 = scd2.drop(columns=["created_at", "next_valid_from"]).reset_index(drop=True)
    scd2["company_key"] = np.arange(1, len(scd2) + 1, dtype=np.int32)

    return scd2[
        [
            "company_key",
            "company_id",
            "name",
            "city",
            "industry",
            "valid_from",
            "valid_to",
            "is_current",
        ]
    ]


def build_dim_channel(reviews: pd.DataFrame) -> pd.DataFrame:
    values = sorted(reviews["channel"].dropna().astype(str).unique().tolist())
    dim_channel = pd.DataFrame({"channel_name": values})
    dim_channel["channel_key"] = np.arange(1, len(dim_channel) + 1, dtype=np.int16)
    return dim_channel[["channel_key", "channel_name"]]


def build_dim_policy_status(policies: pd.DataFrame) -> pd.DataFrame:
    values = sorted(policies["status"].dropna().astype(str).unique().tolist())
    dim_status = pd.DataFrame({"status": values})
    dim_status["status_key"] = np.arange(1, len(dim_status) + 1, dtype=np.int16)
    return dim_status[["status_key", "status"]]


def _attach_company_key_by_date(
    events: pd.DataFrame,
    dim_company: pd.DataFrame,
    event_id_col: str,
    event_date_col: str,
) -> pd.DataFrame:
    company_windows = dim_company[["company_key", "company_id", "valid_from", "valid_to"]]
    merged = events.merge(company_windows, on="company_id", how="left")
    event_dates = pd.to_datetime(merged[event_date_col], errors="coerce").dt.normalize()
    in_window = (event_dates >= merged["valid_from"]) & (
        merged["valid_to"].isna() | (event_dates <= merged["valid_to"])
    )

    matched = merged[in_window].copy()
    matched = matched.sort_values([event_id_col, "valid_from"], kind="mergesort")
    matched = matched.drop_duplicates(subset=[event_id_col], keep="last")
    return matched


def build_fact_review(
    reviews: pd.DataFrame, dim_company: pd.DataFrame, dim_channel: pd.DataFrame
) -> pd.DataFrame:
    fact = _attach_company_key_by_date(
        reviews,
        dim_company,
        event_id_col="event_id",
        event_date_col="review_date",
    )
    fact = fact.merge(dim_channel, left_on="channel", right_on="channel_name", how="left")
    fact["date_key"] = date_to_key(fact["review_date"])
    fact["review_key"] = np.arange(1, len(fact) + 1, dtype=np.int64)

    out = fact[
        ["review_key", "event_id", "date_key", "company_key", "channel_key", "rating"]
    ].copy()
    out["date_key"] = out["date_key"].astype("Int64")
    out["company_key"] = out["company_key"].astype("Int64")
    out["channel_key"] = out["channel_key"].astype("Int64")
    return out


def build_fact_policy_premium(
    policies: pd.DataFrame, dim_company: pd.DataFrame, dim_status: pd.DataFrame
) -> pd.DataFrame:
    fact = _attach_company_key_by_date(
        policies,
        dim_company,
        event_id_col="policy_id",
        event_date_col="start_date",
    )
    fact = fact.merge(dim_status, on="status", how="left")
    fact["date_key"] = date_to_key(fact["start_date"])

    out = fact[["policy_id", "date_key", "company_key", "status_key", "premium_amount"]].copy()
    out["date_key"] = out["date_key"].astype("Int64")
    out["company_key"] = out["company_key"].astype("Int64")
    out["status_key"] = out["status_key"].astype("Int64")
    return out


def _assert_not_null(df: pd.DataFrame, cols: list[str], table_name: str) -> None:
    if df[cols].isnull().any().any():
        raise ValueError(f"Null keys found in {table_name}: {cols}")


def _assert_fk_coverage(
    fact_df: pd.DataFrame,
    fact_key: str,
    dim_df: pd.DataFrame,
    dim_key: str,
    table_name: str,
) -> None:
    missing = set(fact_df[fact_key].dropna().astype(int)) - set(
        dim_df[dim_key].dropna().astype(int)
    )
    if missing:
        raise ValueError(
            f"Foreign key check failed for {table_name}.{fact_key}, missing keys: {len(missing)}"
        )


def run() -> None:
    paths = ensure_base_directories()
    curated_paths = silver_files(paths)
    model_paths = gold_files(paths)

    companies = pd.read_parquet(curated_paths["companies"])
    reviews = pd.read_parquet(curated_paths["reviews"])
    policies = pd.read_parquet(curated_paths["policies"])

    min_fact_date = pd.concat(
        [reviews["review_date"], policies["start_date"]], ignore_index=True
    ).min()

    dim_company = build_dim_company(companies, min_valid_from=min_fact_date)
    dim_channel = build_dim_channel(reviews)
    dim_status = build_dim_policy_status(policies)
    dim_date = build_dim_date(reviews, policies)

    fact_review = build_fact_review(reviews, dim_company, dim_channel)
    fact_policy = build_fact_policy_premium(policies, dim_company, dim_status)

    _assert_not_null(fact_review, ["date_key", "company_key", "channel_key"], "fact_review")
    _assert_not_null(fact_policy, ["date_key", "company_key", "status_key"], "fact_policy_premium")
    _assert_fk_coverage(fact_review, "company_key", dim_company, "company_key", "fact_review")
    _assert_fk_coverage(
        fact_policy, "company_key", dim_company, "company_key", "fact_policy_premium"
    )
    _assert_fk_coverage(fact_review, "date_key", dim_date, "date_key", "fact_review")
    _assert_fk_coverage(fact_policy, "date_key", dim_date, "date_key", "fact_policy_premium")

    write_parquet(dim_date, model_paths["dim_date"])
    write_parquet(dim_company, model_paths["dim_company"])
    write_parquet(dim_channel, model_paths["dim_channel"])
    write_parquet(dim_status, model_paths["dim_policy_status"])
    write_parquet(fact_review, model_paths["fact_review"])
    write_parquet(fact_policy, model_paths["fact_policy_premium"])

    log(f"Gold dim_date written:          {model_paths['dim_date']} ({len(dim_date):,} rows)")
    log(f"Gold dim_company written:       {model_paths['dim_company']} ({len(dim_company):,} rows)")
    log(f"Gold dim_channel written:       {model_paths['dim_channel']} ({len(dim_channel):,} rows)")
    log(
        "Gold dim_policy_status written: "
        f"{model_paths['dim_policy_status']} ({len(dim_status):,} rows)"
    )
    log(f"Gold fact_review written:       {model_paths['fact_review']} ({len(fact_review):,} rows)")
    log(
        "Gold fact_policy_premium written: "
        f"{model_paths['fact_policy_premium']} ({len(fact_policy):,} rows)"
    )


if __name__ == "__main__":
    run()
