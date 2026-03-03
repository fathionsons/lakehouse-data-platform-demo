from __future__ import annotations

import pandas as pd

from lakehouse.config import bronze_files, ensure_base_directories, silver_files
from lakehouse.utils import log, read_json_records, write_parquet

ALLOWED_POLICY_STATUSES = {"active", "lapsed", "cancelled", "pending"}


def clean_companies(df: pd.DataFrame) -> pd.DataFrame:
    clean = df.copy()
    clean["company_id"] = clean["company_id"].astype(str).str.strip().str.upper()
    clean["name"] = clean["name"].fillna("Unknown Company").astype(str).str.strip()
    clean["industry"] = clean["industry"].fillna("Unknown").astype(str).str.strip()
    clean["city"] = clean["city"].fillna("Unknown").astype(str).str.strip()
    clean["created_at"] = pd.to_datetime(
        clean["created_at"], errors="coerce", utc=True
    ).dt.tz_localize(None)

    clean = clean[clean["company_id"].str.fullmatch(r"C\d{6}", na=False)].copy()
    clean = clean.sort_values(["company_id", "created_at"], kind="mergesort")
    clean = clean.drop_duplicates(subset=["company_id"], keep="last")
    return clean[["company_id", "name", "industry", "city", "created_at"]].reset_index(drop=True)


def clean_reviews(df: pd.DataFrame, valid_company_ids: pd.Series) -> pd.DataFrame:
    clean = df.copy()
    clean["event_id"] = clean["event_id"].astype(str).str.strip()
    clean["company_id"] = clean["company_id"].astype(str).str.strip().str.upper()
    clean["rating"] = pd.to_numeric(clean["rating"], errors="coerce")
    clean["review_date"] = pd.to_datetime(
        clean["review_date"], errors="coerce", utc=True
    ).dt.tz_localize(None)
    clean["channel"] = (
        clean["channel"].astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
    )

    clean = clean.drop_duplicates(subset=["event_id"], keep="last")
    clean = clean.dropna(subset=["event_id", "company_id", "rating", "review_date", "channel"])
    clean = clean[clean["rating"].between(1, 5)]
    clean = clean[clean["company_id"].isin(valid_company_ids)]
    clean["rating"] = clean["rating"].astype("int8")

    return clean[["event_id", "company_id", "rating", "review_date", "channel"]].reset_index(
        drop=True
    )


def clean_policies(df: pd.DataFrame, valid_company_ids: pd.Series) -> pd.DataFrame:
    clean = df.copy()
    clean["policy_id"] = clean["policy_id"].astype(str).str.strip()
    clean["company_id"] = clean["company_id"].astype(str).str.strip().str.upper()
    clean["premium_amount"] = pd.to_numeric(clean["premium_amount"], errors="coerce")
    clean["start_date"] = pd.to_datetime(
        clean["start_date"], errors="coerce", utc=True
    ).dt.tz_localize(None)
    clean["status"] = clean["status"].astype(str).str.strip().str.lower()

    clean = clean.drop_duplicates(subset=["policy_id"], keep="last")
    clean.loc[~clean["status"].isin(ALLOWED_POLICY_STATUSES), "status"] = "unknown"
    clean = clean.dropna(
        subset=["policy_id", "company_id", "premium_amount", "start_date", "status"]
    )
    clean = clean[clean["premium_amount"] > 0]
    clean = clean[clean["company_id"].isin(valid_company_ids)]

    return clean[["policy_id", "company_id", "premium_amount", "start_date", "status"]].reset_index(
        drop=True
    )


def run() -> None:
    paths = ensure_base_directories()
    raw_paths = bronze_files(paths)
    curated_paths = silver_files(paths)

    companies_raw = read_json_records(raw_paths["companies"])
    reviews_raw = read_json_records(raw_paths["reviews"])
    policies_raw = pd.read_csv(raw_paths["policies"])

    companies = clean_companies(companies_raw)
    reviews = clean_reviews(reviews_raw, companies["company_id"])
    policies = clean_policies(policies_raw, companies["company_id"])

    write_parquet(companies, curated_paths["companies"])
    write_parquet(reviews, curated_paths["reviews"])
    write_parquet(policies, curated_paths["policies"])

    log(f"Silver companies written: {curated_paths['companies']} ({len(companies):,} rows)")
    log(f"Silver reviews written:   {curated_paths['reviews']} ({len(reviews):,} rows)")
    log(f"Silver policies written:  {curated_paths['policies']} ({len(policies):,} rows)")


if __name__ == "__main__":
    run()
