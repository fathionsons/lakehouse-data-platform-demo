from __future__ import annotations

import numpy as np
import pandas as pd

from lakehouse.config import (
    N_COMPANIES,
    N_POLICIES,
    N_REVIEWS,
    RANDOM_SEED,
    bronze_files,
    ensure_base_directories,
)
from lakehouse.utils import log

INDUSTRIES = np.array(
    [
        "Insurance",
        "Finance",
        "Retail",
        "Healthcare",
        "Technology",
        "Energy",
        "Logistics",
        "Manufacturing",
        "Hospitality",
        "Education",
    ]
)

CITIES = np.array(
    [
        "Copenhagen",
        "Aarhus",
        "Odense",
        "Aalborg",
        "Esbjerg",
        "Randers",
        "Kolding",
        "Horsens",
        "Vejle",
        "Roskilde",
    ]
)

CHANNELS = np.array(["web", "mobile", "email", "partner", "call_center"])
POLICY_STATUSES = np.array(["active", "lapsed", "cancelled", "pending"])
NAME_PREFIXES = np.array(
    [
        "Nordic",
        "Blue",
        "Prime",
        "Solid",
        "Pioneer",
        "Vertex",
        "Green",
        "Apex",
        "Trusted",
        "Atlas",
    ]
)
NAME_SUFFIXES = np.array(
    [
        "Advisors",
        "Solutions",
        "Partners",
        "Holdings",
        "Services",
        "Group",
        "Works",
        "Digital",
        "Systems",
        "Ventures",
    ]
)


def _random_dates(rng: np.random.Generator, start: str, end: str, size: int) -> pd.Series:
    start_ts = pd.Timestamp(start)
    span_days = (pd.Timestamp(end) - start_ts).days
    offsets = rng.integers(0, span_days + 1, size=size)
    return start_ts + pd.to_timedelta(offsets, unit="D")


def generate_companies(n_companies: int = N_COMPANIES, seed: int = RANDOM_SEED) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    ids = pd.Series(np.arange(1, n_companies + 1), dtype="int64").astype(str).str.zfill(6).radd("C")
    base_df = pd.DataFrame(
        {
            "company_id": ids,
            "name": pd.Series(rng.choice(NAME_PREFIXES, size=n_companies))
            + " "
            + pd.Series(rng.choice(NAME_SUFFIXES, size=n_companies)),
            "industry": rng.choice(INDUSTRIES, size=n_companies),
            "city": rng.choice(CITIES, size=n_companies),
            "created_at": _random_dates(rng, "2019-01-01", "2025-12-31", n_companies),
        }
    )

    # Simulate operational updates that require SCD2 handling in Gold.
    updates = base_df.sample(n=220, random_state=seed + 100).copy()
    update_mask = np.arange(len(updates)) % 2 == 0
    city_rotation = {city: CITIES[(idx + 1) % len(CITIES)] for idx, city in enumerate(CITIES)}
    industry_rotation = {
        industry: INDUSTRIES[(idx + 1) % len(INDUSTRIES)] for idx, industry in enumerate(INDUSTRIES)
    }
    updates.loc[update_mask, "city"] = updates.loc[update_mask, "city"].map(city_rotation)
    updates.loc[~update_mask, "industry"] = updates.loc[~update_mask, "industry"].map(
        industry_rotation
    )
    updates["created_at"] = updates["created_at"] + pd.to_timedelta(
        rng.integers(45, 420, size=len(updates)), unit="D"
    )
    updates["created_at"] = updates["created_at"].clip(upper=pd.Timestamp("2026-01-15"))

    exact_duplicates = base_df.sample(n=25, random_state=seed).copy()
    companies_raw = pd.concat([base_df, updates, exact_duplicates], ignore_index=True)

    null_city_index = companies_raw.sample(n=20, random_state=seed + 1).index
    companies_raw.loc[null_city_index, "city"] = None
    return companies_raw


def generate_reviews(
    company_ids: pd.Series, n_reviews: int = N_REVIEWS, seed: int = RANDOM_SEED
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 10)
    base_df = pd.DataFrame(
        {
            "event_id": pd.Series(np.arange(1, n_reviews + 1), dtype="int64")
            .astype(str)
            .str.zfill(7)
            .radd("E"),
            "company_id": rng.choice(company_ids.to_numpy(), size=n_reviews),
            "rating": rng.choice(
                np.array([1, 2, 3, 4, 5]), size=n_reviews, p=[0.08, 0.12, 0.20, 0.30, 0.30]
            ),
            "review_date": _random_dates(rng, "2025-01-01", "2026-01-31", n_reviews),
            "channel": rng.choice(CHANNELS, size=n_reviews, p=[0.40, 0.25, 0.15, 0.10, 0.10]),
        }
    )

    duplicates = base_df.sample(n=400, random_state=seed + 2)
    orphan_rows = base_df.sample(n=250, random_state=seed + 3).copy()
    orphan_rows["event_id"] = "ORPH_" + orphan_rows["event_id"].astype(str)
    orphan_rows["company_id"] = "C999999"

    reviews_raw = pd.concat([base_df, duplicates, orphan_rows], ignore_index=True)
    null_rating_idx = reviews_raw.sample(n=250, random_state=seed + 4).index
    reviews_raw.loc[null_rating_idx, "rating"] = None
    invalid_rating_idx = reviews_raw.sample(n=150, random_state=seed + 5).index
    reviews_raw.loc[invalid_rating_idx, "rating"] = 6
    return reviews_raw


def generate_policies(
    company_ids: pd.Series, n_policies: int = N_POLICIES, seed: int = RANDOM_SEED
) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 20)
    base_df = pd.DataFrame(
        {
            "policy_id": pd.Series(np.arange(1, n_policies + 1), dtype="int64")
            .astype(str)
            .str.zfill(7)
            .radd("P"),
            "company_id": rng.choice(company_ids.to_numpy(), size=n_policies),
            "premium_amount": np.round(rng.gamma(shape=3.2, scale=320.0, size=n_policies), 2),
            "start_date": _random_dates(rng, "2025-01-01", "2026-01-31", n_policies),
            "status": rng.choice(POLICY_STATUSES, size=n_policies, p=[0.58, 0.18, 0.14, 0.10]),
        }
    )

    duplicates = base_df.sample(n=150, random_state=seed + 6)
    orphan_rows = base_df.sample(n=100, random_state=seed + 7).copy()
    orphan_rows["policy_id"] = "ORPH_" + orphan_rows["policy_id"].astype(str)
    orphan_rows["company_id"] = "C999999"

    policies_raw = pd.concat([base_df, duplicates, orphan_rows], ignore_index=True)
    null_premium_idx = policies_raw.sample(n=70, random_state=seed + 8).index
    policies_raw.loc[null_premium_idx, "premium_amount"] = None
    negative_premium_idx = policies_raw.sample(n=50, random_state=seed + 9).index
    policies_raw.loc[negative_premium_idx, "premium_amount"] = -100.0
    bad_status_idx = policies_raw.sample(n=100, random_state=seed + 10).index
    policies_raw.loc[bad_status_idx, "status"] = "in_review"
    null_date_idx = policies_raw.sample(n=40, random_state=seed + 11).index
    policies_raw.loc[null_date_idx, "start_date"] = None
    return policies_raw


def run() -> None:
    paths = ensure_base_directories()
    raw_paths = bronze_files(paths)

    companies_raw = generate_companies()
    reviews_raw = generate_reviews(companies_raw["company_id"])
    policies_raw = generate_policies(companies_raw["company_id"])

    companies_raw.to_json(raw_paths["companies"], orient="records", lines=True, date_format="iso")
    reviews_raw.to_json(raw_paths["reviews"], orient="records", lines=True, date_format="iso")
    policies_raw.to_csv(raw_paths["policies"], index=False)

    log(f"Bronze companies written: {raw_paths['companies']} ({len(companies_raw):,} rows)")
    log(f"Bronze reviews written:   {raw_paths['reviews']} ({len(reviews_raw):,} rows)")
    log(f"Bronze policies written:  {raw_paths['policies']} ({len(policies_raw):,} rows)")


if __name__ == "__main__":
    run()
