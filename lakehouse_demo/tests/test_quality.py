from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from lakehouse import gold, ingest, mart, silver


@pytest.fixture(scope="session")
def pipeline_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("lakehouse_demo_run")
    os.environ["LAKEHOUSE_DEMO_ROOT"] = str(root)

    ingest.run()
    silver.run()
    gold.run()
    mart.run()
    return root


def _read_gold(root: Path, table_name: str) -> pd.DataFrame:
    return pd.read_parquet(root / "data" / "gold" / f"{table_name}.parquet")


def test_row_counts_are_reasonable(pipeline_root: Path) -> None:
    dim_company = _read_gold(pipeline_root, "dim_company")
    fact_review = _read_gold(pipeline_root, "fact_review")
    fact_policy = _read_gold(pipeline_root, "fact_policy_premium")

    assert len(dim_company) == 2_000
    assert 49_000 <= len(fact_review) <= 50_250
    assert 9_600 <= len(fact_policy) <= 10_100


def test_surrogate_keys_not_null_in_facts(pipeline_root: Path) -> None:
    fact_review = _read_gold(pipeline_root, "fact_review")
    fact_policy = _read_gold(pipeline_root, "fact_policy_premium")

    assert fact_review[["date_key", "company_key", "channel_key"]].notna().all().all()
    assert fact_policy[["date_key", "company_key", "status_key"]].notna().all().all()


def test_dimension_keys_are_unique(pipeline_root: Path) -> None:
    dim_company = _read_gold(pipeline_root, "dim_company")
    dim_channel = _read_gold(pipeline_root, "dim_channel")
    dim_status = _read_gold(pipeline_root, "dim_policy_status")
    dim_date = _read_gold(pipeline_root, "dim_date")

    assert dim_company["company_key"].is_unique
    assert dim_company["company_id"].is_unique
    assert dim_channel["channel_key"].is_unique
    assert dim_channel["channel_name"].is_unique
    assert dim_status["status_key"].is_unique
    assert dim_status["status"].is_unique
    assert dim_date["date_key"].is_unique


def test_fact_foreign_keys_exist_in_dimensions(pipeline_root: Path) -> None:
    dim_company = _read_gold(pipeline_root, "dim_company")
    dim_date = _read_gold(pipeline_root, "dim_date")
    dim_channel = _read_gold(pipeline_root, "dim_channel")
    dim_status = _read_gold(pipeline_root, "dim_policy_status")
    fact_review = _read_gold(pipeline_root, "fact_review")
    fact_policy = _read_gold(pipeline_root, "fact_policy_premium")

    assert set(fact_review["company_key"]).issubset(set(dim_company["company_key"]))
    assert set(fact_review["date_key"]).issubset(set(dim_date["date_key"]))
    assert set(fact_review["channel_key"]).issubset(set(dim_channel["channel_key"]))

    assert set(fact_policy["company_key"]).issubset(set(dim_company["company_key"]))
    assert set(fact_policy["date_key"]).issubset(set(dim_date["date_key"]))
    assert set(fact_policy["status_key"]).issubset(set(dim_status["status_key"]))


def test_mart_outputs_and_report_exist(pipeline_root: Path) -> None:
    marts_dir = pipeline_root / "data" / "marts"
    reports_dir = pipeline_root / "reports"

    assert (marts_dir / "avg_rating_by_industry.parquet").exists()
    assert (marts_dir / "premium_sum_by_city_month.parquet").exists()
    assert (marts_dir / "top_companies_by_rating_last_90_days.parquet").exists()
    assert (reports_dir / "kpi_summary.md").exists()
