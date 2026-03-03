from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

RANDOM_SEED = 20260303
N_COMPANIES = 2_000
N_REVIEWS = 50_000
N_POLICIES = 10_000
PARQUET_COMPRESSION = "zstd"


@dataclass(frozen=True)
class Paths:
    project_root: Path
    data_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    marts_dir: Path
    reports_dir: Path
    sql_marts_dir: Path


def get_project_root() -> Path:
    env_root = os.getenv("LAKEHOUSE_DEMO_ROOT")
    if env_root:
        return Path(env_root).resolve()
    return Path(__file__).resolve().parents[2]


def get_paths() -> Paths:
    project_root = get_project_root()
    data_dir = project_root / "data"
    return Paths(
        project_root=project_root,
        data_dir=data_dir,
        bronze_dir=data_dir / "bronze",
        silver_dir=data_dir / "silver",
        gold_dir=data_dir / "gold",
        marts_dir=data_dir / "marts",
        reports_dir=project_root / "reports",
        sql_marts_dir=project_root / "sql" / "marts",
    )


def ensure_base_directories(paths: Paths | None = None) -> Paths:
    active_paths = paths or get_paths()
    for folder in [
        active_paths.data_dir,
        active_paths.bronze_dir,
        active_paths.silver_dir,
        active_paths.gold_dir,
        active_paths.marts_dir,
        active_paths.reports_dir,
    ]:
        folder.mkdir(parents=True, exist_ok=True)
    return active_paths


def bronze_files(paths: Paths | None = None) -> dict[str, Path]:
    active_paths = paths or get_paths()
    return {
        "companies": active_paths.bronze_dir / "companies.json",
        "reviews": active_paths.bronze_dir / "reviews.json",
        "policies": active_paths.bronze_dir / "policies_2026_01.csv",
    }


def silver_files(paths: Paths | None = None) -> dict[str, Path]:
    active_paths = paths or get_paths()
    return {
        "companies": active_paths.silver_dir / "companies.parquet",
        "reviews": active_paths.silver_dir / "reviews.parquet",
        "policies": active_paths.silver_dir / "policies.parquet",
    }


def gold_files(paths: Paths | None = None) -> dict[str, Path]:
    active_paths = paths or get_paths()
    return {
        "dim_date": active_paths.gold_dir / "dim_date.parquet",
        "dim_company": active_paths.gold_dir / "dim_company.parquet",
        "dim_channel": active_paths.gold_dir / "dim_channel.parquet",
        "dim_policy_status": active_paths.gold_dir / "dim_policy_status.parquet",
        "fact_review": active_paths.gold_dir / "fact_review.parquet",
        "fact_policy_premium": active_paths.gold_dir / "fact_policy_premium.parquet",
    }
