from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

from lakehouse.config import ensure_base_directories, get_paths, gold_files
from lakehouse.utils import log, markdown_table, write_parquet


def _register_gold_views(con: duckdb.DuckDBPyConnection, table_paths: dict[str, Path]) -> None:
    for table_name, path in table_paths.items():
        parquet_path = path.as_posix()
        con.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS "
            f"SELECT * FROM read_parquet('{parquet_path}')"
        )


def _run_sql_marts(con: duckdb.DuckDBPyConnection, sql_dir: Path) -> dict[str, pd.DataFrame]:
    outputs: dict[str, pd.DataFrame] = {}
    for sql_file in sorted(sql_dir.glob("*.sql")):
        query_name = sql_file.stem
        outputs[query_name] = con.execute(sql_file.read_text(encoding="utf-8")).df()
    return outputs


def _write_report(results: dict[str, pd.DataFrame], report_path: Path) -> None:
    lines: list[str] = [
        "# Lakehouse KPI Report",
        "",
        f"Generated at: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
    ]

    for name, df in results.items():
        lines.append(f"## {name.replace('_', ' ').title()}")
        lines.append(f"Rows: {len(df)}")
        lines.append("")
        lines.append(markdown_table(df))
        lines.append("")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def run() -> None:
    paths = ensure_base_directories()
    model_paths = gold_files(paths)
    project_paths = get_paths()
    sql_dir = project_paths.sql_marts_dir
    if not sql_dir.exists():
        sql_dir = Path(__file__).resolve().parents[2] / "sql" / "marts"

    con = duckdb.connect(database=":memory:")
    _register_gold_views(con, model_paths)
    marts = _run_sql_marts(con, sql_dir)

    for name, df in marts.items():
        parquet_path = paths.marts_dir / f"{name}.parquet"
        csv_path = paths.marts_dir / f"{name}.csv"
        write_parquet(df, parquet_path)
        df.to_csv(csv_path, index=False)
        log(f"Mart written: {parquet_path} ({len(df):,} rows)")

    report_path = paths.reports_dir / "kpi_summary.md"
    _write_report(marts, report_path)
    log(f"Report written: {report_path}")


if __name__ == "__main__":
    run()
