from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MARTS_DIR = PROJECT_ROOT / "data" / "marts"
REPORT_PATH = PROJECT_ROOT / "reports" / "kpi_summary.md"


def _load_csv(file_name: str) -> pd.DataFrame:
    path = MARTS_DIR / file_name
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


@st.cache_data(show_spinner=False)
def load_marts() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    avg_rating = _load_csv("avg_rating_by_industry.csv")
    premium = _load_csv("premium_sum_by_city_month.csv")
    top_companies = _load_csv("top_companies_by_rating_last_90_days.csv")
    return avg_rating, premium, top_companies


def _prepare_premium_timeseries(premium: pd.DataFrame) -> pd.DataFrame:
    data = premium.copy()
    data["month_start"] = pd.to_datetime(
        data["year"].astype(str) + "-" + data["month"].astype(str).str.zfill(2) + "-01",
        errors="coerce",
    )
    return data.sort_values(["month_start", "city"], kind="mergesort")


def render_dashboard() -> None:
    st.set_page_config(
        page_title="Mini Lakehouse KPI Dashboard",
        page_icon=":bar_chart:",
        layout="wide",
    )
    st.title("Mini Lakehouse KPI Dashboard")
    st.caption("Business consumption view on Gold marts produced by DuckDB SQL.")

    if not MARTS_DIR.exists():
        st.error("Marts folder not found. Run `python -m lakehouse all` first.")
        return

    try:
        avg_rating, premium, top_companies = load_marts()
    except FileNotFoundError:
        st.error(
            "One or more mart files are missing. Run `python -m lakehouse mart` "
            "or `python -m lakehouse all` first."
        )
        return

    with st.sidebar:
        st.header("Filters")
        industries = sorted(avg_rating["industry"].dropna().astype(str).unique().tolist())
        selected_industries = st.multiselect(
            "Industry",
            options=industries,
            default=industries,
        )

        cities = sorted(premium["city"].dropna().astype(str).unique().tolist())
        selected_cities = st.multiselect(
            "City",
            options=cities,
            default=cities[: min(5, len(cities))],
        )

        min_reviews = st.slider(
            "Min reviews (Top companies table)",
            min_value=1,
            max_value=int(top_companies["review_count_90d"].max()),
            value=10,
            step=1,
        )

    rating_view = avg_rating[avg_rating["industry"].isin(selected_industries)].copy()
    premium_view = _prepare_premium_timeseries(premium[premium["city"].isin(selected_cities)])
    top_view = top_companies[top_companies["review_count_90d"] >= min_reviews].copy()

    col1, col2, col3 = st.columns(3)
    col1.metric("Industries", f"{rating_view['industry'].nunique():,}")
    col2.metric("Premium Sum (Filtered)", f"{premium_view['premium_sum'].sum():,.0f}")
    col3.metric("Top Companies (Filtered)", f"{len(top_view):,}")

    left, right = st.columns(2)

    with left:
        st.subheader("Average Rating by Industry")
        chart_data = rating_view.sort_values("avg_rating", ascending=False)
        st.bar_chart(
            chart_data.set_index("industry")["avg_rating"],
            use_container_width=True,
            height=350,
        )
        st.dataframe(chart_data, use_container_width=True, hide_index=True)

    with right:
        st.subheader("Monthly Premium Sum by City")
        line_data = premium_view.pivot_table(
            index="month_start",
            columns="city",
            values="premium_sum",
            aggfunc="sum",
        ).sort_index()
        st.line_chart(line_data, use_container_width=True, height=350)
        st.dataframe(
            premium_view[["city", "year", "month", "premium_sum", "policy_count"]],
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Top Companies by Rating (Last 90 Days)")
    st.dataframe(top_view, use_container_width=True, hide_index=True)
    st.download_button(
        label="Download filtered top companies CSV",
        data=top_view.to_csv(index=False),
        file_name="top_companies_filtered.csv",
        mime="text/csv",
    )

    if REPORT_PATH.exists():
        st.subheader("Pipeline Report Snapshot")
        st.markdown(REPORT_PATH.read_text(encoding="utf-8"))


if __name__ == "__main__":
    render_dashboard()
