# src/arxiv_explorer/routes/papers.py
"""Papers routes."""

import polars as pl
from fastapi import APIRouter, Query

from .state import get_df

router = APIRouter(prefix="/api", tags=["papers"])


@router.get("/papers")
def get_papers(
    year_months: str = Query(None, description="Comma-separated year-months like 2024-01,2024-02"),
    categories: str = Query(None, description="Comma-separated categories like cs.AI,cs.LG"),
):
    """Return all papers with UMAP coordinates, optionally filtered."""
    df = get_df()
    if df is None:
        return []

    print(f"[papers] Total papers: {len(df)}, columns: {df.columns}")
    
    # Check if year_month column exists
    has_year_month = "year_month" in df.columns
    print(f"[papers] has year_month column: {has_year_month}")
    
    if has_year_month:
        sample_ym = df.select("year_month").head(5).to_series().to_list()
        print(f"[papers] Sample year_months: {sample_ym}")

    # Apply filters if provided
    if year_months and has_year_month:
        ym_list = [ym.strip() for ym in year_months.split(",") if ym.strip()]
        if ym_list:
            print(f"[papers] Filtering by year_months: {ym_list[:5]}...")
            df = df.filter(pl.col("year_month").is_in(ym_list))
            print(f"[papers] After year_month filter: {len(df)} papers")

    if categories:
        cat_list = [c.strip() for c in categories.split(",") if c.strip()]
        if cat_list:
            print(f"[papers] Filtering by categories: {cat_list[:5]}...")
            df = df.filter(pl.col("primary_subject").is_in(cat_list))
            print(f"[papers] After category filter: {len(df)} papers")

    return [
        {
            "arxiv_id": r["arxiv_id"],
            "title": r["title"][:100] + "..." if len(r["title"]) > 100 else r["title"],
            "primary_subject": r["primary_subject"],
            "submission_date": r["submission_date"] if "submission_date" in r else None,
            "year_month": r["year_month"] if has_year_month else None,
            "x": round(r["x"], 4),
            "y": round(r["y"], 4),
        }
        for r in df.iter_rows(named=True)
    ]