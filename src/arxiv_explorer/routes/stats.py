# src/arxiv_explorer/routes/stats.py
"""Stats routes."""

from fastapi import APIRouter

from .state import get_df

router = APIRouter(prefix="/api", tags=["stats"])


@router.get("/stats")
def get_stats():
    """Dataset statistics."""
    df = get_df()
    if df is None:
        return {"error": "No embeddings loaded"}

    counts = df.group_by("primary_subject").len().sort("len", descending=True).head(10)
    
    # Add year_month stats
    year_month_counts = {}
    if "year_month" in df.columns:
        ym_counts = df.group_by("year_month").len().sort("year_month")
        year_month_counts = {r["year_month"]: r["len"] for r in ym_counts.iter_rows(named=True) if r["year_month"]}
    
    return {
        "total_papers": len(df),
        "top_subjects": counts.to_dicts(),
        "columns": df.columns,
        "year_month_counts": year_month_counts,
    }