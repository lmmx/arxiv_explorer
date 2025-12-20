# src/arxiv_explorer/routes/search.py
"""Search routes."""

import polars as pl
from fastapi import APIRouter, Query

from ..embed_papers import MODEL_ID
from .state import get_df

router = APIRouter(prefix="/api", tags=["search"])

QUERY_PREFIX = "Represent this sentence for searching relevant passages: "


@router.get("/search")
def search(
    q: str = Query(..., min_length=1),
    k: int = Query(200, ge=1, le=1000),
    year_months: str = Query(None, description="Comma-separated year-months like 2024-01,2024-02"),
    categories: str = Query(None, description="Comma-separated categories like cs.AI,cs.LG"),
):
    """Semantic search with optional filters."""
    df = get_df()
    if df is None:
        return []

    # Apply filters before search
    if year_months:
        ym_list = [ym.strip() for ym in year_months.split(",") if ym.strip()]
        if ym_list:
            df = df.filter(pl.col("year_month").is_in(ym_list))

    if categories:
        cat_list = [c.strip() for c in categories.split(",") if c.strip()]
        if cat_list:
            df = df.filter(pl.col("primary_subject").is_in(cat_list))

    if len(df) == 0:
        return []

    result = df.fastembed.retrieve(
        query=f"{QUERY_PREFIX}{q}",
        model_name=MODEL_ID,
        embedding_column="embedding",
        k=min(k, len(df)),  # Can't retrieve more than we have
    )
    return [
        {
            "arxiv_id": r["arxiv_id"],
            "title": r["title"],
            "authors": r["authors"][:3],
            "submission_date": r["submission_date"],
            "year_month": r["year_month"] if "year_month" in r else None,
            "primary_subject": r["primary_subject"],
            "abstract": r["abstract"][:500] + "..."
            if len(r["abstract"]) > 500
            else r["abstract"],
            "score": round(r["similarity"], 4),
            "x": round(r["x"], 4),
            "y": round(r["y"], 4),
        }
        for r in result.iter_rows(named=True)
    ]