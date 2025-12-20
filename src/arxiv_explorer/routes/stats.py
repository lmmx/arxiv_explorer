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
    return {"total_papers": len(df), "top_subjects": counts.to_dicts()}