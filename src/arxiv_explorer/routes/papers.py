"""Papers routes."""

from fastapi import APIRouter

from .state import get_df

router = APIRouter(prefix="/api", tags=["papers"])


@router.get("/papers")
def get_papers():
    """Return all papers with UMAP coordinates."""
    df = get_df()
    if df is None:
        return []

    return [
        {
            "arxiv_id": r["arxiv_id"],
            "title": r["title"][:100] + "..." if len(r["title"]) > 100 else r["title"],
            "primary_subject": r["primary_subject"],
            "submission_date": r["submission_date"] if "submission_date" in r else None,
            "x": round(r["x"], 4),
            "y": round(r["y"], 4),
        }
        for r in df.iter_rows(named=True)
    ]
