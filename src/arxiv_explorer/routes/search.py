"""Search routes."""

from fastapi import APIRouter, Query

from ..embed_papers import MODEL_ID
from .state import get_df

router = APIRouter(prefix="/api", tags=["search"])

QUERY_PREFIX = "Represent this sentence for searching relevant passages: "


@router.get("/search")
def search(q: str = Query(..., min_length=1), k: int = Query(200, ge=1, le=1000)):
    """Semantic search."""
    df = get_df()
    if df is None:
        return []

    result = df.fastembed.retrieve(
        query=f"{QUERY_PREFIX}{q}",
        model_name=MODEL_ID,
        embedding_column="embedding",
        k=k,
    )
    return [
        {
            "arxiv_id": r["arxiv_id"],
            "title": r["title"],
            "authors": r["authors"][:3],
            "submission_date": r["submission_date"],
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
