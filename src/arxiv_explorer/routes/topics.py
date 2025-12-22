# src/arxiv_explorer/routes/topics.py
"""Topic modeling routes using S³ (Semantic Signal Separation)."""

import hashlib
import json
from pathlib import Path

import polars as pl
from fastapi import APIRouter
from pydantic import BaseModel

from ..data import OUTPUT_DIR
from ..embed_papers import MODEL_ID
from .state import get_df

router = APIRouter(prefix="/api", tags=["topics"])

TOPICS_CACHE_DIR = OUTPUT_DIR / "topics_cache"


class TopicRequest(BaseModel):
    n_components: int = 10
    year_months: list[str] | None = None
    categories: list[str] | None = None


def get_cache_key(
    n_components: int, year_months: list[str] | None, categories: list[str] | None
) -> str:
    """Generate cache key for topic results."""
    key_data = {
        "n": n_components,
        "ym": sorted(year_months) if year_months else None,
        "cat": sorted(categories) if categories else None,
    }
    return hashlib.md5(json.dumps(key_data, sort_keys=True).encode()).hexdigest()[:12]


def get_cache_path(cache_key: str) -> Path:
    return TOPICS_CACHE_DIR / f"topics_{cache_key}.json"


@router.post("/topics/extract")
async def extract_topics(request: TopicRequest):
    """
    Extract topics from the current dataset using S³.

    Returns topic descriptions and document-topic assignments.
    """
    print(f"[topics] Requested n_components: {request.n_components}")

    df = get_df()
    if df is None:
        return {"error": "No embeddings loaded"}

    # Apply filters
    if request.year_months:
        df = df.filter(pl.col("year_month").is_in(request.year_months))
    if request.categories:
        df = df.filter(pl.col("primary_subject").is_in(request.categories))

    print(f"[topics] Papers after filter: {len(df)}")

    if len(df) < request.n_components * 2:
        return {
            "error": f"Not enough papers ({len(df)}) for {request.n_components} topics. Need at least {request.n_components * 2}."
        }

    # Check cache
    cache_key = get_cache_key(
        request.n_components, request.year_months, request.categories
    )
    cache_path = get_cache_path(cache_key)

    if cache_path.exists():
        with open(cache_path) as f:
            cached = json.load(f)
        # Verify paper count matches (invalidate if dataset changed)
        if cached.get("paper_count") == len(df):
            print(
                f"[topics] Returning cached result with {len(cached['topics'])} topics"
            )
            return cached
        else:
            print(f"[topics] Cache invalidated: paper count mismatch")

    # Prepare text column for vocabulary extraction only
    df_with_text = df.with_columns(
        (pl.col("title") + " " + pl.col("abstract")).str.slice(0, 512).alias("text")
    )

    # Get document-topic weights from existing embeddings
    result_df = df_with_text.fastembed.s3_topics(
        embedding_column="embedding",
        n_components=request.n_components,
    )

    # Get topic descriptions (top terms per topic)
    # This needs embeddings + text for vocabulary
    topic_terms = df_with_text.fastembed.extract_topics(
        embedding_column="embedding",
        text_column="text",
        n_components=request.n_components,
        model_name=MODEL_ID,  # Only used for embedding vocabulary words
        top_n=10,
    )

    # Build response
    topics = []
    for i, terms in enumerate(topic_terms):
        # Count documents with this as dominant topic
        doc_count = len(result_df.filter(pl.col("dominant_topic") == i))
        topics.append(
            {
                "id": i,
                "terms": [{"term": t[0], "weight": round(t[1], 4)} for t in terms],
                "doc_count": doc_count,
            }
        )

    # Build document assignments (arxiv_id -> topic weights)
    assignments = {}
    for row in result_df.select(
        "arxiv_id", "topic_weights", "dominant_topic"
    ).iter_rows(named=True):
        assignments[row["arxiv_id"]] = {
            "weights": [round(w, 4) for w in row["topic_weights"]],
            "dominant": row["dominant_topic"],
        }

    response = {
        "topics": topics,
        "assignments": assignments,
        "n_components": request.n_components,
        "paper_count": len(df),
        "cache_key": cache_key,
    }

    # Cache the result
    TOPICS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(response, f)

    return response


@router.get("/topics/status")
async def topics_status():
    """Check if topic modeling is available for current dataset."""
    df = get_df()
    if df is None:
        return {"available": False, "reason": "No embeddings loaded"}

    return {
        "available": True,
        "paper_count": len(df),
        "suggested_topics": min(max(3, len(df) // 100), 15),  # Heuristic
    }
