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


def count_valid_embeddings(df: pl.DataFrame) -> int:
    """Count rows with non-null embeddings."""
    if "embedding" not in df.columns:
        return 0
    return df.filter(pl.col("embedding").is_not_null()).height


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

    # Count VALID embeddings (non-null)
    valid_count = count_valid_embeddings(df)
    print(f"[topics] Valid embeddings: {valid_count}")

    # Need strictly more documents than components for ICA
    min_required = request.n_components + 1
    if valid_count < min_required:
        return {
            "error": f"Not enough papers with embeddings ({valid_count}) for {request.n_components} topics. Need at least {min_required}."
        }

    # Auto-adjust n_components if necessary
    max_components = max(2, valid_count - 1)
    actual_n_components = min(request.n_components, max_components)
    
    if actual_n_components != request.n_components:
        print(f"[topics] Adjusted n_components from {request.n_components} to {actual_n_components}")

    # Check cache
    cache_key = get_cache_key(
        actual_n_components, request.year_months, request.categories
    )
    cache_path = get_cache_path(cache_key)

    if cache_path.exists():
        with open(cache_path) as f:
            cached = json.load(f)
        if cached.get("paper_count") == len(df):
            print(f"[topics] Returning cached result with {len(cached['topics'])} topics")
            return cached
        else:
            print(f"[topics] Cache invalidated: paper count mismatch")

    # Filter to only rows with valid embeddings for processing
    df_valid = df.filter(pl.col("embedding").is_not_null())
    
    # Prepare text column for vocabulary extraction
    df_with_text = df_valid.with_columns(
        (pl.col("title") + " " + pl.col("abstract")).str.slice(0, 512).alias("text")
    )

    print(f"[topics] Running S³ on {len(df_with_text)} papers with {actual_n_components} components...")

    try:
        # Get document-topic weights from existing embeddings
        result_df = df_with_text.fastembed.s3_topics(
            embedding_column="embedding",
            n_components=actual_n_components,
        )
        print(f"[topics] S³ fit complete, extracting topic terms...")

        # Get topic descriptions (top terms per topic)
        # This is the slow part - consider caching or limiting vocab
        topic_terms = df_with_text.fastembed.extract_topics(
            embedding_column="embedding",
            text_column="text",
            n_components=actual_n_components,
            model_name=MODEL_ID,
            top_n=10,
        )
        print(f"[topics] Topic term extraction complete")

    except Exception as e:
        print(f"[topics] Error during extraction: {e}")
        return {"error": f"Topic extraction failed: {str(e)}"}

    # Build response
    topics = []
    for i, terms in enumerate(topic_terms):
        doc_count = len(result_df.filter(pl.col("dominant_topic") == i))
        topics.append(
            {
                "id": i,
                "terms": [{"term": t[0], "weight": round(t[1], 4)} for t in terms],
                "doc_count": doc_count,
            }
        )

    # Build document assignments
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
        "n_components": actual_n_components,
        "paper_count": len(df),
        "valid_embedding_count": valid_count,
        "cache_key": cache_key,
    }

    # Cache the result
    TOPICS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(response, f)

    print(f"[topics] Extraction complete: {len(topics)} topics")
    return response


@router.get("/topics/status")
async def topics_status():
    """Check if topic modeling is available for current dataset."""
    df = get_df()
    if df is None:
        return {"available": False, "reason": "No embeddings loaded"}

    valid_count = count_valid_embeddings(df)
    
    return {
        "available": valid_count > 2,
        "paper_count": len(df),
        "valid_embedding_count": valid_count,
        "max_topics": max(2, valid_count - 1),
        "suggested_topics": min(max(3, valid_count // 100), 15),
    }