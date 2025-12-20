# src/arxiv_explorer/embed_papers.py
"""Embedding logic for ArXiv papers."""

import hashlib
import json
import re
from pathlib import Path

import numpy as np
import polars as pl
from polars_fastembed import register_model
from umap import UMAP

from .data import (
    OUTPUT_DIR,
    download_subject_month,
    get_current_year_month,
    get_subject_codes,
    is_subject_month_cached,
    load_subject_month,
)

MODEL_ID = "SnowflakeArcticEmbedXS"
EMBEDDINGS_DIR = OUTPUT_DIR / "embeddings"
UMAP_CACHE_DIR = OUTPUT_DIR / "umap_cache"


def get_category_file(category: str, year: str, month: str) -> Path:
    """Get path for category embeddings, partitioned by year/month."""
    safe_name = category.replace(".", "_")
    return EMBEDDINGS_DIR / f"year={year}" / f"month={month}" / f"{safe_name}.parquet"


def extract_subject_codes(subjects_str: str) -> list[str]:
    """Extract codes like 'cs.AI' from subjects string."""
    if not subjects_str:
        return []
    return re.findall(r"\(([a-z-]+\.[A-Z]+)\)", subjects_str)


def get_selection_hash(categories: list[str], year: str, months: list[str]) -> str:
    """Generate a hash for a specific selection of categories/year/months."""
    key = json.dumps(
        {"categories": sorted(categories), "year": year, "months": sorted(months)},
        sort_keys=True,
    )
    return hashlib.md5(key.encode()).hexdigest()[:12]


def get_umap_cache_path(categories: list[str], year: str, months: list[str]) -> Path:
    """Get path to cached UMAP result for a selection."""
    selection_hash = get_selection_hash(categories, year, months)
    return UMAP_CACHE_DIR / f"umap_{selection_hash}.parquet"


def is_umap_cached(categories: list[str], year: str, months: list[str]) -> bool:
    """Check if UMAP result is cached for this selection."""
    cache_path = get_umap_cache_path(categories, year, months)
    if not cache_path.exists():
        return False

    # Verify all source embeddings exist and are older than cache
    cache_mtime = cache_path.stat().st_mtime

    for cat in categories:
        for month in months:
            embed_file = get_category_file(cat, year, month)
            if not embed_file.exists():
                return False
            # If embedding is newer than cache, invalidate
            if embed_file.stat().st_mtime > cache_mtime:
                return False

    return True


def load_umap_cache(
    categories: list[str], year: str, months: list[str]
) -> pl.DataFrame | None:
    """Load cached UMAP result if valid."""
    if not is_umap_cached(categories, year, months):
        return None

    cache_path = get_umap_cache_path(categories, year, months)
    try:
        return pl.read_parquet(cache_path)
    except Exception:
        return None


def save_umap_cache(
    df: pl.DataFrame, categories: list[str], year: str, months: list[str]
) -> None:
    """Save UMAP result to cache."""
    UMAP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = get_umap_cache_path(categories, year, months)
    df.write_parquet(cache_path)


def is_category_month_embedded(category: str, year: str, month: str) -> bool:
    """Check if a category/year/month is already embedded."""
    return get_category_file(category, year, month).exists()


def get_embedded_count(category: str, year: str, month: str) -> int:
    """Get count of embedded papers for a category/year/month."""
    path = get_category_file(category, year, month)
    if not path.exists():
        return 0
    try:
        return len(pl.read_parquet(path))
    except Exception:
        return 0


def embed_category_month(
    category: str, year: str, month: str, force: bool = False
) -> int:
    """Embed papers for a single category/month. Returns count."""
    cache_file = get_category_file(category, year, month)

    # Check if already embedded (unless forcing re-embed)
    current_year, current_month = get_current_year_month()
    is_current = year == current_year and month == current_month

    if cache_file.exists() and not force and not is_current:
        count = len(pl.read_parquet(cache_file))
        print(f"[{category}] {year}-{month} cached: {count} papers")
        return count

    # Ensure subject/month data is downloaded
    if not is_subject_month_cached(category, year, month):
        download_subject_month(category, year, month)

    if not is_subject_month_cached(category, year, month):
        print(f"[{category}] {year}-{month}: no data available")
        return 0

    # Load directly from the subject-specific file
    lf = load_subject_month(category, year, month)
    df = lf.collect()

    if len(df) == 0:
        print(f"[{category}] {year}-{month}: 0 papers")
        return 0

    # Add subject_codes column for compatibility
    if "subjects" in df.columns:
        df = df.with_columns(
            pl.col("subjects")
            .map_elements(extract_subject_codes, return_dtype=pl.List(pl.Utf8))
            .alias("subject_codes")
        )
    else:
        df = df.with_columns(pl.lit([category]).alias("subject_codes"))

    df = df.select(
        "arxiv_id",
        "title",
        "authors",
        "submission_date",
        "primary_subject",
        "subject_codes",
        "abstract",
        (pl.col("title") + " " + pl.col("abstract")).str.slice(0, 512).alias("text"),
    )

    print(f"[{category}] Embedding {len(df)} papers for {year}-{month}...")

    register_model(
        MODEL_ID, providers=["CUDAExecutionProvider", "CPUExecutionProvider"]
    )

    df = df.fastembed.embed(
        columns="text",
        model_name=MODEL_ID,
        output_column="embedding",
    )
    df = df.drop("text")

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(cache_file)

    print(f"[{category}] Saved {len(df)} papers")
    return len(df)


def embed_category(
    category: str, year: str = "2025", months: list[str] | None = None
) -> int:
    """Embed papers for a category across specified months. Returns total count."""
    if months is None:
        current_year, current_month = get_current_year_month()
        if year == current_year:
            months = [f"{m:02d}" for m in range(1, int(current_month) + 1)]
        else:
            months = [f"{m:02d}" for m in range(1, 13)]

    total = 0
    for m in months:
        total += embed_category_month(category, year, m)

    return total


def get_all_category_files(
    categories: list[str], year: str, months: list[str]
) -> list[Path]:
    """Get all embedding files for categories across specified months."""
    files = []
    for cat in categories:
        for month in months:
            f = get_category_file(cat, year, month)
            if f.exists():
                files.append(f)
    return files


def combine_with_umap(
    categories: list[str],
    year: str,
    months: list[str],
    use_cache: bool = True,
) -> pl.DataFrame:
    """Combine category embeddings and add UMAP coordinates."""
    # Check cache first
    if use_cache:
        cached = load_umap_cache(categories, year, months)
        if cached is not None:
            print(f"Using cached UMAP result ({len(cached)} papers)")
            return cached

    files = get_all_category_files(categories, year, months)

    if not files:
        raise ValueError("No embedded categories found")

    dfs = [pl.read_parquet(f) for f in files]
    df = pl.concat(dfs)

    # Deduplicate by arxiv_id
    df = df.unique(subset=["arxiv_id"])

    print(f"Running UMAP on {len(df)} papers...")

    embeddings = np.array(df["embedding"].to_list(), dtype=np.float32)
    coords = UMAP(
        n_components=2, n_neighbors=15, min_dist=0.1, random_state=42
    ).fit_transform(embeddings)

    result = df.with_columns(
        pl.Series("x", coords[:, 0]),
        pl.Series("y", coords[:, 1]),
    )

    # Cache the result
    if use_cache:
        save_umap_cache(result, categories, year, months)

    return result


def get_embedding_status(year: str, months: list[str]) -> dict:
    """Get embedding status for all categories for given year/months."""
    codes = get_subject_codes()
    status = {}

    for code in codes:
        embedded_count = 0
        embedded_months = []

        for month in months:
            if is_category_month_embedded(code, year, month):
                count = get_embedded_count(code, year, month)
                embedded_count += count
                embedded_months.append(month)

        status[code] = {
            "embedded": len(embedded_months) > 0,
            "count": embedded_count,
            "months_embedded": len(embedded_months),
            "embedded_months": embedded_months,
        }

    return status


def run():
    """CLI entry point."""
    from .data import download_subject_month, precompute_subject_codes

    OUTPUT_DIR.mkdir(exist_ok=True)
    precompute_subject_codes()

    # Download current month for a few categories
    year, month = get_current_year_month()
    categories = ["cs.AI", "cs.LG", "cs.CL"]
    months = [month]

    for cat in categories:
        download_subject_month(cat, year, month)
        embed_category(cat, year, months)

    print("Running UMAP...")
    df = combine_with_umap(categories, year, months)
    path = OUTPUT_DIR / "arxiv_embeddings.parquet"
    df.write_parquet(path)
    print(f"Saved {len(df)} papers to {path}")
