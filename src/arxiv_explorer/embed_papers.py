# src/arxiv_explorer/embed_papers.py
"""Embedding logic for ArXiv papers."""

import re
from pathlib import Path

import numpy as np
import polars as pl
from polars_fastembed import register_model
from umap import UMAP

from .data import (
    get_current_year_month,
    is_month_cached,
    download_month,
    load_month,
    get_subject_codes,
    OUTPUT_DIR,
)

MODEL_ID = "SnowflakeArcticEmbedXS"
EMBEDDINGS_DIR = OUTPUT_DIR / "embeddings"


def get_category_file(category: str, year: str, month: str) -> Path:
    """Get path for category embeddings, partitioned by year/month."""
    safe_name = category.replace(".", "_")
    return EMBEDDINGS_DIR / f"year={year}" / f"month={month}" / f"{safe_name}.parquet"


def extract_subject_codes(subjects_str: str) -> list[str]:
    """Extract codes like 'cs.AI' from subjects string."""
    if not subjects_str:
        return []
    return re.findall(r"\(([a-z-]+\.[A-Z]+)\)", subjects_str)


def embed_category_month(category: str, year: str, month: str) -> int:
    """Embed papers for a single category/month. Returns count."""
    cache_file = get_category_file(category, year, month)
    
    # Check if already embedded
    current_year, current_month = get_current_year_month()
    is_current = (year == current_year and month == current_month)
    
    if cache_file.exists() and not is_current:
        count = len(pl.read_parquet(cache_file))
        print(f"[{category}] {year}-{month} cached: {count} papers")
        return count
    
    # Ensure month data is downloaded
    if not is_month_cached(year, month):
        download_month(year, month)
    
    if not is_month_cached(year, month):
        print(f"[{category}] {year}-{month}: no data")
        return 0
    
    # Load and filter by category
    lf = load_month(year, month)
    lf = lf.with_columns(
        pl.col("subjects")
        .map_elements(extract_subject_codes, return_dtype=pl.List(pl.Utf8))
        .alias("subject_codes")
    )
    lf = lf.filter(pl.col("subject_codes").list.contains(category))
    
    df = lf.collect()
    
    if len(df) == 0:
        print(f"[{category}] {year}-{month}: 0 papers")
        return 0

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
    
    register_model(MODEL_ID, providers=["CUDAExecutionProvider", "CPUExecutionProvider"])
    
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


def embed_category(category: str, year: str = "2025", month: str | None = None) -> int:
    """Embed papers for a category. Returns total count."""
    if month:
        return embed_category_month(category, year, month)
    
    current_year, current_month = get_current_year_month()
    if year == current_year:
        months = [f"{m:02d}" for m in range(1, int(current_month) + 1)]
    else:
        months = [f"{m:02d}" for m in range(1, 13)]
    
    total = 0
    for m in months:
        total += embed_category_month(category, year, m)
    
    return total


def get_all_category_files(categories: list[str], year: str = "2025") -> list[Path]:
    """Get all embedding files for categories across all months."""
    current_year, current_month = get_current_year_month()
    if year == current_year:
        months = [f"{m:02d}" for m in range(1, int(current_month) + 1)]
    else:
        months = [f"{m:02d}" for m in range(1, 13)]
    
    files = []
    for cat in categories:
        for month in months:
            f = get_category_file(cat, year, month)
            if f.exists():
                files.append(f)
    return files


def combine_with_umap(categories: list[str], year: str = "2025") -> pl.DataFrame:
    """Combine category embeddings and add UMAP coordinates."""
    files = get_all_category_files(categories, year)
    
    if not files:
        raise ValueError("No embedded categories found")
    
    dfs = [pl.read_parquet(f) for f in files]
    df = pl.concat(dfs)
    
    # Deduplicate by arxiv_id
    df = df.unique(subset=["arxiv_id"])
    
    embeddings = np.array(df["embedding"].to_list(), dtype=np.float32)
    coords = UMAP(
        n_components=2, n_neighbors=15, min_dist=0.1, random_state=42
    ).fit_transform(embeddings)

    return df.with_columns(
        pl.Series("x", coords[:, 0]),
        pl.Series("y", coords[:, 1]),
    )


def run():
    """CLI entry point."""
    from .data import precompute_subject_codes, download_month
    
    OUTPUT_DIR.mkdir(exist_ok=True)
    precompute_subject_codes()
    
    # Download current month
    year, month = get_current_year_month()
    download_month(year, month)
    
    categories = ["cs.AI", "cs.LG", "cs.CL"]
    
    for cat in categories:
        embed_category(cat, year)
    
    print("Running UMAP...")
    df = combine_with_umap(categories, year)
    path = OUTPUT_DIR / "arxiv_embeddings.parquet"
    df.write_parquet(path)
    print(f"Saved {len(df)} papers to {path}")