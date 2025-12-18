# src/arxiv_explorer/embed_papers.py
"""
Embed ArXiv papers with batch processing and progress tracking.
"""

from pathlib import Path
from typing import Callable

import numpy as np
import polars as pl
from polars_fastembed import register_model
from umap import UMAP

MODEL_ID = "SnowflakeArcticEmbedXS"
HF_DATASET = "nick007x/arxiv-papers"
BASE_DIR = Path(__file__).parents[2]
OUTPUT_DIR = BASE_DIR / "output"
CACHE_DIR = OUTPUT_DIR / "cache"

BATCH_SIZE = 100  # Embed 100 papers at a time


def get_category_file(category: str) -> Path:
    """Get the parquet file path for a category."""
    return CACHE_DIR / f"{category}.parquet"


def estimate_papers_count(categories: list[str], year_filter: str = "2025") -> dict:
    """Estimate how many papers will be embedded for given categories."""
    print(f"Loading dataset to estimate counts...")

    # Load just the metadata columns
    df = pl.read_parquet(
        f"hf://datasets/{HF_DATASET}/train.parquet",
        columns=["primary_subject", "submission_date"],
    )

    # Filter by year
    df = df.filter(pl.col("submission_date").str.contains(year_filter))

    # Count by category
    counts = {}
    for cat in categories:
        count = len(df.filter(pl.col("primary_subject").str.starts_with(cat)))
        counts[cat] = count

    return counts


def embed_category(
    category: str,
    year_filter: str = "2025",
    progress_callback: Callable[[int, int, str], None] = None,
) -> pl.DataFrame:
    """
    Embed papers from a specific category.
    Returns a DataFrame with embeddings.
    """
    cache_file = get_category_file(category)

    # Check if already embedded
    if cache_file.exists():
        print(f"Loading cached embeddings for {category}...")
        if progress_callback:
            progress_callback(1, 1, f"Loading cached {category}")
        return pl.read_parquet(cache_file)

    print(f"Loading papers for {category}...")
    if progress_callback:
        progress_callback(0, 1, f"Loading {category} papers")

    # Load papers for this category
    df = pl.read_parquet(f"hf://datasets/{HF_DATASET}/train.parquet")

    # Filter by year and category
    df = df.filter(
        pl.col("submission_date").str.contains(year_filter)
        & pl.col("primary_subject").str.starts_with(category)
    )

    if len(df) == 0:
        print(f"No papers found for {category} in {year_filter}")
        return pl.DataFrame()

    print(f"Found {len(df)} papers for {category}")

    # Create text for embedding
    df = df.with_columns(
        [
            (pl.col("title") + " " + pl.col("abstract"))
            .str.slice(0, 512)
            .alias("text_to_embed")
        ]
    )

    # Keep only needed columns
    df = df.select(
        [
            "arxiv_id",
            "title",
            "authors",
            "submission_date",
            "primary_subject",
            "abstract",
            "text_to_embed",
        ]
    )

    # Register model
    register_model(
        MODEL_ID, providers=["CUDAExecutionProvider", "CPUExecutionProvider"]
    )

    # Embed in batches
    total = len(df)
    embeddings_list = []

    for i in range(0, total, BATCH_SIZE):
        batch_end = min(i + BATCH_SIZE, total)
        batch = df[i:batch_end]

        if progress_callback:
            progress_callback(i, total, f"Embedding {category} ({i}/{total})")

        batch_emb = batch.fastembed.embed(
            columns="text_to_embed",
            model_name=MODEL_ID,
            output_column="embedding",
        )

        embeddings_list.append(batch_emb)

    # Concatenate all batches
    df_emb = pl.concat(embeddings_list)

    # Cache the result
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df_emb.write_parquet(cache_file)

    if progress_callback:
        progress_callback(total, total, f"Completed {category}")

    return df_emb.drop("text_to_embed")


def combine_and_visualize(
    categories: list[str],
    progress_callback: Callable[[int, int, str], None] = None,
) -> pl.DataFrame:
    """
    Combine embeddings from multiple categories and create UMAP visualization.
    """
    dfs = []

    for cat in categories:
        cache_file = get_category_file(cat)
        if cache_file.exists():
            df = pl.read_parquet(cache_file)
            dfs.append(df)

    if not dfs:
        raise ValueError("No embedded categories found")

    df_all = pl.concat(dfs)

    if progress_callback:
        progress_callback(0, 1, "Running UMAP...")

    # Run UMAP
    embeddings = np.array(df_all["embedding"].to_list(), dtype=np.float32)
    umap = UMAP(n_components=2, n_neighbors=15, min_dist=0.1, random_state=42)
    coords_2d = umap.fit_transform(embeddings)

    df_final = df_all.with_columns(
        [
            pl.Series("x", coords_2d[:, 0].tolist()),
            pl.Series("y", coords_2d[:, 1].tolist()),
        ]
    )

    if progress_callback:
        progress_callback(1, 1, "UMAP complete")

    return df_final


def run():
    """CLI entry point - just embed everything."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    CACHE_DIR.mkdir(exist_ok=True)

    # Default categories
    categories = ["cs", "physics", "math", "astro-ph"]

    print(f"Embedding categories: {', '.join(categories)}")

    for cat in categories:
        embed_category(cat)

    print("\nCombining and visualizing...")
    df_final = combine_and_visualize(categories)

    parquet_path = OUTPUT_DIR / "arxiv_embeddings.parquet"
    df_final.write_parquet(parquet_path)
    print(f"\nSaved: {parquet_path}")
    print(f"Size: {parquet_path.stat().st_size / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    run()
