# src/arxiv_explorer/embed_papers.py
"""Embed ArXiv papers with semantic search."""

from functools import cache
from pathlib import Path

import numpy as np
import polars as pl
from polars_fastembed import register_model
from umap import UMAP

MODEL_ID = "SnowflakeArcticEmbedXS"
HF_DATASET = "nick007x/arxiv-papers"
BASE_DIR = Path(__file__).parents[2]
OUTPUT_DIR = BASE_DIR / "output"
CACHE_DIR = OUTPUT_DIR / "cache"

ARXIV_CATEGORIES = [
    "cs",
    "physics",
    "math",
    "astro-ph",
    "quant-ph",
    "cond-mat",
    "stat",
    "econ",
    "eess",
    "q-bio",
    "q-fin",
]


def get_category_file(category: str) -> Path:
    return CACHE_DIR / f"{category}.parquet"


@cache
def load_dataset(columns: tuple[str] | None = None) -> pl.DataFrame:
    return pl.read_parquet(f"hf://datasets/{HF_DATASET}/train.parquet", columns=columns)


def embed_category(category: str, year: str = "2025") -> pl.DataFrame:
    """Embed papers from a category. Returns cached if available."""
    cache_file = get_category_file(category)
    if cache_file.exists():
        return pl.read_parquet(cache_file)

    df = load_dataset().filter(
        pl.col("submission_date").str.contains(year)
        & pl.col("primary_subject").str.starts_with(category)
    )

    if len(df) == 0:
        return pl.DataFrame()

    df = df.select(
        "arxiv_id",
        "title",
        "authors",
        "submission_date",
        "primary_subject",
        "abstract",
        (pl.col("title") + " " + pl.col("abstract")).str.slice(0, 512).alias("text"),
    )

    register_model(
        MODEL_ID, providers=["CUDAExecutionProvider", "CPUExecutionProvider"]
    )
    df = df.fastembed.embed(
        columns="text", model_name=MODEL_ID, output_column="embedding"
    )

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(cache_file)
    return df.drop("text")


def combine_with_umap(categories: list[str]) -> pl.DataFrame:
    """Combine category embeddings and add UMAP coordinates."""
    dfs = [
        pl.read_parquet(get_category_file(c))
        for c in categories
        if get_category_file(c).exists()
    ]
    if not dfs:
        raise ValueError("No embedded categories found")

    df = pl.concat(dfs)
    embeddings = np.array(df["embedding"].to_list(), dtype=np.float32)
    coords = UMAP(
        n_components=2, n_neighbors=15, min_dist=0.1, random_state=42
    ).fit_transform(embeddings)

    return df.with_columns(
        pl.Series("x", coords[:, 0]),
        pl.Series("y", coords[:, 1]),
    )


def run():
    """CLI: embed default categories."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    categories = ["cs", "physics", "math", "astro-ph"]

    for cat in categories:
        print(f"Embedding {cat}...")
        embed_category(cat)

    print("Running UMAP...")
    df = combine_with_umap(categories)
    path = OUTPUT_DIR / "arxiv_embeddings.parquet"
    df.write_parquet(path)
    print(f"Saved {len(df)} papers to {path}")
