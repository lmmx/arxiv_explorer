# src/arxiv_explorer/embed_papers.py
"""Embed ArXiv papers with semantic search."""

import json
import re
from functools import cache
from pathlib import Path

import numpy as np
import polars as pl
from polars_fastembed import register_model
from tqdm import tqdm
from umap import UMAP

MODEL_ID = "SnowflakeArcticEmbedXS"
HF_DATASET = "nick007x/arxiv-papers"
BASE_DIR = Path(__file__).parents[2]
OUTPUT_DIR = BASE_DIR / "output"
CACHE_DIR = OUTPUT_DIR / "cache"
SUBJECT_CODES_FILE = CACHE_DIR / "subject_codes.json"

# Legacy high-level categories (keep for backwards compat)
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
    # Replace . with _ for filenames like cs.AI -> cs_AI.parquet
    safe_name = category.replace(".", "_")
    return CACHE_DIR / f"{safe_name}.parquet"


def load_dataset(columns: tuple[str] | None = None) -> pl.LazyFrame:
    lf = pl.scan_parquet(f"hf://datasets/{HF_DATASET}/train.parquet")
    return lf.select(columns) if columns else lf


def extract_subject_codes(subjects_str: str) -> list[str]:
    """Extract codes like 'cs.AI' from 'Artificial Intelligence (cs.AI); Machine Learning (cs.LG)'"""
    if not subjects_str:
        return []
    return re.findall(r"\(([a-z-]+\.[A-Z]+)\)", subjects_str)


def precompute_subject_codes() -> dict[str, str]:
    """Extract all unique subject codes from the dataset. Returns {code: full_name}."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Return cached if exists
    if SUBJECT_CODES_FILE.exists():
        with open(SUBJECT_CODES_FILE) as f:
            return json.load(f)

    print("Precomputing subject codes from dataset...")
    df = load_dataset(columns=("subjects",))

    subject_map = {}
    for subj_str in df.get_column("subjects").drop_nulls().unique().to_list():
        # Parse "Machine Learning (cs.LG); Computation (stat.CO)" etc
        parts = [s.strip() for s in subj_str.split(";")]
        for part in parts:
            match = re.search(r"^(.+?)\s*\(([a-z-]+\.[A-Z]+)\)$", part)
            if match:
                full_name, code = match.groups()
                subject_map[code] = full_name.strip()

    # Sort by code
    subject_map = dict(sorted(subject_map.items()))

    with open(SUBJECT_CODES_FILE, "w") as f:
        json.dump(subject_map, f, indent=2)

    print(f"Found {len(subject_map)} subject codes")
    return subject_map


def get_subject_codes() -> dict[str, str]:
    """Get precomputed subject codes. Must call precompute_subject_codes() first at startup."""
    if not SUBJECT_CODES_FILE.exists():
        return precompute_subject_codes()
    with open(SUBJECT_CODES_FILE) as f:
        return json.load(f)


def embed_category(category: str, year: str = "2025") -> pl.DataFrame:
    """Embed papers from a category (now supports fine-grained like 'cs.AI'). Returns cached if available."""
    cache_file = get_category_file(category)
    if cache_file.exists():
        return pl.read_parquet(cache_file)

    df = load_dataset()

    # Add subject_codes column as list
    df = df.with_columns(
        pl.col("subjects")
        .map_elements(extract_subject_codes, return_dtype=pl.List(pl.Utf8))
        .alias("subject_codes")
    )

    # Filter by year and category (exact match in list)
    df = df.filter(
        pl.col("submission_date").str.contains(year)
        & pl.col("subject_codes").list.contains(category)
    ).collect()

    if len(df) == 0:
        return pl.DataFrame()

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

    register_model(
        MODEL_ID, providers=["CUDAExecutionProvider", "CPUExecutionProvider"]
    )
    print(f"Embedding {len(df)} papers for {category}")
    for _ in tqdm(range(1), desc=f"Embedding {category}", unit="job"):
        df = df.fastembed.embed(
            columns="text",
            model_name=MODEL_ID,
            output_column="embedding",
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

    # Precompute subject codes first
    precompute_subject_codes()

    categories = ["cs", "physics", "math", "astro-ph"]

    for cat in categories:
        print(f"Embedding {cat}...")
        embed_category(cat)

    print("Running UMAP...")
    df = combine_with_umap(categories)
    path = OUTPUT_DIR / "arxiv_embeddings.parquet"
    df.write_parquet(path)
    print(f"Saved {len(df)} papers to {path}")
