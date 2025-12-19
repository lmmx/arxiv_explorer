# src/arxiv_explorer/data.py
"""Dataset management for ArXiv Explorer."""

import json
from datetime import datetime
from pathlib import Path

import polars as pl

HF_DATASET_URL = "hf://datasets/nick007x/arxiv-papers/train.parquet"

BASE_DIR = Path(__file__).parents[2]
OUTPUT_DIR = BASE_DIR / "output"
CACHE_DIR = OUTPUT_DIR / "cache"
DATA_DIR = OUTPUT_DIR / "data"
SUBJECT_CODES_FILE = CACHE_DIR / "subject_codes.json"

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def get_current_year_month() -> tuple[str, str]:
    """Get current year and month as strings."""
    now = datetime.now()
    return str(now.year), f"{now.month:02d}"


def get_month_file(year: str, month: str) -> Path:
    """Get path to cached month data file."""
    return DATA_DIR / f"arxiv_{year}_{month}.parquet"


def is_month_cached(year: str, month: str) -> bool:
    """Check if month data is cached locally."""
    return get_month_file(year, month).exists()


def get_date_pattern(year: str, month: str) -> str:
    """Build pattern for filtering submission_date like '18 Feb 2009'."""
    month_name = MONTH_NAMES[int(month) - 1]
    return f"{month_name} {year}"


def download_month(year: str, month: str, force: bool = False) -> Path:
    """
    Download a single month of data from the remote dataset.
    
    Uses Polars lazy evaluation to filter remotely and only download
    the rows we need (~5-20k papers per month vs 2.5M total).
    """
    cache_file = get_month_file(year, month)
    
    current_year, current_month = get_current_year_month()
    is_current_month = (year == current_year and month == current_month)
    
    # Re-download current month (might have new papers), skip others if cached
    if cache_file.exists() and not force and not is_current_month:
        count = pl.scan_parquet(cache_file).select(pl.len()).collect().item()
        print(f"Month {year}-{month} already cached: {count} papers")
        return cache_file
    
    pattern = get_date_pattern(year, month)
    print(f"Downloading {year}-{month} (pattern: '{pattern}')...")
    
    # Lazy scan remote, filter, collect only matching rows
    df = (
        pl.scan_parquet(HF_DATASET_URL)
        .filter(pl.col("submission_date").str.contains(pattern))
        .collect()
    )
    
    print(f"Downloaded {len(df)} papers for {year}-{month}")
    
    if len(df) > 0:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        df.write_parquet(cache_file)
        print(f"Saved to {cache_file}")
    
    return cache_file


def load_month(year: str, month: str) -> pl.LazyFrame:
    """Load a month's data, downloading if necessary."""
    cache_file = get_month_file(year, month)
    
    if not cache_file.exists():
        download_month(year, month)
    
    if not cache_file.exists():
        # No data for this month
        return pl.LazyFrame()
    
    return pl.scan_parquet(cache_file)


def load_year(year: str) -> pl.LazyFrame:
    """Load all cached months for a year."""
    current_year, current_month = get_current_year_month()
    
    if year == current_year:
        months = [f"{m:02d}" for m in range(1, int(current_month) + 1)]
    else:
        months = [f"{m:02d}" for m in range(1, 13)]
    
    files = [get_month_file(year, m) for m in months if is_month_cached(year, m)]
    
    if not files:
        return pl.LazyFrame()
    
    return pl.scan_parquet(files)


def estimate_from_remote(year: str, month: str | None = None) -> int:
    """
    Get paper count from remote dataset without downloading full data.
    Only fetches the submission_date column for counting.
    """
    if month:
        pattern = get_date_pattern(year, month)
    else:
        pattern = year
    
    count = (
        pl.scan_parquet(HF_DATASET_URL)
        .filter(pl.col("submission_date").str.contains(pattern))
        .select(pl.len())
        .collect()
        .item()
    )
    
    return count


def get_cached_months(year: str) -> list[str]:
    """Get list of months that are cached for a year."""
    months = []
    for m in range(1, 13):
        month = f"{m:02d}"
        if is_month_cached(year, month):
            months.append(month)
    return months


# Subject codes (these rarely change, so cache indefinitely)
def precompute_subject_codes() -> dict[str, str]:
    """Extract all unique subject codes. Returns {code: full_name}."""
    import re
    
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if SUBJECT_CODES_FILE.exists():
        with open(SUBJECT_CODES_FILE) as f:
            return json.load(f)

    print("Precomputing subject codes (one-time operation)...")
    
    # Only fetch the subjects column
    df = (
        pl.scan_parquet(HF_DATASET_URL)
        .select("subjects")
        .unique()
        .collect()
    )

    subject_map = {}
    for subj_str in df.get_column("subjects").drop_nulls().to_list():
        parts = [s.strip() for s in subj_str.split(";")]
        for part in parts:
            match = re.search(r"^(.+?)\s*\(([a-z-]+\.[A-Z]+)\)$", part)
            if match:
                full_name, code = match.groups()
                subject_map[code] = full_name.strip()

    subject_map = dict(sorted(subject_map.items()))

    with open(SUBJECT_CODES_FILE, "w") as f:
        json.dump(subject_map, f, indent=2)

    print(f"Found {len(subject_map)} subject codes")
    return subject_map


def get_subject_codes() -> dict[str, str]:
    """Get subject codes from cache."""
    if not SUBJECT_CODES_FILE.exists():
        return precompute_subject_codes()
    with open(SUBJECT_CODES_FILE) as f:
        return json.load(f)