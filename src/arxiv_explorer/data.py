# src/arxiv_explorer/data.py
"""Dataset management for ArXiv Explorer."""

import json
from datetime import datetime
from pathlib import Path

import polars as pl
from huggingface_hub import hf_hub_download

BASE_DIR = Path(__file__).parents[2]
OUTPUT_DIR = BASE_DIR / "output"
CACHE_DIR = OUTPUT_DIR / "cache"
DATA_DIR = OUTPUT_DIR / "data"
SUBJECT_CODES_FILE = CACHE_DIR / "subject_codes.json"
MASTER_PARQUET = DATA_DIR / "arxiv_papers.parquet"

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def download_master_dataset() -> Path:
    """Download the full 1.7GB parquet file from HuggingFace."""
    if MASTER_PARQUET.exists():
        print(f"Master dataset already exists: {MASTER_PARQUET}")
        return MASTER_PARQUET
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    print("Downloading arxiv-papers dataset (1.7GB)...")
    downloaded_path = hf_hub_download(
        repo_id="nick007x/arxiv-papers",
        repo_type="dataset",
        revision="refs/convert/parquet",
        filename="default/train/0000.parquet",
        local_dir=DATA_DIR,
    )
    
    # Move to expected location
    src = Path(downloaded_path)
    src.rename(MASTER_PARQUET)
    
    # Clean up the nested directories hf_hub_download creates
    nested_dir = DATA_DIR / "default"
    if nested_dir.exists():
        import shutil
        shutil.rmtree(nested_dir)
    
    print(f"Downloaded to {MASTER_PARQUET}")
    return MASTER_PARQUET


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


def ensure_master_dataset() -> Path:
    """Ensure master dataset is downloaded."""
    if not MASTER_PARQUET.exists():
        download_master_dataset()
    return MASTER_PARQUET


def download_month(year: str, month: str, force: bool = False) -> Path:
    """
    Extract a single month of data from the master dataset.
    """
    cache_file = get_month_file(year, month)
    
    current_year, current_month = get_current_year_month()
    is_current_month = (year == current_year and month == current_month)
    
    if cache_file.exists() and not force and not is_current_month:
        count = pl.scan_parquet(cache_file).select(pl.len()).collect().item()
        print(f"Month {year}-{month} already cached: {count} papers")
        return cache_file
    
    # Ensure we have the master dataset
    ensure_master_dataset()
    
    pattern = get_date_pattern(year, month)
    print(f"Extracting {year}-{month} (pattern: '{pattern}')...")
    
    # Filter from local master file
    df = (
        pl.scan_parquet(MASTER_PARQUET)
        .filter(pl.col("submission_date").str.contains(pattern))
        .collect()
    )
    
    print(f"Extracted {len(df)} papers for {year}-{month}")
    
    if len(df) > 0:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        df.write_parquet(cache_file)
        print(f"Saved to {cache_file}")
    
    return cache_file


def load_month(year: str, month: str) -> pl.LazyFrame:
    """Load a month's data, extracting from master if necessary."""
    cache_file = get_month_file(year, month)
    
    if not cache_file.exists():
        download_month(year, month)
    
    if not cache_file.exists():
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


def get_cached_months(year: str) -> list[str]:
    """Get list of months that are cached for a year."""
    months = []
    for m in range(1, 13):
        month = f"{m:02d}"
        if is_month_cached(year, month):
            months.append(month)
    return months


def precompute_subject_codes() -> dict[str, str]:
    """Extract all unique subject codes. Returns {code: full_name}."""
    import re
    
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if SUBJECT_CODES_FILE.exists():
        with open(SUBJECT_CODES_FILE) as f:
            return json.load(f)

    print("Precomputing subject codes (one-time operation)...")
    
    # Ensure we have the master dataset
    ensure_master_dataset()
    
    # Only fetch the subjects column from local file
    df = (
        pl.scan_parquet(MASTER_PARQUET)
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