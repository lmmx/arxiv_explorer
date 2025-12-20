# src/arxiv_explorer/data/cache.py
"""Local file caching logic."""

import json
from pathlib import Path

import polars as pl

from .config import DATA_DIR, CACHE_DIR, SUBJECT_CODES_FILE, get_current_year_month
from . import hub


def get_local_path(subject: str, year: str, month: str) -> Path:
    """Get the local cache path for a subject/year/month file."""
    safe_subject = subject.replace(".", "_")
    return DATA_DIR / year / month / f"{safe_subject}.parquet"


def get_month_file(year: str, month: str) -> Path:
    """Get path to combined month data file."""
    return DATA_DIR / f"arxiv_{year}_{month}.parquet"


def is_cached(subject: str, year: str, month: str) -> bool:
    """Check if a subject file is cached locally."""
    return get_local_path(subject, year, month).exists()


def is_subject_month_cached(subject: str, year: str, month: str) -> bool:
    """Check if a subject/month file is cached locally. Alias for is_cached."""
    return is_cached(subject, year, month)


def is_month_cached(year: str, month: str) -> bool:
    """Check if combined month data is cached locally."""
    return get_month_file(year, month).exists()


def get_cached_count(subject: str, year: str, month: str) -> int:
    """Get paper count from cached file, or 0 if not cached."""
    path = get_local_path(subject, year, month)
    if not path.exists():
        return 0
    try:
        return pl.scan_parquet(path).select(pl.len()).collect().item()
    except Exception:
        return 0


def cache_file(subject: str, year: str, month: str, source_path: str) -> Path:
    """Copy a downloaded file to our cache location."""
    dest = get_local_path(subject, year, month)
    dest.parent.mkdir(parents=True, exist_ok=True)
    
    df = pl.read_parquet(source_path)
    df.write_parquet(dest)
    
    return dest


def download_and_cache(subject: str, year: str, month: str, force: bool = False) -> Path | None:
    """Download from HuggingFace and cache locally."""
    if not force and is_cached(subject, year, month):
        return get_local_path(subject, year, month)
    
    downloaded = hub.download_parquet(subject, year, month)
    if downloaded is None:
        return None
    
    return cache_file(subject, year, month, downloaded)


def download_subject_month(subject: str, year: str, month: str, force: bool = False) -> Path | None:
    """Download a specific subject/month. Alias for download_and_cache."""
    return download_and_cache(subject, year, month, force)


def list_cached_subjects(year: str, month: str) -> list[str]:
    """List subjects that are cached for a given year/month."""
    folder = DATA_DIR / year / month
    if not folder.exists():
        return []
    
    subjects = []
    for f in folder.glob("*.parquet"):
        subject = f.stem.replace("_", ".")
        subjects.append(subject)
    
    return sorted(subjects)


def get_available_months(year: str) -> list[str]:
    """Get months available on HuggingFace for a given year."""
    subjects = list(get_subject_codes().keys())
    if not subjects:
        return []
    
    # Check the first subject to see what months are available
    first_subject = subjects[0]
    return hub.list_months_for_subject_year(first_subject, year)


def download_month(year: str, month: str, force: bool = False) -> Path:
    """Download a month of data from the partitioned HuggingFace dataset."""
    cache_file = get_month_file(year, month)
    
    current_year, current_month = get_current_year_month()
    is_current_month = (year == current_year and month == current_month)
    
    if cache_file.exists() and not force and not is_current_month:
        count = pl.scan_parquet(cache_file).select(pl.len()).collect().item()
        print(f"Month {year}-{month} already cached: {count} papers")
        return cache_file
    
    print(f"Downloading {year}-{month} from HuggingFace...")
    
    subjects = list(get_subject_codes().keys())
    all_dfs = []
    
    for subject in subjects:
        downloaded = hub.download_parquet(subject, year, month)
        if downloaded:
            try:
                df = pl.read_parquet(downloaded)
                all_dfs.append(df)
                print(f"  {subject}: {len(df)} papers")
            except Exception:
                pass
    
    if all_dfs:
        combined = pl.concat(all_dfs).unique(subset=["arxiv_id"])
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        combined.write_parquet(cache_file)
        print(f"Saved {len(combined)} papers to {cache_file}")
    else:
        print(f"No data found for {year}-{month}")
    
    return cache_file


def load_month(year: str, month: str) -> pl.LazyFrame:
    """Load a month's data, downloading if necessary."""
    cache_file = get_month_file(year, month)
    
    if not cache_file.exists():
        download_month(year, month)
    
    if not cache_file.exists():
        return pl.LazyFrame()
    
    return pl.scan_parquet(cache_file)


def get_subject_codes() -> dict[str, str]:
    """Get subject codes, fetching from Hub if not cached."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    if SUBJECT_CODES_FILE.exists():
        with open(SUBJECT_CODES_FILE) as f:
            return json.load(f)
    
    print("Fetching subject codes from HuggingFace...")
    subjects = hub.list_subjects()
    
    subject_map = {code: code for code in subjects}
    
    with open(SUBJECT_CODES_FILE, "w") as f:
        json.dump(subject_map, f, indent=2)
    
    print(f"Found {len(subject_map)} subject codes")
    return subject_map


def precompute_subject_codes() -> dict[str, str]:
    """Alias for get_subject_codes for backward compatibility."""
    return get_subject_codes()