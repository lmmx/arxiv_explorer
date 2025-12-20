# src/arxiv_explorer/data/loader.py
"""Polars data loading utilities."""

import polars as pl

from .config import DATA_DIR


def get_local_path(subject: str, year: str, month: str):
    """Get the local cache path for a subject/year/month file."""
    safe_subject = subject.replace(".", "_")
    return DATA_DIR / year / month / f"{safe_subject}.parquet"


def is_cached(subject: str, year: str, month: str) -> bool:
    """Check if a file is cached locally."""
    return get_local_path(subject, year, month).exists()


def load_subject_month(subject: str, year: str, month: str) -> pl.LazyFrame:
    """Load a specific subject/year/month as LazyFrame."""
    path = get_local_path(subject, year, month)
    if not path.exists():
        return pl.LazyFrame()
    return pl.scan_parquet(path)


def load_subjects_month(subjects: list[str], year: str, month: str) -> pl.LazyFrame:
    """Load multiple subjects for a month."""
    paths = [
        get_local_path(s, year, month)
        for s in subjects
        if is_cached(s, year, month)
    ]
    
    if not paths:
        return pl.LazyFrame()
    
    return pl.scan_parquet(paths)


def load_subject_year(subject: str, year: str, months: list[str]) -> pl.LazyFrame:
    """Load a subject across multiple months."""
    paths = [
        get_local_path(subject, year, m)
        for m in months
        if is_cached(subject, year, m)
    ]
    
    if not paths:
        return pl.LazyFrame()
    
    return pl.scan_parquet(paths)