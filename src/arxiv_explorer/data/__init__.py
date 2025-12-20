# src/arxiv_explorer/data/__init__.py
"""Data management package."""

from .config import (
    BASE_DIR,
    OUTPUT_DIR,
    CACHE_DIR,
    DATA_DIR,
    EMBEDDINGS_DIR,
    DATASET_REPO_ID,
    MONTH_NAMES,
    get_current_year_month,
)
from .hub import (
    list_subjects,
    list_years,
    list_years_for_subject,
    list_months_for_subject_year,
    get_file_info,
    download_parquet,
)
from .cache import (
    get_local_path,
    is_cached,
    is_subject_month_cached,
    is_month_cached,
    get_cached_count,
    download_and_cache,
    download_subject_month,
    list_cached_subjects,
    list_cached_years,
    list_cached_months,
    get_cache_summary,
    get_available_months,
    get_available_years,
    get_subject_codes,
    precompute_subject_codes,
    download_month,
    load_month,
)
from .loader import (
    load_subject_month,
    load_subjects_month,
    load_subject_year,
)
from .estimator import (
    estimate_from_hub,
    get_count,
    get_counts_for_selection,
    estimate_embedding_time,
)

__all__ = [
    # Config
    "BASE_DIR",
    "OUTPUT_DIR",
    "CACHE_DIR",
    "DATA_DIR",
    "EMBEDDINGS_DIR",
    "DATASET_REPO_ID",
    "MONTH_NAMES",
    "get_current_year_month",
    # Hub
    "list_subjects",
    "list_years",
    "list_years_for_subject",
    "list_months_for_subject_year",
    "get_file_info",
    "download_parquet",
    # Cache
    "get_local_path",
    "is_cached",
    "is_subject_month_cached",
    "is_month_cached",
    "get_cached_count",
    "download_and_cache",
    "download_subject_month",
    "list_cached_subjects",
    "list_cached_years",
    "list_cached_months",
    "get_cache_summary",
    "get_available_months",
    "get_available_years",
    "get_subject_codes",
    "precompute_subject_codes",
    "download_month",
    "load_month",
    # Loader
    "load_subject_month",
    "load_subjects_month",
    "load_subject_year",
    # Estimator
    "estimate_from_hub",
    "get_count",
    "get_counts_for_selection",
    "estimate_embedding_time",
]