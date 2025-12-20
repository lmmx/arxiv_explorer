# src/arxiv_explorer/data/config.py
"""Configuration constants for data management."""

from datetime import datetime
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parents[3]
OUTPUT_DIR = BASE_DIR / "output"
CACHE_DIR = OUTPUT_DIR / "cache"
DATA_DIR = OUTPUT_DIR / "data"
EMBEDDINGS_DIR = OUTPUT_DIR / "embeddings"

# Files
SUBJECT_CODES_FILE = CACHE_DIR / "subject_codes.json"

# HuggingFace
DATASET_REPO_ID = "permutans/arxiv-papers-by-subject"

# Month name mapping
MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def get_current_year_month() -> tuple[str, str]:
    """Get current year and month as strings."""
    now = datetime.now()
    return str(now.year), f"{now.month:02d}"