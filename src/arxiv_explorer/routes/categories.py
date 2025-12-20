# src/arxiv_explorer/routes/categories.py
"""Category routes."""

import polars as pl
from fastapi import APIRouter, Query

from ..data import get_current_year_month, get_subject_codes, is_subject_month_cached
from ..embed_papers import (
    get_category_file,
    get_embedded_count,
    is_category_month_embedded,
)

router = APIRouter(prefix="/api", tags=["categories"])


@router.get("/categories")
def get_categories(
    years: str = Query(None, description="Comma-separated years to check"),
    months: str = Query(None, description="Comma-separated year-month pairs like 2024-01,2024-02,2025-01"),
):
    """Get subject codes with embedding status for specified year/months."""
    codes = get_subject_codes()
    status = {}

    current_year, current_month = get_current_year_month()

    # Parse year-month pairs
    if months:
        # Format: "2024-01,2024-02,2025-01"
        year_months = []
        for ym in months.split(","):
            ym = ym.strip()
            if "-" in ym:
                parts = ym.split("-")
                if len(parts) == 2:
                    year_months.append((parts[0], parts[1]))
    else:
        # Default to current year's months up to current month
        year_months = [(current_year, f"{m:02d}") for m in range(1, int(current_month) + 1)]

    for code in codes:
        total_count = 0
        embedded_months = 0
        downloaded_months = 0
        embedded_month_list = []
        downloaded_month_list = []

        for year, month in year_months:
            if is_subject_month_cached(code, year, month):
                downloaded_months += 1
                downloaded_month_list.append(f"{year}-{month}")

            if is_category_month_embedded(code, year, month):
                count = get_embedded_count(code, year, month)
                total_count += count
                embedded_months += 1
                embedded_month_list.append(f"{year}-{month}")

        status[code] = {
            "embedded": embedded_months > 0,
            "downloaded": downloaded_months > 0,
            "count": total_count,
            "months_embedded": embedded_months,
            "months_downloaded": downloaded_months,
            "embedded_month_list": embedded_month_list,
            "downloaded_month_list": downloaded_month_list,
        }

    return {
        "categories": codes,
        "status": status,
        "year_months": [f"{y}-{m}" for y, m in year_months],
    }