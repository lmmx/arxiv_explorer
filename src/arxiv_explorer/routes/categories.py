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
    year: str = Query(None, description="Year to check status for"),
    months: str = Query(None, description="Comma-separated months to check"),
):
    """Get subject codes with embedding status for specified year/months."""
    codes = get_subject_codes()
    status = {}

    current_year, current_month = get_current_year_month()

    # Use provided year/months or default to current year
    check_year = year or current_year

    if months:
        check_months = [m.strip() for m in months.split(",")]
    elif check_year == current_year:
        check_months = [f"{m:02d}" for m in range(1, int(current_month) + 1)]
    else:
        check_months = [f"{m:02d}" for m in range(1, 13)]

    for code in codes:
        total_count = 0
        embedded_months = 0
        downloaded_months = 0
        embedded_month_list = []
        downloaded_month_list = []

        for month in check_months:
            if is_subject_month_cached(code, check_year, month):
                downloaded_months += 1
                downloaded_month_list.append(month)

            if is_category_month_embedded(code, check_year, month):
                count = get_embedded_count(code, check_year, month)
                total_count += count
                embedded_months += 1
                embedded_month_list.append(month)

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
        "year": check_year,
        "months": check_months,
    }
