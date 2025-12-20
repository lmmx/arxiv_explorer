"""Category routes."""

import polars as pl
from fastapi import APIRouter

from ..data import get_current_year_month, get_subject_codes, is_subject_month_cached
from ..embed_papers import get_category_file

router = APIRouter(prefix="/api", tags=["categories"])


@router.get("/categories")
def get_categories():
    """Get subject codes with embedding status."""
    codes = get_subject_codes()
    status = {}

    year, current_month = get_current_year_month()
    months = [f"{m:02d}" for m in range(1, int(current_month) + 1)]

    for code in codes:
        total_count = 0
        embedded_months = 0
        downloaded_months = 0

        for month in months:
            if is_subject_month_cached(code, year, month):
                downloaded_months += 1

            path = get_category_file(code, year, month)
            if path.exists():
                try:
                    total_count += len(pl.read_parquet(path))
                    embedded_months += 1
                except Exception:
                    pass

        status[code] = {
            "embedded": embedded_months > 0,
            "downloaded": downloaded_months > 0,
            "count": total_count,
            "months_embedded": embedded_months,
            "months_downloaded": downloaded_months,
        }

    return {"categories": codes, "status": status}