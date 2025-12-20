"""Month routes."""

import polars as pl
from fastapi import APIRouter

from ..data import DATA_DIR, get_available_months, get_current_year_month

router = APIRouter(prefix="/api", tags=["months"])


@router.get("/months")
def get_months():
    """Get download status for each month."""
    year, current_month = get_current_year_month()
    available_months = get_available_months(year)

    months = []
    for m in range(1, int(current_month) + 1):
        month = f"{m:02d}"

        cached_subjects = 0
        total_papers = 0

        if DATA_DIR.exists():
            for f in DATA_DIR.glob(f"arxiv_{year}_{month}_*.parquet"):
                cached_subjects += 1
                try:
                    total_papers += len(pl.read_parquet(f))
                except Exception:
                    pass

        months.append(
            {
                "year": year,
                "month": month,
                "available": month in available_months,
                "cached_subjects": cached_subjects,
                "count": total_papers,
            }
        )

    return {"months": months}