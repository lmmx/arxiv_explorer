"""Download routes."""

import asyncio

import polars as pl
from fastapi import APIRouter
from pydantic import BaseModel

from ..data import (
    download_subject_month,
    get_current_year_month,
    is_subject_month_cached,
    load_subject_month,
)

router = APIRouter(prefix="/api", tags=["download"])


class DownloadRequest(BaseModel):
    category: str
    year: str = "2025"
    month: str


class EstimateRequest(BaseModel):
    categories: list[str]
    year: str = "2025"
    month: str | None = None


@router.post("/download-subject")
async def trigger_download_subject(request: DownloadRequest):
    """Download a specific subject/month's data."""
    loop = asyncio.get_event_loop()
    path = await loop.run_in_executor(
        None, download_subject_month, request.category, request.year, request.month
    )

    count = 0
    if path and path.exists():
        count = len(pl.read_parquet(path))

    return {"cached": path is not None, "count": count}


@router.post("/estimate")
async def estimate_count(request: EstimateRequest):
    """Estimate paper counts for categories from cached/available data."""
    categories = request.categories
    year = request.year
    month = request.month

    print(f"Estimating for: {categories}, year={year}, month={month}")

    current_year, current_month = get_current_year_month()

    if month:
        months = [month]
    elif year == current_year:
        months = [f"{m:02d}" for m in range(1, int(current_month) + 1)]
    else:
        months = [f"{m:02d}" for m in range(1, 13)]

    counts = {cat: 0 for cat in categories}
    months_with_data = 0

    for cat in categories:
        for m in months:
            if is_subject_month_cached(cat, year, m):
                try:
                    lf = load_subject_month(cat, year, m)
                    count = lf.select(pl.len()).collect().item()
                    counts[cat] += count
                    months_with_data += 1
                except Exception as e:
                    print(f"Error loading {cat} {year}-{m}: {e}")

    for cat, count in counts.items():
        if count > 0:
            print(f"  {cat}: {count}")

    return {
        "counts": counts,
        "total": sum(counts.values()),
        "months_checked": months_with_data,
        "note": "Download categories first to see accurate counts"
        if sum(counts.values()) == 0
        else None,
    }