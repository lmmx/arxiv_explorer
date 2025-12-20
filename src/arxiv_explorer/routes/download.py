"""Download routes."""

import asyncio

import polars as pl
from fastapi import APIRouter
from pydantic import BaseModel

from ..data import (
    download_subject_month,
    estimate_embedding_time,
    get_available_months,
    get_available_years,
    get_cache_summary,
    get_counts_for_selection,
    get_current_year_month,
    is_subject_month_cached,
    list_cached_months,
    list_cached_years,
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
    months: list[str] | None = None


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
    """Estimate paper counts for categories, checking both cache and HuggingFace."""
    categories = request.categories
    year = request.year

    current_year, current_month = get_current_year_month()

    if request.months:
        months = request.months
    elif year == current_year:
        months = [f"{m:02d}" for m in range(1, int(current_month) + 1)]
    else:
        months = [f"{m:02d}" for m in range(1, 13)]

    print(f"Estimating for: {len(categories)} categories, year={year}, months={months}")

    # Use the new estimator that checks both cache and HuggingFace
    loop = asyncio.get_event_loop()
    counts = await loop.run_in_executor(
        None, get_counts_for_selection, categories, year, months
    )

    # Add time estimates
    time_est = estimate_embedding_time(counts["total"])

    return {
        "counts": {cat: data["total"] for cat, data in counts["by_category"].items()},
        "breakdown": counts["by_category"],
        "by_month": counts["by_month"],
        "total": counts["total"],
        "total_cached": counts["total_cached"],
        "total_estimated": counts["total_estimated"],
        "cached_files": counts["cached_count"],
        "estimated_files": counts["estimated_count"],
        "time_estimate": time_est,
        "year": year,
        "months": months,
    }


@router.get("/available-years")
async def get_years():
    """Get available years from HuggingFace and local cache."""
    loop = asyncio.get_event_loop()

    hf_years = await loop.run_in_executor(None, get_available_years)
    cached_years = list_cached_years()

    all_years = sorted(set(hf_years) | set(cached_years), reverse=True)

    return {
        "years": all_years,
        "available_on_hf": hf_years,
        "cached_locally": cached_years,
    }


@router.get("/available-months/{year}")
async def get_months_for_year(year: str):
    """Get available months for a year from HuggingFace and local cache."""
    loop = asyncio.get_event_loop()

    hf_months = await loop.run_in_executor(None, get_available_months, year)
    cached_months = list_cached_months(year)

    all_months = sorted(set(hf_months) | set(cached_months))

    current_year, current_month = get_current_year_month()

    return {
        "year": year,
        "months": all_months,
        "available_on_hf": hf_months,
        "cached_locally": cached_months,
        "is_current_year": year == current_year,
        "current_month": current_month if year == current_year else None,
    }


@router.get("/cache-summary")
async def cache_summary():
    """Get summary of all locally cached data."""
    return get_cache_summary()
