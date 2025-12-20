# src/arxiv_explorer/routes/download.py
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
    year_months: list[str]  # Format: ["2024-01", "2024-02", "2025-01"]


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
    """Estimate paper counts for categories across multiple year-months."""
    categories = request.categories
    year_months = request.year_months

    print(f"Estimating for: {len(categories)} categories, year_months={year_months}")

    # Parse year-months and aggregate counts
    loop = asyncio.get_event_loop()
    
    total_cached = 0
    total_estimated = 0
    by_category = {cat: {"cached": 0, "estimated": 0, "total": 0} for cat in categories}
    by_year_month = {}
    cached_count = 0
    estimated_count = 0

    for ym in year_months:
        if "-" not in ym:
            continue
        year, month = ym.split("-", 1)
        
        counts = await loop.run_in_executor(
            None, get_counts_for_selection, categories, year, [month]
        )
        
        by_year_month[ym] = {
            "cached": counts["total_cached"],
            "estimated": counts["total_estimated"],
        }
        
        total_cached += counts["total_cached"]
        total_estimated += counts["total_estimated"]
        cached_count += counts["cached_count"]
        estimated_count += counts["estimated_count"]
        
        for cat, data in counts["by_category"].items():
            by_category[cat]["cached"] += data["cached"]
            by_category[cat]["estimated"] += data["estimated"]
            by_category[cat]["total"] += data["total"]

    total = total_cached + total_estimated

    # Add time estimates
    time_est = estimate_embedding_time(total)

    return {
        "counts": {cat: data["total"] for cat, data in by_category.items()},
        "breakdown": by_category,
        "by_year_month": by_year_month,
        "total": total,
        "total_cached": total_cached,
        "total_estimated": total_estimated,
        "cached_files": cached_count,
        "estimated_files": estimated_count,
        "time_estimate": time_est,
        "year_months": year_months,
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