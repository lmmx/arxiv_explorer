# src/arxiv_explorer/data/estimator.py
"""Estimate paper counts without downloading full files."""

from . import hub, cache

# Calibrated from actual data: 17,342 papers from files totaling ~10MB
# That's roughly 600 bytes per paper in compressed parquet
BYTES_PER_PAPER = 600


def estimate_from_hub(subject: str, year: str, month: str) -> int:
    """
    Estimate paper count for a single subject/year/month from HuggingFace file size.
    Returns estimated count or 0 if file not found.
    """
    info = hub.get_file_info(subject, year, month)
    if info:
        return info["size"] // BYTES_PER_PAPER
    return 0


def get_count(subject: str, year: str, month: str) -> tuple[int, bool]:
    """
    Get paper count for a subject/year/month.
    Returns (count, is_exact) where is_exact=True if from cache, False if estimated.
    """
    if cache.is_cached(subject, year, month):
        return cache.get_cached_count(subject, year, month), True

    # Estimate from Hub file size
    estimated = estimate_from_hub(subject, year, month)
    return estimated, False


def get_counts_for_selection(
    categories: list[str], year: str, months: list[str]
) -> dict:
    """
    Get paper counts for a selection of categories and months.
    Returns detailed breakdown with cached vs estimated counts.
    """
    result = {
        "by_category": {},
        "by_month": {},
        "total_cached": 0,
        "total_estimated": 0,
        "total": 0,
        "cached_count": 0,
        "estimated_count": 0,
    }

    for cat in categories:
        cat_cached = 0
        cat_estimated = 0

        for month in months:
            count, is_exact = get_count(cat, year, month)

            if is_exact:
                cat_cached += count
            else:
                cat_estimated += count

            # Also track by month
            if month not in result["by_month"]:
                result["by_month"][month] = {"cached": 0, "estimated": 0}

            if is_exact:
                result["by_month"][month]["cached"] += count
            else:
                result["by_month"][month]["estimated"] += count

        result["by_category"][cat] = {
            "cached": cat_cached,
            "estimated": cat_estimated,
            "total": cat_cached + cat_estimated,
        }

        result["total_cached"] += cat_cached
        result["total_estimated"] += cat_estimated

    result["total"] = result["total_cached"] + result["total_estimated"]
    result["cached_count"] = sum(
        1 for cat in categories for m in months if cache.is_cached(cat, year, m)
    )
    result["estimated_count"] = len(categories) * len(months) - result["cached_count"]

    return result


def estimate_embedding_time(paper_count: int) -> dict:
    """
    Estimate embedding time based on paper count.
    
    Calibrated from actual runs:
    - GPU: 17,342 papers in ~26 seconds = ~40,000 papers/minute
    - CPU: ~6x slower than GPU = ~6,700 papers/minute
    - UMAP: 17,342 papers in ~22 seconds (CPU-bound, same either way)
    """
    papers_per_minute_gpu = 40000
    papers_per_minute_cpu = 6700  # ~6x slower than GPU

    # Embedding time
    gpu_embed_minutes = paper_count / papers_per_minute_gpu
    cpu_embed_minutes = paper_count / papers_per_minute_cpu
    
    # UMAP time scales roughly O(n log n), calibrated from 17k papers = 22 sec
    if paper_count > 0:
        umap_minutes = 0.37 * (paper_count / 17342) * (1 + 0.1 * (paper_count / 17342))
    else:
        umap_minutes = 0
    
    gpu_total = gpu_embed_minutes + umap_minutes
    cpu_total = cpu_embed_minutes + umap_minutes

    return {
        "gpu_minutes": round(gpu_total, 2),
        "cpu_minutes": round(cpu_total, 2),
        "gpu_display": format_duration(gpu_total),
        "cpu_display": format_duration(cpu_total),
        "umap_seconds": round(umap_minutes * 60),
    }


def format_duration(minutes: float) -> str:
    """Format duration in minutes to human-readable string."""
    seconds = minutes * 60
    if seconds < 60:
        return f"{int(seconds)} seconds"
    elif minutes < 60:
        mins = int(minutes)
        secs = int((minutes - mins) * 60)
        if secs == 0:
            return f"{mins} minute{'s' if mins > 1 else ''}"
        return f"{mins}m {secs}s"
    else:
        hours = int(minutes // 60)
        mins = int(minutes % 60)
        if mins == 0:
            return f"{hours} hour{'s' if hours > 1 else ''}"
        return f"{hours}h {mins}m"