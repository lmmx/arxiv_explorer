# src/arxiv_explorer/data/estimator.py
"""Estimate paper counts without downloading full files."""

from . import hub, cache


def estimate_from_hub(subjects: list[str], year: str, months: list[str]) -> dict[str, int]:
    """
    Estimate paper counts by checking file sizes on HuggingFace.
    Uses a heuristic: ~1KB per paper in parquet format.
    
    Returns {subject: estimated_count}
    """
    BYTES_PER_PAPER = 1000  # Rough estimate
    
    counts = {}
    for subject in subjects:
        total_size = 0
        for month in months:
            info = hub.get_file_info(subject, year, month)
            if info:
                total_size += info["size"]
        
        counts[subject] = total_size // BYTES_PER_PAPER
    
    return counts


def get_counts(subjects: list[str], year: str, months: list[str]) -> dict[str, int]:
    """
    Get paper counts - from cache if available, otherwise estimate from Hub.
    Returns {subject: count}
    """
    counts = {}
    
    for subject in subjects:
        subject_total = 0
        has_any_cached = False
        
        for month in months:
            if cache.is_cached(subject, year, month):
                subject_total += cache.get_cached_count(subject, year, month)
                has_any_cached = True
        
        if has_any_cached:
            counts[subject] = subject_total
        else:
            # Estimate from Hub file sizes
            total_size = 0
            for month in months:
                info = hub.get_file_info(subject, year, month)
                if info:
                    total_size += info["size"]
            
            # Rough estimate: ~1KB per paper
            counts[subject] = total_size // 1000
    
    return counts