# src/arxiv_explorer/data/hub.py
"""HuggingFace Hub interactions."""

from huggingface_hub import HfApi, hf_hub_download, RepoFolder, RepoFile

from .config import DATASET_REPO_ID, DATA_DIR

# Cache for API results to avoid repeated calls
_api_cache = {
    "subjects": None,
    "years": {},
    "months": {},
    "file_info": {},
}


def list_subjects() -> list[str]:
    """
    List subject codes from the dataset's data/ directory.
    Returns codes like ['astro-ph.CO', 'cs.AI', 'cs.LG', ...]
    """
    if _api_cache["subjects"] is not None:
        return _api_cache["subjects"]

    api = HfApi()

    items = api.list_repo_tree(
        repo_id=DATASET_REPO_ID,
        repo_type="dataset",
        path_in_repo="data",
        recursive=False,
    )

    subjects = []
    for item in items:
        if isinstance(item, RepoFolder):
            subject = item.path.split("/")[-1]
            subjects.append(subject)

    result = sorted(subjects)
    _api_cache["subjects"] = result
    return result


def list_years() -> list[str]:
    """List all available years across all subjects."""
    subjects = list_subjects()
    if not subjects:
        return []

    # Check first subject to get years (they should be consistent)
    return list_years_for_subject(subjects[0])


def list_years_for_subject(subject: str) -> list[str]:
    """List available years for a subject."""
    cache_key = subject
    if cache_key in _api_cache["years"]:
        return _api_cache["years"][cache_key]

    api = HfApi()

    try:
        items = api.list_repo_tree(
            repo_id=DATASET_REPO_ID,
            repo_type="dataset",
            path_in_repo=f"data/{subject}",
            recursive=False,
        )

        years = []
        for item in items:
            if isinstance(item, RepoFolder):
                year = item.path.split("/")[-1]
                years.append(year)

        result = sorted(years)
        _api_cache["years"][cache_key] = result
        return result
    except Exception:
        return []


def list_months_for_subject_year(subject: str, year: str) -> list[str]:
    """List available months for a subject/year."""
    cache_key = f"{subject}/{year}"
    if cache_key in _api_cache["months"]:
        return _api_cache["months"][cache_key]

    api = HfApi()

    try:
        items = api.list_repo_tree(
            repo_id=DATASET_REPO_ID,
            repo_type="dataset",
            path_in_repo=f"data/{subject}/{year}",
            recursive=False,
        )

        months = []
        for item in items:
            if isinstance(item, RepoFolder):
                month = item.path.split("/")[-1]
                months.append(month)

        result = sorted(months)
        _api_cache["months"][cache_key] = result
        return result
    except Exception:
        return []


def get_file_info(subject: str, year: str, month: str) -> dict | None:
    """
    Get info about a specific parquet file without downloading it.
    Returns dict with 'size' in bytes, or None if not found.
    """
    cache_key = f"{subject}/{year}/{month}"
    if cache_key in _api_cache["file_info"]:
        return _api_cache["file_info"][cache_key]

    api = HfApi()

    try:
        items = api.list_repo_tree(
            repo_id=DATASET_REPO_ID,
            repo_type="dataset",
            path_in_repo=f"data/{subject}/{year}/{month}",
            recursive=False,
        )

        for item in items:
            if isinstance(item, RepoFile) and item.path.endswith(".parquet"):
                result = {"size": item.size, "path": item.path}
                _api_cache["file_info"][cache_key] = result
                return result

        _api_cache["file_info"][cache_key] = None
        return None
    except Exception:
        return None


def download_parquet(subject: str, year: str, month: str) -> str | None:
    """
    Download a specific parquet file from HuggingFace.
    Returns local path or None if failed.
    """
    remote_path = f"data/{subject}/{year}/{month}/00000000.parquet"

    try:
        local_path = hf_hub_download(
            repo_id=DATASET_REPO_ID,
            repo_type="dataset",
            filename=remote_path,
            local_dir=DATA_DIR / ".hf_cache",
        )
        return local_path
    except Exception as e:
        print(f"Failed to download {remote_path}: {e}")
        return None


def clear_cache():
    """Clear the API cache (useful for testing or refreshing)."""
    _api_cache["subjects"] = None
    _api_cache["years"].clear()
    _api_cache["months"].clear()
    _api_cache["file_info"].clear()