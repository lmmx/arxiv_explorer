# src/arxiv_explorer/data/hub.py
"""HuggingFace Hub interactions."""

from huggingface_hub import HfApi, hf_hub_download, RepoFolder, RepoFile

from .config import DATASET_REPO_ID, DATA_DIR


def list_subjects() -> list[str]:
    """
    List subject codes from the dataset's data/ directory.
    Returns codes like ['astro-ph.CO', 'cs.AI', 'cs.LG', ...]
    """
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
    
    return sorted(subjects)


def list_years_for_subject(subject: str) -> list[str]:
    """List available years for a subject."""
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
        
        return sorted(years)
    except Exception:
        return []


def list_months_for_subject_year(subject: str, year: str) -> list[str]:
    """List available months for a subject/year."""
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
        
        return sorted(months)
    except Exception:
        return []


def get_file_info(subject: str, year: str, month: str) -> dict | None:
    """
    Get info about a specific parquet file without downloading it.
    Returns dict with 'size' in bytes, or None if not found.
    """
    api = HfApi()
    path = f"data/{subject}/{year}/{month}/00000000.parquet"
    
    try:
        items = api.list_repo_tree(
            repo_id=DATASET_REPO_ID,
            repo_type="dataset",
            path_in_repo=f"data/{subject}/{year}/{month}",
            recursive=False,
        )
        
        for item in items:
            if isinstance(item, RepoFile) and item.path.endswith(".parquet"):
                return {"size": item.size, "path": item.path}
        
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