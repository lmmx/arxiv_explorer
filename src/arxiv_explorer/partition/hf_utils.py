# src/arxiv_explorer/partition/hf_utils.py
"""HuggingFace utilities."""

from datasets import get_dataset_config_names
from huggingface_hub import HfApi, repo_exists


def ensure_logged_in() -> str:
    """Check we're logged in, return username or raise."""
    api = HfApi()
    info = api.whoami()  # Raises if not logged in
    return info["name"]


def dataset_exists(dataset_id: str) -> bool:
    """Check if a dataset repo exists."""
    return repo_exists(dataset_id, repo_type="dataset")


def config_exists(dataset_id: str, config_name: str) -> bool:
    """Check if a config exists in an existing dataset."""
    if not dataset_exists(dataset_id):
        return False
    try:
        configs = get_dataset_config_names(dataset_id)
        return config_name in configs
    except Exception:
        return False