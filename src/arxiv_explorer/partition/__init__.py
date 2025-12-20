# src/arxiv_explorer/partition/__init__.py
"""Partition and upload arXiv dataset to HuggingFace."""

from .upload import main, partition_and_upload

__all__ = ["main", "partition_and_upload"]