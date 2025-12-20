# src/arxiv_explorer/partition/extract.py
"""Extraction utilities for parsing arXiv metadata."""

import re

from .config import MONTH_MAP


def extract_subject_code(primary_subject: str) -> str | None:
    """Extract code like 'cs.AI' from 'Artificial Intelligence (cs.AI)'."""
    if not primary_subject:
        return None
    match = re.search(r"\(([a-z-]+\.[A-Z]+)\)", primary_subject)
    return match.group(1) if match else None


def extract_year_month(submission_date: str) -> tuple[str, str] | None:
    """Extract year and month from '18 Feb 2009' format."""
    if not submission_date:
        return None
    match = re.search(r"(\d{1,2})\s+(\w{3})\s+(\d{4})", submission_date)
    if match:
        month_name = match.group(2)
        year = match.group(3)
        month = MONTH_MAP.get(month_name)
        if month:
            return year, month
    return None