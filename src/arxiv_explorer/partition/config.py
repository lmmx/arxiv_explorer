# src/arxiv_explorer/partition/config.py
"""Configuration for partitioning."""

from pathlib import Path

USERNAME = "permutans"
RESULT_DATASET_ID = f"{USERNAME}/arxiv-papers-by-subject"
INPUT_PATH = Path("output/data/arxiv_papers.parquet")
OUTPUT_DIR = Path("output")

DEFAULT_SAMPLE_SIZE = 0  # 500

MONTH_MAP = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}
