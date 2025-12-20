# src/arxiv_explorer/partition/upload.py
"""Main upload logic."""

import argparse
import shutil
from pathlib import Path

import polars as pl
from huggingface_hub import HfApi

from .config import DEFAULT_SAMPLE_SIZE, INPUT_PATH, OUTPUT_DIR, RESULT_DATASET_ID
from .extract import extract_subject_code, extract_year_month
from .hf_utils import ensure_logged_in


def prepare_lazyframe(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Add subject, year, month columns and filter nulls."""
    lf = lf.with_columns(
        [
            pl.col("primary_subject")
            .map_elements(extract_subject_code, return_dtype=pl.Utf8)
            .alias("subject"),
            pl.col("submission_date")
            .map_elements(
                lambda x: extract_year_month(x)[0] if extract_year_month(x) else None,
                return_dtype=pl.Utf8,
            )
            .alias("year"),
            pl.col("submission_date")
            .map_elements(
                lambda x: extract_year_month(x)[1] if extract_year_month(x) else None,
                return_dtype=pl.Utf8,
            )
            .alias("month"),
        ]
    )

    return lf.filter(
        pl.col("subject").is_not_null()
        & pl.col("year").is_not_null()
        & pl.col("month").is_not_null()
    )


def custom_path(ctx: pl.io.partition.KeyedPartitionContext) -> str:
    """Build path like data/cs.LG/2025/01/00000000.parquet"""
    subject = ctx.keys[0].str_value
    year = ctx.keys[1].str_value
    month = ctx.keys[2].str_value
    # Include 'data/' prefix so it ends up in data/ folder in the repo
    return f"data/{subject}/{year}/{month}/{ctx.in_part_idx:08d}.parquet"


def partition_and_upload(
    input_path: Path = INPUT_PATH,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    dry_run: bool = False,
    skip_partition: bool = False,
):
    """Partition the arXiv dataset by subject/year/month and upload to HuggingFace."""
    username = ensure_logged_in()
    print(f"Logged in as: {username}")

    output_dir = OUTPUT_DIR / "repo"
    data_dir = output_dir / "data"

    if not skip_partition:
        # Clean previous output
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True)

        print(f"Scanning data from {input_path}...")
        lf = pl.scan_parquet(input_path)

        if sample_size:
            print(f"Sampling {sample_size} papers for test run...")
            df = lf.collect().sample(n=sample_size, seed=42)
            lf = df.lazy()
            print(f"Sampled down to {sample_size} papers")

        lf = prepare_lazyframe(lf)

        print(f"Writing partitioned data to {data_dir}...")

        lf.sink_parquet(
            pl.PartitionByKey(
                base_path=str(output_dir),
                by=["subject", "year", "month"],
                file_path=custom_path,
                include_key=False,
            ),
            mkdir=True,
        )

        print(f"Partitioned data written to {data_dir}")
    else:
        print(f"Skipping partition, using existing data in {output_dir}")

    if dry_run:
        print("[DRY RUN] Would upload to HuggingFace")
        return

    print(f"Uploading to {RESULT_DATASET_ID}...")
    api = HfApi()
    api.upload_large_folder(
        folder_path=str(output_dir),
        repo_id=RESULT_DATASET_ID,
        repo_type="dataset",
    )

    print(f"\nDone! Dataset: https://huggingface.co/datasets/{RESULT_DATASET_ID}")


def main():
    parser = argparse.ArgumentParser(description="Partition and upload arXiv dataset")
    parser.add_argument("--input", "-i", type=Path, default=INPUT_PATH)
    parser.add_argument(
        "--sample",
        "-s",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Sample size (0 for full)",
    )
    parser.add_argument("--dry-run", "-n", action="store_true")
    parser.add_argument(
        "--skip-partition",
        action="store_true",
        help="Skip partitioning, just upload existing data",
    )
    args = parser.parse_args()

    partition_and_upload(
        input_path=args.input,
        sample_size=args.sample,
        dry_run=args.dry_run,
        skip_partition=args.skip_partition,
    )
