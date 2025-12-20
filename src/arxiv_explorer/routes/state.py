"""Shared application state."""

import polars as pl

# Global state shared across routes
df: pl.DataFrame | None = None
subject_codes: dict[str, str] = {}


def get_df() -> pl.DataFrame | None:
    return df


def set_df(new_df: pl.DataFrame) -> None:
    global df
    df = new_df


def get_subject_codes_cache() -> dict[str, str]:
    return subject_codes


def set_subject_codes_cache(codes: dict[str, str]) -> None:
    global subject_codes
    subject_codes = codes