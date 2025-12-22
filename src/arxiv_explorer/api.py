# src/arxiv_explorer/api.py
"""FastAPI server for ArXiv Explorer."""

from contextlib import asynccontextmanager
from pathlib import Path

import polars as pl
import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from polars_fastembed import register_model

from .data import OUTPUT_DIR, get_subject_codes
from .embed_papers import MODEL_ID, add_year_month_column
from .routes import (
    categories_router,
    download_router,
    embed_router,
    months_router,
    papers_router,
    search_router,
    stats_router,
    topics_router,
)
from .routes.state import set_df, set_subject_codes_cache

BASE_DIR = Path(__file__).parents[2]
STATIC_DIR = BASE_DIR / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    register_model(
        MODEL_ID, providers=["CUDAExecutionProvider", "CPUExecutionProvider"]
    )

    codes = get_subject_codes()
    set_subject_codes_cache(codes)
    print(f"Loaded {len(codes)} subject codes")

    path = OUTPUT_DIR / "arxiv_embeddings.parquet"
    if path.exists():
        df = pl.read_parquet(path)
        # Ensure year_month column exists
        if "year_month" not in df.columns:
            print("Adding year_month column to loaded data...")
            df = add_year_month_column(df)
        set_df(df)
        print(f"Loaded {len(df)} papers")
        # Debug: show available year_months
        if "year_month" in df.columns:
            year_months = df.select("year_month").unique().to_series().to_list()
            print(f"Available year_months: {sorted([ym for ym in year_months if ym])}")
    yield


app = FastAPI(title="ArXiv Explorer", lifespan=lifespan)

# Register all routers
app.include_router(categories_router)
app.include_router(months_router)
app.include_router(download_router)
app.include_router(embed_router)
app.include_router(search_router)
app.include_router(papers_router)
app.include_router(stats_router)
app.include_router(topics_router)


@app.get("/config")
def config_page():
    return FileResponse(STATIC_DIR / "config.html")


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "arxiv.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def run():
    uvicorn.run("arxiv_explorer.api:app", host="0.0.0.0", port=8001, reload=True)
