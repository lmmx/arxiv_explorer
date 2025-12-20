# src/arxiv_explorer/api.py
"""FastAPI server for ArXiv Explorer."""

import asyncio
import re
import traceback
from contextlib import asynccontextmanager
from pathlib import Path

import polars as pl
import uvicorn
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from polars_fastembed import register_model
from pydantic import BaseModel

from .data import (
    DATA_DIR,
    OUTPUT_DIR,
    download_subject_month,
    get_available_months,
    get_current_year_month,
    get_subject_codes,
    is_subject_month_cached,
    load_subject_month,
    precompute_subject_codes,
)
from .embed_papers import (
    MODEL_ID,
    combine_with_umap,
    embed_category,
    extract_subject_codes,
    get_category_file,
)

BASE_DIR = Path(__file__).parents[2]
STATIC_DIR = BASE_DIR / "static"
QUERY_PREFIX = "Represent this sentence for searching relevant passages: "

df: pl.DataFrame | None = None
subject_codes: dict[str, str] = {}


class EstimateRequest(BaseModel):
    categories: list[str]
    year: str = "2025"
    month: str | None = None


class DownloadRequest(BaseModel):
    category: str
    year: str = "2025"
    month: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    global df, subject_codes
    register_model(
        MODEL_ID, providers=["CUDAExecutionProvider", "CPUExecutionProvider"]
    )

    subject_codes = get_subject_codes()
    print(f"Loaded {len(subject_codes)} subject codes")

    path = OUTPUT_DIR / "arxiv_embeddings.parquet"
    if path.exists():
        df = pl.read_parquet(path)
        print(f"Loaded {len(df)} papers")
    yield


app = FastAPI(title="ArXiv Explorer", lifespan=lifespan)


@app.get("/api/categories")
def get_categories():
    """Get subject codes with embedding status."""
    codes = get_subject_codes()
    status = {}

    year, current_month = get_current_year_month()
    months = [f"{m:02d}" for m in range(1, int(current_month) + 1)]

    for code in codes:
        total_count = 0
        embedded_months = 0
        downloaded_months = 0

        for month in months:
            # Check if downloaded (raw data)
            if is_subject_month_cached(code, year, month):
                downloaded_months += 1

            # Check if embedded
            path = get_category_file(code, year, month)
            if path.exists():
                try:
                    total_count += len(pl.read_parquet(path))
                    embedded_months += 1
                except Exception:
                    pass

        status[code] = {
            "embedded": embedded_months > 0,
            "downloaded": downloaded_months > 0,
            "count": total_count,
            "months_embedded": embedded_months,
            "months_downloaded": downloaded_months,
        }

    return {"categories": codes, "status": status}


@app.get("/api/months")
def get_months():
    """Get download status for each month."""
    year, current_month = get_current_year_month()

    # Get available months from HuggingFace
    available_months = get_available_months(year)

    months = []
    for m in range(1, int(current_month) + 1):
        month = f"{m:02d}"

        # Count how many subjects are cached for this month
        cached_subjects = 0
        total_papers = 0

        if DATA_DIR.exists():
            for f in DATA_DIR.glob(f"arxiv_{year}_{month}_*.parquet"):
                cached_subjects += 1
                try:
                    total_papers += len(pl.read_parquet(f))
                except Exception:
                    pass

        months.append(
            {
                "year": year,
                "month": month,
                "available": month in available_months,
                "cached_subjects": cached_subjects,
                "count": total_papers,
            }
        )

    return {"months": months}


@app.post("/api/download-subject")
async def trigger_download_subject(request: DownloadRequest):
    """Download a specific subject/month's data."""
    loop = asyncio.get_event_loop()
    path = await loop.run_in_executor(
        None, download_subject_month, request.category, request.year, request.month
    )

    count = 0
    if path and path.exists():
        count = len(pl.read_parquet(path))

    return {"cached": path is not None, "count": count}


@app.post("/api/estimate")
async def estimate_count(request: EstimateRequest):
    """Estimate paper counts for categories from cached/available data."""
    categories = request.categories
    year = request.year
    month = request.month

    print(f"Estimating for: {categories}, year={year}, month={month}")

    current_year, current_month = get_current_year_month()

    if month:
        months = [month]
    elif year == current_year:
        months = [f"{m:02d}" for m in range(1, int(current_month) + 1)]
    else:
        months = [f"{m:02d}" for m in range(1, 13)]

    counts = {cat: 0 for cat in categories}
    months_with_data = 0

    for cat in categories:
        for m in months:
            if is_subject_month_cached(cat, year, m):
                try:
                    lf = load_subject_month(cat, year, m)
                    count = lf.select(pl.len()).collect().item()
                    counts[cat] += count
                    months_with_data += 1
                except Exception as e:
                    print(f"Error loading {cat} {year}-{m}: {e}")

    for cat, count in counts.items():
        if count > 0:
            print(f"  {cat}: {count}")

    return {
        "counts": counts,
        "total": sum(counts.values()),
        "months_checked": months_with_data,
        "note": "Download categories first to see accurate counts"
        if sum(counts.values()) == 0
        else None,
    }


@app.websocket("/ws/embed")
async def embed_websocket(websocket: WebSocket):
    """WebSocket for embedding with progress."""
    global df
    await websocket.accept()
    print("WebSocket connected")

    try:
        data = await websocket.receive_json()
        categories = data.get("categories", [])
        year = data.get("year", "2025")
        month = data.get("month")

        print(f"Embed request: {categories}, year={year}, month={month}")

        if not categories:
            await websocket.send_json({"error": "No categories selected"})
            await websocket.close()
            return

        total_cats = len(categories)
        total_papers = 0

        for i, cat in enumerate(categories):
            print(f"Processing {i+1}/{total_cats}: {cat}")

            # First download if needed
            await websocket.send_json(
                {
                    "status": "downloading",
                    "category": cat,
                    "message": f"Downloading {cat}...",
                }
            )

            loop = asyncio.get_event_loop()

            # Download the category data
            current_year, current_month = get_current_year_month()
            if month:
                months_to_process = [month]
            elif year == current_year:
                months_to_process = [
                    f"{m:02d}" for m in range(1, int(current_month) + 1)
                ]
            else:
                months_to_process = [f"{m:02d}" for m in range(1, 13)]

            for m in months_to_process:
                if not is_subject_month_cached(cat, year, m):
                    await loop.run_in_executor(
                        None, download_subject_month, cat, year, m
                    )

            await websocket.send_json(
                {
                    "status": "embedding",
                    "category": cat,
                    "message": f"Embedding {cat}...",
                }
            )

            count = await loop.run_in_executor(None, embed_category, cat, year, month)
            total_papers += count

            await websocket.send_json(
                {
                    "status": "progress",
                    "category": cat,
                    "current": i + 1,
                    "total": total_cats,
                    "count": count,
                    "message": f"Done: {cat} ({count} papers)",
                }
            )

        print("Running UMAP...")
        await websocket.send_json(
            {
                "status": "visualizing",
                "message": "Running UMAP...",
            }
        )

        loop = asyncio.get_event_loop()
        df_final = await loop.run_in_executor(None, combine_with_umap, categories, year)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / "arxiv_embeddings.parquet"
        df_final.write_parquet(output_path)
        df = df_final

        await websocket.send_json(
            {
                "status": "complete",
                "total_papers": len(df_final),
                "message": f"Complete! {len(df_final)} papers embedded.",
            }
        )

    except WebSocketDisconnect:
        print("WebSocket disconnected")
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        try:
            await websocket.send_json({"status": "error", "error": str(e)})
        except Exception:
            pass


@app.get("/api/search")
def search(q: str = Query(..., min_length=1), k: int = Query(50, ge=1, le=100)):
    """Semantic search."""
    if df is None:
        return []

    result = df.fastembed.retrieve(
        query=f"{QUERY_PREFIX}{q}",
        model_name=MODEL_ID,
        embedding_column="embedding",
        k=k,
    )
    return [
        {
            "arxiv_id": r["arxiv_id"],
            "title": r["title"],
            "authors": r["authors"][:3],
            "submission_date": r["submission_date"],
            "primary_subject": r["primary_subject"],
            "abstract": r["abstract"][:500] + "..."
            if len(r["abstract"]) > 500
            else r["abstract"],
            "score": round(r["similarity"], 4),
            "x": round(r["x"], 4),
            "y": round(r["y"], 4),
        }
        for r in result.iter_rows(named=True)
    ]


@app.get("/api/papers")
def get_papers():
    """Return all papers with UMAP coordinates."""
    if df is None:
        return []

    return [
        {
            "arxiv_id": r["arxiv_id"],
            "title": r["title"][:100] + "..." if len(r["title"]) > 100 else r["title"],
            "primary_subject": r["primary_subject"],
            "x": round(r["x"], 4),
            "y": round(r["y"], 4),
        }
        for r in df.iter_rows(named=True)
    ]


@app.get("/api/stats")
def get_stats():
    """Dataset statistics."""
    if df is None:
        return {"error": "No embeddings loaded"}

    counts = df.group_by("primary_subject").len().sort("len", descending=True).head(10)
    return {"total_papers": len(df), "top_subjects": counts.to_dicts()}


@app.get("/config")
def config_page():
    return FileResponse(STATIC_DIR / "config.html")


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "arxiv.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def run():
    uvicorn.run("arxiv_explorer.api:app", host="0.0.0.0", port=8001, reload=True)
