# src/arxiv_explorer/api.py
"""FastAPI server for ArXiv Explorer."""

import asyncio
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

from .embed_papers import (
    CACHE_DIR,
    MODEL_ID,
    OUTPUT_DIR,
    combine_with_umap,
    embed_category,
    get_category_file,
    get_subject_codes,
    load_dataset,
    precompute_subject_codes,
    extract_subject_codes,
)

BASE_DIR = Path(__file__).parents[2]
STATIC_DIR = BASE_DIR / "static"
QUERY_PREFIX = "Represent this sentence for searching relevant passages: "

df: pl.DataFrame | None = None
subject_codes: dict[str, str] = {}


class EstimateRequest(BaseModel):
    categories: list[str]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global df, subject_codes
    register_model(
        MODEL_ID, providers=["CUDAExecutionProvider", "CPUExecutionProvider"]
    )

    # Precompute subject codes at startup
    subject_codes = precompute_subject_codes()
    print(f"Loaded {len(subject_codes)} subject codes")

    path = OUTPUT_DIR / "arxiv_embeddings.parquet"
    if path.exists():
        df = pl.read_parquet(path)
        print(f"Loaded {len(df)} papers")
    yield


app = FastAPI(title="ArXiv Explorer", lifespan=lifespan)


@app.get("/api/categories")
def get_categories():
    """Get fine-grained subject codes with embedding status."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    codes = get_subject_codes()
    status = {}
    
    for code in codes:
        path = get_category_file(code)
        if path.exists():
            try:
                status[code] = {"embedded": True, "count": len(pl.read_parquet(path))}
            except Exception:
                status[code] = {"embedded": False, "count": 0}
        else:
            status[code] = {"embedded": False, "count": 0}
    
    return {"categories": codes, "status": status}


@app.post("/api/estimate")
async def estimate_count(request: EstimateRequest):
    """Estimate paper counts for categories."""
    categories = request.categories
    print(f"Estimating for categories: {categories}")

    meta = load_dataset(columns=("subjects", "submission_date"))
    meta = meta.filter(pl.col("submission_date").str.contains("2025"))
    
    # Add subject_codes column
    meta = meta.with_columns(
        pl.col("subjects").map_elements(extract_subject_codes, return_dtype=pl.List(pl.Utf8)).alias("subject_codes")
    )

    counts = {}
    for cat in categories:
        count = len(meta.filter(pl.col("subject_codes").list.contains(cat)))
        counts[cat] = count
        print(f"  {cat}: {count}")

    return {"counts": counts, "total": sum(counts.values())}


@app.websocket("/ws/embed")
async def embed_websocket(websocket: WebSocket):
    """WebSocket for embedding with progress."""
    global df
    await websocket.accept()

    try:
        data = await websocket.receive_json()
        categories = data.get("categories", [])
        if not categories:
            await websocket.send_json({"error": "No categories selected"})
            return

        for cat in categories:
            await websocket.send_json({"status": "embedding", "category": cat})
            await asyncio.to_thread(embed_category, cat)
            await websocket.send_json(
                {
                    "status": "progress",
                    "category": cat,
                    "current": 1,
                    "total": 1,
                    "message": f"Done: {cat}",
                }
            )

        await websocket.send_json(
            {"status": "visualizing", "message": "Running UMAP..."}
        )
        df_final = await asyncio.to_thread(combine_with_umap, categories)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        df_final.write_parquet(OUTPUT_DIR / "arxiv_embeddings.parquet")
        df = df_final

        await websocket.send_json({"status": "complete", "total_papers": len(df_final)})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        traceback.print_exc()
        await websocket.send_json({"status": "error", "error": str(e)})


@app.get("/api/search")
def search(q: str = Query(..., min_length=1), k: int = Query(50, ge=1, le=100)):
    """Semantic search over papers."""
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