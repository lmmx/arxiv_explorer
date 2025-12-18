# src/arxiv_explorer/api.py
"""
FastAPI server with embedding management UI.
"""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import polars as pl
import uvicorn
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from polars_fastembed import register_model

from .embed_papers import (
    CACHE_DIR,
    MODEL_ID,
    combine_and_visualize,
    embed_category,
    estimate_papers_count,
    get_category_file,
)

BASE_DIR = Path(__file__).parents[2]
OUTPUT_DIR = BASE_DIR / "output"
STATIC_DIR = BASE_DIR / "static"

QUERY_PREFIX = "Represent this sentence for searching relevant passages: "

# Available categories
ARXIV_CATEGORIES = [
    "cs",  # Computer Science
    "physics",  # Physics
    "math",  # Mathematics
    "astro-ph",  # Astrophysics
    "quant-ph",  # Quantum Physics
    "cond-mat",  # Condensed Matter
    "stat",  # Statistics
    "econ",  # Economics
    "eess",  # Electrical Engineering
    "q-bio",  # Quantitative Biology
    "q-fin",  # Quantitative Finance
]

# Global state
df: Optional[pl.DataFrame] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global df
    print(f"Registering model: {MODEL_ID}")
    register_model(
        MODEL_ID, providers=["CUDAExecutionProvider", "CPUExecutionProvider"]
    )

    # Try to load existing embeddings
    parquet_path = OUTPUT_DIR / "arxiv_embeddings.parquet"
    if parquet_path.exists():
        print(f"Loading embeddings from {parquet_path}")
        df = pl.read_parquet(parquet_path)
        print(f"Loaded {len(df)} papers")
    else:
        print("No embeddings found. Use the config page to embed categories.")

    yield


app = FastAPI(
    title="ArXiv Explorer",
    description="Explore ArXiv papers with semantic search",
    lifespan=lifespan,
)


@app.get("/api/categories")
def get_categories():
    """Get available categories and their embedding status."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    status = {}
    for cat in ARXIV_CATEGORIES:
        cache_file = get_category_file(cat)
        if cache_file.exists():
            try:
                df_cat = pl.read_parquet(cache_file)
                status[cat] = {
                    "embedded": True,
                    "count": len(df_cat),
                }
            except Exception as e:
                print(f"Error reading {cat}: {e}")
                status[cat] = {
                    "embedded": False,
                    "count": 0,
                }
        else:
            status[cat] = {
                "embedded": False,
                "count": 0,
            }

    return {
        "categories": ARXIV_CATEGORIES,
        "status": status,
    }


@app.post("/api/estimate")
async def estimate_count(categories: list[str]):
    """Estimate how many papers will be embedded."""
    try:
        counts = estimate_papers_count(categories)
        total = sum(counts.values())
        return {
            "counts": counts,
            "total": total,
        }
    except Exception as e:
        return {"error": str(e)}


@app.websocket("/ws/embed")
async def embed_websocket(websocket: WebSocket):
    """WebSocket endpoint for embedding with progress updates."""
    await websocket.accept()

    try:
        # Receive categories to embed
        data = await websocket.receive_json()
        categories = data.get("categories", [])

        if not categories:
            await websocket.send_json({"error": "No categories selected"})
            return

        # Embed each category
        for cat in categories:
            await websocket.send_json(
                {
                    "status": "embedding",
                    "category": cat,
                }
            )

            def progress(current, total, message):
                asyncio.create_task(
                    websocket.send_json(
                        {
                            "status": "progress",
                            "category": cat,
                            "current": current,
                            "total": total,
                            "message": message,
                        }
                    )
                )

            await asyncio.to_thread(
                embed_category,
                cat,
                progress_callback=progress,
            )

        # Combine and visualize
        await websocket.send_json(
            {
                "status": "visualizing",
                "message": "Creating UMAP visualization...",
            }
        )

        def viz_progress(current, total, message):
            asyncio.create_task(
                websocket.send_json(
                    {
                        "status": "progress",
                        "category": "visualization",
                        "current": current,
                        "total": total,
                        "message": message,
                    }
                )
            )

        df_final = await asyncio.to_thread(
            combine_and_visualize,
            categories,
            progress_callback=viz_progress,
        )

        # Save
        parquet_path = OUTPUT_DIR / "arxiv_embeddings.parquet"
        df_final.write_parquet(parquet_path)

        # Reload global df
        global df
        df = df_final

        await websocket.send_json(
            {
                "status": "complete",
                "total_papers": len(df_final),
            }
        )

    except WebSocketDisconnect:
        print("WebSocket disconnected")
    except Exception as e:
        await websocket.send_json(
            {
                "status": "error",
                "error": str(e),
            }
        )


@app.get("/api/search")
def search(q: str = Query(..., min_length=1), k: int = Query(50, ge=1, le=100)):
    """Semantic search over ArXiv papers."""
    if df is None:
        return {"error": "No embeddings loaded. Please embed categories first."}

    result = df.fastembed.retrieve(
        query=f"{QUERY_PREFIX}{q}",
        model_name=MODEL_ID,
        embedding_column="embedding",
        k=k,
    )
    return [
        {
            "arxiv_id": row["arxiv_id"],
            "title": row["title"],
            "authors": row["authors"][:3]
            if len(row["authors"]) > 3
            else row["authors"],
            "submission_date": row["submission_date"],
            "primary_subject": row["primary_subject"],
            "abstract": row["abstract"][:500] + "..."
            if len(row["abstract"]) > 500
            else row["abstract"],
            "score": round(row["similarity"], 4),
            "x": round(row["x"], 4),
            "y": round(row["y"], 4),
        }
        for row in result.iter_rows(named=True)
    ]


@app.get("/api/papers")
def get_all_papers():
    """Return all papers with UMAP coordinates."""
    if df is None:
        return {"error": "No embeddings loaded. Please embed categories first."}

    return [
        {
            "arxiv_id": row["arxiv_id"],
            "title": row["title"][:100] + "..."
            if len(row["title"]) > 100
            else row["title"],
            "primary_subject": row["primary_subject"],
            "x": round(row["x"], 4),
            "y": round(row["y"], 4),
        }
        for row in df.iter_rows(named=True)
    ]


@app.get("/api/stats")
def get_stats():
    """Return dataset statistics."""
    if df is None:
        return {"error": "No embeddings loaded"}

    subject_counts = (
        df.group_by("primary_subject").len().sort("len", descending=True).head(10)
    )
    return {
        "total_papers": len(df),
        "top_subjects": subject_counts.to_dicts(),
    }


@app.get("/config")
def config_page():
    return FileResponse(STATIC_DIR / "config.html")


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "arxiv.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def run():
    uvicorn.run(
        "arxiv_explorer.api:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
    )


if __name__ == "__main__":
    run()
