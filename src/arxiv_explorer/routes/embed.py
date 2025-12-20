"""Embedding WebSocket route."""

import asyncio
import traceback

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..data import (
    OUTPUT_DIR,
    download_subject_month,
    get_current_year_month,
    is_subject_month_cached,
)
from ..embed_papers import combine_with_umap, embed_category
from .state import set_df

router = APIRouter(tags=["embed"])


@router.websocket("/ws/embed")
async def embed_websocket(websocket: WebSocket):
    """WebSocket for embedding with progress."""
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

            await websocket.send_json(
                {
                    "status": "downloading",
                    "category": cat,
                    "message": f"Downloading {cat}...",
                }
            )

            loop = asyncio.get_event_loop()

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
        set_df(df_final)

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