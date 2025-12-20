"""Embedding WebSocket route."""

import asyncio
import traceback

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..data import (
    OUTPUT_DIR,
    download_subject_month,
    is_subject_month_cached,
)
from ..embed_papers import (
    combine_with_umap,
    embed_category_month,
    is_category_month_embedded,
    is_umap_cached,
)
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
        months = data.get("months", [])

        print(f"Embed request: {categories}, year={year}, months={months}")

        if not categories:
            await websocket.send_json({"error": "No categories selected"})
            await websocket.close()
            return

        if not months:
            await websocket.send_json({"error": "No months selected"})
            await websocket.close()
            return

        # Check if we can use cached UMAP result
        if is_umap_cached(categories, year, months):
            await websocket.send_json(
                {
                    "status": "loading_cache",
                    "message": "Loading cached embeddings...",
                }
            )

            loop = asyncio.get_event_loop()
            df_final = await loop.run_in_executor(
                None, combine_with_umap, categories, year, months, True
            )

            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            output_path = OUTPUT_DIR / "arxiv_embeddings.parquet"
            df_final.write_parquet(output_path)
            set_df(df_final)

            await websocket.send_json(
                {
                    "status": "complete",
                    "total_papers": len(df_final),
                    "message": f"Loaded from cache! {len(df_final)} papers ready.",
                    "from_cache": True,
                }
            )
            return

        # Count what needs to be done
        total_tasks = len(categories) * len(months)
        completed_tasks = 0
        total_papers = 0
        skipped_count = 0

        for cat in categories:
            for month in months:
                completed_tasks += 1

                # Check if already embedded
                if is_category_month_embedded(cat, year, month):
                    skipped_count += 1
                    await websocket.send_json(
                        {
                            "status": "progress",
                            "category": cat,
                            "month": month,
                            "current": completed_tasks,
                            "total": total_tasks,
                            "message": f"Skipped {cat} {year}-{month} (already embedded)",
                            "skipped": True,
                        }
                    )
                    continue

                # Download if needed
                if not is_subject_month_cached(cat, year, month):
                    await websocket.send_json(
                        {
                            "status": "downloading",
                            "category": cat,
                            "month": month,
                            "message": f"Downloading {cat} {year}-{month}...",
                        }
                    )

                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None, download_subject_month, cat, year, month
                    )

                # Embed
                await websocket.send_json(
                    {
                        "status": "embedding",
                        "category": cat,
                        "month": month,
                        "message": f"Embedding {cat} {year}-{month}...",
                    }
                )

                loop = asyncio.get_event_loop()
                count = await loop.run_in_executor(
                    None, embed_category_month, cat, year, month
                )
                total_papers += count

                await websocket.send_json(
                    {
                        "status": "progress",
                        "category": cat,
                        "month": month,
                        "current": completed_tasks,
                        "total": total_tasks,
                        "count": count,
                        "message": f"Done: {cat} {year}-{month} ({count} papers)",
                    }
                )

        print("Running UMAP...")
        await websocket.send_json(
            {
                "status": "visualizing",
                "message": f"Running UMAP on {total_papers} papers...",
            }
        )

        loop = asyncio.get_event_loop()
        df_final = await loop.run_in_executor(
            None, combine_with_umap, categories, year, months, True
        )

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / "arxiv_embeddings.parquet"
        df_final.write_parquet(output_path)
        set_df(df_final)

        await websocket.send_json(
            {
                "status": "complete",
                "total_papers": len(df_final),
                "skipped": skipped_count,
                "message": f"Complete! {len(df_final)} papers embedded.",
                "from_cache": False,
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
