# src/arxiv_explorer/routes/__init__.py
"""Routes package for ArXiv Explorer."""

from .categories import router as categories_router
from .months import router as months_router
from .download import router as download_router
from .embed import router as embed_router
from .search import router as search_router
from .papers import router as papers_router
from .stats import router as stats_router
from .topics import router as topics_router

__all__ = [
    "categories_router",
    "months_router",
    "download_router",
    "embed_router",
    "search_router",
    "papers_router",
    "stats_router",
    "topics_router",
]