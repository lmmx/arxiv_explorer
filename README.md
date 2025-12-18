# arxiv-explorer

Explore arXiv papers with semantic search.

Uses the [arxiv-papers](https://huggingface.co/datasets/nick007x/arxiv-papers) dataset.

## Setup

```bash
uv sync
```

## Usage

1. Generate embeddings (run once):
```bash
arxiv-embed
```

2. Start the server:

```bash
arxiv-serve
```

3. Open http://localhost:8000
