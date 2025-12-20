# arxiv-explorer

Explore arXiv papers with semantic search.

Uses the [permutans/arxiv-papers-by-subject](https://huggingface.co/datasets/nick007x/arxiv-papers) dataset,
a processed version of the [nick007x/arxiv-papers](https://huggingface.co/datasets/nick007x/arxiv-papers) dataset.

## Setup

```bash
uv sync
```

## Usage

1. Start the server to download portions of the dataset and view them as UMAP 2D projection of the embedding:

```bash
arxiv-serve
```

2. Open http://localhost:8001

### Development

1. Generate subject/date partitioned dataset from
   [nick007x/arxiv-papers](https://huggingface.co/datasets/nick007x/arxiv-papers)
   -> [permutans/arxiv-papers-by-subject](https://huggingface.co/datasets/nick007x/arxiv-papers)

```bash
arxiv-partition
```

