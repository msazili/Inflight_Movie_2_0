# Inflight Entertainment Analytics Challenge

## 1. Summary

The purpose of this project is to implement data pipeline using medallion architecture and do analytics for Inflight movie catalog selection.

- **Created By:** Sazili Muhammad


## 2. Features

- **Medallion Architecture** (Bronze -> Silver -> Gold)
- **PySpark 4.1.1** for data processing
- **Incremental Load Support** with proper upsert logic
- **Analytics** including:
  - Top genres & popularity trends over time
  - Hidden Gems identification
  - Inflight Catalog recommendation (100 movies)
- **Development** environment using `uv`


## 3. How to run

```bash
# Install dependencies
uv sync

# Run pipeline and analysis
uv run main.py
