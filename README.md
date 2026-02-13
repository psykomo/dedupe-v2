# dedupe-gemini

A Python CLI tool for deduplicating inmate records using Splink and Google Gemini (in progress).

## Features

- **Database Management**: Docker Compose setup for MariaDB with the `sdp_pusat` schema.
- **Data Seeding**: Powerful synthetic data generator using Faker (Indonesian locale) to create realistic inmate records with controlled duplication rates for testing.
- **EDA Tools**: Built-in Exploratory Data Analysis module to inspect data quality and duplicate rates. Supports analyzing both raw MariaDB data and cleaned DuckDB staging data.
- **ETL Pipeline**: Robust extractor that normalizes data and loads it into a local DuckDB instance for efficient processing. Supports resumable extraction, UPT filtering, and data cleaning (names, parent names, addresses).
- **Deduplication**: Record linkage and deduplication using Splink.
    - **Training**: Unsupervised model training (EM algorithm) on your data.
    - **Incremental Run**: Processes new records against the full dataset with optional batch limits.
    - **Entity Resolution**: Generates unique CIF Numbers for linked clusters.

## Prerequisites

- [uv](https://github.com/astral-sh/uv) (for dependency management)
- Docker & Docker Compose (for the database)

## Quick Start

1.  **Start the Database**:
    ```bash
    docker compose up -d
    ```

2.  **Install Dependencies**:
    ```bash
    uv sync
    ```

3.  **Seed the Database**:
    Generate 10,000 records with 5% duplicates:
    ```bash
    uv run dedupe seed --count 10000 --duplicates 0.05
    ```
    
    Generate 3 million records (bulk test):
    ```bash
    uv run dedupe seed --count 3000000 --batch-size 5000
    ```

4.  **Extract & Normalize (ETL)**:
    Extract data from MariaDB to DuckDB (local staging):
    ```bash
    uv run dedupe etl extract
    ```
    
    Process specific UPTs only:
    ```bash
    uv run dedupe etl extract --upts "001,002"
    ```
    
    Resume from last processed record (default):
    ```bash
    uv run dedupe etl extract --resume
    ```

5.  **Analyze Data (EDA)**:
    Run an exploratory analysis on a random sample of 5000 records from the **source database**:
    ```bash
    uv run dedupe eda analyze --sample-size 5000
    ```
    
    Analyze the **cleaned staging data** (DuckDB) to verify normalization:
    ```bash
    uv run dedupe eda analyze --source duckdb --sample-size 5000
    ```
    This will generate plots and a summary report in the `analysis_output/` directory.

6.  **Train Deduplication Model**:
    Train the Splink model on a sample of extracted data (e.g., 50k records):
    ```bash
    uv run dedupe deduplicate train --sample-size 50000
    ```
    This saves the model settings to `data/splink_model.json`.

7.  **Run Deduplication**:
    Process all new/unprocessed records:
    ```bash
    uv run dedupe deduplicate run
    ```
    
    Process in small batches (e.g., 10k records) to manage memory:
    ```bash
    uv run dedupe deduplicate run --limit 10000
    ```
    This will:
    - Identify new records in DuckDB (respecting the limit).
    - Link them against the full dataset (including previously processed records).
    - Assign unique `CIF_NUMBER` to linked clusters.
    - Update `PROCESSED_AT` timestamps for the processed batch.

## Configuration

You can configure database connections and default settings in `config.yml`:

```yaml
database:
  url: "mysql+pymysql://root:root@127.0.0.1:3306/sdp_pusat"

seeding:
  default_batch_size: 1000
  default_duplicates: 0.05
```

## Project Structure

- `dedupe_gemini/`: Python source code
    - `seeder.py`: Logic for generating synthetic data
    - `etl.py`: ETL pipeline (Extract, Transform, Load)
    - `eda.py`: Exploratory Data Analysis module
    - `deduplication.py`: Splink deduplication logic
    - `db.py`: Database connection handling
    - `config.py`: Configuration loader
- `data/`: Local data storage (DuckDB, state files, models)
- `database/init/`: SQL initialization scripts
- `docker-compose.yml`: Database service configuration
- `pyproject.toml`: Python project configuration
