# dedupe-v2

A Python CLI tool for deduplicating inmate records using Splink and Google Gemini (in progress).

## Features

- **Database Management**: Docker Compose setup for MariaDB with the `sdp_pusat` schema.
- **Data Seeding**: Powerful synthetic data generator using Faker (Indonesian locale) to create realistic inmate records with controlled duplication rates for testing.
- **EDA Tools**: Built-in Exploratory Data Analysis module to inspect data quality and duplicate rates. Supports analyzing both raw MariaDB data and cleaned DuckDB staging data.
- **ETL Pipeline**: Robust extractor that normalizes data and loads it into a local DuckDB instance for efficient processing.
    - Supports resumable extraction (tracks last processed ID).
    - Supports UPT filtering.
    - Supports **Custom SQL Queries** via `config.yml` (for complex JOINs and filters).
- **Deduplication**: Record linkage and deduplication using Splink.
    - **Training**: Unsupervised model training with EM, term-frequency tables, and prior match-probability estimation.
    - **Incremental Run**: True new-vs-all processing in configurable batches (`--batch-size`) with optional run cap (`--limit`).
    - **Entity Resolution**: Component-based CIF assignment with automatic CIF merge when new data bridges existing clusters.
- **Validation**: Built-in configuration checker (`check`) to verify database connections and query schemas.

## Workflow: From Source to Deduplication

1.  **Extract (Source -> Staging)**:
    - The ETL pipeline connects to your source MariaDB database.
    - It executes a paginated query (either default or custom from `config.yml`) to fetch records.
    - Data is normalized on-the-fly (e.g., names are standardized, titles removed).
    - Cleaned records are loaded into a local DuckDB table `staging_identitas`.
    - This process is resumable and tracks progress via `data/etl_state.json`.

2.  **Train (Staging -> Model)**:
    - Splink analyzes a sample of staging data (e.g., 50k–200k records).
    - It estimates `u` probabilities, prior match probability (`probability_two_random_records_match`), and then trains `m` parameters via EM.
    - The trained model is saved to `data/splink_model.json`.

3.  **Deduplicate (Staging + Model -> Clusters)**:
    - The `run` command reads only new rows (`PROCESSED_AT IS NULL`) in batches (`--batch-size`).
    - For each batch, it performs **new-vs-all** matching using the trained model.
    - Pairs above threshold are converted into connected components for CIF assignment.
    - Existing CIFs are reused when available; if a new batch bridges multiple old CIFs, CIFs are merged.
    - Batch results are upserted into `processed_clusters`, and only that batch is marked processed.

4.  **Result (Clusters -> Source)**:
    - You can query `processed_clusters` to get the mapping of `NOMOR_INDUK` to `CIF_NUMBER`.
    - (Future Step) Push these CIF numbers back to your production database.

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

4.  **Validate Configuration**:
    Check if your `config.yml` query matches the database schema:
    ```bash
    uv run dedupe check validate
    ```

5.  **Extract & Normalize (ETL)**:
    Extract data from MariaDB to DuckDB (local staging):
    ```bash
    uv run dedupe etl extract
    ```
    
    or Extract only the next 1000 records
    ```bash
    uv run dedupe etl extract --limit 1000
    ```
    
    or Process specific UPTs only:
    ```bash
    uv run dedupe etl extract --upts "001,002"
    ```
    
    Resume from last processed record (default behavior):
    ```bash
    uv run dedupe etl extract --resume
    ```
    
    **Restart from scratch** (ignores saved state and rescans from ID 0):
    ```bash
    uv run dedupe etl extract --no-resume
    ```

6.  **Analyze Data (EDA)**:
    Run an exploratory analysis on a random sample of 5000 records from the **source database**:
    ```bash
    uv run dedupe eda analyze --sample-size 5000
    ```
    
    Analyze the **cleaned staging data** (DuckDB) to verify normalization:
    ```bash
    uv run dedupe eda analyze --source duckdb --sample-size 5000
    ```
    This will generate plots and a summary report in the `analysis_output/` directory.

7.  **Train Deduplication Model**:
    Train the Splink model on a sample of extracted data:
    ```bash
    uv run dedupe deduplicate train --sample-size 50000
    ```

    Recommended for larger datasets (e.g., 3M):
    ```bash
    uv run dedupe deduplicate train \
      --sample-size 200000 \
      --u-max-pairs 5000000 \
      --deterministic-recall 0.7
    ```

    This saves model settings to `data/splink_model.json`.

8.  **Run Deduplication (Incremental)**:
    Process all new/unprocessed records with default batch processing:
    ```bash
    uv run dedupe deduplicate run
    ```

    Controlled rollout example:
    ```bash
    uv run dedupe deduplicate run \
      --batch-size 50000 \
      --limit 200000 \
      --threshold 0.9 \
      --max-pairs-per-batch 3000000
    ```

    Notes:
    - `--batch-size` = records processed per loop/batch.
    - `--limit` = total records cap for this command run.
    - `--max-pairs-per-batch` = safety guard to stop a batch if candidate edges are too high.

    This flow will:
    - Pull unprocessed records from DuckDB in batches.
    - Match each batch against all records (new-vs-all).
    - Resolve CIF assignments and merge CIFs when bridging occurs.
    - Mark only processed batch rows with `PROCESSED_AT`.

## Configuration

You can configure database connections and default settings in `config.yml`.

### Custom ETL Query
To use complex JOINs or filters during extraction, define a custom query in `config.yml`. The query **must** return the required columns (`NOMOR_INDUK`, `NAMA_LENGKAP`, etc.) and include placeholders for `:last_id` and `:limit`.

```yaml
etl:
  query: |
    SELECT 
      it.NOMOR_INDUK,
      it.NAMA_LENGKAP,
      it.NIK,
      it.TANGGAL_LAHIR,
      it.ID_JENIS_KELAMIN,
      it.ALAMAT,
      p.ID_UPT,
      it.NM_AYAH,
      it.NM_IBU
    FROM identitas it
    LEFT JOIN cif_mapping cm ON it.NOMOR_INDUK = cm.NOMOR_INDUK
    LEFT JOIN perkara p ON p.NOMOR_INDUK = it.NOMOR_INDUK
    WHERE cm.NOMOR_INDUK IS NULL
      AND p.ID_PERKARA IS NOT NULL
      AND p.ID_UPT IS NOT NULL
      AND it.NAMA_LENGKAP <> ''
      AND it.NAMA_LENGKAP NOT LIKE 'NAMA WBP%'
      AND it.NOMOR_INDUK NOT LIKE '11f066%'
      AND it.NOMOR_INDUK > :last_id
    ORDER BY it.NOMOR_INDUK ASC
    LIMIT :limit
```

## Project Structure

- `dedupe_gemini/`: Python source code
    - `seeder.py`: Logic for generating synthetic data
    - `etl.py`: ETL pipeline (Extract, Transform, Load)
    - `eda.py`: Exploratory Data Analysis module
    - `deduplication.py`: Splink deduplication logic
    - `check.py`: Configuration validation
    - `db.py`: Database connection handling
    - `config.py`: Configuration loader
- `data/`: Local data storage (DuckDB, state files, models)
- `database/init/`: SQL initialization scripts
- `docker-compose.yml`: Database service configuration
- `pyproject.toml`: Python project configuration
