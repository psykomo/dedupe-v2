import typer
import duckdb
import os
import json
import logging
from typing import List, Optional
from datetime import datetime
from rich.progress import Progress, SpinnerColumn, TextColumn
from sqlalchemy import create_engine, text
from dedupe_gemini.db import get_engine
from dedupe_gemini.config import load_config
import re
from unidecode import unidecode

app = typer.Typer()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

DUCKDB_PATH = "data/processed.duckdb"
STATE_FILE = "data/etl_state.json"

def get_duckdb_conn():
    os.makedirs("data", exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)
    # Ensure staging table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS staging_identitas (
            NOMOR_INDUK VARCHAR PRIMARY KEY,
            NAMA_LENGKAP VARCHAR,
            NIK VARCHAR,
            TANGGAL_LAHIR DATE,
            ID_JENIS_KELAMIN VARCHAR,
            ALAMAT VARCHAR,
            ID_UPT VARCHAR,
            CLEAN_NAMA VARCHAR,
            CLEAN_ALAMAT VARCHAR,
            CLEAN_NM_AYAH VARCHAR,
            CLEAN_NM_IBU VARCHAR,
            EXTRACTED_AT TIMESTAMP,
            PROCESSED_AT TIMESTAMP DEFAULT NULL
        )
    """)
    return conn

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def normalize_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    
    # 1. Lowercase and strip
    text = text.lower().strip()
    
    # 2. Remove accents/non-ascii
    text = unidecode(text)
    
    # 3. Remove titles (basic list for Indonesia)
    titles = [
        r"\bdr\.\s*", r"\bdrs\.\s*", r"\bir\.\s*", r"\bh\.\s*", r"\bhj\.\s*", 
        r"\balm\.\s*", r"\bbin\b", r"\bbinti\b", r"\bs\.kom\b", r"\bs\.e\b", 
        r"\bm\.kom\b", r"\bprof\.\s*", r"\bpdt\.\s*"
    ]
    for pattern in titles:
        text = re.sub(pattern, "", text)
        
    # 4. Standardize common abbreviations
    replacements = {
        r"\bjl\b": "jalan",
        r"\bjln\b": "jalan",
        r"\bds\b": "desa",
        r"\bkec\b": "kecamatan",
        r"\bkel\b": "kelurahan",
        r"\bkab\b": "kabupaten",
        r"\bprop\b": "provinsi",
        r"\bno\.\s*": "nomor "
    }
    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text)
        
    # 5. Remove extra spaces and special chars
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    
    return text

@app.command()
def extract(
    upts: Optional[str] = typer.Option(None, help="Comma-separated list of ID_UPT to process (e.g. '001,002'). If empty, process all."),
    batch_size: int = typer.Option(10000, help="Number of records to process per batch"),
    resume: bool = typer.Option(True, help="Resume from last processed NOMOR_INDUK per UPT"),
    limit: Optional[int] = typer.Option(None, help="Limit total records to extract (for testing)")
):
    """
    Extract data from MariaDB, normalize it, and load into DuckDB.
    Supports incremental extraction and filtering by UPT.
    """
    engine = get_engine()
    duck_conn = get_duckdb_conn()
    state = load_state()
    
    # Parse UPT list
    target_upts = [u.strip() for u in upts.split(",")] if upts else []
    
    # If no UPTs specified, fetch all distinct UPTs from DB or just process globally?
    # Global processing is simpler if we just iterate by NOMOR_INDUK.
    # However, filtering by UPT requires building a WHERE clause.
    
    where_clauses = []
    params = {}
    
    if target_upts:
        # If filtering by UPT, we might need separate state per UPT?
        # Or just one global state if we process them sequentially?
        # But NOMOR_INDUK is global PK.
        
        # Strategy: 
        # If UPT filter is active, we iterate global NOMOR_INDUK but filter via SQL.
        # This is efficient because NOMOR_INDUK is indexed.
        
        # Construct `ID_UPT IN (...)` clause safely
        placeholders = [f":upt_{i}" for i in range(len(target_upts))]
        where_clauses.append(f"ID_UPT IN ({', '.join(placeholders)})")
        for i, u in enumerate(target_upts):
            params[f"upt_{i}"] = u
            
    # Determine start ID
    start_id = "0"
    if resume:
        # We track the global max NOMOR_INDUK processed.
        # If filtering by UPT, this might skip records if we switch UPTs later.
        # But usually ETL runs forward. 
        # Ideally, state should be keyed by the UPT filter hash, but for simplicity:
        start_id = state.get("last_processed_id", "0")
        logger.info(f"Resuming from NOMOR_INDUK > {start_id}")
    else:
        logger.info("Starting fresh (resetting state)")
        # Optionally truncate staging table?
        # duck_conn.execute("TRUNCATE TABLE staging_identitas") 
        # Using INSERT OR REPLACE/IGNORE usually better.
        pass

    params["last_id"] = start_id
    params["limit"] = batch_size
    
    base_query = """
        SELECT 
            NOMOR_INDUK, NAMA_LENGKAP, NIK, TANGGAL_LAHIR, ID_JENIS_KELAMIN, 
            ALAMAT, ID_UPT, NM_AYAH, NM_IBU
        FROM identitas 
        WHERE NOMOR_INDUK > :last_id
    """
    
    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)
        
    base_query += " ORDER BY NOMOR_INDUK ASC LIMIT :limit"

    # Check for custom query in config
    config = load_config()
    custom_query = config.get("etl", {}).get("query")
    if custom_query:
        logger.info("Using custom ETL query from config")
        # Ensure placeholders exist? Or trust user.
        # Simple check:
        if ":last_id" not in custom_query or ":limit" not in custom_query:
            logger.warning("Custom query missing :last_id or :limit placeholders. This might cause issues.")
        base_query = custom_query
    
    total_processed = 0
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True
    ) as progress:
        task = progress.add_task("Extracting...", total=None)
        
        while True:
            # Check global limit
            if limit and total_processed >= limit:
                break
                
            # Fetch Batch
            try:
                with engine.connect() as conn:
                    # Use SQLAlchemy text() for safe parameter binding
                    result = conn.execute(text(base_query), params)
                    rows = result.fetchall()
            except Exception as e:
                logger.error(f"Database error: {e}")
                break
                
            if not rows:
                logger.info("No more records found.")
                break
                
            # Process Batch
            processed_data = []
            max_id_in_batch = start_id
            
            for row in rows:
                # Normalization
                clean_nama = normalize_text(row.NAMA_LENGKAP)
                clean_alamat = normalize_text(row.ALAMAT)
                clean_ayah = normalize_text(row.NM_AYAH)
                clean_ibu = normalize_text(row.NM_IBU)
                
                processed_data.append((
                    row.NOMOR_INDUK,
                    row.NAMA_LENGKAP,
                    row.NIK,
                    row.TANGGAL_LAHIR,
                    row.ID_JENIS_KELAMIN,
                    row.ALAMAT,
                    row.ID_UPT,
                    clean_nama,
                    clean_alamat,
                    clean_ayah,
                    clean_ibu,
                    datetime.now(),
                    None
                ))
                
                if row.NOMOR_INDUK > max_id_in_batch:
                    max_id_in_batch = row.NOMOR_INDUK
            
            # Load into DuckDB
            if processed_data:
                try:
                    # executemany is cleaner for bulk insert
                    duck_conn.executemany("""
                        INSERT OR REPLACE INTO staging_identitas 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, processed_data)
                    
                    # Update state
                    params["last_id"] = max_id_in_batch
                    state["last_processed_id"] = max_id_in_batch
                    save_state(state)
                    
                    total_processed += len(processed_data)
                    progress.update(task, description=f"Extracted {total_processed} records. Last ID: {max_id_in_batch}")
                    
                except Exception as e:
                    logger.error(f"DuckDB Insert Error: {e}")
                    # Could break or continue?
                    # If we break here, we can resume from previous batch end
                    break
            
            if len(rows) < batch_size:
                # Last page
                break
                
    logger.info(f"Extraction complete. Total records: {total_processed}")
    duck_conn.close()

if __name__ == "__main__":
    app()
