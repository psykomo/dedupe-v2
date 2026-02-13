import typer
import yaml
import duckdb
from sqlalchemy import text
from dedupe_gemini.config import load_config
from dedupe_gemini.db import get_engine
from dedupe_gemini.deduplication import get_duckdb_conn
import logging

app = typer.Typer()
logger = logging.getLogger(__name__)

@app.command()
def validate():
    """
    Validate configuration, database connections, and ETL query schema.
    """
    print("--- 1. Configuration File ---")
    try:
        config = load_config()
        print("✅ config.yml loaded successfully.")
    except Exception as e:
        print(f"❌ Failed to load config.yml: {e}")
        return

    print("\n--- 2. Database Connections ---")
    
    # MariaDB
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ MariaDB connection successful.")
    except Exception as e:
        print(f"❌ MariaDB connection failed: {e}")
        
    # DuckDB
    try:
        conn = get_duckdb_conn()
        conn.execute("SELECT 1")
        print("✅ DuckDB connection successful.")
    except Exception as e:
        print(f"❌ DuckDB connection failed: {e}")

    print("\n--- 3. ETL Query Validation ---")
    custom_query = config.get("etl", {}).get("query")
    
    if custom_query:
        print("ℹ️  Using Custom Query from config.")
        query_to_test = custom_query
    else:
        print("ℹ️  Using Default Query.")
        query_to_test = """
            SELECT 
                NOMOR_INDUK, NAMA_LENGKAP, NIK, TANGGAL_LAHIR, ID_JENIS_KELAMIN, 
                ALAMAT, ID_UPT, NM_AYAH, NM_IBU
            FROM identitas 
            WHERE NOMOR_INDUK > :last_id
            ORDER BY NOMOR_INDUK ASC LIMIT :limit
        """

    # Check Placeholders
    if ":last_id" not in query_to_test or ":limit" not in query_to_test:
        print("❌ Query missing required placeholders: :last_id or :limit")
    else:
        print("✅ Query placeholders present.")

    # Check Columns
    required_columns = {
        "NOMOR_INDUK", "NAMA_LENGKAP", "NIK", "TANGGAL_LAHIR", 
        "ID_JENIS_KELAMIN", "ALAMAT", "ID_UPT", "NM_AYAH", "NM_IBU"
    }
    
    try:
        with engine.connect() as conn:
            # Run with dummy params and limit 0 to check schema
            result = conn.execute(text(query_to_test), {"last_id": "0", "limit": 0})
            columns = set(result.keys())
            
            missing = required_columns - columns
            if missing:
                print(f"❌ Query is missing required columns: {missing}")
                print(f"   Found: {columns}")
            else:
                print("✅ Query schema valid. All required columns present.")
                
    except Exception as e:
        print(f"❌ Query execution failed: {e}")

if __name__ == "__main__":
    app()
