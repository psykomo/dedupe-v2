import typer
import duckdb
import os
import json
import logging
from typing import Optional, List
from splink import Linker, DuckDBAPI, block_on
import splink.comparison_library as cl
import splink.comparison_level_library as cll

app = typer.Typer()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("dedupe.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

DUCKDB_PATH = "data/processed.duckdb"
MODEL_PATH = "data/splink_model.json"

def get_duckdb_conn():
    conn = duckdb.connect(DUCKDB_PATH)
    # Ensure processed table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_clusters (
            NOMOR_INDUK VARCHAR PRIMARY KEY,
            CLUSTER_ID BIGINT,
            CIF_NUMBER VARCHAR,
            PROCESSED_AT TIMESTAMP DEFAULT NOW()
        )
    """)
    return conn

def get_settings():
    """
    Define Splink configuration.
    """
    return {
        "link_type": "dedupe_only",
        "unique_id_column_name": "NOMOR_INDUK",
        "blocking_rules_to_generate_predictions": [
            block_on("NIK"),
            block_on("TANGGAL_LAHIR", "ID_JENIS_KELAMIN"),
            block_on("CLEAN_NAMA"),
            block_on("CLEAN_NM_IBU"),
        ],
        "comparisons": [
            cl.ExactMatch("NIK"),
            cl.ExactMatch("TANGGAL_LAHIR"),
            cl.ExactMatch("ID_JENIS_KELAMIN"),
            cl.LevenshteinAtThresholds("CLEAN_NAMA", [2]),
            cl.LevenshteinAtThresholds("CLEAN_ALAMAT", [5]),
            cl.LevenshteinAtThresholds("CLEAN_NM_AYAH", [2]),
            cl.LevenshteinAtThresholds("CLEAN_NM_IBU", [2]),
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

@app.command()
def train(sample_size: int = 50000):
    """
    Train the deduplication model using unsupervised learning (EM).
    Saves the trained model to data/splink_model.json.
    """
    conn = get_duckdb_conn()
    
    # Check if we have enough data
    try:
        count = conn.execute("SELECT COUNT(*) FROM staging_identitas").fetchone()[0]
    except duckdb.CatalogException:
        logger.error("Table staging_identitas not found. Run 'etl extract' first.")
        return

    if count == 0:
        logger.error("No data in staging_identitas. Run 'etl extract' first.")
        return

    logger.info(f"Training model on sample of {min(sample_size, count)} records...")
    
    # Load data into Splink
    df = conn.execute(f"""
        SELECT * FROM staging_identitas 
        USING SAMPLE {sample_size}
    """).df()
    
    db_api = DuckDBAPI(connection=conn)
    linker = Linker(df, get_settings(), db_api)
    
    # Estimate u (unmatch probability)
    logger.info("Estimating u probabilities...")
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    
    # Estimate m (match probability) using blocking rules
    logger.info("Estimating m probabilities...")
    # Blocking on NIK is strong signal for m
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("NIK"))
    # Blocking on Name+DOB
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("CLEAN_NAMA", "TANGGAL_LAHIR"))
    # Blocking on Mother's Name+Gender
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("CLEAN_NM_IBU", "ID_JENIS_KELAMIN"))
    
    # Save model
    # Splink 4 uses misc methods for saving/exporting or it might be on the linker directly but named differently.
    # Checking docs pattern: linker.save_model_to_json(path, overwrite=True) is standard.
    # Wait, the error `AttributeError: 'Linker' object has no attribute 'save_model_to_json'` implies 
    # the method is gone or moved.
    # In Splink 3.x it was save_model_to_json.
    # Let's try `linker.to_json()` or accessing settings object.
    
    # Try getting the model dictionary
    model_dict = linker.save_model_to_json() if hasattr(linker, "save_model_to_json") else linker._settings_obj.as_dict()
    
    with open(MODEL_PATH, "w") as f:
        json.dump(model_dict, f, indent=4)
        
    logger.info(f"Model saved to {MODEL_PATH}")

@app.command()
def run(threshold: float = 0.9, limit: Optional[int] = typer.Option(None, help="Limit number of new records to process")):
    """
    Run deduplication on unprocessed records.
    Incremental Logic:
    1. Identify new records (PROCESSED_AT IS NULL)
    2. Link New vs All (including self)
    3. Update clusters and CIFs
    """
    if not os.path.exists(MODEL_PATH):
        logger.error("Model not found. Run 'train' first.")
        return

    conn = get_duckdb_conn()
    
    # 1. Identify New Records
    limit_clause = f"LIMIT {limit}" if limit else ""
    try:
        new_records_df = conn.execute(f"""
            SELECT * FROM staging_identitas 
            WHERE PROCESSED_AT IS NULL
            {limit_clause}
        """).df()
    except duckdb.CatalogException:
        logger.error("Table staging_identitas not found.")
        return
    
    if len(new_records_df) == 0:
        logger.info("No new records to process.")
        return
        
    logger.info(f"Processing {len(new_records_df)} new records...")
    
    # 2. Load All Records
    full_df = conn.execute("SELECT * FROM staging_identitas").df()
    
    db_api = DuckDBAPI(connection=conn)
    
    # Load model settings first
    with open(MODEL_PATH, "r") as f:
        model_settings = json.load(f)
        
    # Initialize linker with TRAINED settings
    linker = Linker(full_df, model_settings, db_api)
    
    # Predict
    logger.info("Predicting matches...")
    predict_df = linker.inference.predict(threshold_match_probability=threshold)
    
    # Clustering
    logger.info("Clustering...")
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(predict_df, threshold_match_probability=threshold)
    
    # Try extracting via pandas as a robust fallback for Splink API changes
    clusters_pd = clusters.as_pandas_dataframe()
    # Write to DuckDB
    # `conn` is a duckdb connection object
    # We can use `conn.execute("CREATE TABLE ... AS SELECT ...")` with pandas dataframe
    # But DuckDB handles pandas automatically if registered?
    # No, we need to register it.
    
    clusters_table = "splink_clusters_temp"
    conn.register('clusters_df', clusters_pd)
    conn.execute(f"CREATE OR REPLACE TABLE {clusters_table} AS SELECT * FROM clusters_df")
    conn.unregister('clusters_df')
    
    # Fallback/Debug
    # print(clusters_pd.head())
    
    # Generate CIFs
    
    # Generate CIFs
    logger.info("Reconciling CIFs...")
    
    # We use a temporary table to calculate representative IDs
    conn.execute(f"""
        CREATE OR REPLACE TABLE batch_clusters AS
        SELECT 
            cluster_id, 
            NOMOR_INDUK,
            MIN(NOMOR_INDUK) OVER (PARTITION BY cluster_id) as representative_id
        FROM {clusters_table}
    """)
    
    # Upsert logic
    conn.execute("""
        INSERT OR REPLACE INTO processed_clusters (NOMOR_INDUK, CLUSTER_ID, CIF_NUMBER)
        SELECT 
            bc.NOMOR_INDUK,
            bc.cluster_id,
            COALESCE(pc.CIF_NUMBER, 'CIF-' || bc.representative_id) as CIF_NUMBER,
            NOW()
        FROM batch_clusters bc
        LEFT JOIN processed_clusters pc ON pc.NOMOR_INDUK = bc.representative_id
    """)
    
    # 4. Mark staging as processed
    # We mark ONLY THE RECORDS WE PROCESSED
    # If limit was used, we must be careful not to mark everything.
    
    # We can use the processed DataFrame IDs
    processed_ids = new_records_df["NOMOR_INDUK"].tolist()
    
    # This might be slow for large lists. Better to use a temp table.
    conn.register("processed_ids_df", new_records_df[["NOMOR_INDUK"]])
    conn.execute("""
        UPDATE staging_identitas 
        SET PROCESSED_AT = NOW() 
        WHERE NOMOR_INDUK IN (SELECT NOMOR_INDUK FROM processed_ids_df)
    """)
    conn.unregister("processed_ids_df")
    
    count_clusters = conn.execute("SELECT COUNT(DISTINCT CIF_NUMBER) FROM processed_clusters").fetchone()[0]
    logger.info(f"Deduplication complete. Total Unique Entities (CIFs): {count_clusters}")

if __name__ == "__main__":
    app()
