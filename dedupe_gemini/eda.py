import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from dedupe_gemini.db import get_engine
from dedupe_gemini.deduplication import get_duckdb_conn
import typer
import os
import io

app = typer.Typer()

@app.command()
def analyze(
    sample_size: int = 100000, 
    output_dir: str = "analysis_output",
    source: str = typer.Option("mariadb", help="Source database: 'mariadb' or 'duckdb'")
):
    """
    Perform Exploratory Data Analysis on the inmate database.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    df = None
    
    if source.lower() == "mariadb":
        print(f"Connecting to MariaDB to fetch {sample_size} random records...")
        engine = get_engine()
        query = f"""
        SELECT 
            NOMOR_INDUK, NAMA_LENGKAP, NIK, TANGGAL_LAHIR, ID_JENIS_KELAMIN, 
            ALAMAT, ID_UPT, RESIDIVIS
        FROM identitas 
        ORDER BY RAND() 
        LIMIT {sample_size}
        """
        try:
            df = pl.read_database(query=query, connection=engine)
        except Exception as e:
            print(f"Error reading MariaDB: {e}")
            return
            
    elif source.lower() == "duckdb":
        print(f"Connecting to DuckDB (staging_identitas) to fetch {sample_size} random records...")
        try:
            conn = get_duckdb_conn()
            # DuckDB SAMPLE is very fast
            query = f"""
                SELECT 
                    NOMOR_INDUK, NAMA_LENGKAP, CLEAN_NAMA, NIK, TANGGAL_LAHIR, 
                    ID_JENIS_KELAMIN, ALAMAT, CLEAN_ALAMAT, ID_UPT, 
                    CLEAN_NM_AYAH, CLEAN_NM_IBU
                FROM staging_identitas 
                USING SAMPLE {sample_size}
            """
            # Polars can read from duckdb connection object if using adbc/arrow, 
            # but standard read_database expects sqlalchemy engine or connection string.
            # However, pl.read_database also supports DBAPI2 connections.
            # Let's try passing the duckdb connection directly.
            # Note: duckdb connection object from `duckdb.connect()` is DBAPI compliant.
            df = pl.read_database(query=query, connection=conn)
        except Exception as e:
            print(f"Error reading DuckDB: {e}")
            print("Make sure you have run 'dedupe etl extract' first.")
            return
    else:
        print("Invalid source. Use 'mariadb' or 'duckdb'.")
        return

    print(f"Loaded {len(df)} records into Polars DataFrame.")

    # 1. Missing Values Analysis
    print("\n--- Missing Values Analysis ---")
    null_counts = df.null_count()
    print(null_counts)
    
    # Visualize Missing Values
    plt.figure(figsize=(10, 6))
    null_pd = null_counts.to_pandas().melt(var_name="Column", value_name="Missing Count")
    sns.barplot(data=null_pd, x="Column", y="Missing Count")
    plt.title(f"Missing Values Count (Sample N={len(df)})")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/missing_values.png")
    print(f"Saved missing values plot to {output_dir}/missing_values.png")

    # 2. Duplicate Analysis (Exact Matches)
    print("\n--- Exact Duplicate Analysis ---")
    # Check for exact duplicates on NIK (which should be unique ideally)
    nik_dups = df.filter(pl.col("NIK").is_not_null()).group_by("NIK").len().filter(pl.col("len") > 1)
    print(f"Records sharing the same NIK: {nik_dups.sum().select('len').item() if not nik_dups.is_empty() else 0}")
    
    # Check for exact duplicates on Name + DOB
    name_dob_dups = df.group_by(["NAMA_LENGKAP", "TANGGAL_LAHIR"]).len().filter(pl.col("len") > 1)
    print(f"Records sharing Name + DOB: {name_dob_dups.sum().select('len').item() if not name_dob_dups.is_empty() else 0}")

    # 3. Text Length Distribution
    print("\n--- Text Length Analysis ---")
    df = df.with_columns(
        pl.col("NAMA_LENGKAP").str.len_chars().alias("name_len"),
        pl.col("ALAMAT").str.len_chars().alias("addr_len")
    )
    
    plt.figure(figsize=(12, 5))
    plt.subplot(1, 2, 1)
    sns.histplot(df["name_len"], bins=30, kde=True)
    plt.title("Name Length Distribution")
    
    plt.subplot(1, 2, 2)
    sns.histplot(df["addr_len"].drop_nulls(), bins=30, kde=True)
    plt.title("Address Length Distribution")
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/text_lengths.png")
    print(f"Saved text length plots to {output_dir}/text_lengths.png")

    # 4. Top Value Counts
    print("\n--- Top Value Counts ---")
    print("Top 5 Most Common Names:")
    print(df["NAMA_LENGKAP"].value_counts().sort("count", descending=True).head(5))
    
    print("\nTop 5 UPTs:")
    print(df["ID_UPT"].value_counts().sort("count", descending=True).head(5))

    # Save summary report
    with open(f"{output_dir}/summary.txt", "w") as f:
        f.write(f"Analysis Report (N={len(df)})\n")
        f.write("===========================\n\n")
        f.write("Missing Values:\n")
        f.write(str(null_counts) + "\n\n")
        f.write("Exact Duplicates:\n")
        f.write(f"Shared NIK: {nik_dups.height} groups\n")
        f.write(f"Shared Name+DOB: {name_dob_dups.height} groups\n")

if __name__ == "__main__":
    app()
