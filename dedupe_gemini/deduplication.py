import hashlib
import json
import logging
import os
from typing import Optional

import duckdb
import pandas as pd
import typer
from splink import DuckDBAPI, Linker, block_on
import splink.comparison_library as cl

app = typer.Typer()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("dedupe.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

DUCKDB_PATH = "data/processed.duckdb"
MODEL_PATH = "data/splink_model.json"


class UnionFind:
    def __init__(self):
        self.parent: dict[str, str] = {}
        self.rank: dict[str, int] = {}

    def add(self, x: str):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: str) -> str:
        self.add(x)
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a: str, b: str):
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return

        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1


def get_duckdb_conn():
    os.makedirs("data", exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)
    # Ensure processed table exists
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_clusters (
            NOMOR_INDUK VARCHAR PRIMARY KEY,
            CLUSTER_ID BIGINT,
            CIF_NUMBER VARCHAR,
            PROCESSED_AT TIMESTAMP DEFAULT NOW()
        )
    """
    )
    return conn


def get_blocking_rules():
    """
    Blocking rules tuned for higher cardinality (safer for larger datasets).
    """
    return [
        block_on("NIK"),
        block_on("CLEAN_NAMA", "TANGGAL_LAHIR"),
        block_on("CLEAN_NM_IBU", "TANGGAL_LAHIR"),
        block_on("CLEAN_NAMA", "ID_JENIS_KELAMIN", "CLEAN_NM_IBU"),
    ]


def get_em_training_rules():
    """
    Diverse rules used during EM so more parameters are estimable.
    """
    return [
        block_on("NIK"),
        block_on("CLEAN_NAMA", "TANGGAL_LAHIR"),
        block_on("CLEAN_NM_IBU", "TANGGAL_LAHIR"),
        block_on("CLEAN_NAMA", "ID_JENIS_KELAMIN", "CLEAN_NM_IBU"),
    ]


def get_settings(retain_debug_columns: bool = False):
    """
    Define Splink configuration.
    """
    return {
        "link_type": "dedupe_only",
        "unique_id_column_name": "NOMOR_INDUK",
        "blocking_rules_to_generate_predictions": get_blocking_rules(),
        "comparisons": [
            cl.ExactMatch("NIK"),
            cl.DateOfBirthComparison(
                "TANGGAL_LAHIR",
                input_is_string=False,
                datetime_thresholds=[1, 2],
                datetime_metrics=["month", "year"],
            ),
            cl.ExactMatch("ID_JENIS_KELAMIN"),
            cl.JaroWinklerAtThresholds("CLEAN_NAMA", [0.97, 0.93, 0.88]).configure(
                term_frequency_adjustments=True
            ),
            cl.JaroWinklerAtThresholds("CLEAN_ALAMAT", [0.95, 0.90, 0.85]),
            cl.JaroWinklerAtThresholds("CLEAN_NM_AYAH", [0.97, 0.93, 0.88]).configure(
                term_frequency_adjustments=True
            ),
            cl.JaroWinklerAtThresholds("CLEAN_NM_IBU", [0.97, 0.93, 0.88]).configure(
                term_frequency_adjustments=True
            ),
        ],
        # Keep these off in production-scale runs to reduce output/memory overhead.
        "retain_matching_columns": retain_debug_columns,
        "retain_intermediate_calculation_columns": retain_debug_columns,
    }


def _compute_tf_tables(linker: Linker):
    """
    Precompute TF tables for columns with TF adjustments.
    """
    for col in ["CLEAN_NAMA", "CLEAN_NM_AYAH", "CLEAN_NM_IBU"]:
        try:
            linker.table_management.compute_tf_table(col)
        except Exception as e:
            logger.warning(f"Could not compute TF table for {col}: {e}")


def _stable_cluster_id(representative_id: str) -> int:
    """
    Convert representative string id to deterministic BIGINT.
    """
    if representative_id.isdigit():
        value = int(representative_id)
        if value <= 9_223_372_036_854_775_807:
            return value

    digest = hashlib.sha1(representative_id.encode("utf-8")).hexdigest()
    return int(digest[:15], 16) % 9_223_372_036_854_775_807


def _resolve_batch_assignments(
    conn: duckdb.DuckDBPyConnection,
    new_records_df: pd.DataFrame,
    filtered_pairs_table: str,
):
    """
    Resolve CIF for a batch using connected components over (new_id <-> candidate_id) edges.

    - If a component touches existing CIF(s), reuse the lexicographically smallest CIF.
    - If a component touches multiple existing CIFs, merge them.
    - If no existing CIF is present, generate CIF from min NOMOR_INDUK in the component.
    """
    new_ids = {str(x) for x in new_records_df["NOMOR_INDUK"].tolist()}

    pairs = conn.execute(
        f"""
        SELECT CAST(new_id AS VARCHAR) AS new_id, CAST(candidate_id AS VARCHAR) AS candidate_id
        FROM {filtered_pairs_table}
    """
    ).fetchall()

    uf = UnionFind()
    for nid in new_ids:
        uf.add(nid)

    all_nodes = set(new_ids)
    for l_id, r_id in pairs:
        l_id = str(l_id)
        r_id = str(r_id)
        uf.union(l_id, r_id)
        all_nodes.add(l_id)
        all_nodes.add(r_id)

    nodes_df = pd.DataFrame({"NOMOR_INDUK": list(all_nodes)})
    conn.register("batch_component_nodes_df", nodes_df)
    existing_cif_df = conn.execute(
        """
        SELECT pc.NOMOR_INDUK, pc.CIF_NUMBER
        FROM processed_clusters pc
        INNER JOIN batch_component_nodes_df n ON n.NOMOR_INDUK = pc.NOMOR_INDUK
    """
    ).df()
    conn.unregister("batch_component_nodes_df")

    existing_cif_df = existing_cif_df.dropna(subset=["CIF_NUMBER"])
    existing_cif_by_id = dict(
        zip(
            existing_cif_df["NOMOR_INDUK"].astype(str),
            existing_cif_df["CIF_NUMBER"].astype(str),
        )
    )

    components: dict[str, set[str]] = {}
    for node in all_nodes:
        root = uf.find(node)
        components.setdefault(root, set()).add(node)

    assignment_rows = []
    merge_rows = []

    for nodes in components.values():
        new_members = [n for n in nodes if n in new_ids]
        if not new_members:
            continue

        existing_cifs = sorted(
            {
                existing_cif_by_id[n]
                for n in nodes
                if n in existing_cif_by_id and existing_cif_by_id[n]
            }
        )

        if existing_cifs:
            canonical_cif = existing_cifs[0]
            if len(existing_cifs) > 1:
                for old_cif in existing_cifs[1:]:
                    merge_rows.append((old_cif, canonical_cif))
        else:
            canonical_cif = f"CIF-{min(nodes)}"

        representative_id = min(nodes)
        cluster_id = _stable_cluster_id(representative_id)

        for nid in new_members:
            assignment_rows.append((nid, cluster_id, canonical_cif))

    assignments_df = pd.DataFrame(
        assignment_rows,
        columns=["NOMOR_INDUK", "CLUSTER_ID", "CIF_NUMBER"],
    )

    merges_df = pd.DataFrame(merge_rows, columns=["OLD_CIF", "NEW_CIF"])
    if not merges_df.empty:
        merges_df = merges_df[merges_df["OLD_CIF"] != merges_df["NEW_CIF"]].drop_duplicates()

    return assignments_df, merges_df, len(pairs)


@app.command()
def train(
    sample_size: int = 200000,
    u_max_pairs: int = 5000000,
    deterministic_recall: float = typer.Option(
        0.7,
        min=0.01,
        max=1.0,
        help="Estimated recall of deterministic rules used to estimate prior match probability.",
    ),
    retain_debug_columns: bool = typer.Option(
        False,
        help="Retain matching/intermediate columns in model outputs (useful for debugging, slower for production).",
    ),
):
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

    sample_n = min(sample_size, count)
    logger.info(f"Training model on sample of {sample_n} records...")

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE splink_training_sample AS
        SELECT *
        FROM staging_identitas
        USING SAMPLE {sample_n}
    """
    )

    db_api = DuckDBAPI(connection=conn)
    linker = Linker(
        "splink_training_sample",
        get_settings(retain_debug_columns=retain_debug_columns),
        db_api,
    )

    _compute_tf_tables(linker)

    logger.info("Estimating u probabilities...")
    linker.training.estimate_u_using_random_sampling(max_pairs=int(u_max_pairs))

    logger.info("Estimating probability_two_random_records_match...")
    linker.training.estimate_probability_two_random_records_match(
        deterministic_matching_rules=[block_on("NIK")],
        recall=deterministic_recall,
        max_rows_limit=1_000_000_000,
    )

    logger.info("Estimating m probabilities via EM...")
    for rule in get_em_training_rules():
        try:
            linker.training.estimate_parameters_using_expectation_maximisation(rule)
        except Exception as e:
            logger.warning(f"EM training step failed for rule {rule}: {e}")

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    linker.misc.save_model_to_json(MODEL_PATH, overwrite=True)
    logger.info(f"Model saved to {MODEL_PATH}")


@app.command()
def run(
    threshold: float = typer.Option(
        0.9,
        min=0.0,
        max=1.0,
        help="Match probability threshold.",
    ),
    batch_size: int = typer.Option(
        50000,
        min=1000,
        help="Number of new records processed per batch.",
    ),
    limit: Optional[int] = typer.Option(
        None,
        min=1,
        help="Optional total cap of new records to process in this run.",
    ),
    max_pairs_per_batch: int = typer.Option(
        3000000,
        min=1000,
        help="Safety cap: abort if filtered pair edges exceed this number in a batch.",
    ),
    match_weight_threshold: float = typer.Option(
        -4.0,
        help="Low-level Splink prefilter for find_matches_to_new_records.",
    ),
):
    """
    Incremental deduplication for new records only.

    Flow per batch:
    1) pull unprocessed records
    2) find matches (new vs all) using trained model
    3) build connected components for batch nodes
    4) upsert CIF assignments for new records (+ merge existing CIFs if bridged)
    5) mark batch as processed
    """
    if not os.path.exists(MODEL_PATH):
        logger.error("Model not found. Run 'train' first.")
        return

    conn = get_duckdb_conn()
    db_api = DuckDBAPI(connection=conn)

    with open(MODEL_PATH, "r") as f:
        model_settings = json.load(f)

    linker = Linker("staging_identitas", model_settings, db_api)
    _compute_tf_tables(linker)

    total_processed = 0

    while True:
        if limit is not None and total_processed >= limit:
            break

        current_batch_size = batch_size
        if limit is not None:
            current_batch_size = min(current_batch_size, limit - total_processed)

        try:
            new_records_df = conn.execute(
                f"""
                SELECT *
                FROM staging_identitas
                WHERE PROCESSED_AT IS NULL
                ORDER BY NOMOR_INDUK
                LIMIT {current_batch_size}
            """
            ).df()
        except duckdb.CatalogException:
            logger.error("Table staging_identitas not found.")
            return

        if len(new_records_df) == 0:
            if total_processed == 0:
                logger.info("No new records to process.")
            break

        logger.info(f"Processing batch of {len(new_records_df)} records...")

        conn.register("new_records_batch_df", new_records_df)

        blocking_rules = model_settings.get("blocking_rules_to_generate_predictions")
        if not blocking_rules:
            blocking_rules = get_blocking_rules()

        pairwise_predictions = linker.inference.find_matches_to_new_records(
            "new_records_batch_df",
            blocking_rules=blocking_rules,
            match_weight_threshold=match_weight_threshold,
        )

        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE batch_pairs AS
            WITH raw_pairs AS (
                SELECT
                    CAST(NOMOR_INDUK_l AS VARCHAR) AS id_l,
                    CAST(NOMOR_INDUK_r AS VARCHAR) AS id_r,
                    match_probability
                FROM {pairwise_predictions.physical_name}
                WHERE NOMOR_INDUK_l <> NOMOR_INDUK_r
                  AND match_probability >= {threshold}
            ),
            new_ids AS (
                SELECT CAST(NOMOR_INDUK AS VARCHAR) AS NOMOR_INDUK
                FROM new_records_batch_df
            )
            SELECT DISTINCT
                CASE WHEN nl.NOMOR_INDUK IS NOT NULL THEN rp.id_l ELSE rp.id_r END AS new_id,
                CASE WHEN nl.NOMOR_INDUK IS NOT NULL THEN rp.id_r ELSE rp.id_l END AS candidate_id,
                rp.match_probability
            FROM raw_pairs rp
            LEFT JOIN new_ids nl ON rp.id_l = nl.NOMOR_INDUK
            LEFT JOIN new_ids nr ON rp.id_r = nr.NOMOR_INDUK
            WHERE nl.NOMOR_INDUK IS NOT NULL OR nr.NOMOR_INDUK IS NOT NULL
        """
        )
        conn.unregister("new_records_batch_df")

        pair_count = conn.execute("SELECT COUNT(*) FROM batch_pairs").fetchone()[0]
        matched_new_count = conn.execute(
            "SELECT COUNT(DISTINCT new_id) FROM batch_pairs"
        ).fetchone()[0]

        logger.info(
            f"Batch edges above threshold: {pair_count:,} | New records with >=1 match: {matched_new_count:,}"
        )

        if pair_count > max_pairs_per_batch:
            logger.error(
                "Batch pair count exceeded safety limit (%s > %s). "
                "Reduce --batch-size or tighten blocking/threshold.",
                pair_count,
                max_pairs_per_batch,
            )
            pairwise_predictions.drop_table_from_database_and_remove_from_cache()
            raise typer.Exit(code=1)

        assignments_df, merges_df, _ = _resolve_batch_assignments(
            conn,
            new_records_df,
            "batch_pairs",
        )

        if not merges_df.empty:
            conn.register("cif_merges_df", merges_df)
            conn.execute(
                """
                UPDATE processed_clusters pc
                SET CIF_NUMBER = m.NEW_CIF
                FROM cif_merges_df m
                WHERE pc.CIF_NUMBER = m.OLD_CIF
            """
            )
            conn.unregister("cif_merges_df")
            logger.info(f"Merged {len(merges_df):,} CIF mapping(s) due to bridged components.")

        conn.register("batch_assignments_df", assignments_df)
        conn.execute(
            """
            INSERT OR REPLACE INTO processed_clusters (NOMOR_INDUK, CLUSTER_ID, CIF_NUMBER, PROCESSED_AT)
            SELECT
                NOMOR_INDUK,
                CLUSTER_ID,
                CIF_NUMBER,
                NOW()
            FROM batch_assignments_df
        """
        )
        conn.unregister("batch_assignments_df")

        conn.register("processed_ids_df", new_records_df[["NOMOR_INDUK"]])
        conn.execute(
            """
            UPDATE staging_identitas
            SET PROCESSED_AT = NOW()
            WHERE NOMOR_INDUK IN (SELECT NOMOR_INDUK FROM processed_ids_df)
        """
        )
        conn.unregister("processed_ids_df")

        pairwise_predictions.drop_table_from_database_and_remove_from_cache()

        total_processed += len(new_records_df)
        logger.info(f"Batch complete. Total processed in this run: {total_processed:,}")

    if total_processed > 0:
        count_clusters = conn.execute(
            "SELECT COUNT(DISTINCT CIF_NUMBER) FROM processed_clusters"
        ).fetchone()[0]
        logger.info(
            f"Deduplication complete. Total Unique Entities (CIFs): {count_clusters:,}"
        )


if __name__ == "__main__":
    app()
