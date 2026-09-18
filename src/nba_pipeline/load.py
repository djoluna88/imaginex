"""
Delta table writers.
"""
import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, functions as F

from nba_pipeline.exceptions import EmptySourceError
from nba_pipeline.extract import CORRUPT_COL, RAW_COLUMNS

log = logging.getLogger(__name__)


# Definition of different classes

@dataclass(frozen=True)
class BronzeStats:
    """Summary of one bronze load. Returned by write_bronze, printed by the job."""
    table       : str
    source_file : str
    load_id     : str
    rows_written: int
    corrupt_rows: int
    versions    : dict[str, int]       # how many rows of each game version the file carried

@dataclass(frozen=True)
class SilverStats:
    source_file     : str
    load_id         : str
    clean_rows      : int
    quarantined_rows: int
    snapshots       : dict[str, int]          # rows per snapshot_date, e.g. {"2019-09-06": 429, ...}
    failures_by_rule: dict[str, int]   # how many rows each rule caught

def ensure_bronze_table(spark: SparkSession, fq_table: str) -> None:
    """
    Create the bronze table if it does not exist. Called on every run, harmless if it already exists.

    The schema is built from RAW_COLUMNS so extract.py and this table can never drift apart.
    """
    raw_cols = ",\n            ".join(f"{col} STRING" for col in RAW_COLUMNS)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq_table} (
            {raw_cols},
            {CORRUPT_COL} STRING,
            source_file   STRING    NOT NULL,
            load_id       STRING    NOT NULL,
            ingested_at   TIMESTAMP NOT NULL,
            row_hash      STRING    NOT NULL
        )
        USING DELTA
        COMMENT 'Raw copy of each source CSV, one row per file line. Untyped, uncleaned.'
    """)


def write_bronze(df: DataFrame, fq_table: str, source_file: str, load_id: str) -> BronzeStats:
    """
    Write the DataFrame to bronze, replacing only the rows that belong to this file.

    This is what makes the load idempotent: loading the same CSV twice leaves the table
    unchanged, and loading a new CSV adds its rows without touching earlier ones.
    """
    rows = df.count()
    if rows == 0:
        raise EmptySourceError(f"{source_file} has no data rows, nothing written")

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"source_file = '{source_file}'")
        .saveAsTable(fq_table)
    )

    # Stats for the log and for the caller. Two small queries over 449 rows.
    corrupt = df.filter(F.col(CORRUPT_COL).isNotNull()).count()
    versions = {
        row["version"]: row["n"]
        for row in df.groupBy("version").agg(F.count("*").alias("n")).collect()
    }

    log.info(
        "bronze | %s <- %s | %d rows, %d corrupt, versions %s | load_id=%s",
        fq_table, source_file, rows, corrupt, versions, load_id,
    )
    return BronzeStats(fq_table, source_file, load_id, rows, corrupt, versions)

def read_bronze(spark: SparkSession, fq_table: str, source_file: str) -> DataFrame:
    """Rows of one file, exactly as bronze stored them."""
    return spark.table(fq_table).filter(F.col("source_file") == source_file)


def ensure_silver_tables(spark: SparkSession, cfg) -> None:
    """
    Two tables. The clean one is what gold reads. The quarantine one has the same columns
    plus the list of rules the row failed, and keeps the original raw text so nobody has to
    go back to bronze to understand what was wrong.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg.fq(cfg.clean_table)} (
            player_bk         STRING    NOT NULL COMMENT 'Stable player id: hash of normalized name + birth date',
            snapshot_date     DATE      NOT NULL COMMENT 'Point in time this row describes',
            game_version      STRING    NOT NULL,
            is_full_snapshot  BOOLEAN   NOT NULL COMMENT 'False when the snapshot only carries a handful of players',
            full_name         STRING    NOT NULL,
            birth_date        DATE      NOT NULL,
            age_at_snapshot   INT,
            team              STRING              COMMENT 'NULL means free agent',
            jersey_number     INT,
            position          STRING,
            rating            INT,
            salary_usd        BIGINT,
            height_cm         INT,
            weight_kg         DECIMAL(5,1),
            country           STRING,
            draft_year        INT,
            draft_round       INT                 COMMENT 'NULL when undrafted',
            draft_pick        INT                 COMMENT 'NULL when undrafted',
            is_undrafted      BOOLEAN   NOT NULL,
            college           STRING,
            attr_hash         STRING    NOT NULL COMMENT 'Hash of the columns that can change between snapshots. Gold uses it to detect modifications',
            source_file       STRING    NOT NULL,
            load_id           STRING    NOT NULL,
            processed_at      TIMESTAMP NOT NULL
        )
        USING DELTA
        COMMENT 'Typed and validated players. Grain: one row per player per snapshot.'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg.fq(cfg.quarantine_table)} (
            player_bk         STRING,
            snapshot_date     DATE,
            game_version      STRING,
            full_name         STRING,
            failed_rules      ARRAY<STRING> NOT NULL COMMENT 'Names of the rules this row did not pass',
            raw_row           STRING        NOT NULL COMMENT 'Original bronze row as text, for debugging',
            source_file       STRING        NOT NULL,
            load_id           STRING        NOT NULL,
            processed_at      TIMESTAMP     NOT NULL
        )
        USING DELTA
        COMMENT 'Rows that failed at least one quality rule. Nothing here reaches gold.'
    """)


def write_silver(clean: DataFrame, quarantine: DataFrame, cfg) -> SilverStats:
    """Replace this file's rows in both tables. Same idempotency trick as bronze."""
    where = f"source_file = '{cfg.source_file}'"

    clean.write.format("delta").mode("overwrite").option("replaceWhere", where).saveAsTable(cfg.fq(cfg.clean_table))
    quarantine.write.format("delta").mode("overwrite").option("replaceWhere", where).saveAsTable(cfg.fq(cfg.quarantine_table))

    snapshots = {str(r["snapshot_date"]): r["n"] for r in clean.groupBy("snapshot_date").agg(F.count("*").alias("n")).collect()}
    failures = {r["rule"]: r["n"] for r in quarantine.select(F.explode("failed_rules").alias("rule")).groupBy("rule").agg(F.count("*").alias("n")).collect()}

    stats = SilverStats(cfg.source_file, cfg.load_id, clean.count(), quarantine.count(), snapshots, failures)
    log.info("silver | clean=%d quarantined=%d by_rule=%s", stats.clean_rows, stats.quarantined_rows, failures)
    return stats