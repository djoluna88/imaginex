"""
Delta table writers.
"""
import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, functions as F

from nba_pipeline.exceptions import EmptySourceError
from nba_pipeline.extract import CORRUPT_COL, RAW_COLUMNS

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BronzeStats:
    """Summary of one bronze load. Returned by write_bronze, printed by the job."""
    table: str
    source_file: str
    load_id: str
    rows_written: int
    corrupt_rows: int
    versions: dict[str, int]       # how many rows of each game version the file carried


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