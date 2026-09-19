"""
Bronze layer: raw copy of the source file.

This module only orchestrates. The work happens in:
  extract.py   reading the CSV as text
  load.py      writing the bronze table
"""
import logging
import time

from pyspark.sql import SparkSession

from nba_pipeline.config import BronzeConfig
from nba_pipeline.extract import read_raw
from nba_pipeline.load import BronzeStats, ensure_bronze_table, write_bronze

log = logging.getLogger(__name__)

def run_bronze(spark: SparkSession, cfg: BronzeConfig) -> BronzeStats:
    """
    Load one source file into bronze exactly as it arrived.

    Idempotent: write_bronze replaces only the rows that belong to this source_file,
    so running twice with the same file leaves the table unchanged.

    Raises:
        SchemaDriftError   the CSV header does not match RAW_COLUMNS
        EmptySourceError   the file has no data rows
    """
    t0 = time.perf_counter()
    log.info("bronze start | file=%s -> table=%s | load_id=%s", cfg.path, cfg.fq_table, cfg.load_id)

    ensure_bronze_table(spark, cfg.fq_table)
    raw = read_raw(spark, cfg.path, cfg.source_file, cfg.load_id)
    stats = write_bronze(raw, cfg.fq_table, cfg.source_file, cfg.load_id)

    log.info(
        "bronze done | rows=%d corrupt=%d versions=%s | %.1fs",
        stats.rows_written, stats.corrupt_rows, stats.versions, time.perf_counter() - t0,
    )
    return stats