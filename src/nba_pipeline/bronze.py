"""
Bronze layer: Raw data

Goal: orchestrating bronze layer in general.
Reading process is located in extract.py
Writting process is located in load.py
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
    Load file data without any modification.

    Idempotent property, see write_bronze from load.py

    Raise:
        SchemaDriftError   if the CSV header doesn't match with RAW_COLUMNS
        EmptySourceError   if the file doesn't have raw data
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