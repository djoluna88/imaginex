"""
Silver layer: raw text rows -> typed, validated player snapshots.

Bronze gave us the file exactly as it arrived. Silver turns it into something you can
actually query: real numbers, real dates, a stable player id, and one row per player
per snapshot. Rows that do not pass the quality rules are set aside with a reason,
not silently dropped.

Like bronze.py, this module only orchestrates. The work happens in:
  transform.py   parsing, normalizing, building keys
  quality.py     the rules and how they split good rows from bad ones (It's part of QA)
  load.py        writing the two silver tables
"""
import logging
import time

from pyspark.sql import SparkSession

from nba_pipeline.config import SilverConfig
from nba_pipeline.exceptions import UnknownSnapshotError, ValidationError
from nba_pipeline.load import SilverStats, ensure_silver_tables, read_bronze, write_silver
from nba_pipeline.quality import apply_rules, split_by_quality
from nba_pipeline.transform import clean_players, find_unknown_versions

log = logging.getLogger(__name__)


def run_silver(spark: SparkSession, cfg: SilverConfig) -> SilverStats:
    """
    Process one bronze load (one source_file) into silver.

    Idempotent for the same reason bronze is: the writers replace only the rows
    that came from this source_file.

    Raises:
        UnknownSnapshotError  a version value has no date in cfg.snapshot_dates
        ValidationError       rows failed quality rules and cfg.on_failure is "fail"
    """
    t0 = time.perf_counter()
    log.info("silver start | source_file=%s | load_id=%s", cfg.source_file, cfg.load_id)

    ensure_silver_tables(spark, cfg)

    # 1. Pull the raw rows for this file. Still all text at this point.
    raw = read_bronze(spark, cfg.fq(cfg.bronze_table), cfg.source_file)

    # 2. Before parsing anything, make sure we know what point in time every row belongs to.
    #    An unknown version is a configuration problem, not a data problem, so we stop here.
    unknown = find_unknown_versions(raw, cfg.snapshot_dates)
    if unknown:
        raise UnknownSnapshotError(
            f"Versions {unknown} have no snapshot date. Add them to SNAPSHOT_DATES in config.py."
        )

    # 3. Parse, normalize, build keys. This is the bulk of silver and lives in transform.py.
    typed = clean_players(raw, cfg.snapshot_dates, cfg.load_id)

    # 4. Run every quality rule. Each row gets a list of the rules it failed (empty if none).
    checked = apply_rules(typed)
    clean, quarantine = split_by_quality(checked)

    # 5. Write both tables. Quarantine is written even when empty so the table always exists.
    stats = write_silver(clean, quarantine, cfg)

    log.info(
        "silver done | clean=%d quarantined=%d snapshots=%s | %.1fs",
        stats.clean_rows, stats.quarantined_rows, stats.snapshots, time.perf_counter() - t0,
    )

    # 6. Only now decide whether bad rows should fail the job. The data is already saved
    #    either way, so someone can look at the quarantine table to see what went wrong.
    if stats.quarantined_rows and cfg.on_failure == "fail":
        raise ValidationError(
            f"{stats.quarantined_rows} rows failed quality rules. "
            f"See {cfg.fq(cfg.quarantine_table)} where load_id = '{cfg.load_id}'."
        )

    return stats