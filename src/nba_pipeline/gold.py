"""
Gold layer, incremental version: one snapshot at a time.

Silver holds the full history, one row per player per snapshot, and can be large.
Gold must not re-read it on every run. Each run takes the snapshots of one source file
and, for each snapshot, touches only what that snapshot affects:

  gold_fact_player_snapshot   append-only, one partition per snapshot_date. Reprocessing a
                              snapshot replaces its own partition and nothing else.
  gold_player_current         one row per player with the latest known state. Kept with a
                              MERGE keyed by player_bk. This is what the current-state view
                              and the JSON read, so nobody scans the fact for "latest".
  gold_dim_player             MERGE: insert new players, refresh static attributes, extend last_seen.
  gold_dim_team               MERGE: insert team names not seen before.
  gold_dim_snapshot           MERGE: upsert the row for this snapshot.

change_type (new / modified / unchanged) compares each incoming row with the same player's
previous row in the fact. "Previous" means strictly before this snapshot_date, so running
the same snapshot twice gives the same answer.
"""
import logging
import time
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from nba_pipeline.config import GoldConfig
from nba_pipeline.load import (
    GoldStats, SnapshotStats, export_current_state_json, merge_dim_player, merge_dim_team,
    merge_player_current, read_silver_snapshot, upsert_dim_snapshot, write_fact_partition,
)

log = logging.getLogger(__name__)

STATIC_COLUMNS = ["full_name", "birth_date", "country", "draft_year", "draft_round",
                  "draft_pick", "is_undrafted", "college"]
STATE_COLUMNS = ["jersey_number", "position", "rating", "salary_usd", "height_cm", "weight_kg"]


def run_gold(spark: SparkSession, cfg: GoldConfig) -> GoldStats:
    """Apply every snapshot of this source file, oldest first."""
    t0 = time.perf_counter()

    snapshots = sorted(
        r["snapshot_date"]
        for r in spark.table(cfg.fq(cfg.silver_table))
                      .filter(F.col("source_file") == cfg.source_file)
                      .select("snapshot_date").distinct().collect()
    )
    log.info("gold start | source_file=%s | snapshots=%s | load_id=%s", cfg.source_file, snapshots, cfg.load_id)

    results = [apply_snapshot(spark, cfg, s) for s in snapshots]

    json_path = export_current_state_json(spark, cfg.fq("gold_v_player_current"), cfg.output_dir)
    log.info("gold done | %d snapshots | json=%s | %.1fs", len(results), json_path, time.perf_counter() - t0)
    return GoldStats(cfg.load_id, results, json_path)


def apply_snapshot(spark: SparkSession, cfg: GoldConfig, snapshot: date) -> SnapshotStats:
    """Everything gold does for one snapshot. Five writes, each scoped to this snapshot."""
    incoming = read_silver_snapshot(spark, cfg, snapshot).cache()
    is_full = incoming.select("is_full_snapshot").first()["is_full_snapshot"]

    latest_known = _latest_processed(spark, cfg)
    if latest_known and snapshot < latest_known:
        # Allowed, but the later snapshots' change_type will not be recomputed. Documented limitation.
        log.warning("snapshot %s is older than the latest processed (%s); later change_type values may be stale",
                    snapshot, latest_known)

    # 1. Dimensions first, so the fact can look up team_key.
    merge_dim_player(spark, cfg, incoming)
    merge_dim_team(spark, cfg, incoming.select(F.col("team").alias("team_name")).filter("team_name IS NOT NULL").distinct())
    upsert_dim_snapshot(spark, cfg, incoming.groupBy("snapshot_date", "game_version", "is_full_snapshot")
                                             .agg(F.count("*").alias("players_in_file")))

    # 2. Classify against the previous row of each incoming player. Bounded read of the fact.
    fact_rows = build_fact_rows(spark, cfg, incoming, snapshot)
    fact_rows.cache()

    # 3. Replace only this snapshot's partition in the fact.
    write_fact_partition(spark, cfg, fact_rows, snapshot)

    # 4. Refresh the current-state table for the players in this snapshot.
    merge_player_current(spark, cfg, fact_rows, snapshot)

    counts = {r["change_type"]: r["n"] for r in fact_rows.groupBy("change_type").agg(F.count("*").alias("n")).collect()}
    stats = SnapshotStats(snapshot, is_full, cfg.load_id, incoming.count(), counts)
    log.info("snapshot %s | full=%s | %s", snapshot, is_full, counts)

    incoming.unpersist()
    fact_rows.unpersist()
    return stats


def build_fact_rows(spark: SparkSession, cfg: GoldConfig, incoming: DataFrame, snapshot: date) -> DataFrame:
    """
    Fact rows for one snapshot with change_type filled in.

    The previous state comes from the fact itself, restricted to the incoming players and to
    dates strictly before this snapshot. Two things make that cheap at scale:
      - the semi-join on player_bk keeps only the players we care about
      - liquid clustering on (player_bk, snapshot_date) lets Delta skip most files
    Excluding this snapshot's own date is what makes a rerun idempotent.
    """
    fact = spark.table(cfg.fq("gold_fact_player_snapshot"))
    dim_team = spark.table(cfg.fq("gold_dim_team"))

    previous = (
        fact.filter(F.col("snapshot_date") < F.lit(snapshot))
            .join(incoming.select("player_bk"), "player_bk", "left_semi")
            .withColumn("_rn", F.row_number().over(Window.partitionBy("player_bk").orderBy(F.desc("snapshot_date"))))
            .filter("_rn = 1")
            .select("player_bk", F.col("attr_hash").alias("prev_hash"), F.col("snapshot_date").alias("prev_snapshot_date"))
    )

    change_type = (
        F.when(F.col("prev_hash").isNull(), "new")
         .when(F.col("prev_hash") != F.col("attr_hash"), "modified")
         .otherwise("unchanged")
    )

    return (
        incoming
        .join(previous, "player_bk", "left")
        .join(dim_team, incoming.team == dim_team.team_name, "left")
        .withColumn("change_type", change_type)
        .select(
            "player_bk", "snapshot_date", "team_key", *STATE_COLUMNS,
            "attr_hash", "change_type", "prev_snapshot_date",
            F.lit(cfg.load_id).alias("load_id"), F.current_timestamp().alias("built_at"),
        )
    )


def _latest_processed(spark: SparkSession, cfg: GoldConfig) -> date | None:
    row = spark.table(cfg.fq("gold_dim_snapshot")).agg(F.max("snapshot_date").alias("d")).first()
    return row["d"] if row else None