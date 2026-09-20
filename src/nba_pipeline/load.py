"""
Delta table readers and writers for bronze and silver.

Table definitions live here next to the code that writes them, so a column added to
one cannot be forgotten in the other.
"""
import logging
import os
import shutil
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession, functions as F

from nba_pipeline.exceptions import EmptySourceError
from nba_pipeline.extract import CORRUPT_COL, RAW_COLUMNS

log = logging.getLogger(__name__)


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
    """Summary of one silver run. Returned by write_silver, printed by the job."""
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
            {raw_cols}
           ,{CORRUPT_COL} STRING
           ,source_file   STRING    NOT NULL
           ,load_id       STRING    NOT NULL
           ,ingested_at   TIMESTAMP NOT NULL
           ,row_hash      STRING    NOT NULL
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
            player_bk         STRING    NOT NULL COMMENT 'Stable player id: hash of normalized name + birth date'
           ,snapshot_date     DATE      NOT NULL COMMENT 'Point in time this row describes'
           ,game_version      STRING    NOT NULL
           ,is_full_snapshot  BOOLEAN   NOT NULL COMMENT 'False when the snapshot only carries a handful of players'
           ,full_name         STRING    NOT NULL
           ,birth_date        DATE      NOT NULL
           ,age_at_snapshot   INT
           ,team              STRING             COMMENT 'NULL means free agent'
           ,jersey_number     INT
           ,position          STRING
           ,rating            INT
           ,salary_usd        BIGINT
           ,height_cm         INT
           ,weight_kg         DECIMAL(5,1)
           ,country           STRING
           ,draft_year        INT
           ,draft_round       INT                 COMMENT 'NULL when undrafted'
           ,draft_pick        INT                 COMMENT 'NULL when undrafted'
           ,is_undrafted      BOOLEAN   NOT NULL
           ,college           STRING
           ,attr_hash         STRING    NOT NULL COMMENT 'Hash of the columns that can change between snapshots. Gold uses it to detect modifications'
           ,source_file       STRING    NOT NULL
           ,load_id           STRING    NOT NULL
           ,processed_at      TIMESTAMP NOT NULL
        )
        USING DELTA
        COMMENT 'Typed and validated players. Grain: one row per player per snapshot.'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg.fq(cfg.quarantine_table)} (
            player_bk         STRING
           ,snapshot_date     DATE
           ,game_version      STRING
           ,full_name         STRING
           ,failed_rules      ARRAY<STRING> NOT NULL COMMENT 'Names of the rules this row did not pass'
           ,raw_row           STRING        NOT NULL COMMENT 'Original bronze row as text, for debugging'
           ,source_file       STRING        NOT NULL
           ,load_id           STRING        NOT NULL
           ,processed_at      TIMESTAMP     NOT NULL
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


### Load logic for gold layer 

@dataclass(frozen=True)
class SnapshotStats:
    snapshot_date   : date
    is_full_snapshot: bool
    load_id         : str
    rows_in         : int
    changes         : dict[str, int]


@dataclass(frozen=True)
class GoldStats:
    load_id: str
    snapshots: list[SnapshotStats]
    json_path: str


def ensure_gold_tables(spark: SparkSession, cfg) -> None:
    """
    Called once per run. CLUSTER BY is Delta liquid clustering: it replaces PARTITIONED BY
    and lets Delta skip files by the clustered columns without creating tiny partitions.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg.fq(cfg.fact_table)} (
             player_bk          STRING NOT NULL
            ,snapshot_date      DATE NOT NULL
            ,team_key           STRING
            ,jersey_number      INT
            ,position           STRING
            ,rating             INT
            ,salary_usd         BIGINT
            ,height_cm          INT
            ,weight_kg          DECIMAL(5,1)
            ,attr_hash          STRING NOT NULL
            ,change_type        STRING NOT NULL
            ,prev_snapshot_date DATE
            ,load_id            STRING NOT NULL
            ,built_at           TIMESTAMP NOT NULL
        ) USING DELTA CLUSTER BY (snapshot_date, player_bk)
        COMMENT 'Grain: one player in one snapshot. Incremental logic'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg.fq(cfg.dim_player)} (
            player_bk             STRING NOT NULL
           ,full_name             STRING
           ,birth_date            DATE
           ,country               STRING
           ,draft_year            INT
           ,draft_round           INT
           ,draft_pick            INT
           ,is_undrafted          BOOLEAN
           ,college               STRING
           ,first_seen            DATE NOT NULL
           ,last_seen             DATE NOT NULL
           -- type 1: latest known state, overwritten on every newer snapshot
           ,current_team_name     STRING
           ,current_jersey_number INT
           ,current_position      STRING
           ,current_rating        INT
           ,current_salary_usd    BIGINT
           ,current_height_cm     INT
           ,current_weight_kg     DECIMAL(5,1)
           ,current_as_of         DATE    NOT NULL
           ,load_id               STRING  NOT NULL
           ,updated_at            TIMESTAMP NOT NULL
        ) USING DELTA CLUSTER BY (player_bk)
        COMMENT 'One row per player. Static attributes plus type-1 current state. History lives in the fact.'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg.fq(cfg.dim_team)} (
            team_key  STRING NOT NULL
           ,team_name STRING NOT NULL)
           USING DELTA
           COMMENT 'One row per team. team_key is the first 12 hex chars of sha2(team_name).'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg.fq(cfg.dim_snapshot)} (
             snapshot_date     DATE NOT NULL
            ,game_version     STRING
            ,is_full_snapshot BOOLEAN
            ,players_in_file  INT
            ,processed_at     TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {cfg.fq(cfg.current_view)} AS
        SELECT player_bk              AS player_bk
              ,full_name              AS full_name
              ,birth_date             AS birth_date
              ,country                AS country
              ,current_team_name      AS team_name
              ,current_jersey_number  AS jersey_number
              ,current_position       AS position
              ,current_rating         AS rating
              ,current_salary_usd     AS salary_usd
              ,current_height_cm      AS height_cm
              ,current_weight_kg      AS weight_kg
              ,current_as_of          AS as_of
        FROM {cfg.fq(cfg.dim_player)}
    """)


def read_silver_snapshot(spark, cfg, snapshot: date) -> DataFrame:
    return spark.table(cfg.fq(cfg.silver_table)).filter(F.col("snapshot_date") == F.lit(snapshot))


def merge_dim_player(spark, cfg, incoming: DataFrame) -> None:
    """
    One MERGE keeps the player dimension current.
      new player            -> insert, first_seen = last_seen = current_as_of = this snapshot
      known, newer snapshot -> refresh static attributes and overwrite current_* (type 1)
      known, older snapshot -> only pull first_seen back; never downgrade current_*
    """
    incoming.createOrReplaceTempView("_incoming_players")
    spark.sql(f"""
        MERGE INTO {cfg.fq(cfg.dim_player)} d
        USING _incoming_players s
            ON d.player_bk = s.player_bk

        WHEN MATCHED AND s.snapshot_date >= d.last_seen THEN
            UPDATE SET full_name             = s.full_name
                      ,birth_date            = s.birth_date
                      ,country               = s.country
                      ,draft_year            = s.draft_year
                      ,draft_round           = s.draft_round
                      ,draft_pick            = s.draft_pick
                      ,is_undrafted          = s.is_undrafted
                      ,college               = s.college
                      ,last_seen             = s.snapshot_date
                      ,current_team_name     = s.team
                      ,current_jersey_number = s.jersey_number
                      ,current_position      = s.position
                      ,current_rating        = s.rating
                      ,current_salary_usd    = s.salary_usd
                      ,current_height_cm     = s.height_cm
                      ,current_weight_kg     = s.weight_kg
                      ,current_as_of         = s.snapshot_date
                      ,load_id               = '{cfg.load_id}'
                      ,updated_at            = current_timestamp()

        WHEN MATCHED AND s.snapshot_date < d.first_seen THEN
            UPDATE SET first_seen = s.snapshot_date

        WHEN NOT MATCHED THEN
            INSERT(player_bk
                  ,full_name
                  ,birth_date
                  ,country
                  ,draft_year
                  ,draft_round
                  ,draft_pick
                  ,is_undrafted
                  ,college
                  ,first_seen
                  ,last_seen
                  ,current_team_name
                  ,current_jersey_number
                  ,current_position
                  ,current_rating
                  ,current_salary_usd
                  ,current_height_cm
                  ,current_weight_kg
                  ,current_as_of
                  ,load_id
                  ,updated_at)
            VALUES(s.player_bk
                  ,s.full_name
                  ,s.birth_date
                  ,s.country
                  ,s.draft_year
                  ,s.draft_round
                  ,s.draft_pick
                  ,s.is_undrafted
                  ,s.college
                  ,s.snapshot_date
                  ,s.snapshot_date
                  ,s.team
                  ,s.jersey_number
                  ,s.position
                  ,s.rating
                  ,s.salary_usd
                  ,s.height_cm
                  ,s.weight_kg
                  ,s.snapshot_date
                  ,'{cfg.load_id}'
                  ,current_timestamp())
        """)

def merge_dim_team(spark, cfg, teams: DataFrame) -> None:
    teams.withColumn("team_key", F.substring(F.sha2(F.col("team_name"), 256), 1, 12)).createOrReplaceTempView("_teams")
    spark.sql(f"""
        MERGE INTO {cfg.fq(cfg.dim_team)} d 
        USING _teams s 
            ON d.team_key = s.team_key
        WHEN NOT MATCHED THEN INSERT *
    """)
    
def upsert_dim_snapshot(spark, cfg, snap: DataFrame) -> None:
    snap.withColumn("processed_at", F.current_timestamp()).createOrReplaceTempView("_snap")
    spark.sql(f"""
        MERGE INTO {cfg.fq(cfg.dim_snapshot)} d USING _snap s 
            ON d.snapshot_date = s.snapshot_date
        WHEN MATCHED THEN UPDATE SET * 
        WHEN NOT MATCHED THEN INSERT *
    """)


def write_fact_partition(spark, cfg, fact_rows: DataFrame, snapshot: date) -> None:
    """Replace exactly one snapshot's rows. Other snapshots are never touched."""
    (
        fact_rows.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"snapshot_date = DATE'{snapshot}'")
        .saveAsTable(cfg.fq(cfg.fact_table))
    )


def export_current_state_json(spark, fq_view: str, output_dir: str) -> str:
    """
    At scale the current table can have millions of rows, so the driver does not collect it.
    Spark writes a single JSON part file into a staging folder and it is renamed into place.
    """
    staging = f"{output_dir}/_staging_current_state"
    spark.table(fq_view).orderBy("full_name").coalesce(1).write.mode("overwrite").json(staging)
    part = next(p for p in os.listdir(staging) if p.startswith("part-") and p.endswith(".json"))
    final = f"{output_dir}/players_current_state.json"
    shutil.move(f"{staging}/{part}", final)
    shutil.rmtree(staging)
    return final