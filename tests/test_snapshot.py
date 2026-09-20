"""
Oracle test: replay the committed snapshots through the real transformation, quality and
gold logic, and compare with the numbers the simulator predicted in its change log.

This closes the loop between the two halves of the project. The simulator says "silver
should keep 431 rows, quarantine 8, and gold should report 20 new / N modified / M unchanged".
Here the same CSVs go through transform.py, quality.py and gold.classify_changes on a local
Spark, without Delta or Databricks, and the counts must agree. If a parser, a rule or the
change detection drifts, or if someone regenerates the snapshot without updating the log,
this fails.
"""
import json
import os

import pytest
from pyspark.sql import functions as F

from nba_pipeline.config import SNAPSHOT_DATES
from nba_pipeline.extract import read_raw
from nba_pipeline.gold import classify_changes
from nba_pipeline.quality import apply_rules, split_by_quality
from nba_pipeline.transform import clean_players
from tests.conftest import DATA_RAW

SIMULATED = "nba2k23-simulated.csv"
SOURCE = "nba2k-full.csv"


@pytest.fixture(scope="module")
def expected():
    with open(os.path.join(DATA_RAW, "nba2k23-simulated.changes.json"), encoding="utf-8") as f:
        return json.load(f)["expected_pipeline_results"]


@pytest.fixture(scope="module")
def silver(spark):
    """(clean, quarantine) of both committed files, exactly as the silver task would build them."""
    def run(name):
        raw = read_raw(spark, os.path.join(DATA_RAW, name), name, f"load-{name}")
        return split_by_quality(apply_rules(clean_players(raw, SNAPSHOT_DATES, f"load-{name}")))

    src_clean, src_quar = run(SOURCE)
    sim_clean, sim_quar = run(SIMULATED)
    return {"source": (src_clean, src_quar), "simulated": (sim_clean, sim_quar)}


def test_silver_row_counts_match_the_change_log(silver, expected):
    clean, quarantine = silver["simulated"]
    assert clean.count() == expected["silver_players_rows"]
    assert quarantine.count() == expected["silver_players_quarantine_rows"]


def test_quarantine_failures_by_rule_match_the_change_log(silver, expected):
    _, quarantine = silver["simulated"]
    by_rule = {r["rule"]: r["n"] for r in quarantine.select(F.explode("failed_rules").alias("rule"))
                                                  .groupBy("rule").agg(F.count("*").alias("n")).collect()}
    assert by_rule == expected["quarantine_failures_by_rule"]


def test_source_file_quarantines_only_the_inherited_row(silver):
    """The Kaggle file itself carries one bad row: a draft year before the player turned 17."""
    clean, quarantine = silver["source"]
    assert clean.count() == 448
    assert [(r["full_name"], r["failed_rules"]) for r in quarantine.collect()] == [("Fred VanVleet", ["draft_inconsistent"])]


def test_simulated_snapshot_is_a_full_snapshot(silver):
    clean, _ = silver["simulated"]
    assert clean.select("is_full_snapshot").distinct().collect() == [(True,)]


def test_gold_change_types_match_the_change_log(spark, silver, expected):
    """Replay every snapshot oldest first, exactly like run_gold, and check the last one."""
    all_clean = silver["source"][0].unionByName(silver["simulated"][0])
    dim_team = (all_clean.filter("team IS NOT NULL").select(F.col("team").alias("team_name")).distinct()
                         .withColumn("team_key", F.substring(F.sha2(F.col("team_name"), 256), 1, 12)))
    fact = spark.createDataFrame([], "player_bk string, snapshot_date date, attr_hash string")

    results = {}
    for snapshot in sorted(SNAPSHOT_DATES.values()):
        incoming = all_clean.filter(F.col("snapshot_date") == F.lit(snapshot))
        rows = classify_changes(incoming, fact, dim_team, snapshot, "replay")
        results[snapshot] = {r["change_type"]: r["n"] for r in rows.groupBy("change_type").agg(F.count("*").alias("n")).collect()}
        fact = fact.unionByName(rows.select("player_bk", "snapshot_date", "attr_hash"))

    assert results[SNAPSHOT_DATES["NBA2k20"]] == {"new": 428}                    # 429 players minus the inherited bad row
    assert "new" not in results[SNAPSHOT_DATES["NBA2k21"]]                        # 10 updates of known players
    assert results[SNAPSHOT_DATES["NBA2k23"]] == expected["gold_change_type"]


def test_respelled_names_keep_their_player_key(silver):
    """'Luka Doncic' in the source and 'Luka Dončić' in the snapshot are the same player_bk."""
    src, sim = silver["source"][0], silver["simulated"][0]
    src_key = src.filter("full_name = 'Luka Doncic'").select("player_bk").distinct().first()["player_bk"]
    sim_key = sim.filter("full_name = 'Luka Dončić'").select("player_bk").first()["player_bk"]
    assert src_key == sim_key
