"""
Incremental logic of gold: classify_changes.

Frames are built by hand with only the columns the function reads. attr_hash is a plain
string here ("h1", "h2"): the function only compares hashes for equality, it never derives them.
"""
from datetime import date

import pytest
from pyspark.sql import functions as F

from nba_pipeline.gold import STATE_COLUMNS, classify_changes

D2019, D2020, D2021 = date(2019, 9, 6), date(2020, 9, 4), date(2021, 9, 10)

INCOMING_SCHEMA = ("player_bk string, snapshot_date date, team string, jersey_number int, position string, "
                   "rating int, salary_usd bigint, height_cm int, weight_kg decimal(5,1), attr_hash string")
FACT_SCHEMA = "player_bk string, snapshot_date date, attr_hash string"


def incoming(spark, snapshot, *players):
    """players: (player_bk, team, attr_hash). The state columns get fixed values."""
    rows = [(bk, snapshot, team, 23, "F", 90, 30_000_000, 206, None, h) for bk, team, h in players]
    return spark.createDataFrame(rows, INCOMING_SCHEMA)


def fact(spark, *rows):
    return spark.createDataFrame(list(rows), FACT_SCHEMA)


@pytest.fixture
def dim_team(spark):
    return spark.createDataFrame([("LAL", "Los Angeles Lakers"), ("MIA", "Miami Heat")], "team_key string, team_name string")


def classify(spark, inc, prev, dim_team, snapshot):
    out = classify_changes(inc, prev, dim_team, snapshot, "load-x")
    return {r["player_bk"]: r for r in out.collect()}


def test_first_snapshot_is_all_new(spark, dim_team):
    res = classify(spark, incoming(spark, D2019, ("p1", "Los Angeles Lakers", "h1"), ("p2", None, "h2")),
                   fact(spark), dim_team, D2019)
    assert {bk: r["change_type"] for bk, r in res.items()} == {"p1": "new", "p2": "new"}
    assert res["p1"]["prev_snapshot_date"] is None
    assert res["p1"]["team_key"] == "LAL"
    assert res["p2"]["team_key"] is None                      # free agent


def test_unchanged_modified_and_new_against_previous_state(spark, dim_team):
    prev = fact(spark, ("p1", D2019, "h1"), ("p2", D2019, "h2"))
    inc = incoming(spark, D2020, ("p1", "Los Angeles Lakers", "h1"),    # same hash
                                 ("p2", "Miami Heat", "h2-traded"),      # different hash
                                 ("p3", "Miami Heat", "h3"))             # never seen
    res = classify(spark, inc, prev, dim_team, D2020)
    assert {bk: r["change_type"] for bk, r in res.items()} == {"p1": "unchanged", "p2": "modified", "p3": "new"}
    assert res["p1"]["prev_snapshot_date"] == D2019
    assert res["p2"]["prev_snapshot_date"] == D2019
    assert res["p3"]["prev_snapshot_date"] is None


def test_compares_with_the_latest_previous_snapshot_only(spark, dim_team):
    """p1 was h1 in 2019 and h2 in 2020. Arriving as h2 in 2021 means unchanged, not modified."""
    prev = fact(spark, ("p1", D2019, "h1"), ("p1", D2020, "h2"))
    res = classify(spark, incoming(spark, D2021, ("p1", None, "h2")), prev, dim_team, D2021)
    assert res["p1"]["change_type"] == "unchanged"
    assert res["p1"]["prev_snapshot_date"] == D2020


def test_rerun_of_the_same_snapshot_gives_the_same_answer(spark, dim_team):
    """
    Idempotency: on a rerun the fact already holds this snapshot's own rows. They must be
    ignored (strictly earlier dates only), otherwise every player would compare with itself
    and come out "unchanged".
    """
    prev = fact(spark, ("p1", D2019, "h1"), ("p1", D2020, "h1-new"))    # 2020 row from the first run
    res = classify(spark, incoming(spark, D2020, ("p1", None, "h1-new")), prev, dim_team, D2020)
    assert res["p1"]["change_type"] == "modified"
    assert res["p1"]["prev_snapshot_date"] == D2019


def test_player_absent_from_this_snapshot_produces_no_row(spark, dim_team):
    prev = fact(spark, ("p1", D2019, "h1"), ("gone", D2019, "hg"))
    res = classify(spark, incoming(spark, D2020, ("p1", None, "h1")), prev, dim_team, D2020)
    assert set(res) == {"p1"}                                  # history of "gone" stays in the fact untouched


def test_output_columns_and_lineage(spark, dim_team):
    out = classify_changes(incoming(spark, D2019, ("p1", "Miami Heat", "h1")), fact(spark), dim_team, D2019, "load-42")
    assert out.columns == ["player_bk", "snapshot_date", "team_key", *STATE_COLUMNS,
                           "attr_hash", "change_type", "prev_snapshot_date", "load_id", "built_at"]
    row = out.first()
    assert row["load_id"] == "load-42" and row["team_key"] == "MIA" and row["built_at"] is not None
