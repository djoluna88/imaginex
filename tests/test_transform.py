"""
Silver parsing: raw text -> typed columns, keys and hashes.

Each parser is exercised through a one-column DataFrame under ANSI mode, so a malformed
value has to come back as NULL, never as an exception. Then clean_players is run end to end
on a few bronze-shaped rows to check the columns, the keys and the snapshot context.
"""
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from nba_pipeline import transform
from nba_pipeline.config import SNAPSHOT_DATES
from nba_pipeline.transform import (
    build_attr_hash, build_player_bk, clean_players, find_unknown_versions, is_undrafted,
    parse_birth_date, parse_height_cm, parse_int, parse_jersey_number, parse_salary_usd, parse_weight_kg,
)


def evaluate(spark, parser, value):
    """Run a column parser over a single value and return the result."""
    df = spark.createDataFrame([(value,)], "x string")
    return df.select(parser("x").alias("out")).first()["out"]


# --------------------------------------------------------------------------- #
# Parsers                                                                      #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("text, expected", [
    ("6-9 / 2.06", 206),
    ("7-0 / 2.13", 213),
    ("6-9", 206),                 # metric part missing: feet and inches fallback
    ("garbage", None),
    (None, None),
])
def test_parse_height_cm(spark, text, expected):
    assert evaluate(spark, parse_height_cm, text) == expected


@pytest.mark.parametrize("text, expected", [
    ("250 lbs. / 113.4 kg.", Decimal("113.4")),
    ("205 lbs. / 93 kg.", Decimal("93.0")),      # kg without decimals, as in the source
    ("205 lbs.", Decimal("93.0")),                # pounds fallback
    ("heavy", None),
])
def test_parse_weight_kg(spark, text, expected):
    assert evaluate(spark, parse_weight_kg, text) == expected


@pytest.mark.parametrize("text, expected", [
    ("$37436858", 37436858),
    ("$29542010 ", 29542010),     # trailing space exists in the real file
    ("$1,000", None),             # separators are not accepted
    ("N/A", None),
    ("37436858", None),           # no dollar sign
])
def test_parse_salary_usd(spark, text, expected):
    assert evaluate(spark, parse_salary_usd, text) == expected


@pytest.mark.parametrize("text, expected", [("#23", 23), ("#0", 0), ("#100", None), ("23", None)])
def test_parse_jersey_number(spark, text, expected):
    assert evaluate(spark, parse_jersey_number, text) == expected


@pytest.mark.parametrize("text, expected", [
    ("12/30/84", date(1984, 12, 30)),
    ("02/16/00", date(2000, 2, 16)),
    ("01/01/30", date(1930, 1, 1)),   # pivot: 30 -> 1930
    ("01/01/29", date(2029, 1, 1)),   #        29 -> 2029
    ("02/30/01", None),               # impossible date
    ("1984-12-30", None),             # wrong shape
])
def test_parse_birth_date(spark, text, expected):
    assert evaluate(spark, parse_birth_date, text) == expected


@pytest.mark.parametrize("text, expected", [("97", 97), (" 97 ", 97), ("Undrafted", None), ("", None)])
def test_parse_int_never_raises_under_ansi(spark, text, expected):
    assert evaluate(spark, parse_int, text) == expected


@pytest.mark.parametrize("text, expected", [("Undrafted", True), ("undrafted ", True), ("1", False), (None, False)])
def test_is_undrafted_is_never_null(spark, text, expected):
    assert evaluate(spark, is_undrafted, text) is expected


# --------------------------------------------------------------------------- #
# Keys and hashes                                                              #
# --------------------------------------------------------------------------- #

def player_bk_of(spark, name: str, birth: str = "02/28/99") -> str:
    df = spark.createDataFrame([(name, birth)], "n string, b string")
    return df.select(build_player_bk(transform._clean_text("n"), parse_birth_date("b")).alias("k")).first()["k"]


@pytest.mark.parametrize("variant", ["Luka Dončić", "luka doncic", "LUKA DONCIC", " Luka  Doncic ", "Luka Šarić".replace("Šarić", "Doncic")])
def test_player_bk_ignores_case_accents_and_whitespace(spark, variant):
    assert player_bk_of(spark, variant) == player_bk_of(spark, "Luka Doncic")


def test_player_bk_strips_uppercase_accents(spark):
    """The name is lower-cased before accents are stripped, so 'Š' and 'š' both become 's'."""
    assert player_bk_of(spark, "Dario Šarić") == player_bk_of(spark, "dario saric")


def test_player_bk_separates_same_name_different_birth_date(spark):
    assert player_bk_of(spark, "Marcus Morris", "09/02/89") != player_bk_of(spark, "Marcus Morris", "01/01/95")


def test_attr_hash_moves_only_with_mutable_columns(spark, bronze):
    base = clean_players(bronze([{}]), SNAPSHOT_DATES, "l").first()["attr_hash"]
    traded = clean_players(bronze([{"team": "Miami Heat"}]), SNAPSHOT_DATES, "l").first()["attr_hash"]
    college_fixed = clean_players(bronze([{"college": "St. Vincent"}]), SNAPSHOT_DATES, "l").first()["attr_hash"]
    assert traded != base
    assert college_fixed == base


def test_attr_hash_treats_empty_and_null_team_alike(spark, bronze):
    """A free agent has "" in the file; after cleaning it is NULL. Both must hash the same."""
    empty = clean_players(bronze([{"team": ""}]), SNAPSHOT_DATES, "l").first()
    null = clean_players(bronze([{"team": None}]), SNAPSHOT_DATES, "l").first()
    assert empty["team"] is None and null["team"] is None
    assert empty["attr_hash"] == null["attr_hash"]


# --------------------------------------------------------------------------- #
# clean_players end to end                                                     #
# --------------------------------------------------------------------------- #

SILVER_COLUMNS = [
    "player_bk", "snapshot_date", "game_version", "is_full_snapshot",
    "full_name", "birth_date", "age_at_snapshot",
    "team", "jersey_number", "position", "rating", "salary_usd", "height_cm", "weight_kg",
    "country", "draft_year", "draft_round", "draft_pick", "is_undrafted", "college",
    "attr_hash", "source_file", "load_id", "processed_at", "_raw_row",
]


def test_clean_players_output_matches_silver_table_order(bronze):
    assert clean_players(bronze([{}]), SNAPSHOT_DATES, "l").columns == SILVER_COLUMNS


def test_clean_players_types_one_row(bronze):
    row = clean_players(bronze([{}], load_id="ignored"), SNAPSHOT_DATES, "load-7").first()
    assert row["snapshot_date"] == date(2019, 9, 6)          # NBA2k20 release date
    assert row["game_version"] == "NBA2k20"
    assert row["age_at_snapshot"] == 34                     # born 1984-12-30
    assert row["birth_date"] == date(1984, 12, 30)
    assert (row["jersey_number"], row["rating"], row["salary_usd"]) == (23, 97, 37436858)
    assert (row["height_cm"], row["weight_kg"]) == (206, Decimal("113.4"))
    assert (row["draft_round"], row["draft_pick"], row["is_undrafted"]) == (1, 1, False)
    assert row["college"] is None                           # "" becomes NULL
    assert row["load_id"] == "load-7"
    assert row["_raw_row"].startswith("LeBron James | 97 | #23")


def test_clean_players_undrafted_player(bronze):
    row = clean_players(bronze([{"draft_round": "Undrafted", "draft_peak": "Undrafted"}]), SNAPSHOT_DATES, "l").first()
    assert row["is_undrafted"] is True
    assert row["draft_round"] is None and row["draft_pick"] is None


def test_clean_players_normalizes_text(bronze):
    row = clean_players(bronze([{"full_name": " Spencer  Dinwiddie ", "position": "g-f", "salary": "$10605600 "}]), SNAPSHOT_DATES, "l").first()
    assert row["full_name"] == "Spencer Dinwiddie"
    assert row["position"] == "G-F"
    assert row["salary_usd"] == 10605600


def test_is_full_snapshot_uses_the_row_threshold(bronze, monkeypatch):
    monkeypatch.setattr(transform, "FULL_SNAPSHOT_MIN_ROWS", 2)
    rows = bronze([
        {"version": "NBA2k20"}, {"full_name": "Kawhi Leonard", "version": "NBA2k20"},
        {"version": "NBA2k21"},
    ])
    flags = {r["game_version"]: r["is_full_snapshot"] for r in clean_players(rows, SNAPSHOT_DATES, "l").collect()}
    assert flags == {"NBA2k20": True, "NBA2k21": False}


def test_find_unknown_versions(bronze):
    rows = bronze([{"version": "NBA2k20"}, {"version": "NBA2k99"}])
    assert find_unknown_versions(rows, SNAPSHOT_DATES) == ["NBA2k99"]
    assert find_unknown_versions(bronze([{}]), SNAPSHOT_DATES) == []
