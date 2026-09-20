"""
Snapshot simulator. Pure Python, no Spark: these run anywhere in well under a second.

Two things are protected here:
  1. The parsers and formatters that mirror the source's text conventions round-trip.
  2. The committed snapshot is exactly what the committed code and seed produce, so the
     CSV, its change log and the README numbers cannot drift apart unnoticed.
"""
import csv
import json
from datetime import date
from pathlib import Path

import pytest

from nba_pipeline import simulate as sim
from nba_pipeline.config import RAW_COLUMNS, SNAPSHOT_DATES
from tests.conftest import DATA_RAW, PLAYER

SNAPSHOT = date(2022, 9, 9)


# --------------------------------------------------------------------------- #
# Text conventions                                                             #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("text, expected", [
    ("12/30/84", date(1984, 12, 30)),
    ("02/16/00", date(2000, 2, 16)),
    ("01/01/30", date(1930, 1, 1)),        # pivot: 30..99 -> 1900s
    ("01/01/29", date(2029, 1, 1)),        #        00..29 -> 2000s
    ("02/30/01", None),                    # day does not exist
    ("1984-12-30", None),                  # wrong shape
    ("", None),
])
def test_parse_birth_date_matches_pipeline_pivot(text, expected):
    assert sim.parse_birth_date(text) == expected


def test_birth_date_round_trip():
    assert sim.parse_birth_date(sim.format_birth_date(date(1999, 2, 28))) == date(1999, 2, 28)


@pytest.mark.parametrize("parse, fmt, value", [
    (sim.parse_salary, sim.format_salary, 37_436_858),
    (sim.parse_jersey, sim.format_jersey, 7),
    (sim.parse_weight_kg, sim.format_weight, 113.4),
])
def test_format_then_parse_round_trips(parse, fmt, value):
    assert parse(fmt(value)) == value


def test_format_weight_keeps_both_units_consistent():
    assert sim.format_weight(113.4) == "250 lbs. / 113.4 kg."


def test_format_height_matches_source_convention():
    assert sim.format_height(81) == "6-9 / 2.06"
    assert sim.parse_height_cm("6-9 / 2.06") == 206
    assert sim.parse_height_cm("6-9") == 206            # imperial fallback, same as transform.py


def test_age_on_counts_birthdays_not_calendar_years():
    assert sim.age_on(date(1984, 12, 30), date(2022, 9, 9)) == 37
    assert sim.age_on(date(1984, 9, 9), date(2022, 9, 9)) == 38


# --------------------------------------------------------------------------- #
# Rule mirror                                                                  #
# --------------------------------------------------------------------------- #

def test_well_formed_row_has_no_violations():
    assert sim.rule_violations(dict(PLAYER), SNAPSHOT) == []


@pytest.mark.parametrize("override, rule", [
    ({"b_day": "02/30/01"}, "missing_identity"),
    ({"rating": "30"}, "rating_out_of_range"),
    ({"salary": "N/A"}, "salary_invalid"),
    ({"jersey": "#100"}, "jersey_out_of_range"),
    ({"height": "5-0 / 1.52"}, "height_out_of_range"),
    ({"weight": "100 lbs. / 45.4 kg."}, "weight_out_of_range"),
    ({"b_day": "01/01/70"}, "age_out_of_range"),
    ({"draft_round": "Undrafted", "draft_peak": "45"}, "draft_inconsistent"),
    ({"draft_year": "1995"}, "draft_inconsistent"),     # before the player turned 17
])
def test_rule_mirror_flags_the_same_rules_as_quality_py(override, rule):
    assert rule in sim.rule_violations({**PLAYER, **override}, SNAPSHOT)


# --------------------------------------------------------------------------- #
# The committed snapshot                                                       #
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def committed():
    log = json.loads(Path(DATA_RAW, "nba2k23-simulated.changes.json").read_text(encoding="utf-8"))
    with open(Path(DATA_RAW, "nba2k23-simulated.csv"), newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return log, rows


def test_committed_snapshot_is_reproducible(tmp_path, committed):
    """Same code, same seed, same bytes. If this fails, regenerate the CSV or fix the seed."""
    log, _ = committed
    out = tmp_path / "regen.csv"
    sim.SnapshotSimulator(log["version"], log["seed"], Path(DATA_RAW, log["source"]), out).run()
    assert out.read_bytes() == Path(DATA_RAW, "nba2k23-simulated.csv").read_bytes()


def test_committed_snapshot_has_the_source_header_and_one_version(committed):
    log, rows = committed
    with open(Path(DATA_RAW, "nba2k23-simulated.csv"), newline="", encoding="utf-8") as f:
        assert next(csv.reader(f)) == RAW_COLUMNS
    assert {r["version"] for r in rows} == {log["version"]}
    assert SNAPSHOT_DATES[log["version"]].isoformat() == log["snapshot_date"]


def test_change_log_counts_match_the_file(committed):
    log, rows = committed
    s = log["summary"]
    assert s["rows_written"] == len(rows)
    assert s["players_added"] == len(log["added"])
    assert s["players_removed"] == len(log["removed"])
    assert s["base_players"] - s["players_removed"] == s["players_modified"] + s["players_unchanged_in_file"]


def test_snapshot_mixes_new_unchanged_and_modified(committed):
    """The code test asks for a reasonable combination of all three."""
    log, _ = committed
    s = log["summary"]
    assert s["players_added"] >= 10
    assert s["players_removed"] >= 5
    assert 0.3 <= s["players_modified"] / s["base_players"] <= 0.7
    assert 0.3 <= s["players_unchanged_in_file"] / s["base_players"] <= 0.7


def test_rows_meant_to_be_clean_pass_the_rule_mirror(committed):
    log, rows = committed
    held_back = {sim.player_key(q) for q in log["quality_issues"]}
    for r in rows:
        if sim.player_key(r) not in held_back:
            assert sim.rule_violations(r, SNAPSHOT) == [], r


def test_simulated_snapshot_can_be_chained_as_the_next_source(tmp_path, monkeypatch, committed):
    """
    --source data/raw/nba2k23-simulated.csv must produce a valid later snapshot: the 2023
    rookies carry over as known players, whitespace noise is normalized on the way in, and
    the deliberate bad rows of 2023 are registered as inherited instead of crashing the run.
    """
    log_2023, _ = committed
    monkeypatch.setitem(SNAPSHOT_DATES, "NBA2k24", date(2023, 9, 8))
    out = tmp_path / "nba2k24-simulated.csv"
    report = sim.SnapshotSimulator("NBA2k24", 1, Path(DATA_RAW, "nba2k23-simulated.csv"), out).run()

    with out.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert {r["version"] for r in rows} == {"NBA2k24"}
    # The base is one row per player: every line of the 2023 file minus the deliberate duplicate lines.
    duplicate_lines = sum(1 for q in log_2023["quality_issues"] if q["scenario"] in ("duplicate_row", "conflicting_duplicate"))
    assert report["summary"]["base_players"] == log_2023["summary"]["rows_written"] - duplicate_lines   # 439 - 2

    names = {r["full_name"] for r in rows}
    rookies_2023 = {a["full_name"] for a in log_2023["added"]}
    assert len(rookies_2023 & names) >= 15                            # most 2023 rookies are still around
    # 2023's whitespace noise was normalized on the way in; the only noisy names are the ones 2024 added itself.
    noisy_2024 = {c["new"] for c in report["changes"] if c["scenario"] == "whitespace_noise" and c["field"] == "full_name"}
    assert not any(n != n.strip() or "  " in n for n in names - noisy_2024)

    inherited = [q for q in report["quality_issues"] if q["scenario"] == "inherited_from_source"]
    assert {q["full_name"] for q in inherited} >= {"Fred VanVleet", "Shaquille Harrison", "Jonah Bolden"}


def test_unknown_version_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="SNAPSHOT_DATES"):
        sim.SnapshotSimulator("NBA2k99", 1, Path(DATA_RAW, "nba2k-full.csv"), tmp_path / "x.csv")
