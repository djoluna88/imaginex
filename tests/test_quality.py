"""
Quality rules: each one must catch the row it was written for, and nothing else.

Rows are built as bronze text and pushed through clean_players first, so a rule is tested
against what silver really sees, not against hand-typed typed values.
"""
import pytest

from nba_pipeline.config import SNAPSHOT_DATES
from nba_pipeline.quality import RULES, apply_rules, describe_rules, split_by_quality
from nba_pipeline.transform import clean_players


def failures(bronze, rows: list[dict]) -> list[list[str]]:
    """failed_rules per input row, in input order (by full_name, rating so duplicates are stable)."""
    checked = apply_rules(clean_players(bronze(rows), SNAPSHOT_DATES, "l"))
    return [sorted(r["failed_rules"]) for r in checked.orderBy("full_name", "rating").collect()]


def test_well_formed_row_passes_every_rule(bronze):
    assert failures(bronze, [{}]) == [[]]


@pytest.mark.parametrize("override, rule", [
    ({"b_day": "02/30/01"}, "missing_identity"),
    ({"full_name": "   "}, "missing_identity"),
    ({"rating": "30"}, "rating_out_of_range"),
    ({"rating": "abc"}, "rating_out_of_range"),                    # unparsed -> NULL -> fails
    ({"salary": "N/A"}, "salary_invalid"),
    ({"salary": "$0"}, "salary_invalid"),
    ({"salary": "$60000001"}, "salary_invalid"),
    ({"jersey": "#100"}, "jersey_out_of_range"),
    ({"height": "5-0 / 1.52"}, "height_out_of_range"),
    ({"weight": "100 lbs. / 45.4 kg."}, "weight_out_of_range"),
    ({"b_day": "01/01/70"}, "age_out_of_range"),                    # 49 at the 2019 snapshot
    ({"b_day": "01/01/05"}, "age_out_of_range"),                    # 14 at the 2019 snapshot
    ({"draft_round": "Undrafted", "draft_peak": "45"}, "draft_inconsistent"),
    ({"draft_round": "3"}, "draft_inconsistent"),
    ({"draft_peak": "61"}, "draft_inconsistent"),
    ({"draft_year": "1995"}, "draft_inconsistent"),                # before the player turned 17
    ({"draft_year": "2021"}, "draft_inconsistent"),                # after the 2019 snapshot
])
def test_each_rule_catches_its_case(bronze, override, rule):
    [failed] = failures(bronze, [override])
    assert rule in failed


def test_missing_identity_also_fails_age_because_age_is_null(bronze):
    """Documented behaviour: a row with no birth date fails both identity and age."""
    [failed] = failures(bronze, [{"b_day": "bad"}])
    assert failed == ["age_out_of_range", "missing_identity"]


def test_exact_duplicate_flags_only_the_copy(bronze):
    first, second = failures(bronze, [{}, {}])
    assert sorted([first, second]) == [[], ["duplicate_row"]]


def test_conflicting_duplicate_holds_back_both_rows(bronze):
    rows = failures(bronze, [{"rating": "97"}, {"rating": "92"}])
    assert all("conflicting_duplicate" in r for r in rows)
    assert sum("duplicate_row" in r for r in rows) == 1


def test_same_player_in_different_snapshots_is_not_a_duplicate(bronze):
    assert failures(bronze, [{"version": "NBA2k20"}, {"version": "NBA2k21"}]) == [[], []]


def test_split_by_quality_shapes(bronze):
    checked = apply_rules(clean_players(bronze([{}, {"salary": "N/A", "full_name": "Kawhi Leonard"}]), SNAPSHOT_DATES, "load-3"))
    clean, quarantine = split_by_quality(checked)

    assert clean.count() == 1 and quarantine.count() == 1
    assert "failed_rules" not in clean.columns and "_raw_row" not in clean.columns

    bad = quarantine.first()
    assert quarantine.columns == ["player_bk", "snapshot_date", "game_version", "full_name",
                                  "failed_rules", "raw_row", "source_file", "load_id", "processed_at"]
    assert bad["full_name"] == "Kawhi Leonard"
    assert bad["failed_rules"] == ["salary_invalid"]
    assert "N/A" in bad["raw_row"]                               # the original text survives for debugging
    assert bad["load_id"] == "load-3"


def test_rule_names_are_unique_and_documented():
    names = [r.name for r in RULES]
    assert len(names) == len(set(names))
    table = describe_rules()
    assert all(f"`{n}`" in table for n in names)
    assert all(r.why for r in RULES)
