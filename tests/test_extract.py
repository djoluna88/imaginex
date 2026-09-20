"""
Bronze reader: header protection and lineage columns.
"""
import pytest

from nba_pipeline.config import RAW_COLUMNS
from nba_pipeline.exceptions import SchemaDriftError
from nba_pipeline.extract import CORRUPT_COL, check_header, read_raw
from tests.conftest import PLAYER


def write_csv(path, header, rows):
    lines = [",".join(header)] + [",".join(r.get(c, "") for c in header) for r in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def test_check_header_accepts_the_expected_columns(spark, tmp_path):
    check_header(spark, write_csv(tmp_path / "ok.csv", RAW_COLUMNS, [PLAYER]))


def test_check_header_rejects_swapped_columns(spark, tmp_path):
    """Spark assigns by position when given a schema, so a swap would silently mix columns."""
    swapped = list(RAW_COLUMNS)
    swapped[6], swapped[7] = swapped[7], swapped[6]                 # height <-> weight
    with pytest.raises(SchemaDriftError, match="Received order"):
        check_header(spark, write_csv(tmp_path / "swapped.csv", swapped, [PLAYER]))


def test_check_header_names_missing_and_extra_columns(spark, tmp_path):
    header = [c for c in RAW_COLUMNS if c != "college"] + ["nickname"]
    with pytest.raises(SchemaDriftError, match=r"Missing: \['college'\]. Extra: \['nickname'\]"):
        check_header(spark, write_csv(tmp_path / "drift.csv", header, [PLAYER]))


def test_read_raw_keeps_text_and_adds_lineage(spark, tmp_path):
    path = write_csv(tmp_path / "two.csv", RAW_COLUMNS, [PLAYER, {**PLAYER, "full_name": "Kawhi Leonard"}])
    df = read_raw(spark, path, "two.csv", "load-1")

    assert df.columns == RAW_COLUMNS + [CORRUPT_COL, "source_file", "load_id", "ingested_at", "row_hash"]
    assert [f.dataType.simpleString() for f in df.schema if f.name in RAW_COLUMNS] == ["string"] * len(RAW_COLUMNS)

    rows = df.collect()
    assert len(rows) == 2
    assert {r["source_file"] for r in rows} == {"two.csv"}
    assert {r["load_id"] for r in rows} == {"load-1"}
    assert all(r[CORRUPT_COL] is None for r in rows)
    assert rows[0]["salary"] == "$37436858"                          # untouched text, no cast


def test_row_hash_is_identical_for_identical_lines(spark, tmp_path):
    path = write_csv(tmp_path / "dup.csv", RAW_COLUMNS, [PLAYER, PLAYER, {**PLAYER, "rating": "96"}])
    hashes = [r["row_hash"] for r in read_raw(spark, path, "dup.csv", "l").collect()]
    assert hashes[0] == hashes[1]
    assert hashes[0] != hashes[2]
