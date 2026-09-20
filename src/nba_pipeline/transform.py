"""
Turn bronze text into typed silver rows.

Rule of this module: it never rejects a row. If a value cannot be parsed, the result is NULL
and quality.py decides what that means. Keeping "how do I parse this" separate from
"is this acceptable" makes both halves easier to test and to explain.

Every parser below was written against the formats found when profiling the file:
  height       "6-9 / 2.06"              feet-inches, then meters
  weight       "250 lbs. / 113.4 kg."    pounds, then kilograms (kg part sometimes has no decimal)
  salary       "$37436858"               dollar sign, no separators
  jersey       "#23"                     hash sign, one or two digits
  b_day        "12/30/84"                month/day/two-digit-year
  draft_round  "1", "2" or "Undrafted"
  draft_peak   "15" or "Undrafted"       (it means draft pick; the source misspells it)

 Note: Regular expressions was created with AI assistance, also convertions accented letters
"""
from datetime import date

from pyspark.sql import Column, DataFrame, Window, functions as F

from nba_pipeline.config import FULL_SNAPSHOT_MIN_ROWS
from nba_pipeline.extract import RAW_COLUMNS

# Columns that can legitimately change for the same player between snapshots.
# attr_hash is built only from these, so a typo fix in college does not look like a new version.
MUTABLE_COLUMNS = ["team", "jersey_number", "position", "rating", "salary_usd", "height_cm", "weight_kg"]


# --------------------------------------------------------------------------- #
# Small helpers                                                               #
# --------------------------------------------------------------------------- #

def _clean_text(col: str) -> Column:
    """Trim and collapse inner whitespace. Empty strings become NULL."""
    cleaned = F.regexp_replace(F.trim(F.col(col)), r"\s+", " ")
    return F.when(cleaned == "", None).otherwise(cleaned)


def _strip_accents(col: Column) -> Column: # It was created with IA Assistance
    """
    Replace the accented letters that actually appear in NBA rosters with their plain form.
    Spark has no built-in unaccent, and a full Unicode table would be overkill for names like
    Dončić, Jokić or Nurkić. Used only for the business key, never for display.
    """
    return F.translate(col, "áàäâãåéèëêíìïîóòöôõúùüûçñčćšžđ", "aaaaaaeeeeiiiiooooouuuucnccszd")

# --------------------------------------------------------------------------- #
# Parsers: one per messy column                                               #
# --------------------------------------------------------------------------- #
#
# Two facts about the runtime shape everything here:
#   1. Serverless runs Spark in ANSI mode: casting a malformed string is an error, not NULL.
#   2. Photon may push a cast inside a CASE WHEN and evaluate it on every row, so guarding
#      the cast with when() is not enough.
# The only construct that reliably turns bad input into NULL is try_cast. PySpark does not
# expose it on Column in every version, so the parsers build small SQL snippets with F.expr.
# Inputs are whitespace-trimmed first: the source has at least one salary with a trailing
# space ("$29542010 "), and that is cleaning, not judging. quality.py still decides.

def _trimmed(col: str) -> str:
    """SQL snippet: the column with leading/trailing whitespace removed (spaces, tabs, CR)."""
    return rf"regexp_replace(`{col}`, r'^\s+|\s+$', '')"


def _try_cast(sql_expr: str, data_type: str) -> Column:
    """SQL try_cast: malformed input becomes NULL instead of raising under ANSI mode."""
    return F.expr(f"try_cast({sql_expr} AS {data_type})")


def _regex_number(col: str, pattern: str, data_type: str) -> Column:
    """
    First capture group of `pattern` applied to the trimmed value, then try_cast.
    No match -> regexp_extract gives '' -> try_cast gives NULL. Never raises.
    """
    return _try_cast(f"regexp_extract({_trimmed(col)}, r'{pattern}', 1)", data_type)


def parse_int(col: str) -> Column:
    """"97" -> 97, " 97 " -> 97, "Undrafted" -> NULL. Used for rating, draft_year, draft numbers."""
    return _try_cast(_trimmed(col), "int")


def parse_height_cm(col: str = "height") -> Column:
    """
    "6-9 / 2.06" -> 206

    Prefer the metric part after the slash. If it is missing, fall back to feet and inches.
    Anything else stays NULL.
    """
    # Regular expressions was created with IA assistance
    meters = _regex_number(col, r"/\s*(\d+\.\d+)", "double")
    feet = _regex_number(col, r"^(\d+)-", "int")
    inches = _regex_number(col, r"^\d+-(\d+)", "int")

    from_meters = F.round(meters * 100)
    from_imperial = F.round((feet * 12 + inches) * 2.54)

    return F.coalesce(from_meters, from_imperial).cast("int")


def parse_weight_kg(col: str = "weight") -> Column:
    """
    "250 lbs. / 113.4 kg." -> 113.4
    "205 lbs. / 93 kg."    -> 93.0

    Prefer the kilogram part. Fall back to pounds * 0.4536 if it is missing.
    """
    kg = _regex_number(col, r"/\s*(\d+(?:\.\d+)?)\s*kg", "double")
    lbs = _regex_number(col, r"^(\d+)\s*lbs", "double")

    return F.coalesce(kg, F.round(lbs * 0.4536, 1)).cast("decimal(5,1)")


def parse_salary_usd(col: str = "salary") -> Column:
    """
    "$37436858" -> 37436858, "$29542010 " -> 29542010

    Strict on purpose: a dollar sign followed by digits and nothing else after trimming.
    A value like "$1,000" or "N/A" becomes NULL and the quality rule will flag it.
    """
    return _regex_number(col, r"^\$(\d+)$", "bigint")


def parse_jersey_number(col: str = "jersey") -> Column:
    """"#23" -> 23. Jersey numbers are 0 to 99, so one or two digits."""
    return _regex_number(col, r"^#(\d{1,2})$", "int")


def parse_birth_date(col: str = "b_day") -> Column:
    """
    "12/30/84" -> 1984-12-30

    Two-digit years need a pivot. Java's "yy" pattern would read 84 as 2084, which is
    obviously wrong for a basketball player. Rule used here: 30 to 99 means 1900s,
    00 to 29 means 2000s. Nobody in the NBA was born before 1930 or after 2029.
    """
    text = F.expr(_trimmed(col))
    valid = F.when(text.rlike(r"^\d{2}/\d{2}/\d{2}$"), text)     # NULL when the shape is wrong
    parts = F.split(valid, "/")                                    # split(NULL) is NULL, so are the parts

    month = parts.getItem(0).cast("int")                           # digits or NULL by construction
    day = parts.getItem(1).cast("int")
    yy = parts.getItem(2).cast("int")
    year = F.when(yy >= 30, 1900 + yy).otherwise(2000 + yy)

    # try_to_timestamp returns NULL for impossible dates like 02/30. make_date would raise under ANSI.
    iso = F.concat_ws("-", year, month, day)
    return F.try_to_timestamp(iso, F.lit("yyyy-M-d")).cast("date")


def parse_draft_number(col: str) -> Column:
    """
    "1" -> 1, "Undrafted" -> NULL. Used for both draft_round and draft_peak.
    Being undrafted is not missing data, so it is recorded separately in is_undrafted.
    """
    return parse_int(col)


def is_undrafted(col: str = "draft_round") -> Column:
    """
    TRUE when the source literally says "Undrafted".

    Wrapped in coalesce so it can never be NULL: the silver table declares this column
    NOT NULL. A NULL or unexpected value counts as drafted, and the draft_inconsistent
    quality rule will then catch a "drafted" player with no round or pick.
    """
    return F.coalesce(F.lower(F.trim(F.col(col))) == "undrafted", F.lit(False))

# --------------------------------------------------------------------------- #
# Keys and hashes                                                             #
# --------------------------------------------------------------------------- #

def build_player_bk(name_col: Column, birth_col: Column) -> Column:
    """
    Stable business key for a player: hash of normalized name + birth date.

    Why these two: neither changes when a player is traded, and together they tell apart
    players who share a name. Why normalized: "Luka Dončić" and "luka doncic" must match.
    16 hex chars is plenty for a few hundred players and keeps joins readable.
    """
    # lower() first: the translate table only lists lowercase letters, so "Šarić" must become
    # "šarić" before the accents are stripped. The other order left "Š" untouched.
    normalized_name = _strip_accents(F.lower(name_col))
    return F.substring(F.sha2(F.concat_ws("|", normalized_name, birth_col.cast("string")), 256), 1, 16)


def build_attr_hash(df: DataFrame) -> Column:
    """
    Hash of the mutable columns only. Gold compares this between snapshots: same hash means
    unchanged, different hash means the player's record changed. NULL is hashed as empty string
    so a NULL team (free agent) is stable across loads.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in MUTABLE_COLUMNS]
    return F.sha2(F.concat_ws("|", *parts), 256)


# --------------------------------------------------------------------------- #
# Snapshot handling                                                           #
# --------------------------------------------------------------------------- #

def find_unknown_versions(raw: DataFrame, snapshot_dates: dict[str, date]) -> list[str]:
    """Version values present in the data but missing from the config mapping."""
    present = {r["version"] for r in raw.select("version").distinct().collect()}
    return sorted(v for v in present if v not in snapshot_dates)


def _snapshot_date_column(snapshot_dates: dict[str, date]) -> Column:
    """Build a lookup expression from the config dict: version -> date."""
    pairs = []
    for version, snapshot_day in snapshot_dates.items():
        pairs += [F.lit(version), F.lit(snapshot_day)]
    return F.create_map(*pairs)[F.col("version")]


def _is_full_snapshot_column() -> Column:
    """
    A snapshot is "full" when it describes the whole league, not a handful of updates.
    Decided by row count per version. The threshold lives in config.
    """
    rows_in_version = F.count("*").over(Window.partitionBy("version"))
    return rows_in_version >= FULL_SNAPSHOT_MIN_ROWS


# --------------------------------------------------------------------------- #
# The main entry point                                                        #
# --------------------------------------------------------------------------- #

def clean_players(raw: DataFrame, snapshot_dates: dict[str, date], load_id: str) -> DataFrame:
    """
    Bronze rows in, silver-shaped rows out.

    Output columns match the silver_players table exactly, plus one helper column,
    _raw_row, that quality.py copies into the quarantine table and drops from clean rows.
    """
    # Step 0: freeze the original text of the row BEFORE any column is overwritten.
    # Several parsed columns keep their raw name (rating, draft_year, draft_round), so building
    # this later would mix typed values in, and coalesce(INT, '') makes Spark cast '' to BIGINT.
    raw_text = F.concat_ws(" | ", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in RAW_COLUMNS])

    # Step 1: parse everything into typed intermediate columns.
    # Order matters for the draft columns: is_undrafted must read draft_round while it is
    # still the raw text ("Undrafted"), before parse_draft_number replaces it with an int/NULL.
    typed = (
        raw
        .withColumn("_raw_row", raw_text)
        .withColumn("full_name", _clean_text("full_name"))
        .withColumn("birth_date", parse_birth_date())
        .withColumn("team", _clean_text("team"))                   # NULL here means free agent
        .withColumn("position", F.upper(_clean_text("position")))
        .withColumn("country", _clean_text("country"))
        .withColumn("college", _clean_text("college"))
        .withColumn("rating", parse_int("rating"))
        .withColumn("jersey_number", parse_jersey_number())
        .withColumn("salary_usd", parse_salary_usd())
        .withColumn("height_cm", parse_height_cm())
        .withColumn("weight_kg", parse_weight_kg())
        .withColumn("draft_year", parse_int("draft_year"))
        .withColumn("is_undrafted", is_undrafted())                      # read the raw text first
        .withColumn("draft_round", parse_draft_number("draft_round"))    # then replace it
        .withColumn("draft_pick", parse_draft_number("draft_peak"))      # rename the misspelling here
    )

    # Step 2: snapshot context. Which point in time is this row, and is it a full picture?
    with_snapshot = (
        typed
        .withColumn("game_version", F.col("version"))
        .withColumn("snapshot_date", _snapshot_date_column(snapshot_dates))
        .withColumn("is_full_snapshot", _is_full_snapshot_column())
        .withColumn("age_at_snapshot", F.floor(F.months_between("snapshot_date", "birth_date") / 12).cast("int"))
    )

    # Step 3: keys, hashes, lineage. attr_hash needs the typed mutable columns, so it comes last.
    with_keys = (
        with_snapshot
        .withColumn("player_bk", build_player_bk(F.col("full_name"), F.col("birth_date")))
        .withColumn("attr_hash", build_attr_hash(with_snapshot))
        .withColumn("load_id", F.lit(load_id))
        .withColumn("processed_at", F.current_timestamp())
    )

    # Step 4: select in the exact order of the silver_players table. Nothing extra leaks through.
    return with_keys.select(
        "player_bk", "snapshot_date", "game_version", "is_full_snapshot",
        "full_name", "birth_date", "age_at_snapshot",
        "team", "jersey_number", "position", "rating", "salary_usd", "height_cm", "weight_kg",
        "country", "draft_year", "draft_round", "draft_pick", "is_undrafted", "college",
        "attr_hash", "source_file", "load_id", "processed_at",
        "_raw_row",
    )