"""
Data quality rules for the silver layer.

How this works:
  - Every rule is a Rule object: a name, why it exists, and a condition that is TRUE
    when a row FAILS it.
  - apply_rules() evaluates all of them and adds a failed_rules column: an array with
    the names of the rules the row did not pass. Empty array means the row is fine.
  - split_by_quality() sends rows with an empty array to the clean table and the rest
    to quarantine, keeping the original text so someone can see what came in.

What happens on failure is decided one level up, in silver.py: by default bad rows go to
quarantine and the job continues; with --on-failure fail the job stops after writing.

Why this shape: a rule is one line to read, one line to test, one line in the README.
Nothing here drops or fixes data. transform.py parses, quality.py judges, gold consumes.
"""
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window, functions as F


@dataclass(frozen=True)
class Rule:
    name      : str    # short snake_case id, ends up in the failed_rules array
    why       : str    # one sentence, shown in describe_rules() and copied into the README
    fails_when: Column # TRUE means the row does not pass


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

def _null_or_outside(col: str, low, high) -> Column:
    """TRUE when the value is missing or outside [low, high]. Missing counts as failing:
    if transform.py could not parse it, we do not want it in gold."""
    c = F.col(col)
    return c.isNull() | (c < low) | (c > high)


# The natural key of a silver row. Two rows with the same pair describe the same player
# at the same point in time, which should never happen inside one snapshot.
_SAME_PLAYER_SAME_SNAPSHOT = Window.partitionBy("player_bk", "snapshot_date")


# --------------------------------------------------------------------------- #
# The rules                                                                   #
# --------------------------------------------------------------------------- #

RULES: list[Rule] = [

    # --- Identity: without these we cannot even say who the row is about ---------------

    Rule(
        name="missing_identity",
        why="A row needs a name and a parseable birth date to build the player key. "
            "Without them it cannot be matched across snapshots.",
        fails_when=F.col("full_name").isNull() | F.col("birth_date").isNull(),
    ),

    Rule(
        name="duplicate_row",
        why="The same player appears more than once in the same snapshot with identical "
            "attributes. Almost always a duplicated line in the source file. Only the copies "
            "are flagged; the first occurrence passes.",
        fails_when=F.row_number().over(_SAME_PLAYER_SAME_SNAPSHOT.orderBy("attr_hash")) > 1,
    ),

    Rule(
        name="conflicting_duplicate",
        why="The same player appears more than once in the same snapshot with DIFFERENT "
            "attributes. We cannot tell which version is right, so all of them are held back.",
        fails_when=F.size(F.collect_set("attr_hash").over(_SAME_PLAYER_SAME_SNAPSHOT)) > 1,
    ),

    # --- Plausibility: values a real NBA player can have -------------------------------

    Rule(
        name="age_out_of_range",
        why="Age at snapshot must be between 18 and 45. Catches a wrong century pivot on the "
            "two-digit birth year (a 1984 baby read as 2084 gives a negative age).",
        fails_when=_null_or_outside("age_at_snapshot", 18, 45),
    ),

    Rule(
        name="rating_out_of_range",
        why="NBA 2K ratings are on a 40 to 99 scale. Anything else is a parsing or source error.",
        fails_when=_null_or_outside("rating", 40, 99),
    ),

    Rule(
        name="salary_invalid",
        why="Salary must be a positive amount under 60 million USD. Catches an unparsed "
            "dollar string (NULL) and impossible values like 0 or a missing digit.",
        fails_when=_null_or_outside("salary_usd", 1, 60_000_000),
    ),

    Rule(
        name="height_out_of_range",
        why="Height between 160 and 240 cm. Catches swapped units (feet read as meters) "
            "and unparsed strings.",
        fails_when=_null_or_outside("height_cm", 160, 240),
    ),

    Rule(
        name="weight_out_of_range",
        why="Weight between 60 and 160 kg. Same reasoning as height.",
        fails_when=_null_or_outside("weight_kg", 60, 160),
    ),

    Rule(
        name="jersey_out_of_range",
        why="Jersey numbers go from 0 to 99. NULL means the '#' prefix did not parse.",
        fails_when=_null_or_outside("jersey_number", 0, 99),
    ),

    # --- Consistency: fields that must agree with each other ---------------------------

    Rule(
        name="draft_inconsistent",
        why="An undrafted player has no round or pick; a drafted one has round 1-2 and pick 1-60. "
            "Draft year must be after the player turned 17 and not after the snapshot.",
        fails_when=(
            # undrafted but carries draft numbers
            (F.col("is_undrafted") & (F.col("draft_round").isNotNull() | F.col("draft_pick").isNotNull()))
            # drafted but numbers missing or impossible
            | (~F.col("is_undrafted") & (
                _null_or_outside("draft_round", 1, 2) | _null_or_outside("draft_pick", 1, 60)
            ))
            # draft year does not fit the player's life
            | F.col("draft_year").isNull()
            | (F.col("draft_year") < F.year("birth_date") + 17)
            | (F.col("draft_year") > F.year("snapshot_date"))
        ),
    ),
]


# --------------------------------------------------------------------------- #
# Applying and splitting                                                      #
# --------------------------------------------------------------------------- #

def apply_rules(typed: DataFrame, rules: list[Rule] = RULES) -> DataFrame:
    """
    Add a failed_rules ARRAY<STRING> column with the names of every rule the row fails.

    All rules are evaluated for every row, not just the first failure. Seeing that a row
    failed three rules at once is more useful than fixing them one at a time.
    """
    flags = [F.when(rule.fails_when, F.lit(rule.name)) for rule in rules]
    return typed.withColumn("failed_rules", F.array_compact(F.array(*flags)))


def split_by_quality(checked: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Separate rows into (clean, quarantine).

    clean       matches the silver_players table: helper columns removed.
    quarantine  matches silver_players_quarantine: just enough to identify the row,
                the rules it failed, and the original text from bronze.
    """
    passed = F.size("failed_rules") == 0

    clean = checked.filter(passed).drop("failed_rules", "_raw_row")

    quarantine = checked.filter(~passed).select(
        "player_bk", "snapshot_date", "game_version", "full_name",
        "failed_rules",
        F.col("_raw_row").alias("raw_row"),
        "source_file", "load_id", "processed_at",
    )

    return clean, quarantine


def describe_rules(rules: list[Rule] = RULES) -> str:
    """
    Markdown table of the rules, for the README. Keeping the documentation next to the
    code means the two cannot drift apart.
    """
    lines = ["| Rule | What it checks and why |", "|---|---|"]
    lines += [f"| `{r.name}` | {r.why} |" for r in rules]
    return "\n".join(lines)