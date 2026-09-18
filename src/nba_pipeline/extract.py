"""
Raw CSV reader for the bronze layer.

Bronze rule: everything is read as text. No type casting, no cleaning, no filtering.

The only two things we protect against here:
  1. The file changing its columns without notice (schema drift).
  2. A single broken line killing the whole load (we keep it in _corrupt_record instead).
"""
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from nba_pipeline.exceptions import SchemaDriftError

# The 15 CSV columns, in the exact order they arrive.
# Confirmed by profiling the file. If the source changes, this is the place to update.
RAW_COLUMNS = [
    "full_name", "rating", "jersey", "team", "position", "b_day",
    "height", "weight", "salary", "country", "draft_year",
    "draft_round", "draft_peak", "college", "version",
]

CORRUPT_COL = "_corrupt_record"

# All STRING and nullable. The extra column receives lines Spark could not parse.
RAW_SCHEMA = StructType(
    [StructField(col, StringType(), True) for col in RAW_COLUMNS]
    + [StructField(CORRUPT_COL, StringType(), True)]
)


def check_header(spark: SparkSession, path: str) -> None:
    """
    Compare the file's real header against the expected columns.

    Needed because when we hand Spark a schema, it ignores header names and assigns
    by position. Without this check, if the source swaps two columns we would load
    weight into the height column with no error at all.
    """
    actual = spark.read.option("header", True).csv(path).columns
    if actual == RAW_COLUMNS:
        return

    missing = sorted(set(RAW_COLUMNS) - set(actual))
    extra = sorted(set(actual) - set(RAW_COLUMNS))
    raise SchemaDriftError(
        f"Unexpected header in {path}. "
        f"Missing: {missing or 'none'}. Extra: {extra or 'none'}. Received order: {actual}"
    )


def read_raw(spark: SparkSession, path: str, source_file: str, load_id: str) -> DataFrame:
    """
    Read the CSV as text and add lineage columns.

    Returns the 15 original columns plus:
      _corrupt_record  the full line if Spark could not parse it, NULL otherwise
      source_file      which file the row came from
      load_id          which run loaded it
      ingested_at      when
      row_hash         fingerprint of the whole row, so silver can spot exact duplicates
    """
    check_header(spark, path)

    raw = (
        spark.read
        .schema(RAW_SCHEMA)
        .option("header", True)
        .option("mode", "PERMISSIVE")                        # do not abort on a bad line
        .option("columnNameOfCorruptRecord", CORRUPT_COL)    # keep it here instead of losing it
        .csv(path)
    )

    # For the hash we treat NULL as empty string. That way two identical rows hash the same
    # even if one has NULL and the other "" in college, which look identical in the CSV.
    row_as_text = F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in RAW_COLUMNS])

    return (
        raw
        .withColumn("source_file", F.lit(source_file))
        .withColumn("load_id", F.lit(load_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("row_hash", F.sha2(row_as_text, 256))
    )