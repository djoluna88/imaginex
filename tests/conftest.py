"""
Shared fixtures.

The transformation and quality tests run on a local Spark session configured like the
Databricks serverless runtime in the one way that matters for this code: ANSI mode on.
Under ANSI a bad cast raises instead of returning NULL, which is exactly the behaviour the
parsers in transform.py were written to survive. Testing with ANSI off would hide bugs.

Spark needs a Java runtime. Without one the session fixture fails with a clear message
instead of a JVM stack trace. The Dockerfile ships a JRE, so `docker compose run pipeline
pytest` always works.
"""
import shutil
import os

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW = os.path.join(REPO_ROOT, "data", "raw")

# One well-formed source row. Tests copy it and override the column under test.
PLAYER = {
    "full_name": "LeBron James", "rating": "97", "jersey": "#23", "team": "Los Angeles Lakers",
    "position": "F", "b_day": "12/30/84", "height": "6-9 / 2.06", "weight": "250 lbs. / 113.4 kg.",
    "salary": "$37436858", "country": "USA", "draft_year": "2003", "draft_round": "1",
    "draft_peak": "1", "college": "", "version": "NBA2k20",
}


@pytest.fixture(scope="session")
def spark():
    if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
        pytest.fail(
            "No Java runtime found. PySpark needs a JDK 17+: `brew install openjdk@17` on macOS, "
            "or run the suite in Docker: `docker compose run --rm pipeline pytest`."
        )
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("nba-pipeline-tests")
        .config("spark.sql.ansi.enabled", "true")          # serverless behaviour: casts raise on bad input
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")       # tiny data, fewer tasks, faster tests
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def bronze(spark):
    """
    Factory: list of partial source rows -> DataFrame shaped like the bronze table.

    Missing columns are filled from PLAYER, so a test only spells out what it is about:
        bronze([{"salary": "N/A"}])
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    from nba_pipeline.config import RAW_COLUMNS

    schema = StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS])

    def make(rows: list[dict], source_file: str = "test.csv", load_id: str = "test-load"):
        full = [tuple({**PLAYER, **r}.get(c) for c in RAW_COLUMNS) for r in rows]
        return (
            spark.createDataFrame(full, schema)
            .withColumn("_corrupt_record", F.lit(None).cast("string"))
            .withColumn("source_file", F.lit(source_file))
            .withColumn("load_id", F.lit(load_id))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("row_hash", F.sha2(F.concat_ws("|", *RAW_COLUMNS), 256))
        )

    return make
