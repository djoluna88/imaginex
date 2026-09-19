"""
Bronze task entry point.

This file is what the Databricks job runs (see resources/job.yml, spark_python_task).
The bundle uploads it together with src/ to the workspace, and serverless executes it
with the parameters declared in the YAML.

It does three things and nothing else:
  1. Parse the arguments the job passes in.
  2. Call run_bronze from the library.
  3. Turn a data error into exit code 2, so the task fails cleanly instead of with a traceback.

All the real logic lives in src/nba_pipeline. Keep this file thin.

Note on imports: serverless runs this file through exec(), so __file__ is not defined and we
cannot locate src/ relative to this script. The bundle passes the path in via --src-dir instead,
and the nba_pipeline imports happen inside main() after sys.path is updated.
"""
import argparse
import logging
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load a source CSV from the volume into the bronze table as-is.")
    parser.add_argument("--src-dir",     required=True, help="Workspace path to src/, e.g. ${workspace.file_path}/src")
    parser.add_argument("--catalog",     required=True, help="Unity Catalog name, e.g. workspace")
    parser.add_argument("--schema",      default="nba", help="Bronze schema name")
    parser.add_argument("--raw-dir",     required=True, help="Volume folder holding the CSVs")
    parser.add_argument("--source-file", required=True, help="CSV file name inside raw-dir")
    parser.add_argument("--log-level",   default="INFO")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Make nba_pipeline importable. Must happen before the imports below.
    if args.src_dir not in sys.path:
        sys.path.insert(0, args.src_dir)

    from pyspark.sql import SparkSession
    from nba_pipeline.bronze import run_bronze
    from nba_pipeline.config import BronzeConfig
    from nba_pipeline.exceptions import PipelineError

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("bronze_job")

    # On serverless the session already exists; getOrCreate just picks it up.
    spark = SparkSession.builder.getOrCreate()

    cfg = BronzeConfig(
        catalog=args.catalog,
        schema=args.schema,
        raw_dir=args.raw_dir,
        source_file=args.source_file,
    )

    try:
        stats = run_bronze(spark, cfg)
    except PipelineError as err:
        # Bad data (schema drift, empty file). Log it and exit with 2 so the job marks
        # the task as failed without a traceback that hides the real message.
        log.error("%s: %s", type(err).__name__, err)
        return 2

    print(
        f"OK | {stats.rows_written} rows -> {stats.table} | "
        f"versions {stats.versions} | corrupt {stats.corrupt_rows} | load_id {stats.load_id}"
    )
    return 0

if __name__ == "__main__":
    exit_code = main()
    if exit_code != 0:
        sys.exit(exit_code)