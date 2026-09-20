"""
Gold task entry point.

Same shape as bronze_job.py and silver_job.py: parse the arguments the job passes in,
call the library, let data errors surface with their own name. Wired in resources/job.yml.

Gold takes the snapshots of one source file from silver, oldest first, and updates the
star schema one snapshot at a time. It also writes the current-state JSON to the output volume.

The nba_pipeline imports happen inside main(), after --src-dir is added to sys.path.
Serverless runs this file through exec(), so __file__ is not available and the bundle
tells us where src/ lives instead.
"""

import argparse
import logging
import sys

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update the gold star schema with the snapshots of one silver load and export the current-state JSON."
    )
    parser.add_argument("--src-dir",     required=True, help="Workspace path to src/, e.g. ${workspace.file_path}/src")
    parser.add_argument("--catalog",     required=True, help="Unity Catalog name, e.g. imaginex")
    parser.add_argument("--schema",      default="nba", help="Schema holding silver and gold tables")
    parser.add_argument("--source-file", required=True, help="Which silver load to take snapshots from, e.g. nba2k-full.csv")
    parser.add_argument("--output-dir",  required=True, help="Volume folder for the JSON, e.g. /Volumes/imaginex/nba/output_files")
    parser.add_argument("--log-level",   default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Make nba_pipeline importable. Must happen before the imports below.
    if args.src_dir not in sys.path:
        sys.path.insert(0, args.src_dir)

    from pyspark.sql import SparkSession
    from nba_pipeline.config import GoldConfig
    from nba_pipeline.exceptions import PipelineError
    from nba_pipeline.gold import run_gold

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("gold_job")

    # On serverless the session already exists; getOrCreate just picks it up.
    spark = SparkSession.builder.getOrCreate()

    cfg = GoldConfig(
        catalog     = args.catalog,
        schema      = args.schema,
        source_file = args.source_file,
        output_dir  = args.output_dir,
    )

    try:
        stats = run_gold(spark, cfg)
    except PipelineError as err:
        # Data or config problem. Log a clean line, then re-raise so the task fails and the
        # run page shows the real exception name instead of a generic SystemExit.
        log.error("%s: %s", type(err).__name__, err)
        raise

    # Anything else (bug, permissions, network) raises on its own with a full traceback.

    for snap in stats.snapshots:
        print(f"snapshot {snap.snapshot_date} | full={snap.is_full_snapshot} | rows_in {snap.rows_in} | {snap.changes}")
    print(f"OK | {len(stats.snapshots)} snapshots | json {stats.json_path} | load_id {stats.load_id}")


if __name__ == "__main__":
    main()