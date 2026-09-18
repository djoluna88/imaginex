"""
Silver task entry point.

Same shape as bronze_job.py: parse the arguments the job passes in, call the library,
let data errors surface with their own name. See resources/job.yml for how it is wired.

The nba_pipeline imports happen inside main(), after --src-dir is added to sys.path.
Serverless runs this file through exec(), so __file__ is not available and the bundle
tells us where src/ lives instead.
"""
import argparse
import logging
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Turn one bronze load into typed, validated silver rows."
    )
    parser.add_argument("--src-dir",    required=True, help="Workspace path to src/, e.g. ${workspace.file_path}/src")
    parser.add_argument("--catalog",    required=True, help="Unity Catalog name, e.g. workspace")
    parser.add_argument("--schema",     default="nba", help="Schema holding bronze and silver tables")
    parser.add_argument("--source-file",required=True, help="Which bronze load to process, e.g. nba2k-full.csv")
    parser.add_argument(
        "--on-failure",
        choices=["quarantine", "fail"],
        default="quarantine",
        help="quarantine: keep bad rows aside and finish. fail: stop the job after writing them.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Make nba_pipeline importable. Must happen before the imports below.
    if args.src_dir not in sys.path:
        sys.path.insert(0, args.src_dir)

    from pyspark.sql import SparkSession
    from nba_pipeline.config import SilverConfig
    from nba_pipeline.exceptions import PipelineError
    from nba_pipeline.silver import run_silver

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("silver_job")

    # On serverless the session already exists; getOrCreate just picks it up.
    spark = SparkSession.builder.getOrCreate()

    cfg = SilverConfig(
        catalog=args.catalog,
        schema=args.schema,
        source_file=args.source_file,
        on_failure=args.on_failure,
    )

    try:
        stats = run_silver(spark, cfg)
    except PipelineError as err:
        # Data or config problem: unknown snapshot version, or rows failed rules with
        # --on-failure fail. Log a clean line, then re-raise so the task fails and the
        # run page shows the real exception name instead of a generic SystemExit.
        log.error("%s: %s", type(err).__name__, err)
        raise

    # Anything else (bug, permissions, network) raises on its own with a full traceback.

    print(
        f"OK | clean {stats.clean_rows} | quarantined {stats.quarantined_rows} | "
        f"snapshots {stats.snapshots} | failed_by_rule {stats.failures_by_rule or 'none'} | "
        f"load_id {stats.load_id}"
    )


if __name__ == "__main__":
    main()