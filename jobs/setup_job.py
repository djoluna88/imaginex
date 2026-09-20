"""
Setup task entry point: make sure the Unity Catalog objects exist, then copy the committed
CSVs into the source volume.

Why the job creates catalog, schema and volumes instead of the bundle:
Databricks Asset Bundles create catalogs through the REST API, which requires a storage
root. Free Edition uses Default Storage, where only SQL and the UI know how to assign one.
So the first task of the job runs CREATE ... IF NOT EXISTS from serverless, which works,
and the bundle only deploys code and the job. Result: deploy, run, nothing by hand.

Everything here is idempotent. Reruns cost a few metadata calls and no copies.
"""
import argparse
import logging
import os
import shutil


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create catalog objects if missing and copy committed CSVs into the source volume.")
    parser.add_argument("--src-dir",   required=False, help="Accepted for consistency with the other tasks; not used here")
    parser.add_argument("--catalog",   required=True, help="e.g. imaginex")
    parser.add_argument("--schema",    required=True, help="e.g. nba")
    parser.add_argument("--data-dir",  required=True, help="Workspace folder holding the CSVs, e.g. ${workspace.file_path}/data/raw")
    parser.add_argument("--raw-dir",   required=True, help="Volume folder bronze reads from, e.g. /Volumes/imaginex/nba/source_data")
    parser.add_argument("--output-dir",required=True, help="Volume folder gold writes to, e.g. /Volumes/imaginex/nba/output_files")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def ensure_catalog_objects(spark, catalog: str, schema: str, raw_dir: str, output_dir: str, log) -> None:
    """Catalog, schema and the two volumes. Volume names are the last folder of each path."""
    raw_volume, output_volume = raw_dir.rstrip("/").split("/")[-1], output_dir.rstrip("/").split("/")[-1]
    statements = [
        f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT 'ImagineX code test. Everything the NBA players pipeline produces lives here.'",
        f"CREATE SCHEMA  IF NOT EXISTS {catalog}.{schema} COMMENT 'Bronze, silver and gold tables share this schema, prefixed by layer.'",
        f"CREATE VOLUME  IF NOT EXISTS {catalog}.{schema}.{raw_volume} COMMENT 'Landing zone for the source CSV and simulated snapshots.'",
        f"CREATE VOLUME  IF NOT EXISTS {catalog}.{schema}.{output_volume} COMMENT 'Current-state JSON and insight charts.'",
    ]
    for sql in statements:
        spark.sql(sql)
        log.info("ensured: %s", sql.split(" COMMENT")[0])


def copy_source_files(data_dir: str, raw_dir: str, log) -> tuple[list[str], list[str]]:
    """Copy every CSV from workspace files to the volume. Same-size files are skipped."""
    if not os.path.isdir(data_dir):
        raise FileNotFoundError(f"{data_dir} does not exist in the workspace. Is data/raw committed?")
    csvs = sorted(f for f in os.listdir(data_dir) if f.lower().endswith(".csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV files in {data_dir}. Nothing to ingest.")

    copied, skipped = [], []
    for name in csvs:
        src, dst = os.path.join(data_dir, name), os.path.join(raw_dir, name)
        if os.path.exists(dst) and os.path.getsize(dst) == os.path.getsize(src):
            skipped.append(name)
            continue
        shutil.copyfile(src, dst)
        copied.append(name)
        log.info("copied %s -> %s", name, dst)
    return copied, skipped


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log = logging.getLogger("setup_job")

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    ensure_catalog_objects(spark, args.catalog, args.schema, args.raw_dir, args.output_dir, log)
    copied, skipped = copy_source_files(args.data_dir, args.raw_dir, log)

    print(f"OK | catalog {args.catalog}.{args.schema} ready | copied {len(copied)} | skipped {len(skipped)} | target {args.raw_dir}")


if __name__ == "__main__":
    main()