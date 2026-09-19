# NBA players pipeline

Medallion pipeline (bronze, silver, gold) over the [NBA 2K player dataset](https://www.kaggle.com/datasets/isaienkov/nba2k20-player-dataset), built as a Databricks Asset Bundle and executed on serverless compute.

## Layout

```
databricks.yml            bundle definition and target workspace
resources/
  catalog.yml             schemas and volumes
  job.yml                 the job: bronze task -> silver task
jobs/
  bronze_job.py           entry point for the bronze task (thin, argparse only)
  silver_job.py           entry point for the silver task
src/nba_pipeline/
  source.py               download from Kaggle and upload into the volume (runs locally)
  config.py               dataclasses with the values each layer needs
  extract.py              read the CSV as text, check the header
  transform.py            parse and normalize every column, build keys
  quality.py              data quality rules and the clean / quarantine split
  load.py                 table definitions and Delta writers
  bronze.py, silver.py    orchestration of each layer
  exceptions.py           pipeline-specific errors
diagram_model.md          gold layer model (ER diagram)
```

## Layers

**Bronze.** The CSV is loaded as-is: every column as text, one row per file line, plus lineage columns (`source_file`, `load_id`, `ingested_at`, `row_hash`). Malformed lines land in `_corrupt_record` instead of failing the load. The header is compared against the expected columns before reading, since Spark assigns by position when given a schema.

**Silver.** Rows become typed and validated: heights to centimeters, weights to kilograms, salaries to integers, two-digit birth years with a pivot, a stable `player_bk` built from the normalized name and birth date, and a `snapshot_date` derived from the game version. Every row is checked against the rules below. Rows that fail go to `silver_players_quarantine` with the list of failed rules and the original raw text. By default the job continues; with `on_failure=fail` it stops after writing.

**Gold.** Dimensional model with one fact row per player per snapshot and a `change_type` column (new, modified, unchanged) computed from `attr_hash`. See [diagram_model.md](diagram_model.md).

Both writers use `replaceWhere` on `source_file`, so re-running a load for the same file is idempotent.

## Quality rules

| Rule | What it checks |
|---|---|
| `missing_identity` | Name and parseable birth date are present, otherwise no player key can be built. |
| `duplicate_row` | Same player, same snapshot, identical attributes. Only the copies are flagged. |
| `conflicting_duplicate` | Same player, same snapshot, different attributes. All copies are held back. |
| `age_out_of_range` | Age at snapshot between 18 and 45. |
| `rating_out_of_range` | Rating between 40 and 99. |
| `salary_invalid` | Positive salary under 60M USD. |
| `height_out_of_range` | Height between 160 and 240 cm. |
| `weight_out_of_range` | Weight between 60 and 160 kg. |
| `jersey_out_of_range` | Jersey number between 0 and 99. |
| `draft_inconsistent` | Undrafted players carry no round or pick; drafted ones have round 1-2, pick 1-60, and a plausible draft year. |

The table is generated from `quality.describe_rules()`, so it stays in sync with the code.

## Running it

```bash
cp .env.example .env            # fill in DATABRICKS_HOST / DATABRICKS_TOKEN
pip install -e .
python -c "from nba_pipeline.source import ingest; ingest('/Volumes/workspace/nba/source_data', version=1)"
databricks bundle deploy
databricks bundle run nba_pipeline
```

Job parameters: `source_file` (default `nba2k-full.csv`) and `on_failure` (`quarantine` or `fail`).
