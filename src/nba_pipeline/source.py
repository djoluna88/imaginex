import json
from pathlib import Path

import kagglehub
from databricks.sdk import WorkspaceClient

DATASET = "isaienkov/nba2k20-player-dataset"


def ingest(raw_dir: str, version: int, target_name: str = "nba2k-full.csv") -> str:
    """Download from Kaggle and upload to unity catalog volumne through API Files """
    w = WorkspaceClient()                          # DATABRICKS_HOST / DATABRICKS_TOKEN of .env
    manifest_path = f"{raw_dir}/_manifest.json"

    if _current_version(w, manifest_path) == version:
        return f"{raw_dir}/{target_name}"          # this is already included into this version, do nothing

    local_dir = Path(kagglehub.dataset_download(f"{DATASET}/versions/{version}"))
    csv = next(local_dir.glob("*.csv"))

    target = f"{raw_dir}/{target_name}"
    with open(csv, "rb") as f:
        w.files.upload(target, f, overwrite=True)

    manifest = json.dumps({"dataset": DATASET, "version": version, "file": target_name})
    w.files.upload(manifest_path, manifest.encode(), overwrite=True)
    return target


def _current_version(w: WorkspaceClient, manifest_path: str) -> int | None:
    try:
        return json.load(w.files.download(manifest_path).contents).get("version")
    except Exception:
        return None