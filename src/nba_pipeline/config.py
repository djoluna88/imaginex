"""
Pipeline configuration.

One dataclass per layer. They only hold values and build paths, no logic.
Values come from the job arguments (see jobs/*.py).
"""
from dataclasses import dataclass, field
import uuid


def _new_load_id() -> str:
    """Short unique id per run. Lets us trace which rows each execution wrote."""
    return uuid.uuid4().hex[:12]


@dataclass(frozen=True)
class BronzeConfig:
    catalog    : str                          # workspace
    schema     : str                         # nba_bronze
    raw_dir    : str                          # /Volumes/workspace/nba/source_data
    source_file: str                          # nba2k-full.csv
    table      : str = "bronze_players_raw"
    load_id    : str = field(default_factory=_new_load_id)

    @property
    def path(self) -> str:
        """Full path of the CSV inside the volume."""
        return f"{self.raw_dir}/{self.source_file}"

    @property
    def fq_table(self) -> str:
        """Fully qualified table name: catalog.schema.table"""
        return f"{self.catalog}.{self.schema}.{self.table}"