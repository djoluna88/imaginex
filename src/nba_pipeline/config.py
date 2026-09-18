"""
Pipeline configuration.

One dataclass per layer. They only hold values and build paths, no logic.
Values come from the job arguments (see jobs/*.py).
"""
from dataclasses import dataclass, field
from datetime import date
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


## Silver Config


# Which point in time each game version represents. Release dates of the games.
# A simulated snapshot adds its own version value here, e.g. "SIM2023": date(2023, 9, 1).
SNAPSHOT_DATES: dict[str, date] = {
    "NBA2k20": date(2019, 9, 6),
    "NBA2k21": date(2020, 9, 4),
    "NBA2k22": date(2021, 9, 10),
}

# Below this many rows a snapshot is considered partial (threshold): an update to some players,
# not a picture of the whole league. The 2K20 file has 429, the other two have 10.
FULL_SNAPSHOT_MIN_ROWS = 300  


@dataclass(frozen=True)
class SilverConfig:
    catalog         : str
    schema          : str                       # nba
    source_file     : str                       # which bronze load to process
    bronze_table    : str = "bronze_players_raw"
    clean_table     : str = "silver_players"
    quarantine_table: str = "silver_players_quarantine"
    on_failure      : str = "quarantine"         # "quarantine": keep going, "fail": stop the job
    snapshot_dates  : dict[str, date] = field(default_factory=lambda: dict(SNAPSHOT_DATES))
    load_id         : str = field(default_factory=_new_load_id)

    def fq(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"