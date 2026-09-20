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


# The 15 CSV columns, in the exact order they arrive. Confirmed by profiling the file.
# Shared by extract.py (reads them), load.py (table DDL) and simulate.py (writes them).
# Lives here, not in extract.py, so the simulator can run locally without pyspark.
RAW_COLUMNS = [
    "full_name", "rating", "jersey", "team", "position", "b_day",
    "height", "weight", "salary", "country", "draft_year",
    "draft_round", "draft_peak", "college", "version",
]


@dataclass(frozen=True)
class BronzeConfig:
    catalog    : str                          # workspace
    schema     : str                          # nba
    raw_dir    : str                          # /Volumes/workspace/nba/source_data
    source_file: str                          # nba2k-full.csv
    table      : str = "bronze_players_raw"
    load_id    : str = field(default_factory=_new_load_id)

    @property
    def path(self) -> str:
        """Full path of the CSV inside the volume."""
        return f"{self.raw_dir}/{self.source_file}"

    def fq(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"

    @property
    def fq_table(self) -> str:
        """Bronze writes to one table, so it gets a shortcut."""
        return self.fq(self.table)


# --------------------------------------------------------------------------- #
# Silver                                                                      #
# --------------------------------------------------------------------------- #

# Which point in time each game version represents. Release dates of the games.
# The first three come with the Kaggle file. NBA2k23 is the simulated snapshot produced by
# simulate.py (see data/raw/nba2k23-simulated.csv). Adding a version here is the only
# configuration step a new snapshot needs; silver refuses versions it does not know.
SNAPSHOT_DATES: dict[str, date] = {
    "NBA2k20": date(2019, 9, 6),
    "NBA2k21": date(2020, 9, 4),
    "NBA2k22": date(2021, 9, 10),
    "NBA2k23": date(2022, 9, 9),
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

# --------------------------------------------------------------------------- #
# Gold Layer                                                                  #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class GoldConfig:
    catalog      : str  # Workspace
    schema       : str  # nba
    source_file  : str  # which silver load to pick snapshots from
    output_dir   : str  # /Volumes/workspace/nba/output, for the JSON
    silver_table : str = "silver_players"
    current_view : str = "gold_v_player_current"
    fact_table   : str = "gold_fact_player_snapshot"
    dim_player   : str = "gold_dim_player" 
    dim_snapshot : str = "gold_dim_snapshot"
    dim_team     : str = "gold_dim_team"
    json_name    : str = "players_current_state.json"
    load_id      : str = field(default_factory=_new_load_id)
   
    def fq(self, table: str) -> str:
         """Fully qualified name: catalog.schema.table"""
         return f"{self.catalog}.{self.schema}.{table}"

    @property
    def json_path(self) -> str:
        """Where the current-state JSON is written inside the output volume."""
        return f"{self.output_dir}/{self.json_name}"