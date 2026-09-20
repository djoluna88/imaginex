"""
Snapshot simulator: build a later point-in-time CSV from the committed source file.

The Kaggle file is the initial snapshot (NBA2k20 for the whole league, plus 10 updated
players each in NBA2k21 and NBA2k22). The pipeline needs at least one more snapshot to
prove it can tell new, unchanged and modified players apart across loads. This module
generates that snapshot deterministically, so the CSV can be regenerated bit for bit
from the seed and every reviewer sees the same file.

Runs locally, without Spark or pandas: csv + random from the standard library.

    python -m nba_pipeline.simulate --version NBA2k23

Starting point: the latest known row of every player (429 of them), taking the NBA2k21 and
NBA2k22 updates over the NBA2k20 row. A simulated snapshot can itself be the source of the
next one (--source data/raw/nba2k23-simulated.csv), which chains the history: rookies of the
previous snapshot are veterans in the next, and its deliberate bad rows carry over as
"inherited" issues. From there the simulator applies scenarios chosen to exercise a specific
part of the pipeline:

  unchanged          most players. Same attributes, so gold marks them "unchanged".
  rating_change      ratings drift with age: young players climb, veterans decline.
  trade              new team (and usually a new jersey). Exercises dim_team and attr_hash.
  new_contract       salary up or down.
  signed / released  free agents pick up a team; a few players lose theirs (team -> empty).
  weight_change      a few kilos up or down. weight_kg is part of attr_hash.
  position_change    one or two players listed at a new position.
  name_respelled     "Luka Doncic" -> "Luka Dončić". The name text changes but the business key
                     must not, since player_bk strips accents. Gold should see "unchanged".
  whitespace_noise   trailing / doubled spaces in name and salary. transform.py must trim them.
  retired            players aged 38+ at the snapshot date disappear from the file.
  left_league        a few low-rated players also disappear.
  rookie             new players drafted in the snapshot year. Gold marks them "new".
  undrafted_signing  new players who were never drafted. Exercises the draft rules.

Plus rows meant to land in silver_players_quarantine, so the quality layer has work to do:

  duplicate_row          an exact copy of a line (only the copy is quarantined).
  conflicting_duplicate  the same player twice with different ratings (both held back).
  salary_invalid         salary "N/A".
  jersey_out_of_range    jersey "#100".
  missing_identity       a rookie with the impossible birth date 02/30.
  draft_inconsistent     an undrafted signing that still carries a draft pick.

Next to the CSV the simulator writes <name>.changes.json with every change it made and
the counts the pipeline is expected to report (gold change_type, quarantine rules). That
file is the documentation of the snapshot and the oracle the README numbers come from.

Assumptions: birth dates, heights, draft data and colleges are static; ratings, salaries,
teams, jerseys, positions and weights are the attributes that legitimately move between
snapshots (they are exactly MUTABLE_COLUMNS in transform.py). Rookies and signings have
fictional names, so the file makes no claims about real people beyond the source data.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from nba_pipeline.config import RAW_COLUMNS, SNAPSHOT_DATES

log = logging.getLogger(__name__)

Row = dict[str, str]

# --------------------------------------------------------------------------- #
# Tunables. Fractions apply to the players that survive the retirement step.  #
# --------------------------------------------------------------------------- #

RETIREMENT_AGE      = 38     # age at the snapshot date from which a player leaves the file
N_LEFT_LEAGUE       = 8      # additional departures among the lowest-rated players
SHARE_RATING_CHANGE = 0.30
SHARE_TRADE         = 0.10
SHARE_NEW_CONTRACT  = 0.12
SHARE_SIGNED        = 0.60   # of the current free agents
SHARE_RELEASED      = 0.03   # of players who have a team
SHARE_WEIGHT_CHANGE = 0.05
N_POSITION_CHANGE   = 2
N_NAME_RESPELLED    = 3
N_WHITESPACE_NOISE  = 2
N_ROOKIES           = 18
N_UNDRAFTED_SIGNING = 2
MAX_SALARY_USD      = 55_000_000   # below the 60M ceiling in quality.py

# Real spellings of names the source writes without diacritics. Applied only when present.
DIACRITIC_SPELLINGS = {
    "Luka Doncic": "Luka Dončić",
    "Nikola Jokic": "Nikola Jokić",
    "Bojan Bogdanovic": "Bojan Bogdanović",
    "Goran Dragic": "Goran Dragić",
    "Jusuf Nurkic": "Jusuf Nurkić",
    "Nikola Vucevic": "Nikola Vučević",
    "Dennis Schroder": "Dennis Schröder",
    "Dario Saric": "Dario Šarić",
}

# Fictional rookie names. Combined at random; any combination that already exists is skipped.
FIRST_NAMES = ["Malik", "Devin", "Jalen", "Tyrese", "Kobe", "Andre", "Marcus", "Isaiah", "Jaylen",
               "Cade", "Emeka", "Terrence", "Darius", "Keon", "Zion", "Tariq", "Ousmane", "Mateo",
               "Nikola", "Luka", "Deni", "Kai", "Rayan", "Josh", "Cameron", "Trey", "Jordan", "Elijah"]
LAST_NAMES  = ["Okafor", "Whitfield", "Castellanos", "Brannigan", "Duarte", "Kimani", "Vasquez",
               "Holloway", "Petrovic", "Adebayo", "Lindqvist", "Marchetti", "Okoro", "Beaumont",
               "Sandoval", "Thibodeaux", "Nakamura", "Oyelaran", "Fitzgerald", "Radulovic",
               "Mbeki", "Delacroix", "Hargrove", "Sablan", "Kowalczyk", "Ferreira", "Winslow", "Achebe"]
COUNTRIES_INTL = ["Spain", "Serbia", "Australia", "Argentina", "Lithuania", "France", "Nigeria", "Canada"]

LBS_PER_KG = 2.20462


# --------------------------------------------------------------------------- #
# Parsing and formatting the source's text conventions                         #
# --------------------------------------------------------------------------- #

def parse_birth_date(text: str) -> date | None:
    """"12/30/84" -> 1984-12-30 with the same century pivot as transform.py. None if invalid."""
    if not re.fullmatch(r"\d{2}/\d{2}/\d{2}", text.strip()):
        return None
    month, day, yy = (int(p) for p in text.strip().split("/"))
    year = 1900 + yy if yy >= 30 else 2000 + yy
    try:
        return date(year, month, day)
    except ValueError:
        return None


def format_birth_date(d: date) -> str:
    return f"{d.month:02d}/{d.day:02d}/{d.year % 100:02d}"


def parse_salary(text: str) -> int | None:
    m = re.fullmatch(r"\$(\d+)", text.strip())
    return int(m.group(1)) if m else None


def format_salary(usd: int) -> str:
    return f"${usd}"


def parse_jersey(text: str) -> int | None:
    m = re.fullmatch(r"#(\d{1,2})", text.strip())
    return int(m.group(1)) if m else None


def format_jersey(n: int) -> str:
    return f"#{n}"


def parse_weight_kg(text: str) -> float | None:
    m = re.search(r"/\s*(\d+(?:\.\d+)?)\s*kg", text)
    return float(m.group(1)) if m else None


def format_weight(kg: float) -> str:
    """113.4 -> "250 lbs. / 113.4 kg." Pounds are derived so both halves stay consistent."""
    return f"{round(kg * LBS_PER_KG)} lbs. / {kg:.1f} kg."


def parse_height_cm(text: str) -> int | None:
    m = re.search(r"/\s*(\d+\.\d+)", text)
    if m:
        return round(float(m.group(1)) * 100)
    m = re.fullmatch(r"(\d+)-(\d+).*", text.strip())
    return round((int(m.group(1)) * 12 + int(m.group(2))) * 2.54) if m else None


def format_height(total_inches: int) -> str:
    """81 -> "6-9 / 2.06" """
    return f"{total_inches // 12}-{total_inches % 12} / {total_inches * 0.0254:.2f}"


def age_on(birth: date, on: date) -> int:
    return on.year - birth.year - ((on.month, on.day) < (birth.month, birth.day))


def player_key(row: Row) -> tuple[str, str]:
    """Same identity transform.py uses: normalized name + birth date text."""
    return (re.sub(r"\s+", " ", row["full_name"]).strip().lower(), row["b_day"].strip())


# --------------------------------------------------------------------------- #
# Plausibility check, mirroring the ranges in quality.py                       #
# --------------------------------------------------------------------------- #

def rule_violations(row: Row, snapshot: date) -> list[str]:
    """
    Names of the single-row quality rules this row would fail in silver. Used to guarantee
    that rows meant to be clean really are, and to label the rows meant for quarantine.
    Duplicate rules need the whole file and are handled separately.
    """
    failed = []
    birth = parse_birth_date(row["b_day"])
    name = row["full_name"].strip()
    if not name or birth is None:
        failed.append("missing_identity")

    def outside(value, low, high):
        return value is None or value < low or value > high

    age = age_on(birth, snapshot) if birth else None
    rating = int(row["rating"]) if row["rating"].strip().isdigit() else None
    if outside(age, 18, 45):                                   failed.append("age_out_of_range")
    if outside(rating, 40, 99):                                failed.append("rating_out_of_range")
    if outside(parse_salary(row["salary"]), 1, 60_000_000):    failed.append("salary_invalid")
    if outside(parse_height_cm(row["height"]), 160, 240):      failed.append("height_out_of_range")
    if outside(parse_weight_kg(row["weight"]), 60, 160):       failed.append("weight_out_of_range")
    if outside(parse_jersey(row["jersey"]), 0, 99):            failed.append("jersey_out_of_range")

    undrafted = row["draft_round"].strip().lower() == "undrafted"
    rnd = int(row["draft_round"]) if row["draft_round"].strip().isdigit() else None
    pick = int(row["draft_peak"]) if row["draft_peak"].strip().isdigit() else None
    year = int(row["draft_year"]) if row["draft_year"].strip().isdigit() else None
    draft_bad = (
        (undrafted and (rnd is not None or pick is not None))
        or (not undrafted and (outside(rnd, 1, 2) or outside(pick, 1, 60)))
        or year is None
        or (birth is not None and year < birth.year + 17)
        or year > snapshot.year
    )
    if draft_bad:
        failed.append("draft_inconsistent")
    return failed


# --------------------------------------------------------------------------- #
# Change log                                                                   #
# --------------------------------------------------------------------------- #

@dataclass
class Change:
    full_name: str
    b_day    : str
    scenario : str
    field    : str = ""
    old      : str = ""
    new      : str = ""


@dataclass
class SimulationLog:
    source        : str
    output        : str
    version       : str
    snapshot_date : str
    seed          : int
    base_players  : int
    changes       : list[Change] = field(default_factory=list)   # modifications to surviving players
    removed       : list[Change] = field(default_factory=list)
    added         : list[Change] = field(default_factory=list)
    quality_issues: list[Change] = field(default_factory=list)   # rows meant for quarantine

    def to_dict(self, rows_written: int, expected: dict) -> dict:
        modified_players = {(c.full_name, c.b_day) for c in self.changes if c.scenario not in NON_ATTRIBUTE_SCENARIOS}
        return {
            "source": self.source, "output": self.output, "version": self.version,
            "snapshot_date": self.snapshot_date, "seed": self.seed,
            "how_to_reproduce": f"python -m nba_pipeline.simulate --version {self.version} --seed {self.seed}",
            "summary": {
                "base_players": self.base_players,
                "rows_written": rows_written,
                "players_removed": len(self.removed),
                "players_added": len(self.added),
                "players_modified": len(modified_players),
                "players_unchanged_in_file": self.base_players - len(self.removed) - len(modified_players),   # before quality rules
                "quality_issue_rows": len(self.quality_issues),
                "changes_by_scenario": _count_by(self.changes, "scenario"),
                "removed_by_reason": _count_by(self.removed, "scenario"),
                "added_by_scenario": _count_by(self.added, "scenario"),
                "quality_issues_by_scenario": _count_by(self.quality_issues, "scenario"),
            },
            "expected_pipeline_results": expected,
            "removed": [c.__dict__ for c in self.removed],
            "added": [c.__dict__ for c in self.added],
            "changes": [c.__dict__ for c in self.changes],
            "quality_issues": [c.__dict__ for c in self.quality_issues],
        }


# Scenarios that touch text the pipeline normalizes away, so attr_hash does not move.
NON_ATTRIBUTE_SCENARIOS = {"name_respelled", "whitespace_noise"}


def _count_by(items: list[Change], attr: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        counts[getattr(item, attr)] = counts.get(getattr(item, attr), 0) + 1
    return dict(sorted(counts.items()))


# --------------------------------------------------------------------------- #
# The simulator                                                                #
# --------------------------------------------------------------------------- #

class SnapshotSimulator:
    """Holds the RNG, the snapshot context and the log. One instance per generated file."""

    def __init__(self, version: str, seed: int, source: Path, output: Path):
        if version not in SNAPSHOT_DATES:
            raise ValueError(f"{version!r} has no date in SNAPSHOT_DATES (config.py). Add it first.")
        self.version = version
        self.snapshot = SNAPSHOT_DATES[version]
        self.rng = random.Random(seed)
        self.source, self.output = source, output
        self.log = SimulationLog(source.name, output.name, version, self.snapshot.isoformat(), seed, 0)
        self.held_back_keys: set[tuple[str, str]] = set()     # players silver will quarantine entirely
        self.inherited_keys: set[tuple[str, str]] = set()     # subset of the above that was already bad in the source

    # -- input -------------------------------------------------------------- #

    def load_base(self) -> list[Row]:
        """Latest known row per player, ordered by the version date, then stripped of its version."""
        with self.source.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != RAW_COLUMNS:
                raise ValueError(f"{self.source} header is {reader.fieldnames}, expected {RAW_COLUMNS}")
            rows = list(reader)

        known = [v for v in {r["version"] for r in rows} if v not in SNAPSHOT_DATES]
        if known:
            raise ValueError(f"Source has versions with no snapshot date: {known}")

        # Later versions win. Whitespace is normalized on the way in so a snapshot that was itself
        # simulated (and carries whitespace noise on purpose) can be chained as the next source.
        latest: dict[tuple[str, str], Row] = {}
        for row in sorted(rows, key=lambda r: SNAPSHOT_DATES[r["version"]]):
            latest[player_key(row)] = {c: re.sub(r"\s+", " ", row[c]).strip() for c in RAW_COLUMNS}
        base = list(latest.values())
        for row in base:
            row["version"] = self.version
        self.log.base_players = len(base)
        self.base_keys = set(latest)
        return base

    # -- helpers ------------------------------------------------------------ #

    def _sample(self, pool: list[Row], share: float | None = None, n: int | None = None) -> list[Row]:
        """Random subset of the pool. Players silver will hold back are never picked, so the log stays clean."""
        pool = [r for r in pool if player_key(r) not in self.held_back_keys]
        k = n if n is not None else round(len(pool) * share)
        return self.rng.sample(pool, min(k, len(pool)))

    def _change(self, row: Row, scenario: str, column: str, new_value: str) -> None:
        self.log.changes.append(Change(row["full_name"], row["b_day"], scenario, column, row[column], new_value))
        row[column] = new_value

    # -- scenarios ---------------------------------------------------------- #

    def register_inherited_issues(self, base: list[Row]) -> None:
        """
        The source file itself carries rows that break the quality rules (e.g. a draft year
        before the player turned 17). The simulator keeps them as they are, since a source
        error usually persists across extracts, but records them so the expected quarantine
        counts include them and gold is not expected to see these players.
        """
        for row in base:
            broken = rule_violations(row, self.snapshot)
            if broken:
                self.held_back_keys.add(player_key(row))
                self.inherited_keys.add(player_key(row))
                self.log.quality_issues.append(Change(row["full_name"], row["b_day"], "inherited_from_source", ", ".join(broken),
                                                      f"draft_year={row['draft_year']} round={row['draft_round']} pick={row['draft_peak']}", "kept as-is"))

    def remove_departures(self, base: list[Row]) -> list[Row]:
        def age(r: Row) -> int | None:
            birth = parse_birth_date(r["b_day"])
            return age_on(birth, self.snapshot) if birth else None    # None: unparseable, already held back

        retired = [r for r in base if (age(r) or 0) >= RETIREMENT_AGE]
        for r in retired:
            self.log.removed.append(Change(r["full_name"], r["b_day"], "retired", "age", str(age(r))))
        remaining = [r for r in base if r not in retired]

        eligible = [r for r in remaining if r["rating"].isdigit() and player_key(r) not in self.held_back_keys]
        low_rated = sorted(eligible, key=lambda r: int(r["rating"]))[:40]
        for r in self._sample(low_rated, n=N_LEFT_LEAGUE):
            self.log.removed.append(Change(r["full_name"], r["b_day"], "left_league", "rating", r["rating"]))
            remaining.remove(r)
        return remaining

    def apply_team_moves(self, players: list[Row], teams: list[str]) -> None:
        free_agents = [r for r in players if not r["team"]]
        for r in self._sample(free_agents, SHARE_SIGNED):
            self._change(r, "signed", "team", self.rng.choice(teams))
            self._change(r, "signed", "salary", format_salary(self.rng.randint(1_800_000, 6_000_000)))

        with_team = [r for r in players if r["team"] and r not in free_agents]
        traded = self._sample(with_team, SHARE_TRADE)
        for r in traded:
            self._change(r, "trade", "team", self.rng.choice([t for t in teams if t != r["team"]]))
            if self.rng.random() < 0.5:
                self._change(r, "trade", "jersey", format_jersey(self.rng.randint(0, 55)))

        for r in self._sample([r for r in with_team if r not in traded], SHARE_RELEASED):
            self._change(r, "released", "team", "")

    def apply_contracts(self, players: list[Row]) -> None:
        untouched = [r for r in players if not self._touched(r)]
        for r in self._sample(untouched, SHARE_NEW_CONTRACT):
            factor = self.rng.uniform(1.05, 1.6) if self.rng.random() < 0.8 else self.rng.uniform(0.6, 0.95)
            new = min(round(parse_salary(r["salary"]) * factor), MAX_SALARY_USD)
            self._change(r, "new_contract", "salary", format_salary(new))

    def apply_ratings(self, players: list[Row]) -> None:
        for r in self._sample(players, SHARE_RATING_CHANGE):
            age = age_on(parse_birth_date(r["b_day"]), self.snapshot)
            if age < 25:
                delta = self.rng.choice([1, 1, 2, 2, 3, 4, 5, -1])
            elif age < 33:
                delta = self.rng.choice([-2, -1, -1, 1, 1, 2])
            else:
                delta = self.rng.choice([-4, -3, -2, -2, -1, 1])
            self._change(r, "rating_change", "rating", str(max(40, min(99, int(r["rating"]) + delta))))

    def apply_body_and_position(self, players: list[Row]) -> None:
        for r in self._sample(players, SHARE_WEIGHT_CHANGE):
            kg = parse_weight_kg(r["weight"]) + self.rng.choice([-4, -3, -2, -1, 1, 2, 3, 4])
            self._change(r, "weight_change", "weight", format_weight(kg))

        moves = {"G": "G-F", "F": "F-C", "C": "C-F", "G-F": "F", "F-G": "G", "F-C": "C", "C-F": "F"}
        for r in self._sample(players, n=N_POSITION_CHANGE):
            self._change(r, "position_change", "position", moves.get(r["position"], "F"))

    def apply_text_noise(self, players: list[Row]) -> None:
        """Changes that must NOT count as modifications once the pipeline normalizes them."""
        respellable = [r for r in players if r["full_name"] in DIACRITIC_SPELLINGS]
        for r in respellable[:N_NAME_RESPELLED]:
            self._change(r, "name_respelled", "full_name", DIACRITIC_SPELLINGS[r["full_name"]])

        clean = [r for r in players if not self._touched(r)]
        for r in self._sample(clean, n=N_WHITESPACE_NOISE):
            self._change(r, "whitespace_noise", "full_name", re.sub(" ", "  ", r["full_name"], count=1) + " ")
            self._change(r, "whitespace_noise", "salary", r["salary"] + " ")

    def add_new_players(self, existing_names: set[str], teams: list[str], colleges: list[str]) -> list[Row]:
        rookies = []
        picks = self.rng.sample(range(1, 61), N_ROOKIES)
        for pick in sorted(picks):
            name = self._fresh_name(existing_names)
            rnd = 1 if pick <= 30 else 2
            salary = round(10_100_000 - (pick - 1) * 275_000) if rnd == 1 else self.rng.randint(1_000_000, 2_000_000)
            intl = self.rng.random() < 0.2
            row = self._new_row(
                name, born=self._random_date(date(2000, 1, 1), date(2003, 9, 8)),
                rating=self.rng.randint(68, 79), team=self.rng.choice(teams), salary=salary,
                country=self.rng.choice(COUNTRIES_INTL) if intl else "USA",
                college="" if intl else self.rng.choice(colleges),
                draft_year=str(self.snapshot.year), draft_round=str(rnd), draft_pick=str(pick),
            )
            rookies.append(row)
            self.log.added.append(Change(row["full_name"], row["b_day"], "rookie", "draft_peak", "", str(pick)))

        for _ in range(N_UNDRAFTED_SIGNING):
            name = self._fresh_name(existing_names)
            born = self._random_date(date(1993, 1, 1), date(1997, 12, 31))
            row = self._new_row(
                name, born=born, rating=self.rng.randint(70, 74), team=self.rng.choice(teams),
                salary=self.rng.randint(1_800_000, 3_000_000), country=self.rng.choice(COUNTRIES_INTL),
                college="", draft_year=str(born.year + 22), draft_round="Undrafted", draft_pick="Undrafted",
            )
            rookies.append(row)
            self.log.added.append(Change(row["full_name"], row["b_day"], "undrafted_signing", "country", "", row["country"]))
        return rookies

    def add_quality_issues(self, players: list[Row], teams: list[str], taken_names: set[str]) -> list[Row]:
        """
        Rows meant for quarantine. Returned separately so they are never counted as clean.
        Players whose only row is broken are moved out of `players` and remembered in
        self.held_back_keys: silver will hold them back, so gold must not expect them.
        """
        untouched = self._sample([r for r in players if not self._touched(r)], n=4)
        bad: list[Row] = []

        # 1. Exact copy of a line. The original passes, only the copy is quarantined.
        copy = dict(untouched[0])
        bad.append(copy)
        self.log.quality_issues.append(Change(copy["full_name"], copy["b_day"], "duplicate_row", "", "", "exact copy of the line"))

        # 2. Same player twice with different ratings. Both rows are held back.
        conflict = dict(untouched[1])
        conflict["rating"] = str(int(conflict["rating"]) - 5)
        bad.append(conflict)
        self.held_back_keys.add(player_key(conflict))
        self.log.quality_issues.append(Change(conflict["full_name"], conflict["b_day"], "conflicting_duplicate", "rating", untouched[1]["rating"], conflict["rating"]))

        # 3. Unparseable salary.
        salary_row = untouched[2]
        players.remove(salary_row); bad.append(salary_row); self.held_back_keys.add(player_key(salary_row))
        self.log.quality_issues.append(Change(salary_row["full_name"], salary_row["b_day"], "salary_invalid", "salary", salary_row["salary"], "N/A"))
        salary_row["salary"] = "N/A"

        # 4. Impossible jersey number.
        jersey_row = untouched[3]
        players.remove(jersey_row); bad.append(jersey_row); self.held_back_keys.add(player_key(jersey_row))
        self.log.quality_issues.append(Change(jersey_row["full_name"], jersey_row["b_day"], "jersey_out_of_range", "jersey", jersey_row["jersey"], "#100"))
        jersey_row["jersey"] = "#100"

        # 5. A rookie with a birth date that does not exist: no identity, no player key.
        ghost = self._new_row(self._fresh_name(taken_names), born=date(2001, 6, 15), rating=71, team=self.rng.choice(teams),
                              salary=1_500_000, country="USA", college="Gonzaga",
                              draft_year=str(self.snapshot.year), draft_round="2", draft_pick="58")
        ghost["b_day"] = "02/30/01"
        bad.append(ghost); self.held_back_keys.add(player_key(ghost))
        self.log.quality_issues.append(Change(ghost["full_name"], ghost["b_day"], "missing_identity", "b_day", "", "02/30/01 does not exist"))

        # 6. Undrafted, yet carrying a draft pick.
        odd = self._new_row(self._fresh_name(taken_names), born=date(1996, 3, 3), rating=72, team=self.rng.choice(teams),
                            salary=2_000_000, country="Spain", college="",
                            draft_year="2018", draft_round="Undrafted", draft_pick="45")
        bad.append(odd); self.held_back_keys.add(player_key(odd))
        self.log.quality_issues.append(Change(odd["full_name"], odd["b_day"], "draft_inconsistent", "draft_peak", "", "Undrafted round with pick 45"))
        return bad

    # -- row construction --------------------------------------------------- #

    def _new_row(self, name: str, *, born: date, rating: int, team: str, salary: int, country: str,
                 college: str, draft_year: str, draft_round: str, draft_pick: str) -> Row:
        inches = self.rng.randint(73, 85)                       # 6-1 to 7-1
        kg = round(self.rng.uniform(78, 118), 1)
        return {
            "full_name": name, "rating": str(rating), "jersey": format_jersey(self.rng.randint(0, 55)),
            "team": team, "position": self.rng.choice(["G", "G", "F", "F", "C", "G-F", "F-C"]),
            "b_day": format_birth_date(born), "height": format_height(inches), "weight": format_weight(kg),
            "salary": format_salary(salary), "country": country, "draft_year": draft_year,
            "draft_round": draft_round, "draft_peak": draft_pick, "college": college, "version": self.version,
        }

    def _fresh_name(self, taken: set[str]) -> str:
        while True:
            name = f"{self.rng.choice(FIRST_NAMES)} {self.rng.choice(LAST_NAMES)}"
            if name not in taken:
                taken.add(name)
                return name

    def _random_date(self, start: date, end: date) -> date:
        return date.fromordinal(self.rng.randint(start.toordinal(), end.toordinal()))

    def _touched(self, row: Row) -> bool:
        """Has any scenario already changed this player? Changes are logged under the original name."""
        return any(c.b_day == row["b_day"] and _same_person(c.full_name, row["full_name"]) for c in self.log.changes)

    # -- orchestration ------------------------------------------------------ #

    def run(self) -> dict:
        base = self.load_base()
        teams = sorted({r["team"] for r in base if r["team"]})
        colleges = sorted({r["college"] for r in base if r["college"]})
        log.info("base: %d players, %d teams | target %s (%s)", len(base), len(teams), self.version, self.snapshot)

        self.register_inherited_issues(base)
        players = self.remove_departures(base)
        self.apply_team_moves(players, teams)
        self.apply_contracts(players)
        self.apply_ratings(players)
        self.apply_body_and_position(players)
        self.apply_text_noise(players)
        taken_names = {r["full_name"] for r in base}
        new_players = self.add_new_players(taken_names, teams, colleges)
        bad_rows = self.add_quality_issues(players, teams, taken_names)

        clean_rows = players + new_players
        for row in clean_rows:
            broken = rule_violations(row, self.snapshot)
            if broken and player_key(row) not in self.held_back_keys:
                raise AssertionError(f"Row meant to be clean fails {broken}: {row}")

        all_rows = clean_rows + bad_rows
        self._write_csv(all_rows)
        expected = self._expected_results(players, new_players, bad_rows)
        report = self.log.to_dict(len(all_rows), expected)
        self.output.with_suffix(".changes.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        log.info("wrote %s (%d rows) and %s", self.output, len(all_rows), self.output.with_suffix(".changes.json").name)
        return report

    def _write_csv(self, rows: list[Row]) -> None:
        """Same column order and ordering convention as the source: rating descending."""
        def sort_key(r: Row):
            rating = int(r["rating"]) if r["rating"].isdigit() else -1
            return (-rating, r["full_name"].strip().lower())

        self.output.parent.mkdir(parents=True, exist_ok=True)
        with self.output.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=RAW_COLUMNS, lineterminator="\n")
            writer.writeheader()
            writer.writerows(sorted(rows, key=sort_key))

    def _expected_results(self, players: list[Row], new_players: list[Row], bad_rows: list[Row]) -> dict:
        """What silver and gold should report for this file, derived from the log."""
        # held_back_keys, not "every bad row's key": the exact duplicate shares its key with an
        # original that passes, and the two corrupted players were already removed from `players`.
        clean_existing = [r for r in players if player_key(r) not in self.held_back_keys]
        clean_new = [r for r in new_players if player_key(r) not in self.held_back_keys]
        modified = {(c.full_name, c.b_day) for c in self.log.changes if c.scenario not in NON_ATTRIBUTE_SCENARIOS}

        # The name a change was logged under is the ORIGINAL name; compare on birth date + original identity.
        def was_modified(r: Row) -> bool:
            return any(b == r["b_day"] and (n == r["full_name"] or _same_person(n, r["full_name"])) for n, b in modified)

        n_modified = sum(1 for r in clean_existing if was_modified(r))
        inherited = [r for r in players if player_key(r) in self.held_back_keys and rule_violations(r, self.snapshot)]
        rules: dict[str, int] = {}
        for r in bad_rows + inherited:
            for rule in rule_violations(r, self.snapshot):
                rules[rule] = rules.get(rule, 0) + 1
        # duplicate rules: the exact copy fails duplicate_row; the conflicting pair fails conflicting_duplicate
        # on both rows and duplicate_row on the second one. The original of the conflict is also held back.
        rules["duplicate_row"] = rules.get("duplicate_row", 0) + 2
        rules["conflicting_duplicate"] = rules.get("conflicting_duplicate", 0) + 2

        return {
            "silver_players_rows": len(clean_existing) + len(clean_new),
            "silver_players_quarantine_rows": len(bad_rows) + 1 + len(inherited),   # +1: the original of the conflicting pair
            "quarantine_failures_by_rule": dict(sorted(rules.items())),
            "gold_change_type": {
                "new": len(clean_new),
                "modified": n_modified,
                "unchanged": len(clean_existing) - n_modified,
            },
            # Players gold knows from earlier snapshots that get no row in this one: departures plus the
            # existing players silver holds back here (bad salary, bad jersey, conflicting pair). Rows that
            # were already bad in the source never reached gold, so they are not "absent", just never known.
            # Check: SELECT COUNT(*) FROM gold_dim_player WHERE last_seen < this snapshot_date.
            "known_players_absent_from_gold": len(self.log.removed)
                + sum(1 for k in self.held_back_keys if k in self.base_keys and k not in self.inherited_keys),
        }


def _same_person(logged_name: str, current_name: str) -> bool:
    """A respelled or whitespace-noised name still refers to the same logged player."""
    strip = lambda s: re.sub(r"\s+", " ", s).strip().lower()
    respelled = DIACRITIC_SPELLINGS.get(logged_name, logged_name)
    return strip(current_name) in {strip(logged_name), strip(respelled)}


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a later snapshot of the NBA players file with realistic changes.")
    parser.add_argument("--version", default="NBA2k23", help="Game version of the new snapshot. Must exist in SNAPSHOT_DATES.")
    parser.add_argument("--source", default="data/raw/nba2k-full.csv", help="Initial snapshot to start from.")
    parser.add_argument("--output", default=None, help="Output CSV. Default: <source dir>/<version lowercased>-simulated.csv")
    parser.add_argument("--seed", type=int, default=20220909, help="RNG seed. Same seed, same file.")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    source = Path(args.source)
    output = Path(args.output) if args.output else source.parent / f"{args.version.lower()}-simulated.csv"

    report = SnapshotSimulator(args.version, args.seed, source, output).run()
    summary = report["summary"]
    print(
        f"OK | {output} | rows {summary['rows_written']} | base {summary['base_players']} | "
        f"unchanged {summary['players_unchanged_in_file']} modified {summary['players_modified']} "
        f"added {summary['players_added']} removed {summary['players_removed']} | "
        f"quality issues {summary['quality_issue_rows']} | log {output.with_suffix('.changes.json').name}"
    )


if __name__ == "__main__":
    main()
