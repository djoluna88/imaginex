```mermaid
erDiagram
    gold_dim_player ||--|{ gold_fact_player_snapshot : "has state in"
    gold_dim_snapshot ||--|{ gold_fact_player_snapshot : "contains"
    gold_dim_team |o--o{ gold_fact_player_snapshot : "employs"

    gold_dim_player {
        string  player_bk PK "sha2(normalized name + birth date)"
        string  full_name
        date    birth_date
        string  country
        int     draft_year
        int     draft_round "NULL when undrafted"
        int     draft_pick "NULL when undrafted"
        boolean is_undrafted
        string  college
        date    first_seen
        date    last_seen
    }

    gold_dim_team {
        string team_key PK "sha2(team_name)"
        string team_name
    }

    gold_dim_snapshot {
        date    snapshot_date PK
        string  game_version
        boolean is_full_snapshot
        int     players_in_file
    }

    gold_fact_player_snapshot {
        string  player_bk FK
        date    snapshot_date FK
        string  team_key FK "NULL for free agents"
        int     jersey_number
        string  position
        int     rating
        bigint  salary_usd
        int     height_cm
        decimal weight_kg
        string  attr_hash
        string  change_type "new | modified | unchanged"
        date    prev_snapshot_date
        boolean is_latest
        string  load_id
    }
````