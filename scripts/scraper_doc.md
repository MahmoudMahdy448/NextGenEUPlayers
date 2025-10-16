## FBref Big 5 Leagues Scraper — What This Script Does

- **Output location**
  - Saves all outputs under `OUT_FOLDER` (default: `data/raw`).
  - Organizes by season: `data/raw/<season>/...`.

- **Configured sources (FBref)**
  - Scrapes player tables for Big 5 European leagues.
  - Uses generic URLs plus season-specific maps for selected seasons.
  - Table IDs targeted: `stats_standard`, `stats_shooting`, `stats_passing`, `stats_passing_types`, `stats_gca`, `stats_defense`, `stats_possession`, `stats_playing_time`, `stats_misc`, `stats_keeper`, `stats_keeper_adv`.

- **Season handling**
  - `DEFAULT_SEASONS = ["2025-2026", "2024-2025", "2023-2024"]`.
  - If an explicit map is defined for a season, those URLs are used; otherwise the generic URLs are used.

- **Scraping process**
  - Uses `pandas.read_html(url, attrs={"id": table_id})` to read tables.
  - Flattens MultiIndex headers when present; removes duplicate columns.
  - Drops accidental header rows captured as data (e.g., where `Player == "Player"`).
  - Adds a polite randomized delay of 1–2 seconds between requests.
  - Logs progress and errors without crashing the entire run.

- **Per-season raw outputs**
  - Each successfully scraped table is written as CSV to: `data/raw/<season>/<table_id>.csv`.

- **Merging logic**
  - Requires `stats_standard` to exist; merges all other tables on `['Player', 'Squad']` using left joins.
  - Drops columns that contain the substring "matches" (case-insensitive).
  - Normalizes `Age` from `yy-ddd` to numeric years only (`yy`).

- **Final season outputs**
  - Full merged file: `data/raw/<season>/players_data-<season>.csv`.
  - Light subset file (predefined useful columns, only those that exist): `data/raw/<season>/players_data_light-<season>.csv`.

- **Fault tolerance**
  - If a table fails to download, it is skipped and the script continues.
  - If the main table (`stats_standard`) is missing, the merge is skipped for that season but the run continues to the next season.

- **Entry point**
  - `run_pipeline()` executes the full scrape → save raw tables → merge → clean → save final outputs for each season in `DEFAULT_SEASONS` (or a provided list).

### Configuration tips

- **Change output base path**
  - Edit the constant near the top of `scrape.py`:
    - `OUT_FOLDER = r"C:\\Data\\Football\\raw_data"` (Windows) or `"/data/raw"` (Linux/Mac).
  - Or make it configurable via environment variable:
    - `OUT_FOLDER = os.getenv("RAW_DATA_DIR", r"C:\\Data\\Football\\raw_data")`.
  - Or parse a `--out-dir` CLI argument and set `OUT_FOLDER` from it.


