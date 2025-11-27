# NextGenEUPlayers Copilot Guide
## Architecture
- ELT flow: Web → CSV → DuckDB `raw` → DuckDB `staging`, matching the stages documented in README.md.
- Scrapers (`ingestion/fbref_scraper.py`, `ingestion/scrape_glossary.py`) target 11 FBref tables across seasons 2023-2024 through 2025-2026.
- Raw DuckDB tables follow `raw.<stat>_<season>` naming with seasons underscored; `load_raw.py` also appends a `season_id` column.
- Staging tables are unioned by stat (`staging.standard_stats`, etc.) using `UNION ALL BY NAME` plus a `processed_at` timestamp in `ingestion/transform_stage.py`.
- Glossary definitions land in `data/glossary/glossary.csv`; schema docs are emitted to `schemas/raw/`.
## Key Workflows
- Install core deps once via `pip install -r requirements.txt` (cloudscraper, pandas, duckdb, bs4 stack); install `streamlit` separately when running the UI.
- Extraction: `python ingestion/fbref_scraper.py`; honors `PROXY_URL`, enforces a 4s sleep between tables, and only follows links containing `players`.
- Glossary scrape: `python ingestion/scrape_glossary.py`; reuses the link filter to avoid `squads` pages and deduplicates tooltips per column.
- Load raw: `python ingestion/load_raw.py`; uses DuckDB `read_csv_auto(..., normalize_names=True)` so CSV headers must already be slug-friendly.
- Transform staging: `python ingestion/transform_stage.py`; assumes schemas are aligned—schema drift will fill NULLs, so add columns consistently.
- Profile raw schema: `python scripts/profile_raw_schema.py`; writes `schemas/raw/raw_profile.{json,md}` with row counts and NULL stats.
- Explore DuckDB quickly with `streamlit run admin_ui.py`, which queries `data/duckdb/players.db` in read-only mode and defaults to the `raw` schema.
## Conventions & Pitfalls
- Data layout: raw CSVs under `data/raw/<season>/`, glossary artifacts in `data/glossary/`, DuckDB file at `data/duckdb/players.db`.
- Filenames from scrapers use `clean_filename` (lowercase, underscores); keep this helper when adding new tables or stats.
- fbref URLs must contain `players`; pulling `squads` links produces incompatible tables and will break load/transform steps.
- Keep `TARGET_TABLES` lists synchronized across scraper, glossary, and transformer so staging outputs stay complete.
- DuckDB schemas (`raw`, `staging`) are created lazily; update `admin_ui.py` if you introduce new schemas users should browse.
- Prefer DuckDB SQL for joins/aggregations once data is loaded; pandas is only used during scraping.
- Extending seasons requires updating `SEASONS` in `fbref_scraper.py`; `load_raw.py` globbing will discover matching folders automatically.
- Staging tables overwrite on each run; `processed_at` is generated via `now()`, so don’t populate it in upstream data.
- Profiling queries compute NULL counts column-by-column; if you add computed columns with expensive expressions, consider caching them first in SQL.
- Streamlit caching uses `@st.cache_resource`; clear cache or restart the app when schemas change underneath.
## Integration Notes
- External network calls go through Cloudflare; keep `cloudscraper` (and optional proxy config) instead of bare `requests`.
- `PROXY_URL` affects both scrapers—document it in README.md whenever you add new network tooling.
- DuckDB SQL is embedded as multiline strings; keep formatting readable and avoid string concatenation for identifiers.
- No orchestration tool: developers run stages manually in order, so update README.md if you insert new steps or dependencies.
- Automated tests are absent—validate changes by running the relevant pipeline stage and spot-checking DuckDB via SQL or the Streamlit UI.
