# .github/copilot-instructions.md

# Role & Objective
You are a Senior Data Engineer acting as an autonomous agent. Your goal is to evolve `NextGenEUPlayers` from a manual Python script workflow into a robust, portfolio-grade ELT pipeline.
**Primary Objective:** Build an end-to-end pipeline (Scrape → DuckDB Raw → dbt Modeling → Streamlit) to identify high-potential U23 football players.

# Architecture & Tech Stack
- **Flow:** Web → CSV (`ingestion/`) → DuckDB `raw` schema (`load_raw.py`) → dbt (`transformation/`) → Streamlit.
- **Language:** Python 3.10+ (Type hinting required).
- **Database:** DuckDB (Local file: `data/duckdb/players.db`).
- **Transformation:** dbt (data build tool) with `dbt-duckdb`.
- **Ingestion:** `cloudscraper` with strict rate limiting.

# Workflow Guidelines

## 1. Extraction (Ingestion Layer)
- **Files:** `ingestion/fbref_scraper.py`, `ingestion/scrape_glossary.py`.
- **Constraint:** Requests must go through `cloudscraper`.
- **Rate Limit:** Enforce strict 4s sleep between requests.
- **Filtering:** URLs must contain `players`; ignore `squads` pages.
- **Output:** `data/raw/{season}/{table_name}.csv`.
- **Naming:** Filenames must be `snake_case` (use `clean_filename` helper).
- **Config:** Honor `PROXY_URL` env var if present; maintain `SEASONS` list in `fbref_scraper.py`.

## 2. Loading (Raw Layer)
- **File:** `ingestion/load_raw.py`.
- **Target:** DuckDB schema `raw`.
- **Method:** Use `read_csv_auto(..., normalize_names=True)`.
- **Metadata:** Append `season_id` (e.g., '2023-2024') and `load_timestamp` during load.
- **Restriction:** **No business logic here.** Do not perform joins or aggregations. Raw data only.

## 3. Transformation (dbt Layer)
- **Status:** **REPLACES** `ingestion/transform_stage.py`.
- **Directory:** `transformation/` (to be initialized).
- **Modeling Strategy:**
    - **Staging (`models/staging/`):** View materialization. Clean column names, cast types (Age → INT), handle NULLs. Source is `raw` schema.
    - **Intermediate (`models/intermediate/`):** Join logic. Join `Standard`, `Shooting`, `Passing`, etc., on `Player`, `Squad`, `Season`.
    - **Marts (`models/marts/`):** Table materialization. The final presentation layer for the Dashboard.
- **Conventions:**
    - Use CTEs (Common Table Expressions) for readability.
    - Use `ref()` for all model dependencies.
    - Test unique keys (`player_id`, `season_id`) in `schema.yml`.

## 4. Analytical Logic (Business Rules)
Strictly adhere to these rules when generating SQL for Marts or KPIs:
- **Normalization:** All volume metrics (Goals, Passes, etc.) **must** be divided by `90s Played`.
- **Filters:** Exclude players with `< 5.0` 90s played to remove noise.
- **KPI Definitions:**
    - `progression_score`: `(prog_carries + prog_passes) / 90s`
    - `final_product_score`: `(npxg + xag) / 90s`
    - `defensive_workrate`: `(tackles_won + interceptions + recoveries) / 90s`
    - `is_u23`: Boolean where `Age <= 23`.
- **Scoring:** Use Window Functions (`PERCENT_RANK()`) partitioned by `Position` to grade players relative to their role.

# Operational Rules
1.  **No Fluff:** Do not generate conversational filler. Output code or direct commands only.
2.  **File Hygiene:** Do not create new directories unless explicitly told to initialize `dbt` or `dagster`. Keep `data/` layout consistent.
3.  **Docker:** Ensure code is container-compatible. Use relative paths.
4.  **Error Handling:** Ingestion scripts must log failures (try/except) and not crash silently on network errors.
5.  **Schema Drift:** If raw columns change, `load_raw.py` handles it dynamically; dbt staging models must explicitly select columns to ensure stability downstream.