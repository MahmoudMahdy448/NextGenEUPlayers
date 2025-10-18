# NextGenEUPlayers

# NextGenEUPlayers — Project Vision and Summary

🎯 Project vision

Build an analytical system (Dashboard + Analytical Model) using FBref data from the last 3 seasons to discover under-23 players whose performance and statistics suggest they are “rising stars.”

🧩 Core components

Layer | Role | Output
---|---|---
Data Source Layer | Scrape FBref tables (Standard, Shooting, Passing, Defense, etc.) per season & league | CSVs or normalized tables
Data Preparation Layer | Clean and align tables across seasons | Unified master table
Feature Engineering Layer | Derive advanced performance indicators (KPI & trend features) | Enriched analytical dataset
Analytical Layer | Performance analysis + build talent discovery index | Talent Index
Dashboard Layer | Visualize results (interactive dashboard) | Interactive dashboard for scouts and coaches

🧱 Project stages (end-to-end)

This project follows a straightforward, reproducible pipeline. Each step below is actionable and corresponds to scripts in this repository.

1) Scrape (collect)
- Script: `scripts/scrape_fbref.py` or similar scraper under `scripts/`
- Output: raw CSVs per season at `raw_data/<season>/` (or `data/raw/<season>/` for local samples)

2) Store raw data (archive)
- Keep the original scraped CSVs immutable. Store them by season in `raw_data/<season>/` and avoid manual edits to preserve provenance.

3) Profile raw data (detect structure)
- Script: `python3 src/schema_detect.py`
- Output: `data/schemas/profiles/per_season_<season>.initial_raw_data_profile.json`, `glossary.json`, `profiles_summary.csv`

4) Stage (canonicalize & clean)
- Script: `python src/staging_processor.py`
- Output: canonical staged CSVs in `data/staging_data/<season>/` and per-file processing reports in `data/staging_data/reports/`

5) Verify
- Scripts: `python tools/compare_raw_staged.py` and `python tools/detailed_compare.py`
- Output: verification JSON reports in `data/staging_data/reports/`

6) Feature engineering & modeling
- Use staged CSVs as canonical inputs. Produce features, train models, and build dashboards from the cleaned data.

## Project summary
## End-to-end pipeline (scrape → raw → profile → stage → verify)

This section documents the concrete steps we use in this project to go from scraped FBref pages to canonical staged CSVs ready for feature engineering and modeling.

1) Scrape (data collection)

- Tool: `scripts/scrape_fbref.py` (or the scraper in `scripts/`)
- What it does: fetches FBref tables (Standard, Shooting, Passing, Defense, Keeper, etc.) for each season and saves them as CSVs per season.
- Output: raw CSV files saved under `raw_data/<season>/` (or `data/raw/<season>/` depending on environment).

Notes: keep a one-folder-per-season layout so downstream profiling and staging can treat each season independently.

2) Store raw data (raw staging)

- Location: `raw_data/<season>/*.csv` (fallback: `data/raw/<season>/`)
- Goal: preserve the original scraped CSVs with minimal modification so we have an auditable record of the source.
- Best practices: check-in only metadata and profiles to the repo; raw CSVs can be large so prefer storing them outside version control or in a data store. For local development we keep a small sample in `data/raw/`.

3) Profile raw data (structural discovery)

- Tool: `python3 src/schema_detect.py`
- What it does: reads each CSV under `data/raw/<season>/` and generates per-season profile JSONs and a glossary that summarize:
	- tables discovered (logical name normalized across seasons)
	- per-table column lists
	- per-column inferred types, null counts, unique counts, and small sample values
	- a project glossary summarizing column name occurrences across seasons
- Outputs:
	- `data/schemas/profiles/per_season_<season>.initial_raw_data_profile.json` (one file per season)
	- `data/schemas/profiles/glossary.json`
	- `data/schemas/profiles/profiles_summary.csv`

Why profiling first matters:
- Detects multi-row or messy headers so staging can apply correct header normalization.
- Provides canonical column mapping suggestions (via the glossary) so different season-specific column labels map to the same staged column.
- Lets the staging processor pick stable column order and types instead of inferring them ad-hoc per-run.

4) Staging (canonicalization + cleaning)

- Tool: `python src/staging_processor.py`
- Purpose: transform raw CSVs into canonical, consistent staged CSVs that downstream feature engineering and modeling can depend on.

- High-level pipeline performed by the staging processor:
	- Input discovery
		- Reads raw CSVs from `raw_data/<season>/` (or `data/raw/<season>/`).
		- Loads per-season profile JSONs from `data/schemas/profiles/` to obtain expected tables, header heuristics, and canonical column mappings.

	- Pre-read heuristics
		- Detects file encoding and attempts `utf-8-sig`, then `latin1` on failure.
		- Detects and flattens multi-row headers (common when scraping) into single normalized column names.
		- Uniquifies duplicate column headers by appending numeric suffixes and records the mapping in the per-file report.

	- Robust CSV reading
		- Uses `pandas.read_csv(..., low_memory=False)` with the heuristics above to avoid silent column drops.
		- If a file fails to parse, the error is recorded in the per-file processing report and processing continues.

	- Column canonicalization & mapping
		- Normalizes column names (trim, lower-case, replace spaces/symbols) and applies glossary-backed canonical mappings from profiles to produce consistent staged column names (for example: `Player` → `player_name`).
		- Records unmapped columns so they can be reviewed manually.

	- Value cleaning & type coercion
		- Applies safer numeric parsing heuristics:
			- Removes thousands separators (commas), converts parentheses to negative numbers, and parses percentage strings to fractional values when appropriate.
			- Uses per-column heuristics to avoid forcing numeric conversion on textual columns.
		- Optionally fills numeric NaNs with zeros (controlled by `fillna_numeric`), but always records original null counts in the per-file report.

	- Row/column sanity checks
		- Reports row counts (rows_in / rows_out), duplicate rows (by chosen key), and column counts to the per-file processing JSON so you can detect data loss.

	- Outputs
		- Writes staged CSVs to `data/staging_data/<season>/stg_<table_name>_<season>.csv` with a stable column order.
		- Writes per-file processing reports to `data/staging_data/reports/<file_stem>.processing.json` containing:
			- rows_in, rows_out, cols_in, cols_out
			- column_mapping (raw -> staged)
			- duplicate_columns and how they were renamed
			- parsing_warnings/errors and sample value snippets

5) Verify staging outputs

- Tools:
	- `python tools/compare_raw_staged.py` — quick summary comparisons (row/col counts, md5 checksums, player set diffs)
	- `python tools/detailed_compare.py` — detailed per-table diagnostics (head/tail samples, column diffs, metadata gaps)
- Outputs: `data/staging_data/reports/compare_<season>.json`, `data/staging_data/reports/detailed_compare_<season>.json`

6) Feature engineering & modeling

- Use the staged CSVs as the canonical inputs for feature creation, cross-season alignment, and modeling.

Quick commands

```bash
# Re-generate per-season profiles
python3 src/schema_detect.py

# Run staging
python src/staging_processor.py

# Run verification
python tools/compare_raw_staged.py
python tools/detailed_compare.py
```

Provenance and best practices

- Keep raw scraped CSVs immutable (don't overwrite) — store new raw data in a new season folder or add date-based snapshots.
- Always re-run the profiler when raw inputs change (new season or different CSV shape) before running the staging processor.
- Use the per-file processing JSON reports as a machine-readable source of truth to detect data loss and parsing issues.



## Reproducibility — reproduce from scratch (step-by-step)

Follow these commands on a fresh machine with Docker and Docker Compose installed.

1) Clone the repo

```bash
git clone https://github.com/MahmoudMahdy448/NextGenEUPlayers.git
cd NextGenEUPlayers
```

2) Copy environment template

```bash
cp .env.example .env
# edit .env if you need to override defaults
```

3) (Optional) Inspect or change `.env` values before starting

4) Build and start services

```bash
docker compose up -d --build
```

5) Common host permission fix (if needed)

If the Docker build or startup fails with permission errors on host-mounted folders, run on the host:

```bash
sudo chown -R $(id -u):$(id -g) ./data/db_data ./pgadmin_data ./airflow_logs ./dags ./plugins
docker compose down --remove-orphans
docker compose up -d --build
```

6) Verify status and logs

```bash
docker compose ps
docker compose logs --no-log-prefix --tail 200 airflow
```

7) Confirm Airflow admin user exists (created from `.env` by the startup script)

```bash
docker compose exec -T airflow airflow users list
```

8) Run the profiler locally to generate staging artifacts

```bash
python3 src/generate_staging_ddl.py
# produced files:
# - data/schemas/staging_create_tables.sql
# - data/schemas/staging_schema_profiles.json
```

9) (Optional) Apply DDL to Postgres

```bash
docker compose exec -T db psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -f - < data/schemas/staging_create_tables.sql
```

10) Run smoke test

```bash
make smoke
```

## Pinning & exact reproducibility notes

To ensure exact reproducibility across machines, pin the following:

- Docker images by digest (example digests observed locally):
	- `postgres@sha256:c189d272e4fcdd1ac419adee675d30be3d389c22ee770d593f15819d22a68a0d`
	- `apache/airflow@sha256:e5560ad0b86ee905fc6b4b365d65b0f2645b084d2d98dca6f7bd2957cd5336ac`

- Python dependencies: generate a `requirements.lock` via `pip freeze > requirements.lock` or use `pip-compile` and install from that file in CI.

## Troubleshooting

- Airflow logs/permission errors: ensure mounted folders match `AIRFLOW_UID` or chown them as shown above.
- SQL apply errors: the generator sanitizes identifiers, but if you edited headers manually re-run the profiler and inspect `data/schemas/staging_schema_profiles.json` for problematic names.

## Files of interest

- `src/generate_staging_ddl.py` — profiler + canonicalization + DDL generator
- `data/schemas/` — glossary, canonical mapping, generated profiles and DDL
- `scripts/airflow_create_user.sh` — idempotent startup script used by compose
- `Makefile` and `scripts/smoke_check.sh` — helpers for dev and CI



