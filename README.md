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

🥇 Stage 1 – Data Collection (already completed in this repo)

- FBref (Big 5 leagues) data scraped
- 11 tables × 3 seasons collected (each table covers a different stat category)
- Columns documented in a Glossary

Output: 33 organized raw tables

🥈 Stage 2 – Data Preparation (blueprint)

Main steps:

- Schema alignment across seasons
- Data cleaning (null handling, canonicalizing strings such as clubs/positions)
- Merge tables by player & season
- Append seasons into a single dataset
- Filter by age (<23)
- Save the clean master table

Output: `master_player_stats.csv`

🥉 Stage 3 – Feature Engineering

Example features:

- Season-over-season xG/xA trend
- Goals per xG efficiency
- GCA/SCA per 90
- Minutes reliability (minutes / team minutes)
- Defensive/offensive balance by position
- Player consistency index (variance across seasons)

Output: `analytical_features.csv`

🧠 Stage 4 – Analytical Modeling

Create a Talent Index (0–100) by:

- Normalizing features (z-score or min-max)
- Applying position-specific weights
- Aggregating into a single score
- Producing rankings per position

Output: `talent_index.csv`

📊 Stage 5 – Dashboard & Visualization

Interactive views to include:

- Aggregate KPIs: total players, candidate counts, age distribution
- Per-position pages (CF, LW, RW, CM, CB, GK)
- Hot Prospects (Top 10 per position)
- Player season-over-season comparison
- Filters: season, position, age, club, nationality

Suggested tools: Streamlit (quick), Flask + frontend, or BI tools (Power BI / Tableau)

Output: interactive dashboard

🔁 Stage 6 – Maintenance & Update

- Periodic FBref data refresh (weekly/monthly)
- Add current season data progressively
- Update baselines and retrain indicators annually

## Project summary

This repository contains Python tooling and Docker Compose orchestration to:

- Ingest and store raw FBref CSVs for multiple seasons.
- Enrich and canonicalize a user-supplied FBref glossary.
- Profile CSVs and generate staging CREATE TABLE DDL with provenance.
- Run locally under Docker Compose with Airflow and pgAdmin.

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