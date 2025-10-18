````markdown
# Staging data — pipeline, outputs and provenance

This directory contains the canonical outputs and documentation for the staging step of the ingest pipeline. It is the authoritative place for the "Staging pipeline updates (recent)" section previously stored in the project root README.

Staging pipeline updates (recent)

Note: the staging pipeline has been updated to improve robustness, provenance, and auditing. Key changes:

- IO layout
	- Raw CSVs are expected under: `raw_data/<season>/*.csv` (or `data/raw/<season>/` as a fallback used by the local tooling).
	- Staged outputs are written under: `data/staging_data/<season>/` with CSV filenames of the form `stg_<table_name>_<season>.csv`.
	- Per-file processing reports and verification artifacts are written to: `data/staging_data/reports/`.

- Robustness fixes implemented
	- Heuristic CSV reader that auto-detects and fixes double/multi-row headers and encoding issues (`utf-8-sig` fallback to `latin1`).
	- Duplicate-column detection and safe renaming (appends suffixes) with reporting so ambiguous headers don't silently drop data.
	- Safer numeric parsing: parentheses → negative numbers, percent strings → fractional decimals, thousands separators removed, and per-column heuristics to avoid forcing numeric on textual columns.
	- Column name normalization and table-type-aware mappings to produce consistent staged column names.

- Metadata quality
	- Glossary-backed column comments are now generated for metadata JSONs; the metadata generator attempts normalized and fuzzy matches so many columns now have human-readable descriptions.

- New tools for verification and auditing
	- `python src/staging_processor.py` — run the staging processor over all seasons (reads `raw_data/` and writes `data/staging_data/`).
	- `tools/compare_raw_staged.py` — quick summary comparison of raw vs staged (row/col counts, md5 checksums, player set differences). Produces `data/staging_data/reports/compare_<season>.json`.
	- `tools/detailed_compare.py` — detailed per-table report including head/tail samples, column diffs, metadata gaps, and sample mismatches. Produces `data/staging_data/reports/detailed_compare_<season>.json`.

How the staging processor transforms raw CSVs

See `../README.md` for the full project-level instructions. The staging processor performs these steps:

- Input discovery and profile consumption
	- Reads raw files from `raw_data/<season>/` and uses `data/schemas/profiles/` for column and header heuristics.

- Header normalization & duplicate handling
	- Normalizes multi-row headers into single canonical column names and uniquifies duplicate headers while recording decisions in per-file reports.

- Value cleaning and type heuristics
	- Applies safer numeric parsing (thousands separators, parentheses for negatives, percent parsing) and avoids forcing conversions on textual columns.

- Output & provenance
	- Writes canonical staged CSVs to `data/staging_data/<season>/stg_<table_name>_<season>.csv`.
	- Emits per-file processing JSON reports to `data/staging_data/reports/` containing rows_in/rows_out, cols_in/cols_out, column_mappings, parsing_warnings/errors and sample snippets.

Quick commands

```bash
python src/staging_processor.py
python tools/compare_raw_staged.py
python tools/detailed_compare.py
```

## DDL generation for staging tables

The repository includes a small generator that consumes the staged schema manifest and emits SQL DDL for a single merged staging table per logical table (for example `stg_stats_defense`). This section documents where the generator is, how it works, and how to regenerate the SQL.

- Generated DDL: `sql/create_staging_tables.sql`
- Generator script: `src/generate_staged_ddl.py`
- Manifest input: `data/schemas/staged_schemas_by_season.json`

At a high level the generator does:

1. Read the staged schema manifest (`data/schemas/staged_schemas_by_season.json`).
2. For each logical table name across seasons, build the union of column names seen.
3. Infer a permissive SQL type for each column using conservative heuristics (see below).
4. Emit `DROP TABLE IF EXISTS "stg_<table>" CASCADE;` followed by a `CREATE TABLE "stg_<table>" (...)` that contains the unioned columns and chosen types.

Heuristics used by the generator:

- Prefer `INTEGER` for whole-number columns when examples are integer values.
- Prefer `DECIMAL(10,2)` for fractional/numeric-rate columns.
- Use `VARCHAR(255)` for textual columns (player names, teams, competitions).
- Use `TIMESTAMP` for `processed_at` or other time-like columns.
- When types conflict across seasons, prefer the safe superset (e.g. `DECIMAL` over `INTEGER`, `VARCHAR`/`TEXT` fallback).

The generator also appends common metadata columns to every `stg_` table:

- `season` VARCHAR(50)
- `table_type` VARCHAR(50)
- `processed_at` TIMESTAMP

To regenerate the DDL after the staged manifest changes run:

```bash
python3 src/generate_staged_ddl.py
```

The script will overwrite `sql/create_staging_tables.sql`.

Recommended next step: add a player dimension and `player_id`.

For reliable joins and analytics we recommend adding `player_id BIGINT` to every `stg_` table and maintaining a `dim_players` table. Typical pattern:

- Create `dim_players (player_id BIGINT PRIMARY KEY, player_name VARCHAR(255), birth_year INTEGER, canonical_name VARCHAR(255), created_at TIMESTAMP)`.
- Add `player_id BIGINT` to each `stg_` table (can be added later via `ALTER TABLE`).
- Update the loader to normalize `player_name` and lookup/insert into `dim_players`, returning `player_id` to store on ingest.

Backfill approach:

1. `ALTER TABLE stg_<table> ADD COLUMN player_id BIGINT;` for each staging table.
2. Build `dim_players` by extracting distinct normalized `player_name` (+ birth_year) from staging files, deduplicate via manual or fuzzy matching, and insert canonical rows with `player_id`.
3. Update staging rows with `player_id` from `dim_players` using a normalized equality match (and optionally birth_year).

Caveats: the generator uses heuristics and example values from manifests. Please review the generated DDL before applying it to any production database.

````
