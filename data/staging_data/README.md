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

If you need an example processing report or a small script to aggregate row-loss across seasons, open an issue or request and we'll add it here.

````
